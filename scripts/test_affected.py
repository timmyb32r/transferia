#!/usr/bin/env python3
"""Run the smallest conservative test set for the current change.

This is a development accelerator, not a replacement for the mandatory full
gate. Unknown or cross-cutting files deliberately fall back to the full suite.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
from dataclasses import dataclass, field


ROOT = Path(__file__).resolve().parents[1]


PROVIDER_E2E = {
    "clickhouse": {"e2e_clickhouse_source", "e2e_sinks"},
    "discard": {"e2e_sinks"},
    "kafka": {"e2e_kafka"},
    "logbroker": {
        "e2e_logbroker_pqv1_sink",
        "e2e_logbroker_pqv1_source",
        "e2e_logbroker_ydb_sink",
    },
    "postgres": {"e2e_postgres"},
    "s3": {"e2e_s3_source", "e2e_sinks"},
    "ytsaurus": {"e2e_ytsaurus"},
}

CROSS_CUTTING = {
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    ".cargo/config.toml",
}


@dataclass
class Selection:
    full: bool = False
    reason: str = ""
    rust_modules: set[str] = field(default_factory=set)
    integration_tests: set[str] = field(default_factory=set)
    web_paths: set[str] = field(default_factory=set)
    web_build: bool = False
    selector_self_test: bool = False


def provider_e2e(provider: str, parts: tuple[str, ...]) -> set[str]:
    tests = set(PROVIDER_E2E.get(provider, set()))
    if provider == "clickhouse":
        if "sink" in parts:
            tests.discard("e2e_clickhouse_source")
        elif "src_batch" in parts:
            tests.discard("e2e_sinks")
    elif provider == "s3":
        if "sink" in parts:
            tests.discard("e2e_s3_source")
        elif "src_batch" in parts:
            tests.discard("e2e_sinks")
    return tests


def normalize(path: str) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        try:
            candidate = candidate.relative_to(ROOT)
        except ValueError:
            return path
    value = candidate.as_posix()
    return value.removeprefix("./")


def select(paths: list[str]) -> Selection:
    result = Selection()
    for raw_path in paths:
        path = normalize(raw_path)
        if path in CROSS_CUTTING or path.startswith(("proto/", "vendor/")):
            result.full = True
            result.reason = f"cross-cutting build input changed: {path}"
            return result

        if path.startswith("web/"):
            result.web_paths.add(path.removeprefix("web/"))
            result.web_build |= not path.startswith("web/tests/")
            continue

        if path == "src/server/contracts/server-api.schema.json":
            result.rust_modules.add("server")
            result.integration_tests.add("web_ui")
            result.web_paths.add("src/generated/apiContract.ts")
            result.web_build = True
            continue

        if path.startswith("crates/transferia-core/"):
            result.full = True
            result.reason = f"shared data-plane contract changed: {path}"
            return result

        if path.startswith("tests/support/"):
            result.integration_tests.update(p.stem for p in (ROOT / "tests").glob("*.rs"))
            result.integration_tests.discard("web_ui")
            continue

        if path.startswith("tests/") and path.endswith(".rs"):
            result.integration_tests.add(Path(path).stem)
            continue

        if path.startswith("src/") and path.endswith(".rs"):
            parts = Path(path).parts
            module = parts[1] if len(parts) > 1 else ""
            if len(parts) == 2:
                result.full = True
                result.reason = f"crate-wide Rust entry point changed: {path}"
                return result
            if len(parts) > 2 and module == "providers":
                provider = parts[2]
                if provider.endswith(".rs"):
                    result.rust_modules.add("providers")
                    for tests in PROVIDER_E2E.values():
                        result.integration_tests.update(tests)
                else:
                    result.rust_modules.add(f"providers::{provider}")
                    result.integration_tests.update(provider_e2e(provider, parts))
            elif module == "parsers":
                parser = parts[2] if len(parts) > 2 else ""
                if parser == "mod.rs":
                    parser = ""
                elif parser.endswith(".rs"):
                    parser = f"tests::{Path(parser).stem}"
                result.rust_modules.add(f"parsers::{parser}" if parser else "parsers")
                if "schema_registry" in parts:
                    result.integration_tests.add("e2e_schema_registry")
                elif "json_parser" in parts or not parser:
                    result.integration_tests.add("json_roundtrip")
            elif module == "serializer":
                result.rust_modules.add("serializer")
                result.integration_tests.add("json_roundtrip")
            elif module == "server":
                result.rust_modules.add("server")
                result.integration_tests.add("web_ui")
            else:
                result.rust_modules.add(module)
            continue

        if path == "justfile" or path == "scripts/test_test_affected.py":
            result.selector_self_test = True
            continue

        if path.startswith(("docs/", ".github/")) or path.endswith((".md", ".txt")):
            continue

        if path.startswith("scripts/") and path.endswith(".py"):
            if path == "scripts/test_affected.py":
                result.selector_self_test = True
            else:
                result.full = True
                result.reason = f"unmapped executable script changed: {path}"
                return result
            continue

        result.full = True
        result.reason = f"unmapped file changed: {path}"
        return result
    return result


def changed_paths(base: str) -> list[str]:
    tracked = subprocess.run(
        ["git", "diff", "--relative", "--name-only", "--diff-filter=ACMR", base],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.splitlines()
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.splitlines()
    return sorted({normalize(path) for path in tracked + untracked})


def commands(selection: Selection) -> tuple[list[list[str]], list[list[str]]]:
    if selection.full:
        return ([
            ["cargo", "test", "--workspace", "--all-targets", "--all-features"],
        ], [])

    rust: list[list[str]] = []
    modules = {module for module in selection.rust_modules if module}
    if len(modules) == 1:
        module = next(iter(modules))
        rust.append(["cargo", "test", "--lib", "--all-features", f"{module}::"])
    elif modules:
        rust.append(["cargo", "test", "--lib", "--all-features"])

    if selection.integration_tests:
        command = ["cargo", "test", "--all-features"]
        for target in sorted(selection.integration_tests):
            command.extend(["--test", target])
        rust.append(command)

    if selection.selector_self_test:
        rust.append([sys.executable, "-m", "unittest", "scripts/test_test_affected.py"])

    web: list[list[str]] = []
    if selection.web_paths:
        web.append(
            [
                "npx",
                "--no-install",
                "vitest",
                "related",
                *sorted(selection.web_paths),
                "--run",
                "--passWithNoTests",
            ]
        )
        if selection.web_build:
            web.append(["npm", "run", "build"])
    return rust, web


def display(command: list[str], cwd: Path) -> str:
    return f"(cd {cwd.relative_to(ROOT) or '.'} && {' '.join(command)})"


def run_group(group: list[list[str]], cwd: Path) -> int:
    for command in group:
        print(f"+ {display(command, cwd)}", flush=True)
        completed = subprocess.run(command, cwd=cwd, env=os.environ.copy(), check=False)
        if completed.returncode:
            return completed.returncode
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="HEAD", help="git diff base (default: HEAD)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("paths", nargs="*", help="explicit changed paths instead of git diff")
    args = parser.parse_args()

    paths = [normalize(path) for path in args.paths] if args.paths else changed_paths(args.base)
    if not paths:
        print("No changed files; no affected tests to run.")
        return 0

    print("Changed files:")
    for path in paths:
        print(f"  {path}")

    selection = select(paths)
    if selection.full:
        print(f"Falling back to the full test suite: {selection.reason}")
    rust, web = commands(selection)
    if not rust and not web:
        print("No executable code is affected.")
        return 0

    if args.dry_run:
        for command in rust:
            print(f"+ {display(command, ROOT)}")
        for command in web:
            print(f"+ {display(command, ROOT / 'web')}")
        return 0

    if rust and web:
        rust_process = subprocess.Popen(
            [sys.executable, __file__, "--run-group", "rust", *paths], cwd=ROOT
        )
        web_status = run_group(web, ROOT / "web")
        rust_status = rust_process.wait()
        return web_status or rust_status
    return run_group(rust, ROOT) if rust else run_group(web, ROOT / "web")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1:3] == ["--run-group", "rust"]:
        selected = select(sys.argv[3:])
        rust_commands, _ = commands(selected)
        raise SystemExit(run_group(rust_commands, ROOT))
    raise SystemExit(main())
