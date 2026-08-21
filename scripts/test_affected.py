#!/usr/bin/env python3
"""Run the smallest compile-only development gate for the current change.

Ordinary agent work performs no formatting, linting, tests, E2E, linking, or
code generation. Unknown and cross-cutting Rust inputs fall back to workspace
`cargo check`; expensive verification belongs exclusively to the release gate.
"""

from __future__ import annotations

import argparse
from functools import lru_cache
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field


ROOT = Path(__file__).resolve().parents[1]
TIMINGS_PATH = ROOT / "target/affected-tests-timings.json"
TIMINGS_HISTORY_PATH = ROOT / "target/affected-tests-timings.jsonl"
TIMINGS_LOCK = threading.Lock()
COMMAND_TIMINGS: list[dict[str, object]] = []


CROSS_CUTTING = {
    "build.rs",
    ".cargo/config.toml",
    "rust-toolchain.toml",
}

@dataclass
class Selection:
    full: bool = False
    reason: str = ""
    rust_packages: set[str] = field(default_factory=set)
    downstream_check_packages: set[str] = field(default_factory=set)
    integration_tests: set[str] = field(default_factory=set)
    web_paths: set[str] = field(default_factory=set)
    api_contract: bool = False
    catalog_contract: bool = False


@lru_cache(maxsize=1)
def workspace_reverse_dependencies() -> dict[str, set[str]]:
    """Return the workspace's reverse dependency graph from Cargo metadata."""
    metadata = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    packages = json.loads(metadata.stdout)["packages"]
    workspace_names = {package["name"] for package in packages}
    reverse = {name: set() for name in workspace_names}
    for package in packages:
        for dependency in package["dependencies"]:
            dependency_name = dependency["name"]
            if dependency_name in workspace_names:
                reverse[dependency_name].add(package["name"])
    return reverse


def transitive_dependents(packages: set[str]) -> set[str]:
    reverse = workspace_reverse_dependencies()
    discovered: set[str] = set()
    pending = list(packages)
    while pending:
        package = pending.pop()
        for dependent in reverse.get(package, set()):
            if dependent not in packages and dependent not in discovered:
                discovered.add(dependent)
                pending.append(dependent)
    return discovered


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
        if path == "Cargo.lock":
            # The changed crate manifests select the affected reverse-dependency
            # closure. Lockfile churn alone does not justify compiling every
            # workspace target during ordinary development.
            continue

        if path == "Cargo.toml":
            # Workspace membership/profile edits still compile the root
            # composition without turning every manifest edit into a full gate.
            result.rust_packages.add("transferia")
            continue

        if path in CROSS_CUTTING or path.startswith(("proto/", "vendor/")):
            result.full = True
            result.reason = f"cross-cutting build input changed: {path}"
            return result

        if path.startswith("web/"):
            result.web_paths.add(path.removeprefix("web/"))
            if path in {
                "web/scripts/generate-api.mjs",
                "web/src/generated/apiContract.ts",
                "web/src/infrastructure/controlPlane/httpControlPlane.ts",
                "web/tests/apiContract.test.ts",
            }:
                result.api_contract = True
            continue

        if path in {
            "crates/transferia-server-contracts/contracts/server-api.schema.json",
            "crates/transferia-server-contracts/contracts/server-api.fixture.json",
            "crates/transferia-server-contracts/contracts/server-api.routes.json",
        }:
            result.web_paths.add("src/generated/apiContract.ts")
            result.api_contract = True
            continue

        if path.startswith("crates/transferia-core/"):
            result.full = True
            result.reason = f"shared data-plane contract changed: {path}"
            return result

        if path.startswith("crates/"):
            parts = Path(path).parts
            crate = parts[1] if len(parts) > 1 else ""
            package = crate
            if crate == "transferia-server-contracts":
                result.rust_packages.update({package, "transferia-control-plane"})
                result.api_contract = True
            elif crate:
                result.rust_packages.add(package)
            if (
                "/catalog" in path
                or "/registry" in path
                or "/config" in path
                or path.endswith("/descriptor.rs")
            ) and crate.startswith(("transferia-connector-", "transferia-registry")):
                result.catalog_contract = True
            if (
                path.endswith(("/src/lib.rs", "/Cargo.toml"))
                or crate
                in {
                    "transferia-core",
                    "transferia-delivery-contracts",
                    "transferia-runtime",
                    "transferia-server-contracts",
                }
            ):
                result.downstream_check_packages.add(package)
            continue

        if path.startswith("tests/support/"):
            result.integration_tests.update(p.stem for p in (ROOT / "tests").glob("*.rs"))
            continue

        if path.startswith("tests/") and path.endswith(".rs"):
            if (ROOT / path).exists():
                result.integration_tests.add(Path(path).stem)
            continue

        if path.startswith("src/") and path.endswith(".rs"):
            result.full = True
            result.reason = f"public facade changed: {path}"
            return result

        if path == "justfile" or path.startswith("scripts/"):
            continue

        if path.startswith(("docs/", ".github/")) or path.endswith((".md", ".txt")):
            continue

        result.full = True
        result.reason = f"unmapped file changed: {path}"
        return result
    return result


def changed_paths(base: str) -> list[str]:
    tracked = subprocess.run(
        ["git", "diff", "--relative", "--name-only", "--diff-filter=ACDMR", base],
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
        return (
            [["cargo", "check", "--workspace", "--all-targets", "--all-features"]],
            [],
        )

    rust: list[list[str]] = []
    packages = {package for package in selection.rust_packages if package}
    dependents = transitive_dependents(selection.downstream_check_packages)
    if packages:
        command = ["cargo", "check", "--all-targets", "--all-features"]
        for package in sorted(packages):
            command.extend(["-p", package])
        rust.append(command)
    if dependents:
        command = ["cargo", "check", "--lib", "--bins", "--all-features"]
        for package in sorted(dependents):
            command.extend(["-p", package])
        rust.append(command)

    if selection.integration_tests:
        command = ["cargo", "check", "--all-features"]
        for target in sorted(selection.integration_tests):
            command.extend(["--test", target])
        rust.append(command)

    # Contract checks are the narrow correctness layer between compile-only
    # development checks and the release suite. Keep Cargo commands in this one
    # serial lane so they never contend for the same target directory.
    if selection.api_contract:
        rust.append(["just", "api-contract-check"])
    if selection.catalog_contract:
        rust.append(["just", "catalog-contract-check"])

    web: list[list[str]] = []
    if selection.web_paths:
        web.append(["npm", "run", "typecheck"])
    if selection.api_contract:
        web.append(["npm", "test", "--", "--run", "tests/apiContract.test.ts"])
    return rust, web


def display(command: list[str], cwd: Path) -> str:
    return f"(cd {cwd.relative_to(ROOT) or '.'} && {' '.join(command)})"


def run_group(group: list[list[str]], cwd: Path) -> int:
    group_started = time.monotonic()
    for command in group:
        started = time.monotonic()
        print(f"+ {display(command, cwd)}", flush=True)
        completed = subprocess.run(command, cwd=cwd, env=os.environ.copy(), check=False)
        elapsed = time.monotonic() - started
        with TIMINGS_LOCK:
            COMMAND_TIMINGS.append(
                {
                    "command": command,
                    "cwd": str(cwd.relative_to(ROOT) or "."),
                    "duration_seconds": round(elapsed, 3),
                    "status": completed.returncode,
                }
            )
        print(
            f"[{elapsed:.2f}s] {'PASS' if completed.returncode == 0 else 'FAIL'}: "
            f"{display(command, cwd)}",
            flush=True,
        )
        if completed.returncode:
            return completed.returncode
    print(f"[{time.monotonic() - group_started:.2f}s] group completed", flush=True)
    return 0


def run_parallel_group(group: list[list[str]], cwd: Path) -> int:
    """Run independent commands concurrently and retain complete diagnostics."""
    if len(group) < 2:
        return run_group(group, cwd)
    with ThreadPoolExecutor(max_workers=len(group)) as executor:
        statuses = list(executor.map(lambda command: run_group([command], cwd), group))
    return next((status for status in statuses if status), 0)


def main() -> int:
    total_started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="HEAD", help="git diff base (default: HEAD)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("paths", nargs="*", help="explicit changed paths instead of git diff")
    args = parser.parse_args()

    paths = [normalize(path) for path in args.paths] if args.paths else changed_paths(args.base)
    if not paths:
        print("No changed files; no compile checks to run.")
        return 0

    print("Changed files:")
    for path in paths:
        print(f"  {path}")

    selection = select(paths)
    if selection.full:
        print(f"Falling back to workspace cargo check: {selection.reason}")
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

    def finish(status: int) -> int:
        TIMINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": 1,
            "recorded_at_unix_seconds": int(time.time()),
            "changed_files": paths,
            "duration_seconds": round(time.monotonic() - total_started, 3),
            "status": status,
            "commands": COMMAND_TIMINGS,
        }
        TIMINGS_PATH.write_text(json.dumps(report, indent=2) + "\n")
        with TIMINGS_HISTORY_PATH.open("a") as history:
            history.write(json.dumps(report, separators=(",", ":")) + "\n")
        print(f"Timing report: {TIMINGS_PATH.relative_to(ROOT)}", flush=True)
        return status

    if rust and web:
        with ThreadPoolExecutor(max_workers=2) as executor:
            rust_future = executor.submit(run_group, rust, ROOT)
            web_future = executor.submit(run_parallel_group, web, ROOT / "web")
            rust_status = rust_future.result()
            web_status = web_future.result()
            status = rust_status or web_status
        return finish(status)
    status = run_group(rust, ROOT) if rust else run_parallel_group(web, ROOT / "web")
    return finish(status)


if __name__ == "__main__":
    raise SystemExit(main())
