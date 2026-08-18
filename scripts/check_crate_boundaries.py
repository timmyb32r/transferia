#!/usr/bin/env python3
"""Fail when a workspace crate crosses an architectural dependency boundary."""

from __future__ import annotations

from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {
    "transferia-core": set(),
    "transferia-delivery-contracts": {"transferia-core"},
    "transferia-pipeline": {"transferia-core", "transferia-delivery-contracts"},
    "transferia-providers": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-pipeline",
    },
    "transferia-delivery": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-pipeline",
        "transferia-providers",
    },
    "transferia-runtime": set(),
    "transferia-runtime-local": {"transferia-runtime"},
    "transferia-server-contracts": {"transferia-runtime"},
    "transferia-control-plane": {
        "transferia-core",
        "transferia-delivery",
        "transferia-providers",
        "transferia-runtime",
        "transferia-server-contracts",
    },
    "transferia-composition": {
        "transferia-control-plane",
        "transferia-delivery",
        "transferia-providers",
        "transferia-runtime",
        "transferia-runtime-local",
    },
}


def internal_dependencies(manifest: dict[str, object]) -> set[str]:
    result: set[str] = set()
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        dependencies = manifest.get(section, {})
        if isinstance(dependencies, dict):
            result.update(name for name in dependencies if name.startswith("transferia-"))
    return result


def main() -> int:
    errors: list[str] = []
    discovered: set[str] = set()
    for manifest_path in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        with manifest_path.open("rb") as source:
            manifest = tomllib.load(source)
        package = manifest.get("package", {})
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str):
            errors.append(f"{manifest_path}: missing package.name")
            continue
        discovered.add(name)
        if name not in ALLOWED:
            errors.append(f"{manifest_path}: crate is absent from the architecture allowlist")
            continue
        forbidden = internal_dependencies(manifest) - ALLOWED[name]
        if forbidden:
            errors.append(f"{name}: forbidden internal dependencies: {', '.join(sorted(forbidden))}")

    missing = ALLOWED.keys() - discovered
    if missing:
        errors.append(f"architecture allowlist entries have no crate: {', '.join(sorted(missing))}")
    if list((ROOT / "src").glob("**/*.rs")) != [ROOT / "src/lib.rs"]:
        errors.append("root transferia facade must contain only src/lib.rs")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Rust crate architecture OK ({len(discovered)} crates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
