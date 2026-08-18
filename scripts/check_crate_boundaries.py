#!/usr/bin/env python3
"""Fail when a workspace crate crosses an architectural dependency boundary."""

from __future__ import annotations

from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ALLOWED = {
    "transferia-core": set(),
    "transferia-delivery-contracts": {"transferia-core"},
    "transferia-pipeline": {"transferia-core", "transferia-delivery-contracts"},
    "transferia-registry": {
        "transferia-core",
        "transferia-delivery-contracts",
    },
    "transferia-providers": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
    },
    "transferia-delivery": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-pipeline",
        "transferia-registry",
    },
    "transferia-runtime": set(),
    "transferia-runtime-local": {"transferia-runtime"},
    "transferia-server-contracts": {"transferia-runtime"},
    "transferia-server-ui": set(),
    "transferia-control-plane": {
        "transferia-core",
        "transferia-delivery",
        "transferia-providers",
        "transferia-registry",
        "transferia-runtime",
        "transferia-server-contracts",
        "transferia-server-ui",
    },
    "transferia-composition": {
        "transferia-control-plane",
        "transferia-delivery",
        "transferia-providers",
        "transferia-runtime",
        "transferia-runtime-local",
    },
}

DEV_EXTRA = {
    "transferia-providers": {"transferia-pipeline"},
    "transferia-delivery": {"transferia-providers"},
}

HEAVY_PROVIDER_DEPENDENCIES = {
    "clickhouse-arrow",
    "object_store",
    "postgres-types",
    "rdkafka",
    "tokio-postgres",
    "tokio-postgres-rustls",
    "ydb-grpc",
}


def internal_dependencies(manifest: dict[str, object], section: str) -> set[str]:
    dependencies = manifest.get(section, {})
    if not isinstance(dependencies, dict):
        return set()
    return {name for name in dependencies if name.startswith("transferia-")}


def provider_feature_errors(manifests: dict[str, dict[str, object]]) -> list[str]:
    errors: list[str] = []
    providers = manifests["transferia-providers"]
    dependencies = providers.get("dependencies", {})
    assert isinstance(dependencies, dict)
    for dependency in sorted(HEAVY_PROVIDER_DEPENDENCIES):
        declaration = dependencies.get(dependency)
        if not isinstance(declaration, dict) or declaration.get("optional") is not True:
            errors.append(
                f"transferia-providers: heavy dependency '{dependency}' must be optional"
            )

    expected_consumers = {
        "transferia-control-plane": {"provider-logbroker"},
    }
    for crate, expected_features in expected_consumers.items():
        crate_dependencies = manifests[crate].get("dependencies", {})
        assert isinstance(crate_dependencies, dict)
        declaration = crate_dependencies.get("transferia-providers")
        if not isinstance(declaration, dict):
            errors.append(f"{crate}: transferia-providers dependency must be explicit")
            continue
        if declaration.get("default-features") is not False:
            errors.append(f"{crate}: transferia-providers default features must be disabled")
        actual_features = set(declaration.get("features", []))
        if actual_features != expected_features:
            errors.append(
                f"{crate}: expected provider features {sorted(expected_features)}, got {sorted(actual_features)}"
            )
    return errors


def main() -> int:
    errors: list[str] = []
    discovered: set[str] = set()
    manifests: dict[str, dict[str, object]] = {}
    for manifest_path in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        with manifest_path.open("rb") as source:
            manifest = tomllib.load(source)
        package = manifest.get("package", {})
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str):
            errors.append(f"{manifest_path}: missing package.name")
            continue
        discovered.add(name)
        manifests[name] = manifest
        if name not in PRODUCTION_ALLOWED:
            errors.append(f"{manifest_path}: crate is absent from the architecture allowlist")
            continue
        for section, allowed in (
            ("dependencies", PRODUCTION_ALLOWED[name]),
            ("build-dependencies", PRODUCTION_ALLOWED[name]),
            ("dev-dependencies", PRODUCTION_ALLOWED[name] | DEV_EXTRA.get(name, set())),
        ):
            forbidden = internal_dependencies(manifest, section) - allowed
            if forbidden:
                errors.append(
                    f"{name}: forbidden {section}: {', '.join(sorted(forbidden))}"
                )

    missing = PRODUCTION_ALLOWED.keys() - discovered
    if missing:
        errors.append(f"architecture allowlist entries have no crate: {', '.join(sorted(missing))}")
    if list((ROOT / "src").glob("**/*.rs")) != [ROOT / "src/lib.rs"]:
        errors.append("root transferia facade must contain only src/lib.rs")
    if not (PRODUCTION_ALLOWED.keys() - discovered):
        errors.extend(provider_feature_errors(manifests))

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Rust crate architecture OK ({len(discovered)} crates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
