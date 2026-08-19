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
    "transferia-middleware-datafusion": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
    },
    "transferia-middleware-filter": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
    },
    "transferia-provider-support": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
    },
    "transferia-provider-clickhouse": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-provider-kafka": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-provider-logbroker": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-provider-postgres": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-provider-s3": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-provider-ytsaurus": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-support",
        "transferia-registry",
    },
    "transferia-providers": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-provider-clickhouse",
        "transferia-provider-kafka",
        "transferia-provider-logbroker",
        "transferia-provider-postgres",
        "transferia-provider-s3",
        "transferia-provider-support",
        "transferia-provider-ytsaurus",
        "transferia-middleware-datafusion",
        "transferia-middleware-filter",
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
    "transferia-provider-s3": {"transferia-pipeline"},
    "transferia-provider-support": {"transferia-pipeline"},
    "transferia-providers": {"transferia-pipeline"},
}

HEAVY_PROVIDER_OWNERS = {
    "clickhouse-arrow": "transferia-provider-clickhouse",
    "object_store": "transferia-provider-s3",
    "postgres-types": "transferia-provider-postgres",
    "rdkafka": "transferia-provider-kafka",
    "tokio-postgres": "transferia-provider-postgres",
    "tokio-postgres-rustls": "transferia-provider-postgres",
    "ydb-grpc": "transferia-provider-logbroker",
    "datafusion": "transferia-middleware-datafusion",
}


def internal_dependencies(manifest: dict[str, object], section: str) -> set[str]:
    dependencies = manifest.get(section, {})
    if not isinstance(dependencies, dict):
        return set()
    return {name for name in dependencies if name.startswith("transferia-")}


def provider_isolation_errors(manifests: dict[str, dict[str, object]]) -> list[str]:
    errors: list[str] = []
    for dependency, owner in sorted(HEAVY_PROVIDER_OWNERS.items()):
        for crate, manifest in manifests.items():
            dependencies = manifest.get("dependencies", {})
            if isinstance(dependencies, dict) and dependency in dependencies and crate != owner:
                errors.append(
                    f"{crate}: heavy dependency '{dependency}' belongs only to {owner}"
                )

    provider_crates = {
        name for name in manifests if name.startswith("transferia-provider-")
    } - {"transferia-provider-support"}
    for crate in sorted(provider_crates):
        dependencies = internal_dependencies(manifests[crate], "dependencies")
        siblings = (dependencies & provider_crates) - {crate}
        if siblings:
            errors.append(
                f"{crate}: provider crates must not depend on siblings: {', '.join(sorted(siblings))}"
            )
    middleware_crates = {
        name for name in manifests if name.startswith("transferia-middleware-")
    }
    for crate in sorted(middleware_crates):
        dependencies = internal_dependencies(manifests[crate], "dependencies")
        siblings = (dependencies & middleware_crates) - {crate}
        if siblings:
            errors.append(
                f"{crate}: middleware crates must not depend on siblings: {', '.join(sorted(siblings))}"
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
        errors.extend(provider_isolation_errors(manifests))

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Rust crate architecture OK ({len(discovered)} crates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
