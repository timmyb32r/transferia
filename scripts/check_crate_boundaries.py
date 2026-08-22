#!/usr/bin/env python3
"""Fail when a workspace crate crosses an architectural dependency boundary."""

from __future__ import annotations

from pathlib import Path
import re
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
    "transferia-connector-support": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
    },
    "transferia-connector-clickhouse": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-iceberg": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-kafka": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-logbroker": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-postgres": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-s3": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connector-ytsaurus": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-support",
        "transferia-registry",
    },
    "transferia-connectors": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-connector-clickhouse",
        "transferia-connector-iceberg",
        "transferia-connector-kafka",
        "transferia-connector-logbroker",
        "transferia-connector-postgres",
        "transferia-connector-s3",
        "transferia-connector-support",
        "transferia-connector-ytsaurus",
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
    "transferia-server-contracts": {
        "transferia-core",
        "transferia-delivery-contracts",
        "transferia-registry",
        "transferia-runtime",
    },
    "transferia-server-ui": set(),
    "transferia-test-support": {"transferia-registry"},
    "transferia-control-plane": {
        "transferia-core",
        "transferia-delivery",
        "transferia-delivery-contracts",
        "transferia-connectors",
        "transferia-registry",
        "transferia-runtime",
        "transferia-server-contracts",
        "transferia-server-ui",
    },
    "transferia-composition": {
        "transferia-control-plane",
        "transferia-delivery",
        "transferia-connectors",
        "transferia-runtime",
        "transferia-runtime-local",
    },
}

DEV_EXTRA = {
    "transferia-connector-clickhouse": {"transferia-test-support"},
    "transferia-connector-iceberg": {"transferia-test-support"},
    "transferia-connector-kafka": {"transferia-test-support"},
    "transferia-connector-logbroker": {"transferia-test-support"},
    "transferia-connector-s3": {"transferia-pipeline", "transferia-test-support"},
    "transferia-connector-support": {"transferia-pipeline"},
    "transferia-connector-ytsaurus": {"transferia-test-support"},
    "transferia-connectors": {"transferia-pipeline"},
}

HEAVY_CONNECTOR_OWNERS = {
    "clickhouse-arrow": "transferia-connector-clickhouse",
    "object_store": "transferia-connector-s3",
    "postgres-types": "transferia-connector-postgres",
    "rdkafka": "transferia-connector-kafka",
    "tokio-postgres": "transferia-connector-postgres",
    "tokio-postgres-rustls": "transferia-connector-postgres",
    "ydb-grpc": "transferia-connector-logbroker",
    "datafusion": "transferia-middleware-datafusion",
}

DIRECT_HTTP_CLIENT = re.compile(r"reqwest::Client\s*::|\bClient\s*::builder\s*\(")


def internal_dependencies(manifest: dict[str, object], section: str) -> set[str]:
    dependencies = manifest.get(section, {})
    if not isinstance(dependencies, dict):
        return set()
    return {name for name in dependencies if name.startswith("transferia-")}


def connector_isolation_errors(manifests: dict[str, dict[str, object]]) -> list[str]:
    errors: list[str] = []
    for dependency, owner in sorted(HEAVY_CONNECTOR_OWNERS.items()):
        for crate, manifest in manifests.items():
            dependencies = manifest.get("dependencies", {})
            if isinstance(dependencies, dict) and dependency in dependencies and crate != owner:
                errors.append(
                    f"{crate}: heavy dependency '{dependency}' belongs only to {owner}"
                )

    connector_crates = {
        name for name in manifests if name.startswith("transferia-connector-")
    } - {"transferia-connector-support"}
    for crate in sorted(connector_crates):
        dependencies = internal_dependencies(manifests[crate], "dependencies")
        siblings = (dependencies & connector_crates) - {crate}
        if siblings:
            errors.append(
                f"{crate}: connector crates must not depend on siblings: {', '.join(sorted(siblings))}"
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


def outbound_http_boundary_errors() -> list[str]:
    errors: list[str] = []
    wrapper = ROOT / "crates/transferia-connector-support/src/outbound_http.rs"
    for source_path in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
        if source_path == wrapper or "/tests/" in source_path.as_posix():
            continue
        source = source_path.read_text()
        if DIRECT_HTTP_CLIENT.search(source):
            errors.append(
                f"{source_path.relative_to(ROOT)}: direct reqwest client construction is forbidden; "
                "use transferia_connector_support::outbound_http"
            )
    sdk_guards = {
        "crates/transferia-connector-s3/src/connectors/s3/src_batch/config.rs": ".with_http_connector(",
        "crates/transferia-connector-s3/src/connectors/s3/sink/config.rs": ".with_http_connector(",
        "crates/transferia-connector-iceberg/src/iceberg/catalog.rs": ".with_client(client)",
        "crates/transferia-connector-iceberg/src/iceberg/storage.rs": "HttpClientLayer::new(",
    }
    for relative_path, required in sdk_guards.items():
        source_path = ROOT / relative_path
        if required not in source_path.read_text():
            errors.append(
                f"{relative_path}: SDK HTTP transport must be installed through the shared no-redirect policy"
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
        errors.extend(connector_isolation_errors(manifests))
    errors.extend(outbound_http_boundary_errors())

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Rust crate architecture OK ({len(discovered)} crates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
