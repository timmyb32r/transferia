# ── transferia — task runner ──────────────────────────────────────────────────
# Run `just` to see all commands. Install: brew install just

_default:
    @just --list

# Format the complete Rust workspace before every code-quality command
fmt:
    cargo fmt --all

# Verify formatting without modifying the working tree
fmt-check:
    cargo fmt --all -- --check

# Regenerate the Rust-owned server API schema and its TypeScript projection
api-contract:
    cargo run -p transferia-server-contracts --bin generate-server-api
    cd web && npm run update:api

# Verify generated API artifacts without modifying the working tree.
api-contract-check:
    cargo run -p transferia-server-contracts --bin generate-server-api -- --check
    cd web && npm run check:api

# Connector UI catalog generation is intentionally separate: it compiles the
# concrete runtime composition and should run only after catalog/schema changes.
catalog-contract:
    TRANSFERIA_SKIP_SERVER_UI=1 cargo run -p transferia-control-plane --bin generate-connector-catalog

catalog-contract-check:
    TRANSFERIA_SKIP_SERVER_UI=1 cargo run -p transferia-control-plane --bin generate-connector-catalog -- --check

# Release/merge gate. This is intentionally expensive and must not be used as
# an ordinary agent completion gate.
check-release: fmt-check check-contracts
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    # Start the heavyweight Redpanda fixture before the other Docker E2Es. On
    # constrained developer runtimes it can otherwise starve during a long
    # all-target run even though the same hermetic test passes in isolation.
    cargo test -p transferia-connector-support --test schema_registry_e2e --all-features
    cargo test --workspace --all-targets --all-features --exclude transferia-connector-support
    cargo test -p transferia-connector-support --lib --all-features
    cd web && npm test

# Normal agent development/completion gate: compile checking only. No linking,
# formatting, linting, tests, E2E, Docker, or generated-artifact checks.
check-affected *args:
    python3 scripts/test_affected.py {{args}}

# Explicit middle gate: generated contracts and the selector's own mapping
# tests, without workspace lint, E2E, or unrelated test targets.
check-contracts: api-contract-check catalog-contract-check test-affected-self

# Safe default: ordinary development never starts the release gate implicitly.
check: check-affected

# Full verification: release gate + Miri UB detection in the stable data-plane.
# Tokio networking tests require macOS kqueue, which Miri cannot emulate.
verify: check-release
    cargo miri test -p transferia-core --lib -- --test-threads=1

# Run MIRI UB detection only (run this before PR if you touched unsafe code)
miri: fmt
    cargo miri test -p transferia-core --lib -- --test-threads=1

# Safe compatibility alias. Full tests are intentionally release-only.
test: check-affected

# Alias for the compile-only affected gate.
test-affected *args:
    python3 scripts/test_affected.py {{args}}

# Preview the affected-test decision without executing commands.
test-affected-dry *args:
    python3 scripts/test_affected.py --dry-run {{args}}

# Verify the conservative affected-test selector itself.
test-affected-self:
    python3 -m unittest scripts/test_test_affected.py scripts/test_check_crate_boundaries.py

# Explicit release-only Clippy gate.
clippy-release: fmt-check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# CI/release pipeline. Miri remains an explicit additional gate for unsafe changes.
ci: check-release

# Verify the compiler-enforced crate dependency direction without compiling.
crate-boundaries:
    python3 scripts/check_crate_boundaries.py

# Sort Cargo.toml dependencies alphabetically
cargo-sort:
    cargo sort --check --grouped
