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
    TRANSFERIA_SKIP_SERVER_UI=1 cargo run -p transferia-control-plane --bin generate-server-api
    cd web && npm run update:api

# Verify generated API artifacts without modifying the working tree.
api-contract-check:
    TRANSFERIA_SKIP_SERVER_UI=1 cargo run -p transferia-control-plane --bin generate-server-api -- --check
    cd web && npm run check:api

# Release/merge gate. This is intentionally expensive and must not be used as
# an ordinary agent completion gate.
check-release: fmt-check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo test --workspace --all-targets --all-features

# Normal agent development/completion gate: compile checking only. No linking,
# formatting, linting, tests, E2E, Docker, or generated-artifact checks.
check-affected *args:
    python3 scripts/test_affected.py {{args}}

# Backward-compatible task-runner name for the release gate, not for agents.
check: check-release

# Full verification: release gate + MIRI UB detection
verify: check-release
    cargo miri test -- --test-threads=1

# Run MIRI UB detection only (run this before PR if you touched unsafe code)
miri: fmt
    cargo miri test -- --test-threads=1

# Run tests only
test: fmt-check
    cargo test --workspace --all-targets --all-features

# Alias for the compile-only affected gate.
test-affected *args:
    python3 scripts/test_affected.py {{args}}

# Preview the affected-test decision without executing commands.
test-affected-dry *args:
    python3 scripts/test_affected.py --dry-run {{args}}

# Verify the conservative affected-test selector itself.
test-affected-self:
    python3 -m unittest scripts/test_test_affected.py scripts/test_check_crate_boundaries.py

# Run clippy only (strict — warnings are errors)
clippy: fmt-check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# CI/release pipeline. Miri remains an explicit additional gate for unsafe changes.
ci: check-release

# Verify the compiler-enforced crate dependency direction without compiling.
crate-boundaries:
    python3 scripts/check_crate_boundaries.py

# Sort Cargo.toml dependencies alphabetically
cargo-sort:
    cargo sort --check --grouped
