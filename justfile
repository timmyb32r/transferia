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
    TRANSFERIA_SKIP_SERVER_UI=1 cargo run --bin generate-server-api
    cd web && npm run generate:api

# Complete mandatory gate. Cargo tests include the embedded web UI contract suite.
check: fmt-check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo test --workspace --all-targets --all-features

# Full verification: check + MIRI UB detection
verify: check
    cargo miri test -- --test-threads=1

# Run MIRI UB detection only (run this before PR if you touched unsafe code)
miri: fmt
    cargo miri test -- --test-threads=1

# Run tests only
test: fmt-check
    cargo test --workspace --all-targets --all-features

# Run clippy only (strict — warnings are errors)
clippy: fmt-check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# CI pipeline. Miri remains an explicit additional gate for unsafe changes.
ci: check

# Sort Cargo.toml dependencies alphabetically
cargo-sort:
    cargo sort --check --grouped
