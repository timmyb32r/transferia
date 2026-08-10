# ── transferia — task runner ──────────────────────────────────────────────────
# Run `just` to see all commands. Install: brew install just

_default:
    @just --list

# Format the complete Rust workspace before every code-quality command
fmt:
    cargo fmt --all

# Fast local check: rustfmt + clippy (strict) + tests
check: fmt
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test

# Full verification: check + MIRI UB detection
verify: check
    cargo miri test -- --test-threads=1

# Run MIRI UB detection only (run this before PR if you touched unsafe code)
miri: fmt
    cargo miri test -- --test-threads=1

# Run tests only
test: fmt
    cargo test

# Run clippy only (strict — warnings are errors)
clippy: fmt
    cargo clippy --all-targets --all-features -- -D warnings

# CI pipeline — same as verify
ci: verify

# Sort Cargo.toml dependencies alphabetically
cargo-sort:
    cargo sort --check --grouped
