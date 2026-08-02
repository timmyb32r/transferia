# ── ydb-ch-replicator — task runner ──────────────────────────────────────────
# Run `just` to see all commands. Install: brew install just

_default:
    @just --list

# Fast local check: clippy (strict) + tests
check:
    cargo clippy --all-targets -- -D warnings
    cargo test

# Full verification: check + MIRI UB detection
verify: check
    cargo miri test -- --test-threads=1

# Run MIRI UB detection only (run this before PR if you touched unsafe code)
miri:
    cargo miri test -- --test-threads=1

# Run tests only
test:
    cargo test

# Run clippy only (strict — warnings are errors)
clippy:
    cargo clippy --all-targets -- -D warnings

# CI pipeline — same as verify
ci: verify
