#!/bin/sh
set -eu

# Keep the cache bounded even when this wrapper is invoked outside Cargo and
# therefore does not inherit the project-level `[env]` table.
export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-50G}"

# Codex' macOS seatbelt denies the IPC operation used by sccache. Keep agent
# checks functional without weakening the normal developer setup.
if [ "${CODEX_SANDBOX:-}" = "seatbelt" ]; then
    exec "$@"
fi

exec sccache "$@"
