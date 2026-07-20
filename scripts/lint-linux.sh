#!/usr/bin/env bash
#
# Run CI's clippy check against the Linux target, from any host.
#
# Parts of this codebase are behind #[cfg(target_os = "linux")] — /proc parsing
# in src/error.rs, the lsof/ss handling in src/port/conflict.rs. On macOS that
# code is compiled out, so local clippy cannot see it and a clean local run can
# still fail CI. Cross-compiling doesn't help either: openssl-sys needs a cross
# toolchain. So we lint inside a Linux container instead.
#
# Usage: scripts/lint-linux.sh
#
# Keep RUST_VERSION in sync with the clippy job in .github/workflows/ci.yml.

set -euo pipefail

RUST_VERSION="${RUST_VERSION:-1.97}"

cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
    echo "error: Docker is not running — this script needs it to lint the Linux target." >&2
    exit 1
fi

echo "Running clippy for the Linux target (rust:${RUST_VERSION})..."

# CARGO_TARGET_DIR is redirected so the container's Linux artifacts never
# collide with the host's target/ directory.
exec docker run --rm \
    -v "$PWD":/w \
    -w /w \
    -e CARGO_TARGET_DIR=/tmp/target \
    "rust:${RUST_VERSION}" \
    bash -c '
        set -euo pipefail
        apt-get update -qq >/dev/null
        apt-get install -y -qq pkg-config libssl-dev >/dev/null
        rustup component add clippy >/dev/null
        cargo clippy --all-targets --all-features -- -D warnings
    '
