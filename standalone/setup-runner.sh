#!/bin/bash
# Set up a self-hosted GitHub Actions runner for moxygen CI/publish jobs.
# Run as a user with sudo access on the target machine.
#
# Usage:
#   bash standalone/setup-runner.sh          # Linux (apt)
#   bash standalone/setup-runner.sh          # macOS (brew, no sudo needed)

set -e

echo "=== Setting up self-hosted runner dependencies ==="

if [[ "$(uname)" == "Darwin" ]]; then
    echo "--- macOS detected ---"
    # brew doesn't need sudo
    brew update
    brew install cmake ninja ccache \
        openssl@3 glog gflags double-conversion libevent \
        libsodium zstd boost fmt c-ares gperf

elif [[ -f /etc/os-release ]]; then
    . /etc/os-release
    case "$ID" in
        ubuntu|debian)
            echo "--- $PRETTY_NAME detected ---"
            sudo apt-get update
            sudo apt-get install -y ccache
            # install-system-deps.sh handles the rest
            bash "$(dirname "$0")/install-system-deps.sh"
            ;;
        *)
            echo "Unsupported: $ID" >&2
            exit 1
            ;;
    esac
else
    echo "Unsupported OS" >&2
    exit 1
fi

echo ""
echo "=== Verification ==="
for cmd in git cmake ninja gcc ccache; do
    if command -v $cmd &>/dev/null; then
        echo "  ✓ $cmd"
    else
        echo "  ✗ $cmd  MISSING" >&2
    fi
done

if command -v docker &>/dev/null && docker ps &>/dev/null; then
    echo "  ✓ docker"
else
    echo "  - docker (optional, needed for container jobs)"
fi

echo ""
echo "Done. Runner is ready for moxygen builds."
