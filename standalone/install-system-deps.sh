#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

# Installs system dependencies required for standalone moxygen build

set -e

install_ubuntu() {
    echo "Installing dependencies for Ubuntu/Debian..."
    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        cmake \
        ninja-build \
        git \
        libssl-dev \
        libunwind-dev \
        libgoogle-glog-dev \
        libgflags-dev \
        libdouble-conversion-dev \
        libevent-dev \
        libsodium-dev \
        libzstd-dev \
        libboost-all-dev \
        libfmt-dev \
        zlib1g-dev \
        libc-ares-dev \
        gperf
}

install_fedora() {
    echo "Installing dependencies for Fedora/CentOS/RHEL..."
    sudo dnf install -y \
        cmake \
        git \
        openssl-devel \
        glog-devel \
        gflags-devel \
        double-conversion-devel \
        libevent-devel \
        libsodium-devel \
        libzstd-devel \
        boost-devel \
        fmt-devel \
        zlib-devel \
        c-ares-devel \
        gperf
    # ninja-build is available on Fedora but not CentOS/RHEL base repos.
    # Try to install it; if unavailable, warn with alternatives.
    if ! command -v ninja &>/dev/null; then
        if ! sudo dnf install -y ninja-build 2>/dev/null; then
            echo ""
            echo "WARNING: ninja-build not available in configured repos."
            echo "Install ninja manually, e.g.:"
            echo "  pip install ninja"
            echo "  # or download from https://github.com/ninja-build/ninja/releases"
        fi
    fi
}

install_macos() {
    echo "Installing dependencies for macOS..."
    brew install \
        cmake \
        ninja \
        openssl@3 \
        glog \
        gflags \
        double-conversion \
        libevent \
        libsodium \
        zstd \
        boost \
        fmt \
        c-ares \
        gperf
}

# Detect OS - check macOS first
if [[ "$(uname)" == "Darwin" ]]; then
    install_macos
elif [[ -f /etc/os-release ]]; then
    . /etc/os-release
    case "$ID" in
        ubuntu|debian)
            install_ubuntu
            ;;
        fedora|centos|rhel)
            install_fedora
            ;;
        *)
            echo "Unsupported Linux distribution: $ID"
            echo "Please install dependencies manually (see README.md)"
            exit 1
            ;;
    esac
else
    echo "Unsupported operating system"
    exit 1
fi

echo ""
echo "System dependencies installed successfully!"
echo "You can now build moxygen with:"
echo "  cmake -B _build -S standalone -G Ninja"
echo "  cmake --build _build"
