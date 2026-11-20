#!/usr/bin/env bash
# Idempotent dependency install for Cursor Cloud Agents.
# Runs from the workspace root. On a cloud build the checkout is /agent/repos/rindexer.
set -euo pipefail

if [ -d /agent/repos/rindexer ]; then
  cd /agent/repos/rindexer
fi

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
  -o Dpkg::Options::="--force-confdef" \
  -o Dpkg::Options::="--force-confold" \
  build-essential \
  ca-certificates \
  curl \
  pkg-config \
  libssl-dev \
  docker.io \
  fuse-overlayfs \
  iptables \
  g++

# This image's cc/c++ alternatives can point at clang. Clang does not see GCC's
# libstdc++ headers or the unversioned libstdc++.so, so `cargo test --all-features`
# fails while compiling RocksDB and again at link time (`unable to find library -lstdc++`).
# g++/gcc are the compilers CI's Ubuntu runners use.
if update-alternatives --query c++ >/dev/null 2>&1; then
  sudo update-alternatives --set c++ /usr/bin/g++
fi
if update-alternatives --query cc >/dev/null 2>&1; then
  sudo update-alternatives --set cc /usr/bin/gcc
fi

if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal --component rustfmt,clippy
  sudo ln -sfn "$HOME/.cargo/bin/rustup" /usr/local/bin/rustup
  sudo ln -sfn "$HOME/.cargo/bin/cargo" /usr/local/bin/cargo
  sudo ln -sfn "$HOME/.cargo/bin/rustc" /usr/local/bin/rustc
fi

# rust-toolchain.toml selects stable. Components must be added for that toolchain.
rustup component add rustfmt clippy

if ! command -v node >/dev/null 2>&1 || ! node --version | grep -q '^v22\.'; then
  node_version=22.23.4
  curl -fsSL "https://nodejs.org/dist/v${node_version}/node-v${node_version}-linux-x64.tar.xz" \
    | sudo tar -xJ -C /usr/local --strip-components=1 --no-same-owner
fi

if ! command -v cargo-nextest >/dev/null 2>&1; then
  cargo install cargo-nextest --locked
fi
if [ -x "$HOME/.cargo/bin/cargo-nextest" ]; then
  sudo ln -sfn "$HOME/.cargo/bin/cargo-nextest" /usr/local/bin/cargo-nextest
fi

# e2e-tests needs forge and anvil. Same installer the release Dockerfile uses.
if ! command -v forge >/dev/null 2>&1; then
  curl -fsSL https://foundry.paradigm.xyz | bash
  "$HOME/.foundry/bin/foundryup"
fi
if [ -x "$HOME/.foundry/bin/forge" ]; then
  sudo ln -sfn "$HOME/.foundry/bin/forge" /usr/local/bin/forge
  sudo ln -sfn "$HOME/.foundry/bin/cast" /usr/local/bin/cast
  sudo ln -sfn "$HOME/.foundry/bin/anvil" /usr/local/bin/anvil
fi

# Download crates only. A full compile stays with the agent: release/reth builds
# are slow, and target/ is branch-specific.
cargo fetch --locked
