#!/usr/bin/env bash
# Start Docker so core/tests (testcontainers) and e2e-tests can run.
# Pid 1 is not systemd in this environment, so dockerd is launched directly.
# Safe to run again: if the daemon is already up, exit 0.
set -euo pipefail

if docker info >/dev/null 2>&1; then
  exit 0
fi

sudo mkdir -p /etc/docker /var/lib/docker
if [ ! -s /etc/docker/daemon.json ]; then
  printf '%s\n' '{"storage-driver":"fuse-overlayfs","iptables":false,"ip6tables":false}' \
    | sudo tee /etc/docker/daemon.json >/dev/null
fi

if pgrep -x dockerd >/dev/null 2>&1; then
  if [ -S /var/run/docker.sock ]; then
    sudo chmod 666 /var/run/docker.sock
  fi
  docker info >/dev/null 2>&1
  exit 0
fi

sudo dockerd >/tmp/dockerd.log 2>&1 &
dockerd_pid=$!

ready=0
for _ in $(seq 1 60); do
  if [ -S /var/run/docker.sock ]; then
    sudo chmod 666 /var/run/docker.sock || true
  fi
  if docker info >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "dockerd failed to become ready" >&2
  tail -n 50 /tmp/dockerd.log >&2 || true
  exit 1
fi

wait "$dockerd_pid"
