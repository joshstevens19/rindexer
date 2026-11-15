#!/usr/bin/env bash
# Smoke-test a macOS rindexer_cli binary.
# Usage: check-macos-binary.sh <binary> [expected-minos]
# expected-minos defaults to 15.0 (major.minor).
set -euo pipefail

bin=$1
expected=${2:-15.0}
bin=$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")
chmod +x "$bin"

echo "=== $bin --version ==="
"$bin" --version
"$bin" help

echo "=== otool -L ==="
otool -L "$bin"
if otool -L "$bin" | grep -Ei 'libssl|libcrypto|openssl'; then
  echo "binary links OpenSSL dynamically"
  exit 1
fi

echo "=== LC_BUILD_VERSION ==="
if command -v vtool >/dev/null 2>&1; then
  vtool -show-build "$bin" || true
fi
otool -l "$bin" | awk '
  $1 == "cmd" && $2 == "LC_BUILD_VERSION" { show = 1; print; next }
  show && $1 == "cmd" { show = 0 }
  show { print }
'

minos=$(otool -l "$bin" | awk '$1 == "minos" { print $2; exit }')
norm() { awk -F. '{ printf "%d.%d", $1, ($2 == "" ? 0 : $2) }' <<<"$1"; }
got=$(norm "$minos")
want=$(norm "$expected")
echo "minos=$minos expected=$expected"
if [[ -z "$minos" || "$got" != "$want" ]]; then
  echo "LC_BUILD_VERSION minos is $minos, expected $expected"
  exit 1
fi

work=$(mktemp -d)
cd "$work"
printf 'Smoke\n\n\ncsv\n' | "$bin" new no-code
test -f Smoke/rindexer.yaml
echo "rindexer new created $work/Smoke/rindexer.yaml"
