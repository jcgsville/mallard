#!/usr/bin/env bash
set -euo pipefail

echo "[$(date -Iseconds)] Starting OpenCode install script"

echo "Installing OpenCode CLI"
curl -fsSL https://opencode.ai/install | bash

# The installer places the binary under ~/.opencode/bin.
export PATH="${HOME}/.opencode/bin:${PATH}"

if command -v opencode >/dev/null 2>&1; then
  echo "opencode installed"
  echo "Version:"
  opencode --version
else
  echo "OpenCode install completed, but 'opencode' is not on PATH yet." >&2
fi
