#!/usr/bin/env bash

set -euo pipefail

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

if [[ -n "${REPO_SSH_PRIVATE_KEY_BASE_64:-}" ]]; then
  echo "${REPO_SSH_PRIVATE_KEY_BASE_64}" | base64 -d > "${HOME}/.ssh/id_ed25519"
  chmod 600 "${HOME}/.ssh/id_ed25519"

  touch "${HOME}/.ssh/config"
  chmod 600 "${HOME}/.ssh/config"
  if ! rg -q "IdentityFile ~/.ssh/id_ed25519" "${HOME}/.ssh/config"; then
    cat >> "${HOME}/.ssh/config" <<'EOF'

Host github.com
  User git
  IdentityFile ~/.ssh/id_ed25519
  IdentitiesOnly yes
EOF
  fi
fi

touch "${HOME}/.ssh/known_hosts"
if ! ssh-keygen -F github.com -f "${HOME}/.ssh/known_hosts" >/dev/null 2>&1; then
  ssh-keyscan -t ed25519 github.com >> "${HOME}/.ssh/known_hosts" 2>/dev/null || true
fi
chmod 600 "${HOME}/.ssh/known_hosts"
