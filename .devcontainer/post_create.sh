#!/usr/bin/env bash
set -Eeuo pipefail

trap 'exit_code=$?; echo "[post-create][ERROR] command failed: ${BASH_COMMAND} (line ${LINENO}, exit ${exit_code})" >&2; exit "${exit_code}"' ERR

echo "[post-create] Starting post-create setup"

echo "[post-create] Setting up git SSH"
bash .devcontainer/setup_git_ssh.sh

echo "[post-create] Syncing and initializing git submodules"
git submodule sync --recursive
git submodule update --init --recursive

echo "[post-create] Installing GitHub CLI"
bash .devcontainer/install_github_cli.sh

echo "[post-create] Installing OpenCode CLI and config"
bash .devcontainer/install_opencode.sh

echo "[post-create] Completed successfully"
