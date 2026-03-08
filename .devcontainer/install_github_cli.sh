#!/usr/bin/env bash
set -euo pipefail

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "${ARCH}" in
  x86_64)
    ARCH="amd64"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

if [[ "${OS}" != "linux" ]]; then
  echo "Unsupported operating system: ${OS}" >&2
  exit 1
fi

INSTALL_DIR="${HOME}/.local/bin"
TARGET_PATH="${INSTALL_DIR}/gh"
TMP_DIR="$(mktemp -d)"
DOWNLOAD_URL="$(
  curl -fsSL "https://api.github.com/repos/cli/cli/releases/latest" \
    | jq -r '.assets[].browser_download_url | select(test("gh_[0-9.]+_linux_'"${ARCH}"'\\.tar\\.gz$"))' \
    | head -n 1
)"

if [[ -z "${DOWNLOAD_URL}" ]]; then
  echo "Could not determine GitHub CLI download URL for ${ARCH}" >&2
  exit 1
fi

echo "Downloading GitHub CLI (${ARCH})"
curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/gh.tar.gz"
tar -xzf "${TMP_DIR}/gh.tar.gz" -C "${TMP_DIR}"

mkdir -p "${INSTALL_DIR}"
cp "${TMP_DIR}"/gh_*_linux_"${ARCH}"/bin/gh "${TARGET_PATH}"
chmod +x "${TARGET_PATH}"
rm -rf "${TMP_DIR}"

for rc_file in "${HOME}/.bashrc" "${HOME}/.zshrc"; do
  touch "${rc_file}"
  if ! grep -q '^export PATH="\$HOME/.local/bin:\$PATH"$' "${rc_file}"; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "${rc_file}"
  fi
done

echo "gh installed at ${TARGET_PATH}"
echo "Version:"
"${TARGET_PATH}" --version
