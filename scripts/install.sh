#!/usr/bin/env sh
set -eu

REPO="jcgsville/mallard"
VERSION=""
INSTALL_DIR="${MALLARD_INSTALL_DIR:-$HOME/.local/bin}"

usage() {
  cat <<'EOF'
Install the latest Mallard release.

Usage:
  install.sh [--version <version>] [--to <dir>]

Examples:
  install.sh
  install.sh --version 0.1.0
  install.sh --to /usr/local/bin
EOF
}

fail() {
  printf '%s\n' "$*" >&2
  exit 1
}

download() {
  url="$1"
  destination="$2"

  if command -v curl >/dev/null 2>&1; then
    curl --connect-timeout 10 --max-time 300 -fsSL "$url" -o "$destination"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    case "$(wget --help 2>&1 || true)" in
      *--retry-on-http-error*)
        wget --timeout=10 --read-timeout=300 --tries=3 --retry-connrefused --retry-on-http-error=429,500,502,503,504 -O "$destination" "$url"
        ;;
      *)
        wget -T 300 -O "$destination" "$url"
        ;;
    esac
    return
  fi

  fail "Need either curl or wget to download Mallard releases."
}

normalize_version() {
  case "$1" in
    "")
      printf ''
      ;;
    v*)
      printf '%s' "$1"
      ;;
    *)
      printf 'v%s' "$1"
      ;;
  esac
}

detect_target() {
  kernel="$(uname -s)"
  machine="$(uname -m)"

  case "$kernel" in
    Linux)
      os="unknown-linux-gnu"
      archive_ext="tar.gz"
      binary_name="mallard"
      ;;
    Darwin)
      os="apple-darwin"
      archive_ext="tar.gz"
      binary_name="mallard"
      ;;
    *)
      fail "Unsupported OS: $kernel. Download a release artifact manually from GitHub Releases."
      ;;
  esac

  case "$machine" in
    x86_64|amd64)
      arch="x86_64"
      ;;
    arm64|aarch64)
      arch="aarch64"
      ;;
    *)
      fail "Unsupported architecture: $machine. Download a release artifact manually from GitHub Releases."
      ;;
  esac

  if [ "$kernel" = "Linux" ] && command -v ldd >/dev/null 2>&1; then
    case "$(ldd --version 2>&1 || true)" in
      *musl*)
        fail "No prebuilt release for musl-based Linux systems. Build Mallard from source or install it manually on a glibc-based system."
        ;;
    esac
  fi

  case "${arch}-${os}" in
    x86_64-unknown-linux-gnu|x86_64-apple-darwin|aarch64-apple-darwin)
      ;;
    *)
      fail "No prebuilt release for ${arch}-${os}. Download a release artifact manually from GitHub Releases."
      ;;
  esac

  TARGET="${arch}-${os}"
  ARCHIVE_EXT="$archive_ext"
  BINARY_NAME="$binary_name"
}

verify_checksum() {
  archive_path="$1"
  checksum_path="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$archive_path")" && sha256sum -c "$(basename "$checksum_path")")
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    expected="$(awk '{print $1}' "$checksum_path")"
    actual="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
    [ "$expected" = "$actual" ] || fail "Checksum verification failed for $(basename "$archive_path")"
    return
  fi

  fail "Need sha256sum or shasum to verify downloaded artifacts."
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || fail "--version requires a value"
      VERSION="$(normalize_version "$2")"
      shift 2
      ;;
    --to)
      [ "$#" -ge 2 ] || fail "--to requires a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
done

detect_target

archive_name="mallard-${TARGET}.${ARCHIVE_EXT}"
checksum_name="${archive_name}.sha256"

if [ -n "$VERSION" ]; then
  download_root="https://github.com/${REPO}/releases/download/${VERSION}"
else
  download_root="https://github.com/${REPO}/releases/latest/download"
fi

temp_dir="$(mktemp -d 2>/dev/null || mktemp -d -t mallard-install)"
trap 'rm -rf "$temp_dir"' EXIT INT TERM

archive_path="${temp_dir}/${archive_name}"
checksum_path="${temp_dir}/${checksum_name}"
extract_dir="${temp_dir}/extract"

mkdir -p "$extract_dir"

printf 'Downloading %s\n' "$archive_name"
download "${download_root}/${archive_name}" "$archive_path"
download "${download_root}/${checksum_name}" "$checksum_path"
verify_checksum "$archive_path" "$checksum_path"

case "$ARCHIVE_EXT" in
  tar.gz)
    tar -xzf "$archive_path" -C "$extract_dir"
    ;;
  *)
    fail "Unsupported archive format: $ARCHIVE_EXT"
    ;;
esac

binary_path="${extract_dir}/mallard-${TARGET}/${BINARY_NAME}"
[ -f "$binary_path" ] || fail "Downloaded archive did not contain ${BINARY_NAME}"

mkdir -p "$INSTALL_DIR"
cp "$binary_path" "${INSTALL_DIR}/${BINARY_NAME}"
chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

printf 'Installed %s to %s\n' "$BINARY_NAME" "$INSTALL_DIR"

case ":${PATH}:" in
  *":${INSTALL_DIR}:"*)
    ;;
  *)
    printf 'WARNING: %s is not in your PATH. Add it to your shell profile to use %s.\n' "$INSTALL_DIR" "$BINARY_NAME" >&2
    ;;
esac
