#!/usr/bin/env bash
set -eu

if [ "$#" -ne 3 ]; then
  printf 'Usage: %s <target> <binary-path> <output-dir>\n' "$0" >&2
  exit 1
fi

TARGET="$1"
BINARY_PATH="$2"
OUTPUT_DIR="$3"

if [ ! -f "$BINARY_PATH" ]; then
  printf 'Binary not found: %s\n' "$BINARY_PATH" >&2
  exit 1
fi

case "$TARGET" in
  *-pc-windows-*)
    ARCHIVE_EXT="zip"
    BINARY_NAME="mallard.exe"
    ;;
  *)
    ARCHIVE_EXT="tar.gz"
    BINARY_NAME="mallard"
    ;;
esac

mkdir -p "$OUTPUT_DIR"
STAGE_ROOT="$(mktemp -d 2>/dev/null || mktemp -d -t mallard-package)"
trap 'rm -rf "$STAGE_ROOT"' EXIT INT TERM

PACKAGE_DIR="${STAGE_ROOT}/mallard-${TARGET}"
ARCHIVE_PATH="${OUTPUT_DIR}/mallard-${TARGET}.${ARCHIVE_EXT}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"

mkdir -p "$PACKAGE_DIR"
cp "$BINARY_PATH" "${PACKAGE_DIR}/${BINARY_NAME}"
cp README.md LICENSE "$PACKAGE_DIR/"

case "$ARCHIVE_EXT" in
  tar.gz)
    tar -C "$STAGE_ROOT" -czf "$ARCHIVE_PATH" "$(basename "$PACKAGE_DIR")"
    ;;
  zip)
    POWERSHELL_BIN=""
    for candidate in pwsh powershell; do
      if command -v "$candidate" >/dev/null 2>&1; then
        POWERSHELL_BIN="$candidate"
        break
      fi
    done
    if [ -z "$POWERSHELL_BIN" ]; then
      printf 'Need pwsh or powershell to create Windows zip archives.\n' >&2
      exit 1
    fi
    PACKAGE_DIR_NATIVE="$PACKAGE_DIR"
    ARCHIVE_PATH_NATIVE="$ARCHIVE_PATH"
    if command -v cygpath >/dev/null 2>&1; then
      PACKAGE_DIR_NATIVE="$(cygpath -w "$PACKAGE_DIR")"
      ARCHIVE_PATH_NATIVE="$(cygpath -w "$ARCHIVE_PATH")"
    fi
    "$POWERSHELL_BIN" -NoLogo -NoProfile -Command "Compress-Archive -Path \"$PACKAGE_DIR_NATIVE\" -DestinationPath \"$ARCHIVE_PATH_NATIVE\""
    ;;
  *)
    printf 'Unsupported archive format: %s\n' "$ARCHIVE_EXT" >&2
    exit 1
    ;;
esac

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$OUTPUT_DIR" && sha256sum "$(basename "$ARCHIVE_PATH")" > "$(basename "$CHECKSUM_PATH")")
elif command -v shasum >/dev/null 2>&1; then
  digest="$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
  printf '%s  %s\n' "$digest" "$(basename "$ARCHIVE_PATH")" > "$CHECKSUM_PATH"
elif command -v openssl >/dev/null 2>&1; then
  digest="$(openssl dgst -sha256 -r "$ARCHIVE_PATH" | awk '{print $1}')"
  printf '%s  %s\n' "$digest" "$(basename "$ARCHIVE_PATH")" > "$CHECKSUM_PATH"
else
  printf 'Need sha256sum, shasum, or openssl to compute checksums.\n' >&2
  exit 1
fi

printf 'Packaged %s\n' "$ARCHIVE_PATH"
