# Releasing Mallard

Mallard ships as prebuilt binaries through GitHub Releases.

## Supported release targets

- `x86_64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`

Each release publishes a platform archive, `install.sh`, and matching `.sha256` files.

Use the per-file `.sha256` asset that matches the file you downloaded for verification.
`checksums.txt` is a convenience manifest for the whole release, not a single-download shortcut.

## Release flow

1. Make sure `Cargo.toml` has the version you want to release.
2. Merge the release-ready branch to `main`.
3. Create and push a semver tag such as `v0.1.0`.

```bash
git checkout main
git pull --ff-only
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag triggers `.github/workflows/release.yml`, which:

- runs the test suite on each release runner
- builds the release binary for each supported target
- packages the binary with `README.md` and `LICENSE`
- uploads `install.sh` as a release asset
- writes SHA-256 checksum files
- creates the GitHub Release and uploads all artifacts

## Install script

`scripts/install.sh` installs the latest GitHub Release on macOS and Linux.

It can also install a specific release:

```bash
sh scripts/install.sh --version 0.1.0
```

By default it installs to `~/.local/bin`. Override that with `--to <dir>` or `MALLARD_INSTALL_DIR`.
