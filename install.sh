#!/bin/sh
# greenmask installer
# Usage:
#   curl -fsSL https://greenmask.io/install.sh | sh
#   sh install.sh --bin-dir /usr/local/bin --yes
#   sh install.sh --bin-dir /usr/local/bin --binary greenmask-mysql --yes

set -eu

APP="tarbaev-vl"
REPO="greenmask"
BIN_DIR="/usr/local/bin"
VERSION="latest"
BINARY="greenmask"

ASSUME_YES=0
DEBUG=0


info() { printf "> %s\n" "$*"; }
warn() { printf "! %s\n" "$*"; }
err()  { printf "x %s\n" "$*" >&2; }
die()  { err "$*"; exit 1; }


has() { command -v "$1" >/dev/null 2>&1; }

usage() {
  cat <<EOF
greenmask installer

Options:
  -b, --bin-dir DIR     install directory (default: ${BIN_DIR})
  -y, --yes             skip confirmation (non-interactive)
  -v, --version VER     version tag (e.g. v1.2.3) (default: ${VERSION})
  --binary BINARY       binary to install: greenmask or greenmask-mysql (default: ${BINARY})
  --debug               verbose output
  -h, --help            show this help
EOF
}

fetch() {
  url="$1"; out="$2"
  if has curl; then
    curl -fsSL --proto '=https' --tlsv1.2 "$url" -o "$out"
  elif has wget; then
    wget -qO "$out" "$url"
  else
    die "need 'curl' or 'wget' to download: $url"
  fi
}

detect_os() {
  case "$(uname -s | tr '[:upper:]' '[:lower:]')" in
    linux)  printf "linux" ;;
    darwin) printf "darwin" ;;
    *) die "unsupported OS (linux, darwin only)";;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)    printf "amd64" ;;
    aarch64|arm64)   printf "arm64" ;;
    armv7l|armv7)    printf "armv7" ;;
    armv6l|armv6|arm)printf "armv6" ;;
    ppc64le)         printf "ppc64le" ;;
    riscv64)         printf "riscv64" ;;
    s390x)           printf "s390x" ;;
    *) die "unsupported ARCH";;
  esac
}

build_base_url() {
  if [ "$VERSION" = "latest" ]; then
    printf "https://github.com/%s/%s/releases/latest/download" "$APP" "$REPO"
  else
    printf "https://github.com/%s/%s/releases/download/%s" "$APP" "$REPO" "$VERSION"
  fi
}

calc_sha256() {
  f="$1"
  if has sha256sum; then sha256sum "$f" | awk '{print $1}'; elif has shasum; then shasum -a 256 "$f" | awk '{print $1}'; else die "need sha256sum or shasum"; fi
}

verify_checksum_from_archive() {
  tmpdir="$1"
  bin="$2"

  sha_file="$tmpdir/${BINARY}.sha256"

  [ -f "$sha_file" ] || die "checksum file not found in archive"

  expected="$(awk '{print $1}' "$sha_file")"
  actual="$(calc_sha256 "$bin")"

  if [ "$expected" != "$actual" ]; then
    die "checksum mismatch: expected $expected, got $actual"
  fi

  info "Checksum verified ✓"
}

is_writable() {
  dir="$1"
  tmp="$dir/.gm_write_test.$$"
  ( : >"$tmp" ) 2>/dev/null && { rm -f "$tmp"; return 0; }
  return 1
}

need_sudo_or_exit() {
  if ! has sudo; then
    die "need write access to ${BIN_DIR} or 'sudo' for install"
  fi
  if ! sudo -v; then
    die "sudo failed; cannot install to ${BIN_DIR}"
  fi
}

check_bin_in_path() {
  # remove the trailing /
  dir="${1%/}"
  OLDIFS="$IFS"
  IFS=:
  for p in $PATH; do [ "${p%/}" = "$dir" ] && return 0; done
  IFS="$OLDIFS"
  return 1
}

print_path_tips() {
  dir="$1"
  warn "Bin directory ${dir} is not in your PATH."
  printf "\nAdd it to your shell config:\n\n"
  printf "bash: echo 'export PATH=\"%s:\$PATH\"' >> ~/.bashrc && . ~/.bashrc\n" "$dir"
  printf "zsh: echo 'export PATH=\"%s:\$PATH\"' >> ~/.zshrc && . ~/.zshrc\n" "$dir"
  printf "\n"
}

unpack_tar_to() {
  archive="$1"; dest="$2"; 
  has tar || die "need 'tar' to extract archives"
  tar xzf "$archive" -C "$dest"
}

install_asset() {
  os="$1"; arch="$2"; base="$3"
  asset="${BINARY}-${os}-${arch}.tar.gz"
  url="${base}/${asset}"

  has mktemp || die "need 'mktemp' to create temp directories"
  tmp="$(mktemp -d)"

  # clean the tmp directory at the end
  trap 'rm -rf "$tmp"' EXIT INT TERM

  info "Downloading: ${url}"
  fetch "$url" "$tmp/$asset"

  info "Extracting…"
  unpack_tar_to "$tmp/$asset" "$tmp"

  info "Verifying checksum…"
  # find the executable file
  bin_path="$(find "$tmp" -type f -name "$BINARY*" \( -perm -u+x -o -perm -g+x -o -perm -o+x \) 2>/dev/null | head -n1 || true)"
  [ -n "$bin_path" ] || die "binary '$BINARY' not found in archive"
  
  verify_checksum_from_archive "$tmp" "$bin_path"

  sudo=""
  if ! is_writable "$BIN_DIR"; then
    warn "Escalated permissions required for ${BIN_DIR}"
    need_sudo_or_exit
    sudo="sudo"
  fi

  info "Installing to ${BIN_DIR}…"
  if has install; then
    $sudo install -m 0755 "$bin_path" "$BIN_DIR/$BINARY"
  else
    $sudo cp "$bin_path" "$BIN_DIR/$BINARY"
    $sudo chmod 0755 "$BIN_DIR/$BINARY"
  fi

  info "Installed: $BIN_DIR/$BINARY"

  # Shadowed binary warning
  present="$(command -v "$BINARY" 2>/dev/null || true)"
  if [ -n "$present" ] && [ "$present" != "$BIN_DIR/$BINARY" ]; then
    warn "'$BINARY' on PATH is '$present' (not '$BIN_DIR/$BINARY')"
  fi

  if ! check_bin_in_path "$BIN_DIR"; then
    if has realpath; then print_path_tips "$(realpath "$BIN_DIR")"; else print_path_tips "$BIN_DIR"; fi
  fi

  printf "✓ %s %sinstalled\n" "$BINARY" "$( [ "$VERSION" = "latest" ] && printf '' || printf "%s " "$VERSION")"
}

main() {
  # Parse args
  while [ $# -gt 0 ]; do
    case "$1" in
      -b|--bin-dir) BIN_DIR="$2"; shift 2 ;;
      -v|--version) VERSION="$2"; shift 2 ;;
      --binary) BINARY="$2"; shift 2 ;;
      -y|--yes) ASSUME_YES=1;shift;;
      --debug) DEBUG=1; shift;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: $1" ;;
    esac
  done

  [ "$DEBUG" -eq 1 ] && set -x
  
  # Validate binary parameter
  case "$BINARY" in
    greenmask|greenmask-mysql) ;;
    *) die "invalid binary: $BINARY (must be 'greenmask' or 'greenmask-mysql')" ;;
  esac
  
  # If we're not connected to a TTY (piped/CI), default to non-interactive
  if [ "$ASSUME_YES" -ne 1 ] && [ ! -t 0 ]; then
    ASSUME_YES=1
  fi

  os="$(detect_os)"
  arch="$(detect_arch)"
  base="$(build_base_url)"

  printf "Configuration\n"
  info "Binary      : ${BINARY}"
  info "Bin directory: ${BIN_DIR}"
  info "OS/ARCH     : ${os}/${arch}"
  info "Version     : ${VERSION}"
  printf "\n"

  if [ "$ASSUME_YES" -ne 1 ]; then
    printf "? Install %s %s to %s? [y/N] " "$BINARY" "$VERSION" "$BIN_DIR"
    read -r ans || true
    case "$ans" in y|Y|yes|YES) ;; *) die "aborted" ;; esac
  fi

  install_asset "$os" "$arch" "$base"

  info "Uninstall: rm -f $BIN_DIR/$BINARY"
}

main "$@"

