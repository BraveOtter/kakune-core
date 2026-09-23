#!/bin/sh
set -eu

repository="BraveOtter/kakune-core"
version="${KAKUNE_VERSION:-latest}"
install_dir="${KAKUNE_INSTALL_DIR:-$HOME/.local/bin}"

usage() {
  cat <<'EOF'
Install or update Kakune Core from GitHub Releases.

Usage: install.sh [--version VERSION] [--install-dir DIRECTORY]
                  [--help]

Defaults to the latest release in ~/.local/bin. Re-run the command to update.
KAKUNE_VERSION and KAKUNE_INSTALL_DIR can also set the corresponding values.
EOF
}

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || fail "--version requires a value"
      version="$2"
      shift 2
      ;;
    --install-dir)
      [ "$#" -ge 2 ] || fail "--install-dir requires a value"
      install_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option '$1' (use --help for usage)"
      ;;
  esac
done

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"

system=$(uname -s)
architecture=$(uname -m)
case "$system:$architecture" in
  Linux:x86_64|Linux:amd64)
    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
      fail "the current Linux release requires glibc; musl-based systems are not supported"
    fi
    target="x86_64-unknown-linux-gnu"
    ;;
  Darwin:arm64|Darwin:aarch64)
    target="aarch64-apple-darwin"
    ;;
  Darwin:x86_64|Darwin:amd64)
    fail "the current macOS release provides an Apple Silicon (arm64) build only"
    ;;
  *)
    fail "unsupported platform '$system $architecture'; see the Kakune GitHub Releases page for supported builds"
    ;;
esac

case "$version" in
  latest)
    release_url=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$repository/releases/latest") \
      || fail "could not find the latest GitHub release"
    tag=${release_url##*/}
    ;;
  *)
    case "$version" in
      v*) tag="$version"; version=${version#v} ;;
      *) tag="v$version" ;;
    esac
    case "$version" in
      ''|*[!A-Za-z0-9.+-]*) fail "invalid version '$version'" ;;
    esac
    ;;
esac

case "$tag" in
  v*) release_version=${tag#v} ;;
  *) fail "GitHub returned an unexpected release tag '$tag'" ;;
esac
case "$release_version" in
  ''|*[!A-Za-z0-9.+-]*) fail "invalid release tag '$tag'" ;;
esac

archive_name="kakune-core-$release_version-$target.tar.gz"
release_download="https://github.com/$repository/releases/download/$tag"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/kakune-install.XXXXXX") || fail "could not create a temporary directory"
staged_binary=""
cleanup() {
  if [ -n "$staged_binary" ]; then
    rm -f "$staged_binary"
  fi
  rm -rf "$temporary_directory"
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

curl -fsSL "$release_download/$archive_name" -o "$temporary_directory/$archive_name" \
  || fail "could not download '$archive_name' from release '$tag'"
curl -fsSL "$release_download/SHA256SUMS" -o "$temporary_directory/SHA256SUMS" \
  || fail "could not download SHA256SUMS from release '$tag'"

expected_hash=$(awk -v name="$archive_name" '$2 == name || $2 == "*" name { print $1 }' "$temporary_directory/SHA256SUMS")
case "$expected_hash" in
  ''|*[!0-9A-Fa-f]*) fail "SHA256SUMS has no valid entry for '$archive_name'" ;;
esac
[ "${#expected_hash}" -eq 64 ] || fail "SHA256SUMS has no valid entry for '$archive_name'"

if command -v sha256sum >/dev/null 2>&1; then
  actual_hash=$(sha256sum "$temporary_directory/$archive_name" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
  actual_hash=$(shasum -a 256 "$temporary_directory/$archive_name" | awk '{ print $1 }')
else
  fail "sha256sum or shasum is required to verify the download"
fi
expected_hash=$(printf '%s' "$expected_hash" | tr '[:upper:]' '[:lower:]')
actual_hash=$(printf '%s' "$actual_hash" | tr '[:upper:]' '[:lower:]')
[ "$actual_hash" = "$expected_hash" ] || fail "SHA-256 verification failed for '$archive_name'"

mkdir "$temporary_directory/extracted"
tar -xzf "$temporary_directory/$archive_name" -C "$temporary_directory/extracted" \
  || fail "could not extract '$archive_name'"
[ -f "$temporary_directory/extracted/kakune" ] || fail "the verified archive does not contain kakune"

mkdir -p "$install_dir" || fail "could not create install directory '$install_dir'"
staged_binary="$install_dir/.kakune.new.$$"
cp "$temporary_directory/extracted/kakune" "$staged_binary" \
  || fail "could not stage Kakune in '$install_dir'"
chmod 755 "$staged_binary"
mv -f "$staged_binary" "$install_dir/kakune" \
  || fail "could not install Kakune in '$install_dir'"
staged_binary=""

"$install_dir/kakune" --version
printf 'Installed Kakune %s at %s\n' "$tag" "$install_dir/kakune"

case ":${PATH:-}:" in
  *":$install_dir:"*) ;;
  *)
    if [ "$install_dir" = "$HOME/.local/bin" ]; then
      shell_name=${SHELL##*/}
      case "$shell_name" in
        zsh)
          startup_file="$HOME/.zprofile"
          path_line='export PATH="$HOME/.local/bin:$PATH"'
          ;;
        bash)
          if [ -f "$HOME/.bash_profile" ]; then
            startup_file="$HOME/.bash_profile"
          else
            startup_file="$HOME/.profile"
          fi
          path_line='export PATH="$HOME/.local/bin:$PATH"'
          ;;
        fish)
          startup_file="$HOME/.config/fish/config.fish"
          path_line='fish_add_path "$HOME/.local/bin"'
          mkdir -p "$(dirname "$startup_file")"
          ;;
        *)
          startup_file="$HOME/.profile"
          path_line='export PATH="$HOME/.local/bin:$PATH"'
          ;;
      esac
      if ! grep -Fqx "$path_line" "$startup_file" 2>/dev/null; then
        {
          printf '\n# Added by the Kakune Core installer\n'
          printf '%s\n' "$path_line"
        } >> "$startup_file"
      fi
      printf 'Added %s to PATH in %s; open a new terminal to apply it.\n' "$install_dir" "$startup_file"
    else
      printf 'Add %s to PATH to run kakune without its full path.\n' "$install_dir"
    fi
    ;;
esac

printf "Run '%s/kakune init' to initialize Kakune Core.\n" "$install_dir"
case "$system" in
  Linux)
    printf 'After an update, restart an installed user service with: systemctl --user restart kakune-core.service\n'
    ;;
  Darwin)
    printf 'After an update, restart an installed launchd agent with: launchctl kickstart -k gui/%s/dev.kakune.core\n' "$(id -u)"
    ;;
esac
