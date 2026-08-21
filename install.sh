#!/bin/sh
set -eu

script_directory="$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)"
invocation_directory="$(pwd -P)"

# Canonical managed-source remote. A retired Cursor Origin remote is rewritten
# to this so `nakode update` follows the GitHub project. A remote that already
# points at this repository is left exactly as the user configured it.
canonical_source_remote='https://github.com/QuillDev/nakode.git'

usage() {
  cat <<EOF
Usage: ./install.sh [--debug] [--system | --prefix PATH]

Build and install the Nakode executable. Rerun the same command after updating
this checkout to replace an existing installation.

Options:
  --debug        Use the faster development build for local iteration. The
                 installed executable will be larger and less optimized.
  --system       Install to /usr/local/bin, using sudo only for the copy when
                 the destination is not writable.
  --prefix PATH  Install to PATH/bin without using sudo.
  -h, --help     Show this help.

The default prefix is \$HOME/.local, or \$PREFIX when that variable is set.

Clone and update from GitHub:
  $canonical_source_remote
EOF
}

strip_url_credentials() {
  url=$1
  case "$url" in
    *://*@*)
      scheme=${url%%://*}
      rest=${url#*://}
      userinfo=${rest%%@*}
      hostpath=${rest#*@}
      case "$userinfo" in
        */*)
          printf '%s\n' "$url"
          ;;
        *)
          printf '%s\n' "$scheme://$hostpath"
          ;;
      esac
      ;;
    *)
      printf '%s\n' "$url"
      ;;
  esac
}

normalize_upstream_url() {
  cleaned=$(strip_url_credentials "$1")
  cleaned=${cleaned%/}
  cleaned=${cleaned%.git}
  printf '%s\n' "$cleaned"
}

# Every spelling of the canonical GitHub repository. A remote matching one of
# these already points at the right place, so it is preserved exactly as
# configured instead of being rewritten to the HTTPS form.
is_canonical_upstream_url() {
  case "$(normalize_upstream_url "$1")" in
    https://github.com/QuillDev/nakode | \
      http://github.com/QuillDev/nakode | \
      git@github.com:QuillDev/nakode | \
      ssh://git@github.com/QuillDev/nakode | \
      ssh://github.com/QuillDev/nakode)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

# Retired Cursor Origin locations. These are migrated to the canonical GitHub
# remote so an existing Origin checkout can still update.
is_retired_upstream_url() {
  case "$(normalize_upstream_url "$1")" in
    https://origin.cursor.com/git/fragile-inc/nakode | \
      http://origin.cursor.com/git/fragile-inc/nakode | \
      https://origin.cursor.com/fragile-inc/nakode | \
      http://origin.cursor.com/fragile-inc/nakode | \
      git@origin.cursor.com:fragile-inc/nakode | \
      git@origin.cursor.com:git/fragile-inc/nakode | \
      ssh://git@origin.cursor.com/fragile-inc/nakode | \
      ssh://git@origin.cursor.com/git/fragile-inc/nakode | \
      ssh://origin.cursor.com/fragile-inc/nakode | \
      ssh://origin.cursor.com/git/fragile-inc/nakode)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

normalize_managed_source_remote() {
  if ! command -v git >/dev/null 2>&1; then
    return 0
  fi
  if ! git -C "$script_directory" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return 0
  fi

  current_remote="$(git -C "$script_directory" config --get remote.origin.url 2>/dev/null || true)"
  if [ -z "$current_remote" ] || is_canonical_upstream_url "$current_remote"; then
    return 0
  fi
  if ! is_retired_upstream_url "$current_remote"; then
    return 0
  fi

  printf '%s\n' "Retargeting the source remote to $canonical_source_remote"
  git -C "$script_directory" remote set-url origin "$canonical_source_remote"
}

system_install=false
debug_build=false
prefix="${PREFIX:-${HOME:?HOME must be set}/.local}"
prefix_was_set=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --debug)
      debug_build=true
      ;;
    --system)
      if [ "$prefix_was_set" = true ]; then
        printf '%s\n' 'Choose either --system or --prefix, not both.' >&2
        exit 2
      fi
      system_install=true
      prefix=/usr/local
      ;;
    --prefix)
      if [ "$system_install" = true ]; then
        printf '%s\n' 'Choose either --system or --prefix, not both.' >&2
        exit 2
      fi
      shift
      if [ "$#" -eq 0 ] || [ -z "$1" ]; then
        printf '%s\n' '--prefix requires a non-empty path.' >&2
        exit 2
      fi
      case "$1" in
        -*)
          printf '%s\n' '--prefix requires a path, not another option.' >&2
          exit 2
          ;;
      esac
      prefix=$1
      prefix_was_set=true
      ;;
    --prefix=*)
      if [ "$system_install" = true ]; then
        printf '%s\n' 'Choose either --system or --prefix, not both.' >&2
        exit 2
      fi
      prefix=${1#--prefix=}
      if [ -z "$prefix" ]; then
        printf '%s\n' '--prefix requires a non-empty path.' >&2
        exit 2
      fi
      prefix_was_set=true
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
  printf '%s\n' \
    'Do not run install.sh through sudo.' \
    'Run it as your normal user; --system will use sudo only to copy the finished binary.' >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  printf '%s\n' 'cargo is required to build Nakode but was not found in PATH.' >&2
  exit 1
fi

case "$prefix" in
  /*) ;;
  *) prefix="$invocation_directory/$prefix" ;;
esac

case "$prefix" in
  /) bin_directory=/bin ;;
  */) bin_directory="${prefix%/}/bin" ;;
  *) bin_directory="$prefix/bin" ;;
esac
destination="$bin_directory/nakode"

if [ -d "$destination" ]; then
  printf 'Cannot install Nakode because %s is a directory.\n' "$destination" >&2
  exit 1
fi

cd "$script_directory"
normalize_managed_source_remote
if [ -z "${CARGO_TARGET_DIR:-}" ]; then
  CARGO_TARGET_DIR="$script_directory/target"
  export CARGO_TARGET_DIR
fi

if [ "$debug_build" = true ]; then
  printf '%s\n' 'Building Nakode in development mode...'
  cargo build --locked
  build_directory=debug
else
  printf '%s\n' 'Building Nakode in release mode...'
  cargo build --release --locked
  build_directory=release
fi

case "$CARGO_TARGET_DIR" in
  /*) built_binary="$CARGO_TARGET_DIR/$build_directory/nakode" ;;
  *) built_binary="$script_directory/$CARGO_TARGET_DIR/$build_directory/nakode" ;;
esac

if [ ! -x "$built_binary" ]; then
  printf 'Cargo completed but the Nakode executable was not found at %s.\n' \
    "$built_binary" >&2
  exit 1
fi
built_version="$("$built_binary" --version)"

install_without_privileges() {
  target_directory=$1
  target_path=$2
  source_path=$3

  mkdir -p "$target_directory"
  temporary_path="$(mktemp "$target_directory/.nakode.install.XXXXXXXX")"
  cleanup_temporary() {
    if [ -n "${temporary_path:-}" ]; then
      rm -f "$temporary_path"
    fi
  }
  trap cleanup_temporary 0 1 2 15

  cp "$source_path" "$temporary_path"
  chmod 0755 "$temporary_path"
  mv -f "$temporary_path" "$target_path"
  temporary_path=
  trap - 0 1 2 15
}

if { [ -d "$bin_directory" ] && [ -w "$bin_directory" ]; } \
  || { [ ! -e "$bin_directory" ] && mkdir -p "$bin_directory" 2>/dev/null; }; then
  install_without_privileges "$bin_directory" "$destination" "$built_binary"
elif [ "$system_install" = true ]; then
  if ! command -v sudo >/dev/null 2>&1; then
    printf 'Installing to %s requires elevated privileges, but sudo was not found.\n' \
      "$bin_directory" >&2
    exit 1
  fi
  printf 'Installing %s with elevated privileges...\n' "$destination"
  sudo mkdir -p "$bin_directory"
  sudo install -m 0755 "$built_binary" "$destination"
else
  printf 'Cannot write to %s. Choose a writable --prefix or use --system.\n' \
    "$bin_directory" >&2
  exit 1
fi

# A running service keeps executing the inode it started from after an atomic
# replacement. Refresh every idle stale workspace service only after the new
# executable is in place. The helper is invoked by explicit path so this works
# even when the caller is still running the previous Nakode binary.
printf '%s\n' 'Refreshing stale Nakode services...'
"$destination" restart-stale ||
  printf '%s\n' 'Nakode installed, but stale workspace services could not be refreshed.' >&2

printf '\nInstalled %s\n' "$destination"
printf '%s\n' "$built_version"

case ":${PATH:-}:" in
  *:"$bin_directory":*) ;;
  *)
    printf '\n%s is not currently in PATH. Add this line to your shell profile:\n' \
      "$bin_directory"
    printf '  export PATH="%s:$PATH"\n' "$bin_directory"
    ;;
esac
