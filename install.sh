#!/bin/sh
set -eu

script_directory="$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)"
invocation_directory="$(pwd -P)"

usage() {
  cat <<'EOF'
Usage: ./install.sh [--system | --prefix PATH]

Build and install the Nakode executable. Rerun the same command after updating
this checkout to replace an existing installation.

Options:
  --system       Install to /usr/local/bin, using sudo only for the copy when
                 the destination is not writable.
  --prefix PATH  Install to PATH/bin without using sudo.
  -h, --help     Show this help.

The default prefix is $HOME/.local, or $PREFIX when that variable is set.
EOF
}

system_install=false
prefix="${PREFIX:-${HOME:?HOME must be set}/.local}"
prefix_was_set=false

while [ "$#" -gt 0 ]; do
  case "$1" in
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
if [ -z "${CARGO_TARGET_DIR:-}" ]; then
  CARGO_TARGET_DIR="$script_directory/target"
  export CARGO_TARGET_DIR
fi

printf '%s\n' 'Building Nakode in release mode...'
cargo build --release --locked

case "$CARGO_TARGET_DIR" in
  /*) built_binary="$CARGO_TARGET_DIR/release/nakode" ;;
  *) built_binary="$script_directory/$CARGO_TARGET_DIR/release/nakode" ;;
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
# replacement. Stop it only after the new executable is in place; attached TUIs
# will detect the disconnect and launch this newly installed version.
printf '%s\n' 'Refreshing the shared Nakode control service...'
"$destination" service shutdown

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
