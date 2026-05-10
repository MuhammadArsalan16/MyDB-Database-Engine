#!/usr/bin/env bash
# build.sh — configure, build, and install MyDB.
#
# Wraps the cmake configure + build steps and symlinks the `mydb`
# binary into ~/.local/bin so it resolves as a bare command from
# anywhere. After this succeeds:
#
#   mydb init  -u root    # bootstrap the engine (interactive)
#   mydb start -u root    # log in and open the SQL REPL
#
# Usage:
#   ./build.sh           # incremental build + install
#   ./build.sh --clean   # wipe build/ for a from-scratch rebuild

set -euo pipefail

# Run from the repo root regardless of cwd.
cd "$(dirname "$(readlink -f "$0")")"

if [[ "${1:-}" == "--clean" ]]; then
    echo "==> --clean: removing build/"
    rm -rf build
elif [[ -n "${1:-}" ]]; then
    echo "build.sh: unknown argument '$1' (did you mean --clean?)" >&2
    exit 2
fi

echo "==> configuring"
cmake -S . -B build

echo "==> building"
cmake --build build

# Install: symlink ./build/bin/mydb into a directory on $PATH.
# ~/.local/bin is on PATH by default on Fedora and most modern distros
# (per systemd / XDG). A symlink (not a copy) means subsequent rebuilds
# are picked up automatically — no re-install after every change.
INSTALL_DIR="$HOME/.local/bin"
INSTALL_LINK="$INSTALL_DIR/mydb"
mkdir -p "$INSTALL_DIR"
ln -sf "$PWD/build/bin/mydb" "$INSTALL_LINK"
echo "==> installed: $INSTALL_LINK -> $PWD/build/bin/mydb"

# Warn if ~/.local/bin isn't on PATH so the symlink isn't a no-op.
case ":${PATH:-}:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        echo
        echo "WARNING: $INSTALL_DIR is not on \$PATH."
        echo "  Add this to your shell rc to make 'mydb' resolve:"
        echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
        ;;
esac

echo
echo "Done. Next:  mydb init -u root"
