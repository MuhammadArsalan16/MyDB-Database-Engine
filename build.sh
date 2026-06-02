#!/usr/bin/env bash
# build.sh — configure, build, install, and (first-run) bootstrap MyDB.
#
# Builds the two binaries and symlinks both into ~/.local/bin so they
# resolve as bare commands from anywhere:
#
#   mydbd                 # the daemon (server-side engine)
#   mydb  connect -u root # the light client REPL
#
# On the first build (engine not yet bootstrapped) it also runs the
# interactive bootstrap (`mydbd init`), prompting for the root password.
# After this succeeds:
#
#   mydbd                  # start the server (foreground)
#   mydb connect -u root   # in another terminal: log in and run SQL
#
# Usage:
#   ./build.sh           # incremental build + install (+ init on first run)
#   ./build.sh --clean   # wipe build/ and $MYDB_HOME for a from-scratch rebuild

set -euo pipefail

# Run from the repo root regardless of cwd.
cd "$(dirname "$(readlink -f "$0")")"

if [[ "${1:-}" == "--clean" ]]; then
    echo "==> --clean: removing build/"
    rm -rf build
    MYDB_DIR="${MYDB_HOME:-$HOME/.mydb}"
    if [[ -d "$MYDB_DIR" ]]; then
        echo "==> --clean: removing $MYDB_DIR"
        rm -rf "$MYDB_DIR"
    fi
elif [[ -n "${1:-}" ]]; then
    echo "build.sh: unknown argument '$1' (did you mean --clean?)" >&2
    exit 2
fi

echo "==> configuring"
cmake -S . -B build

echo "==> building"
cmake --build build

# Install: symlink both binaries into a directory on $PATH.
# ~/.local/bin is on PATH by default on Fedora and most modern distros
# (per systemd / XDG). Symlinks (not copies) mean subsequent rebuilds are
# picked up automatically — no re-install after every change.
INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"
for b in mydbd mydb; do
    ln -sf "$PWD/build/bin/$b" "$INSTALL_DIR/$b"
    echo "==> installed: $INSTALL_DIR/$b -> $PWD/build/bin/$b"
done

# Warn if ~/.local/bin isn't on PATH so the symlinks aren't a no-op.
case ":${PATH:-}:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        echo
        echo "WARNING: $INSTALL_DIR is not on \$PATH."
        echo "  Add this to your shell rc so 'mydbd' / 'mydb' resolve:"
        echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
        ;;
esac

# First-run bootstrap: if the engine has never been initialised, run the
# interactive `mydbd init` now (prompts for the root password).  Skipped on
# every later build once $MYDB_HOME/__database.mydb exists.
MYDB_DIR="${MYDB_HOME:-$HOME/.mydb}"
INIT_USER="${MYDB_INIT_USER:-root}"
if [[ ! -f "$MYDB_DIR/__database.mydb" ]]; then
    if [[ -t 0 ]]; then
        echo
        echo "==> engine not initialised — bootstrapping user '$INIT_USER'"
        "$PWD/build/bin/mydbd" init -u "$INIT_USER"
    else
        echo
        echo "==> engine not initialised. Run it interactively:"
        echo "    mydbd init -u $INIT_USER"
    fi
fi

echo
echo "Done."
echo "  Start the server :  mydbd"
echo "  Connect a client :  mydb connect -u $INIT_USER"
