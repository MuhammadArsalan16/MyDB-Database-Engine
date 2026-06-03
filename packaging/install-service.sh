#!/usr/bin/env bash
# install-service.sh — install MyDB as a system-wide systemd service.
#
# This is the privileged counterpart to build.sh. Where build.sh is the
# no-sudo *developer* install (symlinks into ~/.local/bin, bootstraps in
# your own $MYDB_HOME, runs foreground), this script performs the
# *production* deployment that packaging/mydbd.service was written for:
#
#   - create a dedicated 'mydb' system user + group
#   - copy both binaries to /usr/local/bin   (where ExecStart points)
#   - install the unit to /etc/systemd/system
#   - create /var/lib/mydb (MYDB_HOME) owned by the mydb user
#   - bootstrap the engine AS the mydb user  (interactive root password)
#   - enable + (re)start the service
#
# It plays the role an RPM's %pre/%post scriptlets would: this script is
# the honest stepping-stone to a real .spec package. Building stays in
# build.sh and never needs root; only this install step does.
#
# All privileged actions are idempotent — re-running re-copies the
# (possibly rebuilt) binaries, refreshes the unit, and restarts. Run it
# again after every rebuild you want reflected in the running service.
#
# Usage:
#   ./packaging/install-service.sh            # build + install + start
#   ./packaging/install-service.sh --uninstall  # stop, remove unit/binaries/user/data

set -euo pipefail

# Run from the repo root regardless of cwd (this script lives in packaging/).
cd "$(dirname "$(readlink -f "$0")")/.."

# --- Configuration -----------------------------------------------------------
# These MUST agree with packaging/mydbd.service (ExecStart, User, MYDB_HOME).
# The unit is the source of truth; change both together if they ever move.
SERVICE_USER="mydb"
SERVICE_GROUP="mydb"
DATA_DIR="/var/lib/mydb"
BIN_DIR="/usr/local/bin"
UNIT_SRC="packaging/mydbd.service"
UNIT_DEST="/etc/systemd/system/mydbd.service"
INIT_USER="${MYDB_INIT_USER:-root}"

# --- Privilege helper --------------------------------------------------------
# Run privileged commands via sudo, unless we're already root (e.g. invoked
# from an RPM scriptlet). Verify sudo exists before we rely on it.
if [[ "${EUID}" -eq 0 ]]; then
    SUDO=""
else
    if ! command -v sudo >/dev/null 2>&1; then
        echo "install-service.sh: needs root, and 'sudo' is not installed." >&2
        echo "  Re-run as root, or install sudo." >&2
        exit 1
    fi
    SUDO="sudo"
fi

# --- Uninstall ---------------------------------------------------------------
if [[ "${1:-}" == "--uninstall" ]]; then
    echo "==> stopping + disabling service"
    $SUDO systemctl disable --now mydbd 2>/dev/null || true

    echo "==> removing unit + binaries"
    $SUDO rm -f "$UNIT_DEST" "$BIN_DIR/mydbd" "$BIN_DIR/mydb"
    $SUDO systemctl daemon-reload

    # Data + user are removed last and announced loudly — this is destructive.
    if [[ -d "$DATA_DIR" ]]; then
        echo "==> removing data directory $DATA_DIR (all databases)"
        $SUDO rm -rf "$DATA_DIR"
    fi
    if id -u "$SERVICE_USER" >/dev/null 2>&1; then
        echo "==> removing system user '$SERVICE_USER'"
        $SUDO userdel "$SERVICE_USER" 2>/dev/null || true
    fi
    echo
    echo "Uninstalled."
    exit 0
elif [[ -n "${1:-}" ]]; then
    echo "install-service.sh: unknown argument '$1' (did you mean --uninstall?)" >&2
    exit 2
fi

# --- 1. Build (no root) ------------------------------------------------------
# Build first so the binaries we copy are current. This mirrors build.sh's
# configure/build steps and produces build/bin/{mydbd,mydb}.
echo "==> configuring"
cmake -S . -B build
echo "==> building"
cmake --build build

for b in mydbd mydb; do
    if [[ ! -x "build/bin/$b" ]]; then
        echo "install-service.sh: build/bin/$b missing after build — aborting." >&2
        exit 1
    fi
done

# --- 2. System user + group --------------------------------------------------
# --system: no aging, low UID range, not a login account. --home-dir is the
# data directory; --shell nologin since it never logs in interactively.
if id -u "$SERVICE_USER" >/dev/null 2>&1; then
    echo "==> system user '$SERVICE_USER' already exists"
else
    echo "==> creating system user '$SERVICE_USER'"
    $SUDO useradd --system \
        --home-dir "$DATA_DIR" \
        --shell /usr/sbin/nologin \
        "$SERVICE_USER"
fi

# --- 3. Data directory -------------------------------------------------------
# 0750: owner (mydb) full, group read/traverse, world nothing. The socket does
# NOT live here — it goes in /run/mydb (see the unit) so clients need no access
# to this private directory.
echo "==> ensuring data directory $DATA_DIR"
$SUDO install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_DIR"

# --- 4. Binaries -------------------------------------------------------------
# Copies, not symlinks: a system-service user cannot follow a symlink into a
# developer's home directory. Re-running this script refreshes them.
echo "==> installing binaries to $BIN_DIR"
$SUDO install -m 0755 "build/bin/mydbd" "$BIN_DIR/mydbd"
$SUDO install -m 0755 "build/bin/mydb"  "$BIN_DIR/mydb"

# --- 5. systemd unit ---------------------------------------------------------
echo "==> installing unit to $UNIT_DEST"
$SUDO install -m 0644 "$UNIT_SRC" "$UNIT_DEST"
$SUDO systemctl daemon-reload

# --- 6. Bootstrap the engine (interactive) -----------------------------------
# Run AS the mydb user so $DATA_DIR fills with mydb-owned files the daemon can
# read. Only on first install (when __database.mydb does not yet exist).
if $SUDO test ! -f "$DATA_DIR/__database.mydb"; then
    if [[ -t 0 ]]; then
        echo "==> engine not initialised — bootstrapping user '$INIT_USER' as '$SERVICE_USER'"
        $SUDO -u "$SERVICE_USER" env MYDB_HOME="$DATA_DIR" \
            "$BIN_DIR/mydbd" init -u "$INIT_USER"
    else
        echo "==> engine not initialised. Run it interactively (as the mydb user):"
        echo "    sudo -u $SERVICE_USER env MYDB_HOME=$DATA_DIR $BIN_DIR/mydbd init -u $INIT_USER"
        echo "    then: sudo systemctl enable --now mydbd"
        exit 0
    fi
else
    echo "==> engine already initialised in $DATA_DIR — skipping bootstrap"
fi

# --- 7. Enable + (re)start ---------------------------------------------------
# enable is idempotent; restart picks up a freshly copied binary on re-runs.
echo "==> enabling + restarting service"
$SUDO systemctl enable mydbd
$SUDO systemctl restart mydbd

echo
echo "Done."
echo "  Service status :  systemctl status mydbd"
echo "  Logs           :  journalctl -u mydbd -e"
# The service publishes its socket at /run/mydb/mydb.sock (the client's default
# when MYDB_HOME is unset), so a bare connect just works. If you have MYDB_HOME
# exported from dev work, unset it first or it redirects the client's socket.
echo "  Connect client :  mydb connect -u $INIT_USER"
