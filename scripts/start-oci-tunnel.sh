#!/usr/bin/env bash
set -euo pipefail

KEY_FILE="${KEY_FILE:-./instance-team9.key}"
OCI_HOST="${OCI_HOST:-146.56.175.190}"
OCI_USER="${OCI_USER:-opc}"
OCI_SSH_PORT="${OCI_SSH_PORT:-22}"

# Local backend port (on your laptop/workstation)
LOCAL_BACKEND_PORT="${LOCAL_BACKEND_PORT:-8002}"
# Exposed port on OCI host (api proxy points to this)
REMOTE_BACKEND_PORT="${REMOTE_BACKEND_PORT:-4000}"

if [ ! -f "$KEY_FILE" ]; then
  printf 'error: key file not found: %s\n' "$KEY_FILE" >&2
  exit 1
fi

if ! chmod 600 "$KEY_FILE" 2>/dev/null; then
  TMP_KEY="$(mktemp /tmp/querylens-key.XXXXXX)"
  cp "$KEY_FILE" "$TMP_KEY"
  chmod 600 "$TMP_KEY"
  KEY_FILE="$TMP_KEY"
  trap 'rm -f "$TMP_KEY"' EXIT
fi

printf '[start-oci-tunnel] %s@%s\n' "$OCI_USER" "$OCI_HOST"
printf '[start-oci-tunnel] remote 0.0.0.0:%s -> local 127.0.0.1:%s\n' "$REMOTE_BACKEND_PORT" "$LOCAL_BACKEND_PORT"

exec ssh \
  -i "$KEY_FILE" \
  -p "$OCI_SSH_PORT" \
  -o StrictHostKeyChecking=accept-new \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -N \
  -R "0.0.0.0:${REMOTE_BACKEND_PORT}:127.0.0.1:${LOCAL_BACKEND_PORT}" \
  "${OCI_USER}@${OCI_HOST}"
