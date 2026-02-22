#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[deploy-oci] %s\n' "$*"
}

KEY_FILE="${KEY_FILE:-./instance-team9.key}"
OCI_HOST="${OCI_HOST:-146.56.175.190}"
OCI_USER="${OCI_USER:-opc}"
OCI_SSH_PORT="${OCI_SSH_PORT:-22}"
OCI_REMOTE_DIR="${OCI_REMOTE_DIR:-/home/${OCI_USER}/querylens}"

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

SSH_CMD=(ssh -i "$KEY_FILE" -p "$OCI_SSH_PORT" -o StrictHostKeyChecking=accept-new)
RSYNC_SSH="$(printf 'ssh -i %q -p %q -o StrictHostKeyChecking=accept-new' "$KEY_FILE" "$OCI_SSH_PORT")"

log "creating remote directory: $OCI_REMOTE_DIR"
"${SSH_CMD[@]}" "$OCI_USER@$OCI_HOST" "mkdir -p '$OCI_REMOTE_DIR'"

log "uploading project to OCI"
rsync -az --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude 'venv' \
  --exclude 'node_modules' \
  --exclude '*.key' \
  --exclude '*.pem' \
  --exclude '__pycache__' \
  --exclude 'backend/query-visualization/oracle' \
  --exclude 'backend/text-to-sql/oracle/instantclient_*' \
  --exclude 'backend/text-to-sql/var/mongo' \
  -e "$RSYNC_SSH" \
  ./ "$OCI_USER@$OCI_HOST:$OCI_REMOTE_DIR/"

log "restarting OCI stack"
"${SSH_CMD[@]}" "$OCI_USER@$OCI_HOST" "
  cd '$OCI_REMOTE_DIR' &&
  docker compose down &&
  docker compose up -d --build &&
  docker compose ps
"

log "deployment completed"
