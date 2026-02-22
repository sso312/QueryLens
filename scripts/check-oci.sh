#!/usr/bin/env bash
set -euo pipefail

FRONTEND_PORT="${FRONTEND_PORT:-8000}"
API_PORT="${API_PORT:-80}"
PUBLIC_IP="${PUBLIC_IP:-146.56.175.190}"
OCI_HOST="${OCI_HOST:-$PUBLIC_IP}"
OCI_USER="${OCI_USER:-opc}"
OCI_SSH_PORT="${OCI_SSH_PORT:-22}"
OCI_REMOTE_DIR="${OCI_REMOTE_DIR:-/home/${OCI_USER}/querylens}"
KEY_FILE="${KEY_FILE:-./instance-team9.key}"

printf '[check-oci] Frontend URL: http://%s:%s\n' "$PUBLIC_IP" "$FRONTEND_PORT"
curl -fsSI "http://${PUBLIC_IP}:${FRONTEND_PORT}" | head -n 1

printf '[check-oci] API proxy healthz: http://%s:%s/healthz\n' "$PUBLIC_IP" "$API_PORT"
curl -fsS "http://${PUBLIC_IP}:${API_PORT}/healthz"
printf '\n'

printf '[check-oci] API -> backend health: http://%s:%s/api/health\n' "$PUBLIC_IP" "$API_PORT"
curl -fsS "http://${PUBLIC_IP}:${API_PORT}/api/health"
printf '\n'

if [ -f "$KEY_FILE" ]; then
  if ! chmod 600 "$KEY_FILE" 2>/dev/null; then
    TMP_KEY="$(mktemp /tmp/querylens-key.XXXXXX)"
    cp "$KEY_FILE" "$TMP_KEY"
    chmod 600 "$TMP_KEY"
    KEY_FILE="$TMP_KEY"
    trap 'rm -f "$TMP_KEY"' EXIT
  fi

  SSH_CMD=(ssh -i "$KEY_FILE" -p "$OCI_SSH_PORT" -o StrictHostKeyChecking=accept-new)
  printf '[check-oci] Remote docker compose ps (%s@%s)\n' "$OCI_USER" "$OCI_HOST"
  "${SSH_CMD[@]}" "${OCI_USER}@${OCI_HOST}" "cd '$OCI_REMOTE_DIR' && docker compose ps"
  printf '[check-oci] Remote :4000 listener (reverse tunnel)\n'
  "${SSH_CMD[@]}" "${OCI_USER}@${OCI_HOST}" "ss -ltnp | grep ':4000' || true"
else
  printf '[check-oci] skip remote ssh checks (key not found: %s)\n' "$KEY_FILE"
fi
