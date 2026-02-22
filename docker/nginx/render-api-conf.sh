#!/bin/sh
set -eu

if [ -z "${BACKEND_URL:-}" ]; then
  echo "BACKEND_URL is required" >&2
  exit 1
fi

BACKEND_URL="${BACKEND_URL%/}"
export BACKEND_URL

envsubst '${BACKEND_URL}' < /etc/nginx/api.conf.tmpl > /etc/nginx/conf.d/default.conf
