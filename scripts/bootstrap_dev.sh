#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-}"

log() {
  printf '[bootstrap] %s\n' "$*"
}

relpath() {
  local p="$1"
  case "$p" in
    "$ROOT_DIR"/*) printf '%s\n' "${p#"$ROOT_DIR"/}" ;;
    *) printf '%s\n' "$p" ;;
  esac
}

ensure_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    printf 'error: required command not found: %s\n' "$cmd" >&2
    exit 1
  fi
}

python_version_ok() {
  local bin="$1"
  "$bin" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
}

resolve_python_bin() {
  local candidate
  if [ -n "$PYTHON_BIN" ]; then
    ensure_cmd "$PYTHON_BIN"
    if ! python_version_ok "$PYTHON_BIN"; then
      printf 'error: PYTHON_BIN=%s is below Python 3.10\n' "$PYTHON_BIN" >&2
      exit 1
    fi
    printf '%s\n' "$PYTHON_BIN"
    return
  fi

  for candidate in python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1 && python_version_ok "$candidate"; then
      printf '%s\n' "$candidate"
      return
    fi
  done

  if command -v brew >/dev/null 2>&1; then
    printf '[bootstrap] python>=3.10 not found, installing python@3.11 via Homebrew\n' >&2
    brew install python@3.11
    if command -v python3.11 >/dev/null 2>&1 && python_version_ok python3.11; then
      printf '%s\n' "python3.11"
      return
    fi
  fi

  printf 'error: Python 3.10+ is required but not found\n' >&2
  exit 1
}

ensure_env_file() {
  local target="$1"
  local example="$2"
  if [ -f "$target" ]; then
    return
  fi
  if [ -f "$example" ]; then
    cp "$example" "$target"
    log "created $(relpath "$target") from example"
    return
  fi
  printf 'warning: missing env and example: %s\n' "$target" >&2
}

install_python_requirements() {
  local service_dir="$1"
  local requirements_rel="$2"
  local venv_dir="$service_dir/.venv"
  local python_path="$venv_dir/bin/python"
  local req_path="$service_dir/$requirements_rel"
  local target_ver current_ver

  if [ ! -f "$req_path" ]; then
    printf 'error: requirements file not found: %s\n' "$req_path" >&2
    exit 1
  fi

  target_ver="$("$PYTHON_BIN" - <<'PY'
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
)"

  if [ -x "$python_path" ]; then
    current_ver="$("$python_path" - <<'PY'
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
)"
    if [ "$current_ver" != "$target_ver" ]; then
      rm -rf "$venv_dir"
    fi
  fi

  if [ ! -x "$python_path" ]; then
    log "setting up venv: $(relpath "$venv_dir")"
    "$PYTHON_BIN" -m venv "$venv_dir"
  else
    log "reusing venv: $(relpath "$venv_dir")"
  fi

  "$python_path" -m pip install --upgrade pip setuptools wheel
  "$python_path" -m pip install -r "$req_path"
}

install_ui_deps() {
  local ui_dir="$ROOT_DIR/frontend"
  if ! command -v npm >/dev/null 2>&1; then
    printf 'warning: npm not found; skipping UI dependency install\n' >&2
    return
  fi
  log "installing UI dependencies"
  (
    cd "$ui_dir"
    if [ -f package-lock.json ]; then
      npm ci
    else
      npm install
    fi
  )
}

main() {
  PYTHON_BIN="$(resolve_python_bin)"
  log "using python: $PYTHON_BIN"
  ensure_env_file "$ROOT_DIR/backend/text-to-sql/.env" "$ROOT_DIR/backend/text-to-sql/.env.example"
  ensure_env_file "$ROOT_DIR/backend/query-visualization/.env" "$ROOT_DIR/backend/query-visualization/.env.example"
  if [ ! -f "$ROOT_DIR/.env" ]; then
    printf 'error: missing required env file: %s\n' "$ROOT_DIR/.env" >&2
    exit 1
  fi

  install_python_requirements "$ROOT_DIR/backend/query-visualization" "requirements.txt"
  install_python_requirements "$ROOT_DIR/backend/text-to-sql/backend" "requirements.txt"
  install_ui_deps

  log "done"
  log "next: run ./scripts/smoke_local.sh"
}

main "$@"
