#!/usr/bin/env bash
set -euo pipefail

# Required envs:
# DO_SSH_TARGET=user@host
# REMOTE_DIR=/opt/leadgenerator
# SERVICE_NAME=leadgenerator
# Optional envs:
# PRE_HEALTHCHECK_URL, POST_HEALTHCHECK_URL, EXPO_CHECK_URL
# DO_SSH_OPTS='-i ~/.ssh/key -o StrictHostKeyChecking=accept-new'

: "${DO_SSH_TARGET:?missing DO_SSH_TARGET}"
: "${REMOTE_DIR:?missing REMOTE_DIR}"
: "${SERVICE_NAME:?missing SERVICE_NAME}"
DO_SSH_OPTS="${DO_SSH_OPTS:-}"
SSH_CMD=(ssh)
if [[ -n "$DO_SSH_OPTS" ]]; then
  # shellcheck disable=SC2206
  EXTRA_OPTS=($DO_SSH_OPTS)
  SSH_CMD+=( "${EXTRA_OPTS[@]}" )
fi

TS_UTC() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
LOG_JSON() {
  local stage="$1"
  local status="$2"
  local msg="$3"
  printf '{"timestamp_utc":"%s","stage":"%s","status":"%s","message":"%s"}\n' "$(TS_UTC)" "$stage" "$status" "$msg"
}

run_healthcheck() {
  local url="$1"
  if [[ -z "$url" ]]; then
    return 0
  fi
  curl -fsS "$url" >/dev/null
}

rollback() {
  LOG_JSON "rollback" "started" "Rolling back from latest backup"
  "${SSH_CMD[@]}" "$DO_SSH_TARGET" "set -e; cd '$REMOTE_DIR'; if ls backup-*.tgz >/dev/null 2>&1; then latest=\$(ls -1t backup-*.tgz | head -n1); tar -xzf \"$latest\"; fi; sudo systemctl restart '$SERVICE_NAME'"
  LOG_JSON "rollback" "finished" "Rollback completed"
}

trap 'LOG_JSON "deploy" "failed" "Deploy failed, running rollback"; rollback' ERR

LOG_JSON "precheck" "started" "Running pre-deploy checks"
run_healthcheck "${PRE_HEALTHCHECK_URL:-}"
LOG_JSON "precheck" "finished" "Pre-deploy checks passed"

LOG_JSON "backup" "started" "Creating remote backup"
"${SSH_CMD[@]}" "$DO_SSH_TARGET" "set -e; mkdir -p '$REMOTE_DIR'; cd '$REMOTE_DIR'; tar -czf backup-$(date -u +%Y%m%dT%H%M%SZ).tgz src scripts requirements.txt .env 2>/dev/null || true"
LOG_JSON "backup" "finished" "Remote backup created"

LOG_JSON "deploy" "started" "Syncing files to remote"
RSYNC_SSH="ssh ${DO_SSH_OPTS}"
rsync -az --delete \
  -e "$RSYNC_SSH" \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude '.env' \
  --exclude 'logs/' \
  --exclude 'output/' \
  ./ "$DO_SSH_TARGET:$REMOTE_DIR/"
"${SSH_CMD[@]}" "$DO_SSH_TARGET" "set -e; cd '$REMOTE_DIR'; python3 -m venv .venv; . .venv/bin/activate; pip install -U pip; pip install -r requirements.txt; python -m playwright install chromium; sudo systemctl restart '$SERVICE_NAME'"
LOG_JSON "deploy" "finished" "Remote deploy completed"

LOG_JSON "postcheck" "started" "Running post-deploy checks"
run_healthcheck "${POST_HEALTHCHECK_URL:-${PRE_HEALTHCHECK_URL:-}}"
LOG_JSON "postcheck" "finished" "Post-deploy checks passed"

LOG_JSON "expo_check" "started" "Validating Expo runtime"
run_healthcheck "${EXPO_CHECK_URL:-}"
LOG_JSON "expo_check" "finished" "Expo validation passed"

LOG_JSON "deploy" "success" "Deployment completed successfully"
