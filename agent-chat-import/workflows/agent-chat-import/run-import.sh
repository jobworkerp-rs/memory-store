#!/usr/bin/env bash
# Enqueue agent-chat-import.yaml on jobworkerp via jobworkerp-client.
#
# This is the focused import-only counterpart of
# agent-chat-import/workflows/agent-chat-pipeline/run-pipeline.sh. Use it when running
# import on a separate cron from summary (split-execution setup with
# per-host imports feeding a single memories summary worker).
#
# Usage:
#   agent-chat-import/workflows/agent-chat-import/run-import.sh [options]
#
# Required (no defaults):
#   --memories-grpc-url <url>          memories gRPC URL.
#
# Common options:
#   --source <claude-code|codex|plain> default: claude-code
#   --user-id <i64>                    default: 1
#   --since-date <YYYY-MM-DD>          default: per --since-mode
#   --end-date <YYYY-MM-DD>            default: auto-derived (today in
#                                      --tz when --since-date is set,
#                                      else yesterday/today per
#                                      --since-mode). Marks the END
#                                      (inclusive) of the processing
#                                      window. Pass to back-fill a
#                                      fixed historical range
#                                      (e.g. --since-date 2026-04-01
#                                      --end-date 2026-04-30) without
#                                      pulling today in. Must be on/
#                                      after --since-date.
#   --since-override <UTC ISO Z>       overrides since calculation
#   --since-mode <day_start|now_minus> default: day_start
#   --since-lookback-seconds <s>       default: 0 (effective only when
#                                      since_mode=now_minus)
#   --tz <hours>                       default: 9
#   --memories-grpc-host <host>        rarely needed; carried for parity
#   --memories-grpc-port <port>        rarely needed; carried for parity
#   --import-command <path>            default: target/release/memories-import.
#                                      Same resolution rules as
#                                      run-pipeline.sh (absolute / repo-
#                                      relative / PATH lookup).
#   --base-dir <path>                  default: this script's repo root
#   --no-all                           do NOT pass --all-projects/--all-sessions
#   --claude-dir <path>                memories-import --claude-dir
#   --codex-dir <path>                 memories-import --codex-dir
#   --strip-path-prefix <csv>          memories-import -P
#   --extra-import-args <csv>          extra args appended verbatim
#
# Workflow URLs:
#   --import-yaml <path|url>           default: sibling agent-chat-import.yaml
#
# jobworkerp connection:
#   --jobworkerp-addr <url>            default: env JOBWORKERP_ADDR
#                                      or http://localhost:9000
#   --timeout-sec <sec>                default: 7200 (= 2 h, matches workflow)
#   --channel <name>                   default: workflow_base
#   --format <table|card|json>         default: card
#
# Misc:
#   --print-only                       show payload + command, do not exec
#   -h | --help                        show this header
#
# Exit codes match jobworkerp-client (0 on success).
# ---END-HELP---

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKFLOWS_DIR="${CRATE_DIR}/workflows"
WORKERS_DIR="${CRATE_DIR}/workers"
BASE_DIR_DEFAULT="$(cd "${CRATE_DIR}/.." && pwd)"

# ----------------------------------------------------------------
# Defaults
# ----------------------------------------------------------------
SOURCE="claude-code"
USER_ID="1"
SINCE_DATE=""
END_DATE=""
SINCE_OVERRIDE=""
SINCE_MODE="day_start"
SINCE_LOOKBACK_SECONDS="0"
TZ_HOURS="9"
MEMORIES_GRPC_URL=""
MEMORIES_GRPC_HOST=""
MEMORIES_GRPC_PORT=""

BASE_DIR="${BASE_DIR_DEFAULT}"
IMPORT_COMMAND_REL="target/release/memories-import"
ALL_FLAG="true"
CLAUDE_DIR=""
CODEX_DIR=""
STRIP_PATH_PREFIX=""
EXTRA_IMPORT_ARGS_CSV=""

IMPORT_YAML="${SCRIPT_DIR}/agent-chat-import.yaml"

JOBWORKERP_ADDR_DEFAULT="${JOBWORKERP_ADDR:-http://localhost:9000}"
JOBWORKERP_ADDR="${JOBWORKERP_ADDR_DEFAULT}"
TIMEOUT_SEC="7200"
CHANNEL="workflow_base"
FORMAT="card"

PRINT_ONLY="false"

# ----------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------
print_help() {
    sed -n '2,/^# ---END-HELP---/p' "$0" \
        | sed -e '$d' -e 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)                    SOURCE="$2"; shift 2 ;;
        --user-id)                   USER_ID="$2"; shift 2 ;;
        --since-date)                SINCE_DATE="$2"; shift 2 ;;
        --end-date)                  END_DATE="$2"; shift 2 ;;
        --since-override)            SINCE_OVERRIDE="$2"; shift 2 ;;
        --since-mode)                SINCE_MODE="$2"; shift 2 ;;
        --since-lookback-seconds)    SINCE_LOOKBACK_SECONDS="$2"; shift 2 ;;
        --tz)                        TZ_HOURS="$2"; shift 2 ;;
        --memories-grpc-url)         MEMORIES_GRPC_URL="$2"; shift 2 ;;
        --memories-grpc-host)        MEMORIES_GRPC_HOST="$2"; shift 2 ;;
        --memories-grpc-port)        MEMORIES_GRPC_PORT="$2"; shift 2 ;;
        --base-dir)                  BASE_DIR="$2"; shift 2 ;;
        --import-command)            IMPORT_COMMAND_REL="$2"; shift 2 ;;
        --no-all)                    ALL_FLAG="false"; shift 1 ;;
        --claude-dir)                CLAUDE_DIR="$2"; shift 2 ;;
        --codex-dir)                 CODEX_DIR="$2"; shift 2 ;;
        --strip-path-prefix)         STRIP_PATH_PREFIX="$2"; shift 2 ;;
        --extra-import-args)         EXTRA_IMPORT_ARGS_CSV="$2"; shift 2 ;;
        --import-yaml)               IMPORT_YAML="$2"; shift 2 ;;
        --jobworkerp-addr)           JOBWORKERP_ADDR="$2"; shift 2 ;;
        --timeout-sec)               TIMEOUT_SEC="$2"; shift 2 ;;
        --channel)                   CHANNEL="$2"; shift 2 ;;
        --format)                    FORMAT="$2"; shift 2 ;;
        --print-only)                PRINT_ONLY="true"; shift 1 ;;
        -h|--help)                   print_help; exit 0 ;;
        *)
            echo "error: unknown option: $1" >&2
            echo "run with --help for usage." >&2
            exit 2
            ;;
    esac
done

# ----------------------------------------------------------------
# Validation
# ----------------------------------------------------------------
need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required command not found: $1" >&2
        exit 1
    fi
}
need_cmd jobworkerp-client
need_cmd python3

case "$SOURCE" in
    claude-code|codex|plain) ;;
    *)
        echo "error: --source must be one of claude-code|codex|plain (got: $SOURCE)" >&2
        exit 2
        ;;
esac

if [[ -z "$MEMORIES_GRPC_URL" ]]; then
    echo "error: --memories-grpc-url is required (e.g. http://memories.example.com:9100)" >&2
    exit 2
fi
if ! [[ "$MEMORIES_GRPC_URL" =~ ^https?://[^:/]+(:[0-9]+)?(/.*)?$ ]]; then
    echo "error: --memories-grpc-url must match http(s)://host[:port][/path] (got: $MEMORIES_GRPC_URL)" >&2
    exit 2
fi

if [[ -n "$SINCE_DATE" ]] && ! [[ "$SINCE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --since-date must be YYYY-MM-DD (got: $SINCE_DATE)" >&2
    exit 2
fi

if [[ -n "$END_DATE" ]] && ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --end-date must be YYYY-MM-DD (got: $END_DATE)" >&2
    exit 2
fi

# Reject end_date < since_date early; the workflow itself does not
# re-check this (jaq date comparison is awkward), so catch the typo
# before burning a worker slot on a guaranteed-empty window.
# `<` inside `[[ ]]` is an ASCII lex compare, which coincides with
# calendar order for zero-padded YYYY-MM-DD strings.
if [[ -n "$SINCE_DATE" && -n "$END_DATE" && "$END_DATE" < "$SINCE_DATE" ]]; then
    echo "error: --end-date ($END_DATE) must be on or after --since-date ($SINCE_DATE)" >&2
    exit 2
fi

case "$SINCE_MODE" in
    day_start|now_minus) ;;
    *)
        echo "error: --since-mode must be day_start or now_minus (got: $SINCE_MODE)" >&2
        exit 2
        ;;
esac

# Resolve memories-import path. Same three forms as run-pipeline.sh.
case "$IMPORT_COMMAND_REL" in
    /*)
        IMPORT_COMMAND="$IMPORT_COMMAND_REL"
        if [[ ! -x "$IMPORT_COMMAND" ]]; then
            echo "error: memories-import binary not executable at: $IMPORT_COMMAND" >&2
            exit 1
        fi
        ;;
    */*)
        IMPORT_COMMAND="${BASE_DIR}/${IMPORT_COMMAND_REL}"
        if [[ ! -x "$IMPORT_COMMAND" ]]; then
            echo "error: memories-import binary not executable at: $IMPORT_COMMAND" >&2
            echo "       build with: (cd $BASE_DIR && cargo build --release -p agent-chat-import)" >&2
            exit 1
        fi
        ;;
    *)
        if ! IMPORT_COMMAND="$(command -v "$IMPORT_COMMAND_REL")"; then
            echo "error: '$IMPORT_COMMAND_REL' not found on PATH" >&2
            echo "       pass an absolute path or build the binary into a PATH entry." >&2
            exit 1
        fi
        ;;
esac

# Workflow YAML must exist if it's a local path.
case "$IMPORT_YAML" in
    http://*|https://*) ;;
    /*)
        if [[ ! -f "$IMPORT_YAML" ]]; then
            echo "error: IMPORT_YAML not found: $IMPORT_YAML" >&2
            exit 1
        fi
        ;;
    *)
        echo "error: IMPORT_YAML must be absolute path or http(s) URL (got: $IMPORT_YAML)" >&2
        exit 2
        ;;
esac

# ----------------------------------------------------------------
# Build workflow input JSON via python3 (safe escaping)
# ----------------------------------------------------------------
INPUT_JSON=$(python3 - <<'PY' \
    "$SOURCE" "$USER_ID" "$SINCE_DATE" "$END_DATE" "$SINCE_OVERRIDE" "$SINCE_MODE" \
    "$SINCE_LOOKBACK_SECONDS" "$TZ_HOURS" \
    "$MEMORIES_GRPC_URL" "$MEMORIES_GRPC_HOST" "$MEMORIES_GRPC_PORT" \
    "$IMPORT_COMMAND" "$ALL_FLAG" "$CLAUDE_DIR" "$CODEX_DIR" \
    "$STRIP_PATH_PREFIX" "$EXTRA_IMPORT_ARGS_CSV"
import json, sys
(_, source, user_id, since_date, end_date, since_override, since_mode,
 since_lookback, tz_hours,
 grpc_url, grpc_host, grpc_port,
 import_cmd, all_flag, claude_dir, codex_dir,
 strip_path, extra_args_csv) = sys.argv

payload = {
    "source": source,
    "user_id": int(user_id),
    "memories_grpc_url": grpc_url,
    "timezone_offset_hours": int(tz_hours),
    "import_command": import_cmd,
    "all_projects_or_sessions": all_flag == "true",
    "since_mode": since_mode,
    "since_lookback_seconds": int(since_lookback),
}
if grpc_host:
    payload["memories_grpc_host"] = grpc_host
if grpc_port:
    payload["memories_grpc_port"] = int(grpc_port)
if since_date:
    payload["since_date"] = since_date
if end_date:
    payload["end_date"] = end_date
if since_override:
    payload["since_override"] = since_override
if claude_dir:
    payload["claude_dir"] = claude_dir
if codex_dir:
    payload["codex_dir"] = codex_dir
if strip_path:
    payload["strip_path_prefix"] = strip_path
if extra_args_csv:
    payload["extra_import_args"] = [s for s in extra_args_csv.split(",") if s]

print(json.dumps(payload, ensure_ascii=False))
PY
)

# ----------------------------------------------------------------
# Compose jobworkerp-client command
# ----------------------------------------------------------------
CMD=(
    jobworkerp-client
    -a "$JOBWORKERP_ADDR"
    job enqueue-workflow
    -i "$INPUT_JSON"
    -w "$IMPORT_YAML"
    -t "$TIMEOUT_SEC"
    -c "$CHANNEL"
    --format "$FORMAT"
)

echo "============================================================" >&2
echo "Resolved environment:" >&2
echo "  JOBWORKERP_ADDR    = $JOBWORKERP_ADDR" >&2
echo "  IMPORT_YAML        = $IMPORT_YAML" >&2
echo "  IMPORT_COMMAND     = $IMPORT_COMMAND" >&2
echo "Workflow input JSON:" >&2
echo "$INPUT_JSON" | python3 -m json.tool >&2 || echo "$INPUT_JSON" >&2
echo "Command (one arg per line):" >&2
for a in "${CMD[@]}"; do
    printf '    %s\n' "$a" >&2
done
echo "============================================================" >&2

if [[ "$PRINT_ONLY" == "true" ]]; then
    echo "(--print-only) not executing." >&2
    exit 0
fi

exec "${CMD[@]}"
