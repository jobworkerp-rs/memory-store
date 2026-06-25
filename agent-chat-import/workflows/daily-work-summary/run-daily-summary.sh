#!/usr/bin/env bash
# Enqueue daily-work-summary-batch.yaml on jobworkerp via jobworkerp-client.
#
# This script wraps the `jobworkerp-client job enqueue-workflow` invocation
# so callers do not need to hand-craft the JSON input or remember the
# workflow paths. It supports three date-selection modes mirroring the
# workflow's input schema:
#
#   * --last-n-days N            : last N days ending yesterday
#   * --start-date / --end-date  : explicit inclusive range
#   * (neither)                  : single fallback day = yesterday
#
# Optional kubectl port-forward bring-up is provided for the production
# k8s deployment, matching production-import/run-import.sh's pattern.
#
# Usage:
#   agent-chat-import/workflows/daily-work-summary/run-daily-summary.sh [options]
#
# Common options:
#   --last-n-days <N>                  generate summaries for the last N days
#   --start-date <YYYY-MM-DD>          range start (inclusive)
#   --end-date <YYYY-MM-DD>            range end (inclusive)
#   --target-date <YYYY-MM-DD>         shortcut: same as --start --end <date>
#   --source-user-id <i64>             default: 100000 (the summary-agent user)
#   --extra-labels <csv>               extra AND-matched labels (default: none)
#   --summary-label <str>              default: summary
#   --daily-label <str>                default: daily_summary
#   --min-thread-count <n>             default: 1
#   --max-context-chars <n>            default: 200000
#   --summary-model <name>             default: qwen3.6:27b
#   --ollama-base-url <url>            default: http://192.168.1.2:11434
#   --output-language <ja|en>          default: MEMORY_DEFAULT_LANGUAGE or ja
#   --timezone-offset-hours <int>      default: 9 (JST)
#   --force-resummarize                flag (default off)
#
# Workflow paths:
#   --batch-yaml <path|url>            default: ./daily-work-summary-batch.yaml (this dir)
#                                      Must be readable from the jobworkerp pod;
#                                      use an absolute filesystem path or http(s) URL.
#
# jobworkerp connection:
#   --jobworkerp-addr <url>            default: env JOBWORKERP_ADDR
#                                      or http://localhost:9000
#   --memories-grpc-host <host>        default: localhost
#   --memories-grpc-port <port>        default: 9100
#   --timeout-sec <sec>                default: 86400 (24h, matches workflow timeout)
#   --channel <name>                   default: workflow_base
#   --format <table|card|json>         default: card
#
# Optional k8s port-forward (off by default):
#   --port-forward                     enable port-forward (memories + jobworkerp)
#   --jobworkerp-local-port <port>     default: 19000 (used only with --port-forward)
#   --memories-local-port <port>       default: 19010 (used only with --port-forward)
#   --memories-namespace <ns>          default: env MEMORIES_NAMESPACE or memories
#   --memories-service <name>          default: env MEMORIES_SERVICE or memories
#   --memories-port <port>             default: env MEMORIES_PORT or 9000
#   --jobworker-namespace <ns>         default: env JOBWORKER_NAMESPACE or jobworker
#   --jobworker-service <name>         default: env JOBWORKER_SERVICE or jobworker-front-service
#   --jobworker-port <port>            default: env JOBWORKER_PORT or 9000
#
# Misc:
#   --print-only                       resolve env + show command, do not exec
#   -h | --help                        show this header
#
# Exit codes match jobworkerp-client (0 on success).

set -euo pipefail

# ----------------------------------------------------------------
# Defaults
# ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LAST_N_DAYS=""
START_DATE=""
END_DATE=""

SOURCE_USER_ID="100000"
EXTRA_LABELS_CSV=""
SUMMARY_LABEL="summary"
DAILY_LABEL="daily_summary"
MIN_THREAD_COUNT="1"
MAX_CONTEXT_CHARS="200000"
SUMMARY_MODEL="qwen3.6:27b"
OLLAMA_BASE_URL="http://192.168.1.2:11434"
OUTPUT_LANGUAGE="${MEMORY_DEFAULT_LANGUAGE:-ja}"
TIMEZONE_OFFSET_HOURS="9"
FORCE_RESUMMARIZE="false"

BATCH_YAML="${SCRIPT_DIR}/daily-work-summary-batch.yaml"

JOBWORKERP_ADDR_DEFAULT="${JOBWORKERP_ADDR:-http://localhost:9000}"
JOBWORKERP_ADDR="${JOBWORKERP_ADDR_DEFAULT}"
MEMORIES_GRPC_HOST="localhost"
MEMORIES_GRPC_PORT="9100"
TIMEOUT_SEC="86400"
CHANNEL="workflow_base"
FORMAT="card"

PORT_FORWARD="false"
JOBWORKERP_LOCAL_PORT="19000"
MEMORIES_LOCAL_PORT="19010"
MEMORIES_NAMESPACE="${MEMORIES_NAMESPACE:-memories}"
MEMORIES_SERVICE="${MEMORIES_SERVICE:-memories}"
MEMORIES_PORT="${MEMORIES_PORT:-9000}"
JOBWORKER_NAMESPACE="${JOBWORKER_NAMESPACE:-jobworker}"
JOBWORKER_SERVICE="${JOBWORKER_SERVICE:-jobworker-front-service}"
JOBWORKER_PORT="${JOBWORKER_PORT:-9000}"

PRINT_ONLY="false"

# ----------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------
print_help() {
    sed -n '2,80p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --last-n-days)              LAST_N_DAYS="$2"; shift 2 ;;
        --start-date)               START_DATE="$2"; shift 2 ;;
        --end-date)                 END_DATE="$2"; shift 2 ;;
        --target-date)              START_DATE="$2"; END_DATE="$2"; shift 2 ;;
        --source-user-id)           SOURCE_USER_ID="$2"; shift 2 ;;
        --extra-labels)             EXTRA_LABELS_CSV="$2"; shift 2 ;;
        --summary-label)            SUMMARY_LABEL="$2"; shift 2 ;;
        --daily-label)              DAILY_LABEL="$2"; shift 2 ;;
        --min-thread-count)         MIN_THREAD_COUNT="$2"; shift 2 ;;
        --max-context-chars)        MAX_CONTEXT_CHARS="$2"; shift 2 ;;
        --summary-model)            SUMMARY_MODEL="$2"; shift 2 ;;
        --ollama-base-url)          OLLAMA_BASE_URL="$2"; shift 2 ;;
        --output-language)          OUTPUT_LANGUAGE="$2"; shift 2 ;;
        --timezone-offset-hours)    TIMEZONE_OFFSET_HOURS="$2"; shift 2 ;;
        --force-resummarize)        FORCE_RESUMMARIZE="true"; shift 1 ;;
        --batch-yaml)               BATCH_YAML="$2"; shift 2 ;;
        --jobworkerp-addr)          JOBWORKERP_ADDR="$2"; shift 2 ;;
        --memories-grpc-host)       MEMORIES_GRPC_HOST="$2"; shift 2 ;;
        --memories-grpc-port)       MEMORIES_GRPC_PORT="$2"; shift 2 ;;
        --timeout-sec)              TIMEOUT_SEC="$2"; shift 2 ;;
        --channel)                  CHANNEL="$2"; shift 2 ;;
        --format)                   FORMAT="$2"; shift 2 ;;
        --port-forward)             PORT_FORWARD="true"; shift 1 ;;
        --jobworkerp-local-port)    JOBWORKERP_LOCAL_PORT="$2"; shift 2 ;;
        --memories-local-port)      MEMORIES_LOCAL_PORT="$2"; shift 2 ;;
        --memories-namespace)       MEMORIES_NAMESPACE="$2"; shift 2 ;;
        --memories-service)         MEMORIES_SERVICE="$2"; shift 2 ;;
        --memories-port)            MEMORIES_PORT="$2"; shift 2 ;;
        --jobworker-namespace)      JOBWORKER_NAMESPACE="$2"; shift 2 ;;
        --jobworker-service)        JOBWORKER_SERVICE="$2"; shift 2 ;;
        --jobworker-port)           JOBWORKER_PORT="$2"; shift 2 ;;
        --print-only)               PRINT_ONLY="true"; shift 1 ;;
        -h|--help)                  print_help; exit 0 ;;
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
need_cmd python3   # used to assemble the JSON input safely

# Validate date selection: at most one of (last_n_days, range) — both
# are accepted by the workflow but the batch picks the range when both
# are set; we surface a warning instead of silently dropping last-n.
if [[ -n "$LAST_N_DAYS" && ( -n "$START_DATE" || -n "$END_DATE" ) ]]; then
    echo "warn: --last-n-days is ignored when --start-date/--end-date are set (workflow precedence)" >&2
fi
if [[ -n "$START_DATE" && -z "$END_DATE" ]] || [[ -z "$START_DATE" && -n "$END_DATE" ]]; then
    echo "error: --start-date and --end-date must be specified together (use --target-date for a single day)" >&2
    exit 2
fi

date_re='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
for d in "$START_DATE" "$END_DATE"; do
    if [[ -n "$d" ]] && ! [[ "$d" =~ $date_re ]]; then
        echo "error: date must match YYYY-MM-DD (got: $d)" >&2
        exit 2
    fi
done

case "$OUTPUT_LANGUAGE" in
    ja|en) ;;
    *)
        echo "error: --output-language must be one of ja|en (got: $OUTPUT_LANGUAGE)" >&2
        exit 2
        ;;
esac

# Workflow paths: must be either a path the jobworkerp pod can read
# (absolute is safest — see thread-summary README warning) or http(s).
for label in BATCH_YAML; do
    val="${!label}"
    case "$val" in
        http://*|https://*) ;;
        *)
            if [[ ! -f "$val" ]]; then
                echo "error: $label not found locally: $val" >&2
                echo "       pass an http(s) URL or an absolute path readable by jobworkerp." >&2
                exit 1
            fi
            ;;
    esac
done

# ----------------------------------------------------------------
# Optional port-forward bring-up
# ----------------------------------------------------------------
PF_PIDS=()
cleanup() {
    local rc=$?
    if (( ${#PF_PIDS[@]} > 0 )); then
        echo "→ stopping port-forwards (pids: ${PF_PIDS[*]})" >&2
        for pid in "${PF_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
        wait 2>/dev/null || true
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

wait_port() {
    local port="$1" label="$2" timeout="${3:-30}" i=0
    while (( i < timeout )); do
        if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            exec 3<&-; exec 3>&-
            return 0
        fi
        sleep 1; i=$((i + 1))
    done
    echo "error: timed out waiting for ${label} on localhost:${port}" >&2
    return 1
}

start_pf() {
    local ns="$1" target="$2" local_port="$3" remote_port="$4" label="$5"
    echo "→ port-forward ${label}: ${ns}/${target} ${local_port}:${remote_port}" >&2
    kubectl -n "$ns" port-forward "$target" "${local_port}:${remote_port}" >/dev/null 2>&1 &
    PF_PIDS+=("$!")
    wait_port "$local_port" "$label"
}

if [[ "$PORT_FORWARD" == "true" ]]; then
    need_cmd kubectl
    start_pf "$MEMORIES_NAMESPACE" "svc/${MEMORIES_SERVICE}" "$MEMORIES_LOCAL_PORT" "$MEMORIES_PORT" "memories"
    start_pf "$JOBWORKER_NAMESPACE" "svc/${JOBWORKER_SERVICE}" "$JOBWORKERP_LOCAL_PORT" "$JOBWORKER_PORT" "jobworkerp"
    JOBWORKERP_ADDR="http://127.0.0.1:${JOBWORKERP_LOCAL_PORT}"
    MEMORIES_GRPC_HOST="127.0.0.1"
    MEMORIES_GRPC_PORT="${MEMORIES_LOCAL_PORT}"
fi

# ----------------------------------------------------------------
# Build workflow input JSON
# ----------------------------------------------------------------
# Built via python3 to avoid hand-rolled escaping. Optional fields
# (start_date / end_date / last_n_days / extra_labels) are omitted when
# empty so the workflow's defaults / mode-selection logic kicks in.
INPUT_JSON=$(python3 - <<'PY' "$SOURCE_USER_ID" "$MEMORIES_GRPC_HOST" "$MEMORIES_GRPC_PORT" "$START_DATE" "$END_DATE" "$LAST_N_DAYS" "$TIMEZONE_OFFSET_HOURS" "$SUMMARY_LABEL" "$DAILY_LABEL" "$EXTRA_LABELS_CSV" "$MIN_THREAD_COUNT" "$MAX_CONTEXT_CHARS" "$SUMMARY_MODEL" "$OLLAMA_BASE_URL" "$OUTPUT_LANGUAGE" "$FORCE_RESUMMARIZE"
import json, sys
(_, source_user_id, host, port, start_date, end_date, last_n,
 tz, summary_label, daily_label, extra_csv, min_thread, max_chars,
 model, ollama, output_language, force) = sys.argv

payload = {
    "source_user_id": int(source_user_id),
    "memories_grpc_host": host,
    "memories_grpc_port": int(port),
    "summary_label": summary_label,
    "daily_label": daily_label,
    "min_thread_count": int(min_thread),
    "max_context_chars": int(max_chars),
    "summary_model": model,
    "ollama_base_url": ollama,
    "output_language": output_language,
    "timezone_offset_hours": int(tz),
    "force_resummarize": force == "true",
}
if start_date and end_date:
    payload["start_date"] = start_date
    payload["end_date"] = end_date
if last_n and not (start_date and end_date):
    payload["last_n_days"] = int(last_n)
if extra_csv:
    payload["extra_labels_filter"] = [s.strip() for s in extra_csv.split(",") if s.strip()]
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
    -w "$BATCH_YAML"
    -t "$TIMEOUT_SEC"
    -c "$CHANNEL"
    --format "$FORMAT"
)

# ----------------------------------------------------------------
# Print resolved env + command
# ----------------------------------------------------------------
echo "============================================================" >&2
echo "Resolved environment:" >&2
echo "  JOBWORKERP_ADDR    = $JOBWORKERP_ADDR" >&2
echo "  BATCH_YAML         = $BATCH_YAML" >&2
echo "  MEMORIES_GRPC      = ${MEMORIES_GRPC_HOST}:${MEMORIES_GRPC_PORT}" >&2
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
