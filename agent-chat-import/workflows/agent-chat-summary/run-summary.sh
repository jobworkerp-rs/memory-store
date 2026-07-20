#!/usr/bin/env bash
# Enqueue agent-chat-summary.yaml on jobworkerp via jobworkerp-client.
#
# Focused summary-only counterpart of
# agent-chat-import/workflows/agent-chat-pipeline/run-pipeline.sh. Use it when summary
# runs on a different cron (or different host) from import — the
# split-execution use case.
#
# This script must know the processing date window. You can supply it
# in either of two ways:
#   1. Explicitly: --since-date / --end-date / --range-start-ms /
#      --since-ms-utc (all four). Use these when consuming the output
#      of agent-chat-import to keep the windows identical byte-for-byte.
#   2. Lazily: --since-date alone (+ optional --end-date). The script
#      derives range_start_ms and since_ms_utc from since_date in the
#      configured tz. End_date defaults to today (in tz). This is
#      convenient for manual back-fills.
#
# Usage:
#   agent-chat-import/workflows/agent-chat-summary/run-summary.sh [options]
#
# Required:
#   --memories-grpc-url <url>
#   --user-id <i64>                    owner of the source threads
#   --since-date <YYYY-MM-DD>          (or all four window flags)
#
# Window flags (override calculated values):
#   --end-date <YYYY-MM-DD>            default: today in tz
#   --range-start-ms <epoch ms>        default: derived from since-date
#   --since-ms-utc <epoch ms>          default: 0 (day_start fallback)
#   --tz <hours>                       default: 9
#
# Endpoint override:
#   --memories-grpc-host <host>
#   --memories-grpc-port <port>
#
# Workflow URLs (default = sibling files via absolute path):
#   --summary-yaml <path|url>          default: sibling agent-chat-summary.yaml
#   --thread-summary-batch-yaml  <path|url>
#   --daily-work-summary-batch-yaml  <path|url>
#
# Per-stage single workflows are no longer passed in: every stage fans
# out to language workers (memories-<feature>-single-<lang>) registered
# via `memories-import upsert-generation-workers`. Only the *-batch-yaml
# paths are forwarded.
#
# Summary settings:
#   --summary-model <name>             default: qwen3.6:27b
#   --ollama-base-url <url>            default: http://localhost:11434
#   --label-prefix <str>               default: summary
#   --daily-label <str>                default: daily_summary
#   --extra-labels-filter <csv>        scope filter for daily summary
#   --force-resummarize                flag
#   --min-thread-count <n>             default: 1
#   --max-context-chars <n>            default: 200000
#
# Reflection stage (opt-in; off by default):
#   --enable-reflection                run thread-reflection-batch
#                                      using sibling defaults
#   --thread-reflection-batch-yaml  <path|url>
#   --reflector-model <name>           default: "" (= reuse summary_model)
#   --reflector-base-url <url>         default: "" (= reuse ollama_base_url)
#   --prompt-version <ver>             default: v1
#   --output-language <ja|en>           default: MEMORY_DEFAULT_LANGUAGE or ja
#   --reflector-id <id>                default: self
#   --reflection-force                 flag
#
# Personality stage (opt-in; off by default):
#   --enable-personality
#   --no-user-merge                    skip layer-2 merge
#   --thread-personality-batch-yaml  <path|url>
#   --user-personality-merge-yaml    <path|url>
#   --personality-model <name>         default: ""
#   --min-user-messages <n>            default: 2
#   --force-reextract                  flag
#   --force-remerge                    flag
#   --max-signals <n>                  default: 200
#
# jobworkerp connection:
#   --jobworkerp-addr <url>            default: env JOBWORKERP_ADDR
#                                      or http://localhost:9000
#   --timeout-sec <sec>                default: 86400 (= 24 h)
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

# ----------------------------------------------------------------
# Defaults
# ----------------------------------------------------------------
USER_ID=""
SINCE_DATE=""
END_DATE=""
RANGE_START_MS=""
SINCE_MS_UTC=""
TZ_HOURS="9"
MEMORIES_GRPC_URL=""
MEMORIES_GRPC_HOST=""
MEMORIES_GRPC_PORT=""

SUMMARY_YAML="${SCRIPT_DIR}/agent-chat-summary.yaml"
THREAD_SUMMARY_BATCH_YAML="${WORKFLOWS_DIR}/thread-summary/thread-summary-batch.yaml"
DAILY_WORK_SUMMARY_BATCH_YAML="${WORKFLOWS_DIR}/daily-work-summary/daily-work-summary-batch.yaml"

SUMMARY_MODEL="qwen3.6:27b"
OLLAMA_BASE_URL="http://localhost:11434"
LABEL_PREFIX="summary"
DAILY_LABEL="daily_summary"
EXTRA_LABELS_FILTER_CSV=""
FORCE_RESUMMARIZE="false"
MIN_THREAD_COUNT="1"
MAX_CONTEXT_CHARS="200000"

# Reflection
ENABLE_REFLECTION="false"
THREAD_REFLECTION_BATCH_YAML="${WORKFLOWS_DIR}/thread-reflection/thread-reflection-batch.yaml"
REFLECTOR_MODEL=""
REFLECTOR_BASE_URL=""
PROMPT_VERSION="v1"
OUTPUT_LANGUAGE="${MEMORY_DEFAULT_LANGUAGE:-ja}"
REFLECTOR_ID="self"
REFLECTION_FORCE="false"

# Personality (same opt-in pattern as run-pipeline.sh)
ENABLE_PERSONALITY="false"
ENABLE_USER_MERGE_EXPLICIT_OFF="false"
THREAD_PERSONALITY_BATCH_YAML="${WORKFLOWS_DIR}/personality/thread-personality-batch.yaml"
USER_PERSONALITY_MERGE_YAML="${WORKERS_DIR}/personality/user-personality-merge.yaml"
USER_PERSONALITY_MERGE_YAML_EXPLICIT="false"
PERSONALITY_MODEL=""
MIN_USER_MESSAGES="2"
FORCE_REEXTRACT="false"
FORCE_REMERGE="false"
MAX_SIGNALS="200"

JOBWORKERP_ADDR_DEFAULT="${JOBWORKERP_ADDR:-http://localhost:9000}"
JOBWORKERP_ADDR="${JOBWORKERP_ADDR_DEFAULT}"
TIMEOUT_SEC="86400"
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
        --user-id)                         USER_ID="$2"; shift 2 ;;
        --since-date)                      SINCE_DATE="$2"; shift 2 ;;
        --end-date)                        END_DATE="$2"; shift 2 ;;
        --range-start-ms)                  RANGE_START_MS="$2"; shift 2 ;;
        --since-ms-utc)                    SINCE_MS_UTC="$2"; shift 2 ;;
        --tz)                              TZ_HOURS="$2"; shift 2 ;;
        --memories-grpc-url)               MEMORIES_GRPC_URL="$2"; shift 2 ;;
        --memories-grpc-host)              MEMORIES_GRPC_HOST="$2"; shift 2 ;;
        --memories-grpc-port)              MEMORIES_GRPC_PORT="$2"; shift 2 ;;
        --summary-yaml)                    SUMMARY_YAML="$2"; shift 2 ;;
        --thread-summary-batch-yaml)       THREAD_SUMMARY_BATCH_YAML="$2"; shift 2 ;;
        --daily-work-summary-batch-yaml)   DAILY_WORK_SUMMARY_BATCH_YAML="$2"; shift 2 ;;
        --summary-model)                   SUMMARY_MODEL="$2"; shift 2 ;;
        --ollama-base-url)                 OLLAMA_BASE_URL="$2"; shift 2 ;;
        --label-prefix)                    LABEL_PREFIX="$2"; shift 2 ;;
        --daily-label)                     DAILY_LABEL="$2"; shift 2 ;;
        --extra-labels-filter)             EXTRA_LABELS_FILTER_CSV="$2"; shift 2 ;;
        --force-resummarize)               FORCE_RESUMMARIZE="true"; shift 1 ;;
        --min-thread-count)                MIN_THREAD_COUNT="$2"; shift 2 ;;
        --max-context-chars)               MAX_CONTEXT_CHARS="$2"; shift 2 ;;
        --enable-reflection)               ENABLE_REFLECTION="true"; shift 1 ;;
        --thread-reflection-batch-yaml)
            THREAD_REFLECTION_BATCH_YAML="$2"
            ENABLE_REFLECTION="true"
            shift 2 ;;
        --reflector-model)                 REFLECTOR_MODEL="$2"; shift 2 ;;
        --reflector-base-url)              REFLECTOR_BASE_URL="$2"; shift 2 ;;
        --prompt-version)                  PROMPT_VERSION="$2"; shift 2 ;;
        --output-language)                 OUTPUT_LANGUAGE="$2"; shift 2 ;;
        --reflector-id)                    REFLECTOR_ID="$2"; shift 2 ;;
        --reflection-force)                REFLECTION_FORCE="true"; shift 1 ;;
        --enable-personality)              ENABLE_PERSONALITY="true"; shift 1 ;;
        --no-user-merge)                   ENABLE_USER_MERGE_EXPLICIT_OFF="true"; shift 1 ;;
        --thread-personality-batch-yaml)
            THREAD_PERSONALITY_BATCH_YAML="$2"
            ENABLE_PERSONALITY="true"
            shift 2 ;;
        --user-personality-merge-yaml)
            USER_PERSONALITY_MERGE_YAML="$2"
            USER_PERSONALITY_MERGE_YAML_EXPLICIT="true"
            ENABLE_PERSONALITY="true"
            shift 2 ;;
        --personality-model)               PERSONALITY_MODEL="$2"; shift 2 ;;
        --min-user-messages)               MIN_USER_MESSAGES="$2"; shift 2 ;;
        --force-reextract)                 FORCE_REEXTRACT="true"; shift 1 ;;
        --force-remerge)                   FORCE_REMERGE="true"; shift 1 ;;
        --max-signals)                     MAX_SIGNALS="$2"; shift 2 ;;
        --jobworkerp-addr)                 JOBWORKERP_ADDR="$2"; shift 2 ;;
        --timeout-sec)                     TIMEOUT_SEC="$2"; shift 2 ;;
        --channel)                         CHANNEL="$2"; shift 2 ;;
        --format)                          FORMAT="$2"; shift 2 ;;
        --print-only)                      PRINT_ONLY="true"; shift 1 ;;
        -h|--help)                         print_help; exit 0 ;;
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

if [[ -z "$USER_ID" ]]; then
    echo "error: --user-id is required" >&2
    exit 2
fi
if [[ -z "$MEMORIES_GRPC_URL" ]]; then
    echo "error: --memories-grpc-url is required" >&2
    exit 2
fi
if ! [[ "$MEMORIES_GRPC_URL" =~ ^https?://[^:/]+(:[0-9]+)?(/.*)?$ ]]; then
    echo "error: --memories-grpc-url must match http(s)://host[:port][/path]" >&2
    exit 2
fi
if [[ -z "$SINCE_DATE" ]]; then
    echo "error: --since-date is required (window start)" >&2
    exit 2
fi
if ! [[ "$SINCE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --since-date must be YYYY-MM-DD" >&2
    exit 2
fi
if [[ -n "$END_DATE" ]] && ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --end-date must be YYYY-MM-DD" >&2
    exit 2
fi

case "$OUTPUT_LANGUAGE" in
    ja|en) ;;
    *)
        echo "error: --output-language must be one of ja|en (got: $OUTPUT_LANGUAGE)" >&2
        exit 2
        ;;
esac

# Resolve user-merge gate (same idiom as run-pipeline.sh).
if [[ "$ENABLE_USER_MERGE_EXPLICIT_OFF" == "true" \
      && "$USER_PERSONALITY_MERGE_YAML_EXPLICIT" == "true" ]]; then
    echo "warning: --no-user-merge and --user-personality-merge-yaml both set; \
honouring the explicit path." >&2
    ENABLE_USER_MERGE="true"
elif [[ "$ENABLE_USER_MERGE_EXPLICIT_OFF" == "true" ]]; then
    ENABLE_USER_MERGE="false"
else
    ENABLE_USER_MERGE="true"
fi

# Path validation.
WORKFLOW_LABELS=(SUMMARY_YAML THREAD_SUMMARY_BATCH_YAML DAILY_WORK_SUMMARY_BATCH_YAML)
if [[ "$ENABLE_REFLECTION" == "true" ]]; then
    WORKFLOW_LABELS+=(THREAD_REFLECTION_BATCH_YAML)
fi
if [[ "$ENABLE_PERSONALITY" == "true" ]]; then
    WORKFLOW_LABELS+=(THREAD_PERSONALITY_BATCH_YAML)
    if [[ "$ENABLE_USER_MERGE" == "true" ]]; then
        WORKFLOW_LABELS+=(USER_PERSONALITY_MERGE_YAML)
    fi
fi
for label in "${WORKFLOW_LABELS[@]}"; do
    val="${!label}"
    case "$val" in
        http://*|https://*) ;;
        /*)
            if [[ ! -f "$val" ]]; then
                echo "error: $label not found: $val" >&2
                exit 1
            fi
            ;;
        *)
            echo "error: $label must be absolute path or http(s) URL (got: $val)" >&2
            exit 2
            ;;
    esac
done

# ----------------------------------------------------------------
# Build workflow input JSON via python3 (safe escaping).
# Auto-derive end_date / range_start_ms / since_ms_utc from since_date
# when not supplied so manual back-fills don't need to compute them.
# ----------------------------------------------------------------
INPUT_JSON=$(python3 - <<'PY' \
    "$USER_ID" "$SINCE_DATE" "$END_DATE" "$RANGE_START_MS" "$SINCE_MS_UTC" \
    "$TZ_HOURS" "$MEMORIES_GRPC_URL" "$MEMORIES_GRPC_HOST" "$MEMORIES_GRPC_PORT" \
    "$SUMMARY_MODEL" "$OLLAMA_BASE_URL" \
    "$LABEL_PREFIX" "$DAILY_LABEL" "$EXTRA_LABELS_FILTER_CSV" \
    "$FORCE_RESUMMARIZE" "$MIN_THREAD_COUNT" "$MAX_CONTEXT_CHARS" \
    "$THREAD_SUMMARY_BATCH_YAML" \
    "$DAILY_WORK_SUMMARY_BATCH_YAML" \
    "$ENABLE_REFLECTION" \
    "$THREAD_REFLECTION_BATCH_YAML" \
    "$REFLECTOR_MODEL" "$REFLECTOR_BASE_URL" "$PROMPT_VERSION" "$OUTPUT_LANGUAGE" \
    "$REFLECTOR_ID" "$REFLECTION_FORCE" \
    "$ENABLE_PERSONALITY" "$ENABLE_USER_MERGE" \
    "$THREAD_PERSONALITY_BATCH_YAML" \
    "$USER_PERSONALITY_MERGE_YAML" \
    "$PERSONALITY_MODEL" "$MIN_USER_MESSAGES" \
    "$FORCE_REEXTRACT" "$FORCE_REMERGE" "$MAX_SIGNALS"
import datetime as dt
import json, sys

(_, user_id, since_date, end_date, range_start_ms, since_ms_utc,
 tz_hours, grpc_url, grpc_host, grpc_port,
 model, ollama,
 label_prefix, daily_label, extra_filter_csv,
 force, min_thread, max_chars,
 ts_batch, dws_batch,
 enable_reflection,
 tr_batch,
 reflector_model, reflector_base_url, prompt_version, output_language,
 reflector_id, reflection_force,
 enable_personality, enable_user_merge,
 tp_batch, upm,
 personality_model, min_user_msgs,
 force_reextract, force_remerge, max_signals) = sys.argv

tz_h = int(tz_hours)
# Derive missing window values from since_date in the configured tz.
# Stay UTC-aware throughout: a naive datetime + .timestamp() picks up
# the OS-local TZ and would diverge from agent-chat-import.yaml's jq
# math (which assumes since_date 00:00 is in the operator's logical
# tz, not the host TZ).
y, m, d = (int(x) for x in since_date.split("-"))
since_dt_utc = dt.datetime(y, m, d, 0, 0, 0, tzinfo=dt.timezone.utc) \
    - dt.timedelta(hours=tz_h)
derived_range_start_ms = int(since_dt_utc.timestamp() * 1000)

# Today in tz, for default end_date.
now_local = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=tz_h)
derived_end_date = now_local.strftime("%Y-%m-%d")

payload = {
    "user_id": int(user_id),
    "memories_grpc_url": grpc_url,
    "since_date": since_date,
    "end_date": end_date if end_date else derived_end_date,
    "range_start_ms": int(range_start_ms) if range_start_ms else derived_range_start_ms,
    "since_ms_utc": int(since_ms_utc) if since_ms_utc else 0,
    "timezone_offset_hours": tz_h,
    "thread_summary_batch_yaml": ts_batch,
    "daily_work_summary_batch_yaml": dws_batch,
    "summary_model": model,
    "ollama_base_url": ollama,
    "memory_thread_label_prefix": label_prefix,
    "daily_summary_label": daily_label,
    "output_language": output_language,
    "force_resummarize": force == "true",
    "min_thread_count": int(min_thread),
    "max_context_chars": int(max_chars),
}
if grpc_host:
    payload["memories_grpc_host"] = grpc_host
if grpc_port:
    payload["memories_grpc_port"] = int(grpc_port)
if extra_filter_csv:
    payload["extra_labels_filter"] = [s.strip() for s in extra_filter_csv.split(",") if s.strip()]

if enable_reflection == "true":
    payload["thread_reflection_batch_yaml"] = tr_batch
    if reflector_model:
        payload["reflector_model"] = reflector_model
    if reflector_base_url:
        payload["reflector_base_url"] = reflector_base_url
    payload["prompt_version"] = prompt_version
    payload["reflector_id"] = reflector_id
    payload["reflection_force"] = reflection_force == "true"

if enable_personality == "true":
    payload["thread_personality_batch_yaml"] = tp_batch
    if enable_user_merge == "true":
        payload["user_personality_merge_yaml"] = upm
    if personality_model:
        payload["personality_model"] = personality_model
    payload["min_user_messages"] = int(min_user_msgs)
    payload["force_reextract"] = force_reextract == "true"
    payload["force_remerge"] = force_remerge == "true"
    payload["max_signals"] = int(max_signals)

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
    -w "$SUMMARY_YAML"
    -t "$TIMEOUT_SEC"
    -c "$CHANNEL"
    --format "$FORMAT"
)

echo "============================================================" >&2
echo "Resolved environment:" >&2
echo "  JOBWORKERP_ADDR    = $JOBWORKERP_ADDR" >&2
echo "  SUMMARY_YAML       = $SUMMARY_YAML" >&2
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
