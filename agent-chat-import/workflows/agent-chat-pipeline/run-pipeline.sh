#!/usr/bin/env bash
# Enqueue agent-chat-pipeline.yaml on jobworkerp via jobworkerp-client.
#
# Wraps the JSON input assembly and workflow path resolution so callers
# do not need to hand-craft the payload. Designed for the local
# jobworkerp + remote memories deployment.
#
# Usage:
#   agent-chat-import/workflows/agent-chat-pipeline/run-pipeline.sh [options]
#
# Required (no defaults):
#   --memories-grpc-url <url>          memories gRPC URL (e.g.
#                                      http://memories.example.com:9010).
#                                      Used as-is for memories-import
#                                      --server-url; the workflow
#                                      parses it into host/port for
#                                      the inner WORKFLOW runner.
#
# Common options:
#   --source <claude-code|codex|plain> default: claude-code
#   --user-id <i64>                    default: 1
#   --since-date <YYYY-MM-DD>          default: yesterday (in --tz) for
#                                      since_mode=day_start, today (in --tz)
#                                      for since_mode=now_minus.
#                                      Marks the START of the processing
#                                      window — import and daily-summary
#                                      cover [since-date, today]
#                                      inclusive. A bare daily cron run
#                                      (no --since-date) collapses to a
#                                      single-day window so existing
#                                      behaviour is preserved.
#   --end-date <YYYY-MM-DD>            default: auto-derived by
#                                      agent-chat-import (today in --tz
#                                      when --since-date is set, else
#                                      yesterday/today per since_mode).
#                                      Marks the END (inclusive) of the
#                                      processing window. Pass an
#                                      explicit value to back-fill a
#                                      fixed historical range (e.g.
#                                      --since-date 2026-04-01
#                                      --end-date 2026-04-30) without
#                                      pulling today's in-progress
#                                      summary into the window. Must
#                                      be on/after --since-date.
#   --tz <hours>                       default: 9
#   --memories-grpc-host <host>        override host parsed from URL
#                                      (rare; only when WORKFLOW runner
#                                      must reach memories via a
#                                      different name than the URL)
#   --memories-grpc-port <port>        override port parsed from URL
#   --import-command <path>            default: target/release/memories-import
#                                      Resolution rules:
#                                        absolute path (/...) → used as-is
#                                        contains a slash     → treated as
#                                          relative to --base-dir
#                                        no slash             → resolved
#                                          via PATH (`command -v`)
#   --base-dir <path>                  default: this script's repo root
#   --no-all                           do NOT pass --all-projects/--all-sessions
#   --claude-dir <path>                memories-import --claude-dir
#   --codex-dir <path>                 memories-import --codex-dir
#   --strip-path-prefix <csv>          memories-import -P
#   --extra-import-args <csv>          extra args appended verbatim
#                                      (split on commas, no quoting)
#
# Summary settings:
#   --summary-user-id <i64>            default: 100000
#   --summary-model <name>             default: qwen3.6:27b
#   --ollama-base-url <url>            default: http://localhost:11434
#   --label-prefix <str>               default: summary
#   --daily-label <str>                default: daily_summary
#   --extra-labels-filter <csv>        scope filter for daily summary
#   --force-resummarize                flag (default off)
#   --min-thread-count <n>             default: 1
#   --max-context-chars <n>            default: 200000
#
# Workflow URLs (default = sibling files via absolute path):
#   --pipeline-yaml <path|url>
#   --thread-summary-batch-yaml  <path|url>
#   --agent-chat-import-yaml  <path|url>   # child workflow run by the
#   --agent-chat-summary-yaml <path|url>   # 1.1.0+ thin wrapper.
#                                            Defaults to sibling files.
#
# Per-stage single workflows are no longer passed in: every stage fans
# out to language workers (memories-<feature>-single-<lang>) registered
# via `memories-import upsert-generation-workers`. Only the *-batch-yaml
# paths below are forwarded.
#
# Daily-work-summary stage (opt-in; off by default since 2.0.0):
#   --enable-daily-summary             run the daily-work-summary stage
#                                      using sibling-file defaults.
#   --daily-work-summary-batch-yaml  <path|url>
#                                      passing this flag also turns
#                                      the daily-summary stage on (so
#                                      `--enable-daily-summary` is only
#                                      needed when you want all-defaults
#                                      with no path overrides).
#
# Reflection stage (opt-in; off by default):
#   --enable-reflection                run thread-reflection-batch at
#                                      the end of summaryBranch using
#                                      sibling defaults. Reflection
#                                      failure is FATAL (no try/catch),
#                                      unlike personality which is
#                                      non-fatal.
#   --thread-reflection-batch-yaml  <path|url>
#                                      defaults to the workflow file under
#                                      agent-chat-import/.
#                                      Passing this flag also turns
#                                      reflection on.
#   --reflector-model <name>           default: "" (= reuse summary_model)
#   --reflector-base-url <url>         default: "" (= reuse ollama_base_url)
#   --prompt-version <ver>             default: v1. Bump when changing
#                                      the reflector prompt to keep
#                                      experiment cohorts distinct.
#   --output-language <ja|en>           default: MEMORY_DEFAULT_LANGUAGE or ja
#   --reflector-id <id>                default: self
#   --reflection-force                 flag (default off). Forwarded as
#                                      `force` to thread-reflection-batch,
#                                      kept separate from
#                                      --force-resummarize / --force-reextract.
#   Reflection tuning knobs (context_limit_tokens, window_size_turns,
#   etc.) are NOT exposed as flags; if you need to tune them, enqueue
#   thread-reflection-batch.yaml directly.
#
# Personality stage (opt-in; off by default):
#   --enable-personality               run thread-personality-batch and
#                                      user-personality-merge in parallel
#                                      with the summary stages, using the
#                                      sibling-file defaults below.
#   --thread-personality-batch-yaml  <path|url>
#   --user-personality-merge-yaml    <path|url>
#                                      defaults to worker/workflow files under
#                                      agent-chat-import/. Passing
#                                      either flag also turns
#                                      the personality stage on (so
#                                      `--enable-personality` is only
#                                      needed when you want all-defaults
#                                      with no path overrides).
#   --no-user-merge                    when personality is enabled, skip
#                                      the layer-2 merge (run only the
#                                      per-thread layer). Ignored when
#                                      --user-personality-merge-yaml is
#                                      explicitly set.
#   --personality-user-id <i64>        default: 200000
#   --personality-model <name>         default: "" (= reuse summary_model)
#   --min-user-messages <n>            default: 2
#   --force-reextract                  flag (default off)
#   --force-remerge                    flag (default off)
#   --max-signals <n>                  default: 200
#
# jobworkerp connection:
#   --jobworkerp-addr <url>            default: env JOBWORKERP_ADDR
#                                      or http://localhost:9000
#   --timeout-sec <sec>                default: 93600 (= 26 h, matches the
#                                      wrapper YAML's `timeout: 26h` which
#                                      sums import (2 h) + summary (24 h))
#   --channel <name>                   default: workflow_base
#   --format <table|card|json>         default: card
#
# Misc:
#   --print-only                       resolve env + show command, do not exec
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
TZ_HOURS="9"
MEMORIES_GRPC_URL=""
MEMORIES_GRPC_HOST=""   # optional override (workflow parses URL when empty)
MEMORIES_GRPC_PORT=""   # optional override (workflow parses URL when empty)

BASE_DIR="${BASE_DIR_DEFAULT}"
IMPORT_COMMAND_REL="target/release/memories-import"
ALL_FLAG="true"
CLAUDE_DIR=""
CODEX_DIR=""
STRIP_PATH_PREFIX=""
EXTRA_IMPORT_ARGS_CSV=""

SUMMARY_USER_ID="100000"
SUMMARY_MODEL="qwen3.6:27b"
OLLAMA_BASE_URL="http://localhost:11434"
LABEL_PREFIX="summary"
DAILY_LABEL="daily_summary"
EXTRA_LABELS_FILTER_CSV=""
FORCE_RESUMMARIZE="false"
MIN_THREAD_COUNT="1"
MAX_CONTEXT_CHARS="200000"

PIPELINE_YAML="${SCRIPT_DIR}/agent-chat-pipeline.yaml"
THREAD_SUMMARY_BATCH_YAML="${WORKFLOWS_DIR}/thread-summary/thread-summary-batch.yaml"

# Daily-work-summary stage is opt-in since 2.0.0. Same idiom as
# personality/reflection: the path defaults to the sibling file, but the
# stage runs only when ENABLE_DAILY_SUMMARY is "true". Passing
# --daily-work-summary-batch-yaml explicitly turns it on as a convenience.
ENABLE_DAILY_SUMMARY="false"
DAILY_WORK_SUMMARY_BATCH_YAML="${WORKFLOWS_DIR}/daily-work-summary/daily-work-summary-batch.yaml"

# Personality stage is opt-in. Set any of the three YAML path flags
# (or `--enable-personality` to use sibling defaults) to turn it on;
# without any of those, none of the personality keys are sent so
# existing pipelines run byte-for-byte unchanged.
#
# `*_EXPLICIT` tracks whether the user passed the flag explicitly,
# so we can both:
#   1. Auto-enable the personality branch when any path flag was set
#      (saves users from the previous footgun where passing
#      `--thread-personality-batch-yaml` without `--enable-personality`
#      silently dropped the personality stage).
#   2. Distinguish "user-merge YAML was explicitly set to a path" from
#      "user-merge YAML defaulted to the sibling file" — only the
#      latter should be automatically suppressed by `--no-user-merge`.
ENABLE_PERSONALITY="false"
ENABLE_USER_MERGE_EXPLICIT_OFF="false"
THREAD_PERSONALITY_BATCH_YAML="${WORKFLOWS_DIR}/personality/thread-personality-batch.yaml"
USER_PERSONALITY_MERGE_YAML="${WORKERS_DIR}/personality/user-personality-merge.yaml"
USER_PERSONALITY_MERGE_YAML_EXPLICIT="false"
PERSONALITY_USER_ID="200000"
PERSONALITY_MODEL=""
MIN_USER_MESSAGES="2"
FORCE_REEXTRACT="false"
FORCE_REMERGE="false"
MAX_SIGNALS="200"

# Reflection stage is opt-in. Passing the reflection batch YAML path
# flag (or `--enable-reflection`) flips it on. Reflection failure is
# fatal: unlike personality, it is wired without branch-level try/catch
# in agent-chat-summary.yaml.
ENABLE_REFLECTION="false"
THREAD_REFLECTION_BATCH_YAML="${WORKFLOWS_DIR}/thread-reflection/thread-reflection-batch.yaml"
REFLECTOR_MODEL=""
REFLECTOR_BASE_URL=""
PROMPT_VERSION="v1"
OUTPUT_LANGUAGE="${MEMORY_DEFAULT_LANGUAGE:-ja}"
REFLECTOR_ID="self"
REFLECTION_FORCE="false"

# Child workflow paths consumed by agent-chat-pipeline.yaml 1.1.0+.
# REQUIRED by its input schema; these defaults make the wrapper
# self-contained for sibling-file deployments.
AGENT_CHAT_IMPORT_YAML="${WORKFLOWS_DIR}/agent-chat-import/agent-chat-import.yaml"
AGENT_CHAT_SUMMARY_YAML="${WORKFLOWS_DIR}/agent-chat-summary/agent-chat-summary.yaml"

JOBWORKERP_ADDR_DEFAULT="${JOBWORKERP_ADDR:-http://localhost:9000}"
JOBWORKERP_ADDR="${JOBWORKERP_ADDR_DEFAULT}"
TIMEOUT_SEC="93600"
CHANNEL="workflow_base"
FORMAT="card"

PRINT_ONLY="false"

# ----------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------
print_help() {
    # Anchor to the `# ---END-HELP---` sentinel so future help-block
    # additions don't silently truncate at a hard-coded line number.
    sed -n '2,/^# ---END-HELP---/p' "$0" \
        | sed -e '$d' -e 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)                          SOURCE="$2"; shift 2 ;;
        --user-id)                         USER_ID="$2"; shift 2 ;;
        --since-date)                      SINCE_DATE="$2"; shift 2 ;;
        --end-date)                        END_DATE="$2"; shift 2 ;;
        --target-date)
            echo "error: --target-date is removed; use --since-date." >&2
            echo "       --since-date marks the START of the window;" >&2
            echo "       the pipeline now processes [since-date, today]." >&2
            exit 2 ;;
        --tz)                              TZ_HOURS="$2"; shift 2 ;;
        --memories-grpc-url)               MEMORIES_GRPC_URL="$2"; shift 2 ;;
        --memories-grpc-host)              MEMORIES_GRPC_HOST="$2"; shift 2 ;;
        --memories-grpc-port)              MEMORIES_GRPC_PORT="$2"; shift 2 ;;
        --base-dir)                        BASE_DIR="$2"; shift 2 ;;
        --import-command)                  IMPORT_COMMAND_REL="$2"; shift 2 ;;
        --no-all)                          ALL_FLAG="false"; shift 1 ;;
        --claude-dir)                      CLAUDE_DIR="$2"; shift 2 ;;
        --codex-dir)                       CODEX_DIR="$2"; shift 2 ;;
        --strip-path-prefix)               STRIP_PATH_PREFIX="$2"; shift 2 ;;
        --extra-import-args)               EXTRA_IMPORT_ARGS_CSV="$2"; shift 2 ;;
        --summary-user-id)                 SUMMARY_USER_ID="$2"; shift 2 ;;
        --summary-model)                   SUMMARY_MODEL="$2"; shift 2 ;;
        --ollama-base-url)                 OLLAMA_BASE_URL="$2"; shift 2 ;;
        --label-prefix)                    LABEL_PREFIX="$2"; shift 2 ;;
        --daily-label)                     DAILY_LABEL="$2"; shift 2 ;;
        --extra-labels-filter)             EXTRA_LABELS_FILTER_CSV="$2"; shift 2 ;;
        --force-resummarize)               FORCE_RESUMMARIZE="true"; shift 1 ;;
        --min-thread-count)                MIN_THREAD_COUNT="$2"; shift 2 ;;
        --max-context-chars)               MAX_CONTEXT_CHARS="$2"; shift 2 ;;
        --pipeline-yaml)                   PIPELINE_YAML="$2"; shift 2 ;;
        --thread-summary-batch-yaml)       THREAD_SUMMARY_BATCH_YAML="$2"; shift 2 ;;
        --enable-daily-summary)            ENABLE_DAILY_SUMMARY="true"; shift 1 ;;
        --daily-work-summary-batch-yaml)
            DAILY_WORK_SUMMARY_BATCH_YAML="$2"
            ENABLE_DAILY_SUMMARY="true"
            shift 2 ;;
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
        --personality-user-id)             PERSONALITY_USER_ID="$2"; shift 2 ;;
        --personality-model)               PERSONALITY_MODEL="$2"; shift 2 ;;
        --min-user-messages)               MIN_USER_MESSAGES="$2"; shift 2 ;;
        --force-reextract)                 FORCE_REEXTRACT="true"; shift 1 ;;
        --force-remerge)                   FORCE_REMERGE="true"; shift 1 ;;
        --max-signals)                     MAX_SIGNALS="$2"; shift 2 ;;
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
        --agent-chat-import-yaml)          AGENT_CHAT_IMPORT_YAML="$2"; shift 2 ;;
        --agent-chat-summary-yaml)         AGENT_CHAT_SUMMARY_YAML="$2"; shift 2 ;;
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
need_cmd python3   # safe JSON assembly

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
# Sanity-check that the URL parses host/port — otherwise the workflow's
# capture() will fail at runtime with a less obvious error message.
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

case "$OUTPUT_LANGUAGE" in
    ja|en) ;;
    *)
        echo "error: --output-language must be one of ja|en (got: $OUTPUT_LANGUAGE)" >&2
        exit 2
        ;;
esac

# Reject end_date < since_date early — the workflow itself does not
# re-check this (jaq date comparison is awkward), so catch the typo
# here before we burn a worker slot on a guaranteed-empty window.
# `<` inside `[[ ]]` is an ASCII lex compare, which coincides with
# calendar order for zero-padded YYYY-MM-DD strings.
if [[ -n "$SINCE_DATE" && -n "$END_DATE" && "$END_DATE" < "$SINCE_DATE" ]]; then
    echo "error: --end-date ($END_DATE) must be on or after --since-date ($SINCE_DATE)" >&2
    exit 2
fi

# Resolve memories-import path. Three forms:
#   /abs/path           → use as-is (must be executable)
#   dir/bin or ./bin    → relative to BASE_DIR (must be executable)
#   bare name (no `/`)  → resolved via PATH (`command -v`)
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
        # Bare name → resolve via PATH so the workflow's COMMAND runner
        # also relies on PATH lookup at execution time.
        if ! IMPORT_COMMAND="$(command -v "$IMPORT_COMMAND_REL")"; then
            echo "error: '$IMPORT_COMMAND_REL' not found on PATH" >&2
            echo "       pass an absolute path or build the binary into a PATH entry." >&2
            exit 1
        fi
        ;;
esac

# Resolve the user-merge gate. Path-set wins over `--no-user-merge`
# (the explicit path signals stronger intent than the negative flag),
# but warn so the operator notices the conflict.
if [[ "$ENABLE_USER_MERGE_EXPLICIT_OFF" == "true" \
      && "$USER_PERSONALITY_MERGE_YAML_EXPLICIT" == "true" ]]; then
    echo "warning: both --no-user-merge and --user-personality-merge-yaml were given; \
honouring the explicit path and running the layer-2 merge." >&2
    ENABLE_USER_MERGE="true"
elif [[ "$ENABLE_USER_MERGE_EXPLICIT_OFF" == "true" ]]; then
    ENABLE_USER_MERGE="false"
else
    ENABLE_USER_MERGE="true"
fi

# Workflow YAML files: must be absolute path or http(s) URL (jobworkerp
# pod / WORKFLOW runner reads them itself).
WORKFLOW_LABELS=(PIPELINE_YAML AGENT_CHAT_IMPORT_YAML AGENT_CHAT_SUMMARY_YAML THREAD_SUMMARY_BATCH_YAML)
if [[ "$ENABLE_DAILY_SUMMARY" == "true" ]]; then
    WORKFLOW_LABELS+=(DAILY_WORK_SUMMARY_BATCH_YAML)
fi
if [[ "$ENABLE_PERSONALITY" == "true" ]]; then
    WORKFLOW_LABELS+=(THREAD_PERSONALITY_BATCH_YAML)
    if [[ "$ENABLE_USER_MERGE" == "true" ]]; then
        WORKFLOW_LABELS+=(USER_PERSONALITY_MERGE_YAML)
    fi
fi
if [[ "$ENABLE_REFLECTION" == "true" ]]; then
    WORKFLOW_LABELS+=(THREAD_REFLECTION_BATCH_YAML)
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
# Build workflow input JSON via python3 (safe escaping)
# ----------------------------------------------------------------
INPUT_JSON=$(python3 - <<'PY' \
    "$SOURCE" "$USER_ID" "$SINCE_DATE" "$END_DATE" "$TZ_HOURS" \
    "$MEMORIES_GRPC_URL" "$MEMORIES_GRPC_HOST" "$MEMORIES_GRPC_PORT" \
    "$IMPORT_COMMAND" "$ALL_FLAG" "$CLAUDE_DIR" "$CODEX_DIR" \
    "$STRIP_PATH_PREFIX" "$EXTRA_IMPORT_ARGS_CSV" \
    "$SUMMARY_USER_ID" "$SUMMARY_MODEL" "$OLLAMA_BASE_URL" \
    "$LABEL_PREFIX" "$DAILY_LABEL" "$EXTRA_LABELS_FILTER_CSV" \
    "$FORCE_RESUMMARIZE" "$MIN_THREAD_COUNT" "$MAX_CONTEXT_CHARS" \
    "$PIPELINE_YAML" "$THREAD_SUMMARY_BATCH_YAML" \
    "$ENABLE_DAILY_SUMMARY" \
    "$DAILY_WORK_SUMMARY_BATCH_YAML" \
    "$ENABLE_PERSONALITY" "$ENABLE_USER_MERGE" \
    "$THREAD_PERSONALITY_BATCH_YAML" \
    "$USER_PERSONALITY_MERGE_YAML" \
    "$PERSONALITY_USER_ID" "$PERSONALITY_MODEL" "$MIN_USER_MESSAGES" \
    "$FORCE_REEXTRACT" "$FORCE_REMERGE" "$MAX_SIGNALS" \
    "$ENABLE_REFLECTION" \
    "$THREAD_REFLECTION_BATCH_YAML" \
    "$REFLECTOR_MODEL" "$REFLECTOR_BASE_URL" "$PROMPT_VERSION" "$OUTPUT_LANGUAGE" \
    "$REFLECTOR_ID" "$REFLECTION_FORCE" \
    "$AGENT_CHAT_IMPORT_YAML" "$AGENT_CHAT_SUMMARY_YAML"
import json, sys
(_, source, user_id, since_date, end_date, tz_hours,
 grpc_url, grpc_host, grpc_port,
 import_cmd, all_flag, claude_dir, codex_dir,
 strip_path, extra_args_csv,
 summary_uid, model, ollama,
 label_prefix, daily_label, extra_filter_csv,
 force, min_thread, max_chars,
 _pipeline_yaml, ts_batch,
 enable_daily_summary,
 dws_batch,
 enable_personality, enable_user_merge,
 tp_batch, upm,
 personality_uid, personality_model, min_user_msgs,
 force_reextract, force_remerge, max_signals,
 enable_reflection,
 tr_batch,
 reflector_model, reflector_base_url, prompt_version, output_language,
 reflector_id, reflection_force,
 ac_import_yaml, ac_summary_yaml) = sys.argv

payload = {
    "source": source,
    "user_id": int(user_id),
    "memories_grpc_url": grpc_url,
    "timezone_offset_hours": int(tz_hours),
    "import_command": import_cmd,
    "all_projects_or_sessions": all_flag == "true",
    "summary_user_id": int(summary_uid),
    "summary_model": model,
    "ollama_base_url": ollama,
    "memory_thread_label_prefix": label_prefix,
    "daily_summary_label": daily_label,
    "output_language": output_language,
    "force_resummarize": force == "true",
    "min_thread_count": int(min_thread),
    "max_context_chars": int(max_chars),
    "thread_summary_batch_yaml": ts_batch,
    # Child workflow paths consumed by the wrapper itself. Since
    # 2.0.0 the wrapper provides Gitea `main` defaults, so JSON-only
    # callers may omit these; here we always emit the resolved
    # sibling-file paths so script-driven runs are deterministic
    # (and work offline without contacting Gitea).
    "agent_chat_import_yaml": ac_import_yaml,
    "agent_chat_summary_yaml": ac_summary_yaml,
}
# Optional host/port overrides — omit when unset so the workflow
# falls back to parsing memories_grpc_url.
if grpc_host:
    payload["memories_grpc_host"] = grpc_host
if grpc_port:
    payload["memories_grpc_port"] = int(grpc_port)
if since_date:
    payload["since_date"] = since_date
if end_date:
    payload["end_date"] = end_date
if claude_dir:
    payload["claude_dir"] = claude_dir
if codex_dir:
    payload["codex_dir"] = codex_dir
if strip_path:
    payload["strip_path_prefix"] = strip_path
if extra_args_csv:
    payload["extra_import_args"] = [s for s in extra_args_csv.split(",") if s]
if extra_filter_csv:
    payload["extra_labels_filter"] = [s.strip() for s in extra_filter_csv.split(",") if s.strip()]

# Daily-work-summary stage is opt-in since 2.0.0. Only emit the YAML
# path keys when enabled — agent-chat-summary's empty-string defaults
# skip the dailyWorkSummary step when these keys are absent.
if enable_daily_summary == "true":
    payload["daily_work_summary_batch_yaml"] = dws_batch

# Personality stage is opt-in. Only emit the keys when enabled — the
# workflow's empty-string defaults disable the stage when these keys
# are absent, so omitting them keeps the existing behaviour byte-for-
# byte for callers that don't pass --enable-personality.
if enable_personality == "true":
    payload["thread_personality_batch_yaml"] = tp_batch
    if enable_user_merge == "true":
        payload["user_personality_merge_yaml"] = upm
    payload["personality_user_id"] = int(personality_uid)
    if personality_model:
        payload["personality_model"] = personality_model
    payload["min_user_messages"] = int(min_user_msgs)
    payload["force_reextract"] = force_reextract == "true"
    payload["force_remerge"] = force_remerge == "true"
    payload["max_signals"] = int(max_signals)

# Reflection stage. Same opt-in pattern as personality. Empty model /
# base_url / non-default reflector_id are omitted so the summary
# workflow's fallback chain (reflector → summary_model,
# reflector_base_url → ollama_base_url) takes over for single-model
# deployments.
if enable_reflection == "true":
    payload["thread_reflection_batch_yaml"] = tr_batch
    if reflector_model:
        payload["reflector_model"] = reflector_model
    if reflector_base_url:
        payload["reflector_base_url"] = reflector_base_url
    payload["prompt_version"] = prompt_version
    payload["reflector_id"] = reflector_id
    payload["reflection_force"] = reflection_force == "true"

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
    -w "$PIPELINE_YAML"
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
echo "  PIPELINE_YAML      = $PIPELINE_YAML" >&2
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
