#!/bin/sh
# Entry point for every binary in this image: the gRPC server (`front`,
# the default) and the operational batch jobs
# (`migrate-attachment-to-media`, `cleanup-orphan-media`). They all need
# the same CNPG→POSTGRES_URL translation below, so it must not be
# bypassed by invoking a binary directly.
#
# Usage: start.sh [BINARY [ARGS...]]   (BINARY defaults to `front`)
#
# Translate the CNPG `pg-memories-app` Secret keys (lowercase
# username/password/host/port/dbname/uri, exposed via
# `envFrom: secretRef`) into the POSTGRES_URL env var that
# infra/src/infra/resource.rs reads.
#
# We use the URL form (POSTGRES_URL) because the separate form
# (POSTGRES_HOST/PORT/USER/PASSWORD/DBNAME) does NOT actually load:
# `setup_rdb_by_env()` calls
#   envy::prefixed("POSTGRES_").from_env::<RdbConfig>()
# where RdbConfig is an untagged enum { Separate, Url }. envy cannot
# deserialize an enum from a flat env, so the call always returns Err
# and falls through to `RdbConfig::Separate(RdbConfigImpl::default())`
# which is hard-coded to `mysql:mysql@127.0.0.1:5432/default`. Confirmed
# experimentally: setting only POSTGRES_HOST/USER/... is silently
# ignored. Only POSTGRES_URL is honored (via the URL fallback path).
#
# CNPG ships a ready-made `uri` key (postgresql://user:pass@host:port/db
# with sslmode included). Prefer it; fall back to assembling from the
# individual fields when only those are present (e.g. for cnpg-spec's
# psql-test.sh-style invocation).
set -eu

# Binary to exec (default: the gRPC server). Batch jobs pass their name
# + flags, e.g. `start.sh cleanup-orphan-media --grace-sec 86400`.
_bin="${1:-front}"
[ "$#" -gt 0 ] && shift || true

# Honor an operator-supplied POSTGRES_URL first: infra::resource reads
# it directly via envy, so non-CNPG deployments (plain `docker run -e
# POSTGRES_URL=...`, sidecar pgbouncer, externally-managed Postgres) must
# not be forced through the CNPG lowercase-key path below.
if [ -n "${POSTGRES_URL:-}" ]; then
    :
elif [ -n "${uri:-}" ]; then
    export POSTGRES_URL="$uri"
else
    : "${username:?CNPG secret key 'username' missing — is pg-memories-app reflected into this namespace?}"
    : "${password:?CNPG secret key 'password' missing}"
    : "${host:?CNPG secret key 'host' missing}"
    : "${port:?CNPG secret key 'port' missing}"
    : "${dbname:?CNPG secret key 'dbname' missing}"
    # Percent-encode the userinfo before assembling the URL. Postgres
    # passwords legally contain `@ : / ? # [ ] %`, all of which break
    # `url::Url::parse` on the SQLx side if pasted in raw — the URL
    # parser treats the trailing `@` as the userinfo terminator and
    # silently misroutes to the wrong host. Encode anything outside the
    # RFC 3986 unreserved set (ALPHA / DIGIT / `-` `.` `_` `~`) and
    # leave host/port/dbname alone (CNPG-emitted, already constrained).
    # Uses perl-core only (sprintf + ord); no extra Debian package.
    _urlenc='s/([^A-Za-z0-9._~-])/sprintf("%%%02X", ord($1))/ge'
    _enc_user=$(printf '%s' "$username" | perl -pe "$_urlenc")
    _enc_pw=$(printf '%s' "$password" | perl -pe "$_urlenc")
    export POSTGRES_URL="postgres://${_enc_user}:${_enc_pw}@${host}:${port}/${dbname}?sslmode=require"
    unset _urlenc _enc_user _enc_pw
fi

# `RdbUrlConfigImpl` requires BOTH `url` and `max_connections` for the
# URL fallback path to load. If max_connections is missing, the
# from_env<RdbConfig>() call further up the chain fails and the binary
# silently uses the hard-coded mysql:mysql@127.0.0.1/default — same
# class of footgun as the separate-form fall-through described above.
# So default it here when the operator hasn't set it via ConfigMap.
# (env from ConfigMap wins because envFrom runs before this script.)
: "${POSTGRES_MAX_CONNECTIONS:=20}"
export POSTGRES_MAX_CONNECTIONS

# Required by infra::infra::require_grpc_callback_env() — but ONLY for
# the `front` server AND only when auto-embedding or RAG tools are
# enabled. grpc-admin/src/lib.rs gates the validation on
# MEMORY_AUTO_EMBEDDING_ENABLED / MEMORY_RAG_TOOLS_ENABLED, so a
# read/write-only deployment with both flags off boots fine without
# these vars. The batch jobs never serve gRPC nor register a callback,
# so the check is irrelevant to them even when the ConfigMap leaves
# those flags on — skip it for anything but `front`.
#
# The Rust side does case-insensitive matching on "true"; POSIX sh has
# no equivalent, so accept the common casings explicitly. There is
# intentionally no fallback to GRPC_ADDR (which is 0.0.0.0 and would
# silently break callbacks from jobworkerp).
if [ "$_bin" = "front" ]; then
    _needs_grpc_callback=
    case "${MEMORY_AUTO_EMBEDDING_ENABLED:-}" in
        true|TRUE|True) _needs_grpc_callback=1 ;;
    esac
    case "${MEMORY_RAG_TOOLS_ENABLED:-}" in
        true|TRUE|True) _needs_grpc_callback=1 ;;
    esac
    if [ -n "$_needs_grpc_callback" ]; then
        : "${MEMORY_GRPC_HOST:?MEMORY_GRPC_HOST must be set when MEMORY_AUTO_EMBEDDING_ENABLED or MEMORY_RAG_TOOLS_ENABLED is true (e.g. memories.memories.svc.cluster.local)}"
        : "${MEMORY_GRPC_PORT:?MEMORY_GRPC_PORT must be set when MEMORY_AUTO_EMBEDDING_ENABLED or MEMORY_RAG_TOOLS_ENABLED is true (e.g. 9000)}"
    fi
    unset _needs_grpc_callback
fi

exec "./${_bin}" "$@"
