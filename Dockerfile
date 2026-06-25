# Multi-stage build for the memories gRPC server (`grpc-admin` -> binary `front`).
# Built with: postgres + lindera (no default features). The LanceDB-backed
# vector / FTS stack is a required dependency and always linked in.
# Lindera dictionary files are NOT bundled — mount via PVC at LANCE_LANGUAGE_MODEL_HOME.

# ---------- Builder stage ----------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        libssl-dev \
        pkg-config \
        ca-certificates \
        clang \
        cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifest + lockfile first to maximize layer cache reuse.
COPY Cargo.toml Cargo.lock ./

# Workspace members (must match memories/Cargo.toml `members`).
COPY modules modules/
COPY protobuf protobuf/
COPY infra infra/
COPY app app/
COPY grpc-admin grpc-admin/
COPY llm-memory-plugin llm-memory-plugin/
COPY agent-chat-import agent-chat-import/

# Workflows are read at runtime by the binary but the build does not need them;
# they are copied into the runtime image instead.

RUN cargo build --release \
        --no-default-features \
        --features summarize-after \
        -p agent-chat-import --bin memories-import

# `front` is the server; `migrate-attachment-to-media` and
# `cleanup-orphan-media` are operational batch jobs run as one-off k8s
# Pods from this same image (see docs/image-memory-staged-rollout.md).
# Same feature set as `front` so they link the identical FTS tokenizer
# (lindera) and postgres backend.
RUN cargo build --release \
        --no-default-features \
        --features postgres,lindera \
        -p grpc-admin \
        --bin front \
        --bin migrate-attachment-to-media \
        --bin cleanup-orphan-media

# ---------- Runtime stage ----------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tini \
        # Required at RUNTIME by grpc_admin::rag_tools: when
        # MEMORY_RAG_TOOLS_ENABLED=true, the manifest YAML embeds a proto
        # that the server compiles on the fly via prost-build, which
        # shells out to `protoc`. Without it the RAG tool registration
        # fails with `Could not find protoc` and the server still starts
        # but never exposes the RAG tools.
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Non-root user with an EXPLICIT numeric UID/GID. Required for k8s
# `securityContext.runAsNonRoot: true` to be enforceable: the kubelet
# only verifies non-root by matching against a numeric UID, so an image
# that declares only `USER memories` (name) is rejected with
# "image has non-numeric user (memories), cannot verify user is non-root".
#
# UID/GID are hardcoded (not ARG) on purpose: the final `USER` instruction
# below must be a literal numeric value so kubelet's runAsNonRoot check
# can verify it without resolving Dockerfile build args. Mixing an ARG
# here with a literal `USER 10001:10001` was the previous bug — non-default
# build-arg values produced an image whose home dir owner mismatched USER.
RUN groupadd --system --gid 10001 memories \
    && useradd --system --uid 10001 --gid memories \
       --home-dir /home/memories --create-home memories

WORKDIR /home/memories

# Application binary + the runtime assets it reads from disk.
COPY --from=builder --chown=memories:memories /build/target/release/memories-import ./memories-import
COPY --from=builder --chown=memories:memories /build/target/release/front ./front
# Operational batch jobs, run as one-off k8s Pods from this image.
COPY --from=builder --chown=memories:memories /build/target/release/migrate-attachment-to-media ./migrate-attachment-to-media
COPY --from=builder --chown=memories:memories /build/target/release/cleanup-orphan-media ./cleanup-orphan-media
COPY --chown=memories:memories workflows ./workflows
COPY --chown=memories:memories infra/sql/postgres ./sql/postgres
COPY --chown=memories:memories docker/start.sh ./start.sh
RUN chmod +x ./start.sh

# Mount points for the two PVCs (LanceDB data + Lindera dictionary).
RUN mkdir -p /var/lib/memories/lancedb /var/lib/lance/language_models \
    && chown -R memories:memories /var/lib/memories /var/lib/lance

# NO `ENV ...=...` here on purpose. Every runtime knob — including the
# paths that target image-internal files (workflows/*.yaml under
# /home/memories) and the PVC mount points (/var/lib/memories/lancedb,
# /var/lib/lance/language_models) — is supplied externally. In k8s that
# means the ConfigMaps under memories-spec/manifests/config/*.env. For
# local `docker run` testing pass them via -e or --env-file.
#
# Rationale: keeping env in two places (Dockerfile + ConfigMap) makes it
# impossible to tell where a value comes from without grepping both
# repos. Single source of truth = memories-spec/manifests/config/.

# Numeric form (10001) — see the rationale above the user creation.
# K8s `runAsNonRoot` checks against the numeric value here.
USER 10001:10001
# Application default; can be overridden by GRPC_ADDR (e.g. via the
# memories-spec ConfigMap). EXPOSE is documentation only — it does not
# actually publish or restrict the port.
EXPOSE 9000
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./start.sh"]
