//! Writes the data a Lookback user produces through the v0.0.13 server, using
//! the same app-layer calls the gRPC services make.
//!
//! Copied to `grpc-admin/examples/fixture_data.rs` of a v0.0.13 checkout
//! (commit 34399e8) and run once per database with
//! `cargo run -p grpc-admin --example fixture_data`. It is never compiled in
//! the current tree.
//!
//! The stack is built like `front::server::create_server` at v0.0.13:
//! `RepositoryModule::new_by_env().await` (which also executes the embedded
//! startup schema) followed by `AppModule::new_by_env(..).await`.
//!
//! Required environment:
//!   SQLITE_URL                  e.g. `sqlite:///tmp/x/fresh.sqlite3?mode=rwc`
//!   SQLITE_MAX_CONNECTIONS      e.g. `1`
//!   MEMORY_CACHE_NUM_COUNTERS   e.g. `12960` (no default; AppModule exits without it)
//!   MEMORY_CACHE_MAX_COST       e.g. `12960`
//!   MEMORY_CACHE_USE_METRICS    `false`
//!   MEDIA_STORAGE_LOCAL_DIR     a scratch directory (the default `./media`
//!                               is relative to the working directory)
//! Must be unset or not `true` (no LanceDB, no jobworkerp dispatch):
//!   MEMORY_VECTOR_ENABLED, THREAD_VECTOR_ENABLED, REFLECTION_INTENT_VECTOR_ENABLED,
//!   MEMORY_AUTO_EMBEDDING_ENABLED, MEMORY_REFLECTION_DISPATCH_ENABLED
//! Must be unset: JOBWORKERP_ADDR (otherwise a jobworkerp client is dialed).
//! MEDIA_STORAGE_BACKEND must be unset or `file`.
//! Optional: SQLITE_DISABLE_WAL, RUST_LOG.
//!
//! `.env` is deliberately not loaded: the generator controls every variable.
//!
//! What the server decides, not this program: ids are snowflakes, and
//! `thread.created_at` / `updated_at`, `thread_memory.created_at` and
//! `thread_label.created_at` are wall-clock times (the thread repository
//! ignores the requested timestamps). Memory timestamps are the fixed,
//! non-zero values below; the repository would only fill a 0.
//!
//! On success one summary line is printed; any failure exits non-zero.

use anyhow::{Context, Result, ensure};
use app::app::memory::MemoryApp;
use app::app::thread::{
    AddMemoriesBatchInput, AddMemoriesBatchOutput, BatchMemoryInput, BatchThreadTarget,
    ThreadApp, ThreadAppImpl,
};
use app::module::AppModule;
use infra::infra::module::RepositoryModule;
use protobuf::llm_memory::data::{
    ContentType, MemoryData, MemoryId, MemoryKind, MessageRole, ThreadData, ThreadId, UserId,
};

const USER_A: i64 = 1;
const USER_B: i64 = 2;
/// 2026-11-20T01:00:00Z: after every date used by the legacy seeds, so the
/// derived thread message times stay ordered after a migration.
const BASE_MS: i64 = 1_795_136_400_000;
const SESSION_ID: &str = "8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f";
const SESSION_LABELS: [&str; 5] = [
    "agent:claude_code",
    "branch:main",
    "coding_agent",
    "dir:lookback",
    "path:works/lookback",
];

fn at(minutes: i64) -> i64 {
    BASE_MS + minutes * 60_000
}

fn user(value: i64) -> Option<UserId> {
    Some(UserId { value })
}

fn memory_ids(ids: &[i64]) -> Vec<MemoryId> {
    ids.iter().map(|&value| MemoryId { value }).collect()
}

fn message(
    user_id: i64,
    role: MessageRole,
    content: &str,
    created_at: i64,
    parent_ids: &[i64],
) -> MemoryData {
    MemoryData {
        parent_ids: memory_ids(parent_ids),
        user_id: user(user_id),
        content: content.to_string(),
        content_type: ContentType::Text as i32,
        created_at,
        updated_at: created_at,
        role: role as i32,
        memory_kind: MemoryKind::Raw as i32,
        ..Default::default()
    }
}

fn thread(user_id: i64, description: Option<&str>, created_at: i64) -> ThreadData {
    ThreadData {
        user_id: user(user_id),
        description: description.map(str::to_string),
        created_at,
        updated_at: created_at,
        memory_kind: MemoryKind::Raw as i32,
        ..Default::default()
    }
}

/// One imported Claude Code block, shaped like the v0.0.13 importer output
/// (owner-scoped external_id, `metadata.source`).
fn imported_block(
    kind: &str,
    role: MessageRole,
    content_type: ContentType,
    content: &str,
    created_at: i64,
    block_uuid: &str,
    parent_external_ids: &[String],
) -> BatchMemoryInput {
    let metadata = serde_json::json!({
        "source": "claude_code",
        "kind": kind,
        "session": {"cwd": "/home/user/works/lookback", "git_branch": "main"},
    });
    BatchMemoryInput {
        memory: MemoryData {
            user_id: user(USER_A),
            content: content.to_string(),
            content_type: content_type as i32,
            metadata: Some(metadata.to_string()),
            created_at,
            updated_at: created_at,
            role: role as i32,
            external_id: Some(external_id(block_uuid)),
            memory_kind: MemoryKind::Raw as i32,
            ..Default::default()
        },
        parent_external_ids: parent_external_ids.to_vec(),
    }
}

fn external_id(block_uuid: &str) -> String {
    format!("claude_code:{USER_A}:{SESSION_ID}:{block_uuid}:b0")
}

fn session_thread() -> ThreadData {
    ThreadData {
        channel: Some(format!("claude_code:{SESSION_ID}")),
        ..thread(USER_A, Some("lookback (main)"), at(70))
    }
}

#[derive(Default)]
struct Counts {
    threads_created: u32,
    threads_deleted: u32,
    memories_created: u32,
    memories_deleted: u32,
}

struct Writer {
    thread_app: ThreadAppImpl,
    memory_app: app::app::memory::MemoryAppImpl,
    counts: Counts,
}

impl Writer {
    async fn create_thread(&mut self, data: ThreadData) -> Result<i64> {
        let id = self.thread_app.create_thread(&data).await?.value;
        self.counts.threads_created += 1;
        Ok(id)
    }

    async fn add(&mut self, thread_id: i64, data: MemoryData) -> Result<i64> {
        let id = self
            .thread_app
            .add_memory(&ThreadId { value: thread_id }, &data)
            .await?
            .value;
        self.counts.memories_created += 1;
        Ok(id)
    }

    async fn delete_memory(&mut self, memory_id: i64) -> Result<()> {
        let deleted = self
            .memory_app
            .delete_memory(&MemoryId { value: memory_id })
            .await?;
        ensure!(deleted, "memory {memory_id} was not deleted");
        self.counts.memories_deleted += 1;
        Ok(())
    }

    async fn batch(&mut self, input: AddMemoriesBatchInput) -> Result<AddMemoriesBatchOutput> {
        let requested = input.memories.len();
        let output = self.thread_app.add_memories_batch(input).await?;
        ensure!(
            output.outcomes.len() == requested && output.outcomes.iter().all(|o| o.created),
            "batch did not insert every memory"
        );
        self.counts.memories_created += requested as u32;
        if output.thread_created {
            self.counts.threads_created += 1;
        }
        Ok(output)
    }

    async fn thread_member_count(&self, thread_id: i64) -> Result<usize> {
        Ok(self
            .thread_app
            .find_memories_by_thread_id(&ThreadId { value: thread_id }, None, None, &[], &[])
            .await?
            .len())
    }

    /// A chat whose default system prompt is a standalone ROLE_SYSTEM memory,
    /// as a client creates it: MemoryService.Create, then ThreadService.Create.
    async fn chat_with_default_system_prompt(&mut self) -> Result<(i64, i64, i64)> {
        let system_id = self
            .memory_app
            .create_memory(&message(
                USER_A,
                MessageRole::RoleSystem,
                "あなたは Lookback のアシスタントです。簡潔に日本語で答えてください。",
                at(0),
                &[],
            ))
            .await?
            .value;
        self.counts.memories_created += 1;
        let chat = self
            .create_thread(ThreadData {
                default_system_memory_id: Some(system_id),
                labels: vec!["topic:shopping".to_string()],
                ..thread(USER_A, Some("週末の買い物リスト"), at(0))
            })
            .await?;
        // create_thread anchors the default system memory in thread_memory.
        ensure!(
            self.thread_app
                .is_memory_in_thread(&ThreadId { value: chat }, &MemoryId { value: system_id })
                .await?,
            "default system memory was not anchored in its thread"
        );
        let u1 = self
            .add(
                chat,
                message(USER_A, MessageRole::RoleUser, "週末の買い物リストを作って。", at(1), &[]),
            )
            .await?;
        let stored = self
            .memory_app
            .find_memory(&MemoryId { value: u1 }, None)
            .await?
            .and_then(|m| m.data)
            .context("first chat message disappeared")?;
        ensure!(
            stored.parent_ids.first().map(|p| p.value) == Some(system_id),
            "default system memory was not injected as the first parent"
        );
        let a1 = self
            .add(
                chat,
                message(
                    USER_A,
                    MessageRole::RoleAssistant,
                    "牛乳、卵、食パン、玉ねぎ、鶏もも肉です。",
                    at(2),
                    &[u1],
                ),
            )
            .await?;
        let u2 = self
            .add(
                chat,
                message(USER_A, MessageRole::RoleUser, "飲み物も足して。", at(3), &[a1]),
            )
            .await?;
        self.add(
            chat,
            message(
                USER_A,
                MessageRole::RoleAssistant,
                "麦茶のパックと炭酸水を追加しました。",
                at(4),
                &[u2],
            ),
        )
        .await?;
        Ok((system_id, chat, a1))
    }

    /// Continuing from a memory of another thread attaches that memory to the
    /// new thread, so it belongs to both.
    async fn branch_from(&mut self, shared_memory_id: i64) -> Result<i64> {
        let branch = self
            .create_thread(thread(USER_A, Some("週末の買い物リスト (別案)"), at(10)))
            .await?;
        let u3 = self
            .add(
                branch,
                message(
                    USER_A,
                    MessageRole::RoleUser,
                    "鶏もも肉を豚こまに変えた場合は?",
                    at(11),
                    &[shared_memory_id],
                ),
            )
            .await?;
        self.add(
            branch,
            message(
                USER_A,
                MessageRole::RoleAssistant,
                "豚こまとキャベツで回鍋肉にできます。",
                at(12),
                &[u3],
            ),
        )
        .await?;
        let owners = self
            .thread_app
            .count_threads_for_memory(&MemoryId {
                value: shared_memory_id,
            })
            .await?;
        ensure!(owners == 2, "shared memory belongs to {owners} threads, expected 2");
        Ok(branch)
    }

    async fn second_user_chat(&mut self) -> Result<i64> {
        let chat = self.create_thread(thread(USER_B, None, at(30))).await?;
        let question = self
            .add(
                chat,
                message(USER_B, MessageRole::RoleUser, "明日の朝 6 時に起きるコツは?", at(31), &[]),
            )
            .await?;
        self.add(
            chat,
            message(
                USER_B,
                MessageRole::RoleAssistant,
                "寝る 1 時間前に画面を見るのをやめ、起床時刻を毎日そろえることです。",
                at(32),
                &[question],
            ),
        )
        .await?;
        Ok(chat)
    }

    async fn thread_with_all_memories_deleted(&mut self) -> Result<i64> {
        let cleared = self
            .create_thread(thread(USER_B, Some("消したメモ"), at(40)))
            .await?;
        let note = self
            .add(
                cleared,
                message(USER_B, MessageRole::RoleUser, "暗証番号は 0000 にした。", at(41), &[]),
            )
            .await?;
        let reply = self
            .add(
                cleared,
                message(
                    USER_B,
                    MessageRole::RoleAssistant,
                    "推測されやすい番号は避けることをおすすめします。",
                    at(42),
                    &[note],
                ),
            )
            .await?;
        self.delete_memory(reply).await?;
        self.delete_memory(note).await?;
        ensure!(
            self.thread_member_count(cleared).await? == 0,
            "thread still has memories after deleting all of them"
        );
        Ok(cleared)
    }

    async fn thread_created_then_deleted(&mut self) -> Result<()> {
        let doomed = self
            .create_thread(thread(USER_A, Some("あとで消すスレッド"), at(50)))
            .await?;
        self.add(
            doomed,
            message(USER_A, MessageRole::RoleUser, "テスト投稿です。", at(51), &[]),
        )
        .await?;
        let (deleted, exclusive) = self
            .thread_app
            .delete_thread(&ThreadId { value: doomed })
            .await?;
        ensure!(
            deleted && exclusive.len() == 1,
            "thread delete removed {} exclusive memories (deleted={deleted})",
            exclusive.len()
        );
        self.counts.threads_deleted += 1;
        self.counts.memories_deleted += 1;
        Ok(())
    }

    /// AddMemoriesBatch as the importer sends it: the first chunk creates the
    /// thread by channel, the second reuses it and links to the first chunk.
    async fn imported_session(&mut self) -> Result<i64> {
        let labels: Vec<String> = SESSION_LABELS.iter().map(|l| l.to_string()).collect();
        let first = self
            .batch(AddMemoriesBatchInput {
                thread_target: BatchThreadTarget::UpsertByChannel(Box::new(session_thread())),
                memories: vec![
                    imported_block(
                        "user",
                        MessageRole::RoleUser,
                        ContentType::Text,
                        "設定画面の保存ボタンが二重送信される。",
                        at(70),
                        "a1b2c3d4-0001",
                        &[],
                    ),
                    imported_block(
                        "assistant",
                        MessageRole::RoleAssistant,
                        ContentType::Text,
                        "送信中はボタンを無効化します。",
                        at(71),
                        "a1b2c3d4-0002",
                        &[external_id("a1b2c3d4-0001")],
                    ),
                    imported_block(
                        "tool_call",
                        MessageRole::RoleAssistant,
                        ContentType::Tool,
                        r#"{"name":"Edit","input":{"file_path":"src/settings.tsx"}}"#,
                        at(72),
                        "a1b2c3d4-0003",
                        &[external_id("a1b2c3d4-0002")],
                    ),
                ],
                upsert_by_external_id: true,
                labels: labels.clone(),
            })
            .await?;
        ensure!(first.thread_created, "first import chunk did not create its thread");
        let second = self
            .batch(AddMemoriesBatchInput {
                thread_target: BatchThreadTarget::UpsertByChannel(Box::new(session_thread())),
                memories: vec![imported_block(
                    "tool_output",
                    MessageRole::RoleTool,
                    ContentType::Tool,
                    "The file src/settings.tsx has been updated.",
                    at(73),
                    "a1b2c3d4-0004",
                    &[external_id("a1b2c3d4-0003")],
                )],
                upsert_by_external_id: true,
                labels,
            })
            .await?;
        ensure!(
            !second.thread_created && second.thread_id.value == first.thread_id.value,
            "second import chunk did not reuse the session thread"
        );
        Ok(first.thread_id.value)
    }

    /// THREAD_SUMMARY thread shaped like the v0.0.13 thread-summary workflow.
    async fn summary_of(&mut self, source_thread_id: i64) -> Result<i64> {
        let mut labels: Vec<String> = SESSION_LABELS.iter().map(|l| l.to_string()).collect();
        let description = format!("{} の要約集", labels.join(", "));
        labels.push("summary".to_string());
        let summary = self
            .create_thread(ThreadData {
                labels,
                memory_kind: MemoryKind::ThreadSummary as i32,
                ..thread(USER_A, Some(&description), at(80))
            })
            .await?;
        let content = serde_json::json!({
            "category": "bugfix",
            "title": "設定画面の二重送信修正",
            "summary": "保存ボタンを送信中に無効化して二重送信を防いだ。",
            "key_decisions": ["送信中はボタンを無効化"],
            "status": "completed",
        });
        let metadata = serde_json::json!({
            "source_thread_id": source_thread_id.to_string(),
            "category": "bugfix",
            "summary_version": "1.0",
        });
        self.add(
            summary,
            MemoryData {
                content: content.to_string(),
                metadata: Some(metadata.to_string()),
                external_id: Some(format!("summary:{source_thread_id}")),
                memory_kind: MemoryKind::ThreadSummary as i32,
                ..message(USER_A, MessageRole::RoleAssistant, "", at(70), &[])
            },
        )
        .await?;
        Ok(summary)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let repository_module = RepositoryModule::new_by_env().await;
    let pool = repository_module.pool();
    let app_module = AppModule::new_by_env(repository_module).await;
    let mut writer = Writer {
        thread_app: app_module.thread_app,
        memory_app: app_module.memory_app,
        counts: Counts::default(),
    };

    let (system_id, _chat, shared_id) = writer.chat_with_default_system_prompt().await?;
    writer.branch_from(shared_id).await?;
    let empty = writer.create_thread(thread(USER_A, None, at(20))).await?;
    writer.second_user_chat().await?;
    let cleared = writer.thread_with_all_memories_deleted().await?;
    writer.thread_created_then_deleted().await?;
    let session = writer.imported_session().await?;
    writer.summary_of(session).await?;

    ensure!(
        writer.thread_member_count(empty).await? == 0,
        "empty thread gained memories"
    );
    let total_threads = writer.thread_app.count().await?;
    let total_memories = writer.memory_app.count().await?;
    let counts = &writer.counts;
    println!(
        "fixture-data: threads_created={} threads_deleted={} memories_created={} \
         memories_deleted={} default_system_memory_id={system_id} shared_memory_id={shared_id} \
         empty_thread_id={empty} cleared_thread_id={cleared} session_thread_id={session} \
         total_threads={total_threads} total_memories={total_memories}",
        counts.threads_created,
        counts.threads_deleted,
        counts.memories_created,
        counts.memories_deleted,
    );
    // Closing the pool checkpoints the WAL so the database file alone holds
    // the written rows when it is dumped.
    pool.close().await;
    Ok(())
}
