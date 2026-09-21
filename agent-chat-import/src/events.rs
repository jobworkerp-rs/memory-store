use serde::Serialize;
use std::io::Write;
use std::sync::Mutex;

const EVENT_SCHEMA: &str = "memories-import-event";
const EVENT_VERSION: u32 = 1;

pub trait ImportEventSink: Send + Sync {
    fn thread_created(&self, session_key: &str, thread_id: i64) -> Result<(), String>;
    fn session_completed(
        &self,
        session_key: &str,
        thread_id: i64,
        imported_count: usize,
        success: bool,
    ) -> Result<(), String>;
}

pub struct EventOutput<W> {
    enabled: bool,
    source: &'static str,
    writer: Mutex<W>,
}

impl<W> EventOutput<W> {
    pub fn new(enabled: bool, source: &'static str, writer: W) -> Self {
        Self {
            enabled,
            source,
            writer: Mutex::new(writer),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_inner(self) -> W {
        self.writer
            .into_inner()
            .expect("event output mutex poisoned")
    }
}

#[derive(Serialize)]
struct ImportEvent<'a> {
    schema: &'static str,
    version: u32,
    event: &'static str,
    source: &'a str,
    session_key: &'a str,
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    success: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ImportCompletedReport {
    pub sessions_processed: usize,
    pub threads_created: usize,
    pub memories_imported: usize,
    pub memories_skipped_duplicate: usize,
    pub memories_skipped_ignored: usize,
    pub errors_count: usize,
    pub sessions: Vec<ImportCompletedSession>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportCompletedSession {
    pub session_key: Option<String>,
    pub status: String,
    pub imported_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ImportSessionError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportSessionError {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
}

#[derive(Serialize)]
struct ImportCompletedEvent<'a> {
    schema: &'static str,
    version: u32,
    event: &'static str,
    source: &'a str,
    sessions_processed: usize,
    threads_created: usize,
    memories_imported: usize,
    memories_skipped_duplicate: usize,
    memories_skipped_ignored: usize,
    errors_count: usize,
    success: bool,
    sessions: &'a [ImportCompletedSession],
}

impl<'a> ImportCompletedEvent<'a> {
    fn from_report(source: &'a str, report: &'a ImportCompletedReport) -> Self {
        Self {
            schema: EVENT_SCHEMA,
            version: EVENT_VERSION,
            event: "import_completed",
            source,
            sessions_processed: report.sessions_processed,
            threads_created: report.threads_created,
            memories_imported: report.memories_imported,
            memories_skipped_duplicate: report.memories_skipped_duplicate,
            memories_skipped_ignored: report.memories_skipped_ignored,
            errors_count: report.errors_count,
            success: report.errors_count == 0,
            sessions: &report.sessions,
        }
    }
}

impl<W: Write> EventOutput<W> {
    fn write_event<T: Serialize>(&self, event: &T) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| "event output mutex poisoned".to_string())?;
        serde_json::to_writer(&mut *writer, event)
            .map_err(|error| format!("failed to serialize import event: {error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|()| writer.flush())
            .map_err(|error| format!("failed to write import event: {error}"))
    }

    pub fn import_completed(&self, report: &ImportCompletedReport) -> Result<(), String> {
        self.write_event(&ImportCompletedEvent::from_report(self.source, report))
    }
}

impl<W: Write + Send> ImportEventSink for EventOutput<W> {
    fn thread_created(&self, session_key: &str, thread_id: i64) -> Result<(), String> {
        self.write_event(&ImportEvent {
            schema: EVENT_SCHEMA,
            version: EVENT_VERSION,
            event: "thread_created",
            source: self.source,
            session_key,
            thread_id: thread_id.to_string(),
            imported_count: None,
            success: None,
        })
    }

    fn session_completed(
        &self,
        session_key: &str,
        thread_id: i64,
        imported_count: usize,
        success: bool,
    ) -> Result<(), String> {
        self.write_event(&ImportEvent {
            schema: EVENT_SCHEMA,
            version: EVENT_VERSION,
            event: "session_completed",
            source: self.source,
            session_key,
            thread_id: thread_id.to_string(),
            imported_count: Some(imported_count),
            success: Some(success),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn disabled_output_preserves_legacy_stdout() {
        let output = EventOutput::new(false, "codex", Vec::new());
        output.thread_created("codex:s1", 42).unwrap();
        output.session_completed("codex:s1", 42, 3, true).unwrap();
        assert!(output.into_inner().is_empty());
    }

    #[test]
    fn events_are_json_lines_and_large_ids_are_strings() {
        let output = EventOutput::new(true, "claude-code", Vec::new());
        let thread_id = 9_007_199_254_740_993_i64;
        output.thread_created("claude_code:s1", thread_id).unwrap();
        output
            .session_completed("claude_code:s1", thread_id, 7, true)
            .unwrap();
        let text = String::from_utf8(output.into_inner()).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0],
            serde_json::json!({
                "schema": EVENT_SCHEMA,
                "version": EVENT_VERSION,
                "event": "thread_created",
                "source": "claude-code",
                "session_key": "claude_code:s1",
                "thread_id": thread_id.to_string(),
            })
        );
        assert_eq!(
            events[1],
            serde_json::json!({
                "schema": EVENT_SCHEMA,
                "version": EVENT_VERSION,
                "event": "session_completed",
                "source": "claude-code",
                "session_key": "claude_code:s1",
                "thread_id": thread_id.to_string(),
                "imported_count": 7,
                "success": true,
            })
        );
    }

    #[test]
    fn plain_channel_keeps_custom_source_name_in_the_session_key() {
        let output = EventOutput::new(true, "plain", Vec::new());
        output
            .session_completed("notes:file:abc", 42, 1, true)
            .unwrap();
        let text = String::from_utf8(output.into_inner()).unwrap();
        let event: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(event["source"], "plain");
        assert_eq!(event["session_key"], "notes:file:abc");
    }

    #[test]
    fn concurrent_event_writes_remain_one_json_object_per_line() {
        let output = Arc::new(EventOutput::new(true, "codex", Vec::new()));
        let handles = (0..4)
            .map(|worker| {
                let output = Arc::clone(&output);
                std::thread::spawn(move || {
                    for session in 0..25 {
                        output
                            .session_completed(
                                &format!("codex:{worker}:{session}"),
                                i64::from(worker * 100 + session + 1),
                                1,
                                true,
                            )
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        let output = Arc::try_unwrap(output).ok().unwrap();
        let text = String::from_utf8(output.into_inner()).unwrap();
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 100);
        assert!(lines.iter().all(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .is_ok_and(|event| event["event"] == "session_completed")
        }));
    }

    #[test]
    fn sequential_sessions_keep_creation_completion_order() {
        let output = EventOutput::new(true, "codex", Vec::new());
        for (session, thread_id) in [("codex:first", 41), ("codex:second", 42)] {
            output.thread_created(session, thread_id).unwrap();
            output
                .session_completed(session, thread_id, 1, true)
                .unwrap();
        }
        let text = String::from_utf8(output.into_inner()).unwrap();
        let order = text
            .lines()
            .map(|line| {
                let event: serde_json::Value = serde_json::from_str(line).unwrap();
                format!("{}:{}", event["session_key"], event["event"])
            })
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![
                "\"codex:first\":\"thread_created\"",
                "\"codex:first\":\"session_completed\"",
                "\"codex:second\":\"thread_created\"",
                "\"codex:second\":\"session_completed\"",
            ]
        );
    }

    #[test]
    fn import_completed_reports_partial_success_counts_and_session_errors() {
        let output = EventOutput::new(true, "codex", Vec::new());
        output
            .import_completed(&ImportCompletedReport {
                sessions_processed: 3,
                threads_created: 1,
                memories_imported: 2,
                memories_skipped_duplicate: 3,
                memories_skipped_ignored: 4,
                errors_count: 1,
                sessions: vec![
                    ImportCompletedSession {
                        session_key: Some("codex:ok".into()),
                        status: "completed".into(),
                        imported_count: 5,
                        error: None,
                    },
                    ImportCompletedSession {
                        session_key: Some("codex:failed".into()),
                        status: "failed".into(),
                        imported_count: 0,
                        error: Some(ImportSessionError {
                            code: None,
                            message: "chunk failed".into(),
                        }),
                    },
                    ImportCompletedSession {
                        session_key: Some("codex:ignored".into()),
                        status: "completed".into(),
                        imported_count: 0,
                        error: None,
                    },
                ],
            })
            .unwrap();
        let event: serde_json::Value =
            serde_json::from_str(String::from_utf8(output.into_inner()).unwrap().trim()).unwrap();

        assert_eq!(event["event"], "import_completed");
        assert_eq!(event["sessions_processed"], 3);
        assert_eq!(event["threads_created"], 1);
        assert_eq!(event["memories_imported"], 2);
        assert_eq!(event["memories_skipped_duplicate"], 3);
        assert_eq!(event["memories_skipped_ignored"], 4);
        assert_eq!(event["errors_count"], 1);
        assert_eq!(event["success"], false);
        assert_eq!(event["sessions"][1]["status"], "failed");
        assert_eq!(event["sessions"][1]["imported_count"], 0);
        assert_eq!(event["sessions"][1]["error"]["message"], "chunk failed");
        assert!(event["sessions"][1]["error"].get("code").is_none());
    }

    #[test]
    fn import_completed_reports_all_failed_sessions_as_unsuccessful() {
        let output = EventOutput::new(true, "plain", Vec::new());
        output
            .import_completed(&ImportCompletedReport {
                sessions_processed: 2,
                threads_created: 0,
                memories_imported: 0,
                memories_skipped_duplicate: 0,
                memories_skipped_ignored: 0,
                errors_count: 2,
                sessions: vec![
                    ImportCompletedSession {
                        session_key: None,
                        status: "failed".into(),
                        imported_count: 0,
                        error: Some(ImportSessionError {
                            code: Some("parse_error".into()),
                            message: "invalid JSON".into(),
                        }),
                    },
                    ImportCompletedSession {
                        session_key: Some("plain:file:b".into()),
                        status: "failed".into(),
                        imported_count: 0,
                        error: Some(ImportSessionError {
                            code: None,
                            message: "permission denied".into(),
                        }),
                    },
                ],
            })
            .unwrap();
        let event: serde_json::Value =
            serde_json::from_str(String::from_utf8(output.into_inner()).unwrap().trim()).unwrap();

        assert_eq!(event["success"], false);
        assert_eq!(event["sessions"].as_array().unwrap().len(), 2);
        assert_eq!(event["sessions"][0]["session_key"], serde_json::Value::Null);
        assert_eq!(event["sessions"][0]["error"]["code"], "parse_error");
        assert_eq!(event["sessions"][1]["status"], "failed");
    }

    #[test]
    fn import_completed_wire_format_matches_current_json_lines() {
        let output = EventOutput::new(true, "codex", Vec::new());
        output
            .import_completed(&ImportCompletedReport {
                sessions_processed: 2,
                threads_created: 1,
                memories_imported: 3,
                memories_skipped_duplicate: 1,
                memories_skipped_ignored: 0,
                errors_count: 1,
                sessions: vec![
                    ImportCompletedSession {
                        session_key: Some("codex:ok".into()),
                        status: "completed".into(),
                        imported_count: 4,
                        error: None,
                    },
                    ImportCompletedSession {
                        session_key: None,
                        status: "failed".into(),
                        imported_count: 0,
                        error: Some(ImportSessionError {
                            code: None,
                            message: "invalid JSON".into(),
                        }),
                    },
                ],
            })
            .unwrap();

        assert_eq!(
            String::from_utf8(output.into_inner()).unwrap(),
            r#"{"schema":"memories-import-event","version":1,"event":"import_completed","source":"codex","sessions_processed":2,"threads_created":1,"memories_imported":3,"memories_skipped_duplicate":1,"memories_skipped_ignored":0,"errors_count":1,"success":false,"sessions":[{"session_key":"codex:ok","status":"completed","imported_count":4},{"session_key":null,"status":"failed","imported_count":0,"error":{"message":"invalid JSON"}}]}
"#
        );
    }

    #[test]
    fn import_completed_includes_parse_error_even_without_a_session_key() {
        let output = EventOutput::new(true, "claude-code", Vec::new());
        output
            .import_completed(&ImportCompletedReport {
                sessions_processed: 1,
                threads_created: 0,
                memories_imported: 0,
                memories_skipped_duplicate: 0,
                memories_skipped_ignored: 0,
                errors_count: 1,
                sessions: vec![ImportCompletedSession {
                    session_key: None,
                    status: "failed".into(),
                    imported_count: 0,
                    error: Some(ImportSessionError {
                        code: None,
                        message: "session parse failed".into(),
                    }),
                }],
            })
            .unwrap();
        let event: serde_json::Value =
            serde_json::from_str(String::from_utf8(output.into_inner()).unwrap().trim()).unwrap();

        assert_eq!(event["event"], "import_completed");
        assert_eq!(event["sessions_processed"], 1);
        assert_eq!(event["sessions"][0]["session_key"], serde_json::Value::Null);
        assert_eq!(event["sessions"][0]["status"], "failed");
        assert_eq!(
            event["sessions"][0]["error"]["message"],
            "session parse failed"
        );
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("closed stdout"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn event_output_failure_is_not_ignored() {
        let output = EventOutput::new(true, "codex", FailingWriter);
        let error = output.thread_created("codex:s1", 42).unwrap_err();
        assert!(error.contains("failed to serialize import event"));
    }
}
