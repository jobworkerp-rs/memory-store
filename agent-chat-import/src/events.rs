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

impl<W: Write> EventOutput<W> {
    fn write_event(&self, event: &ImportEvent<'_>) -> Result<(), String> {
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
        assert_eq!(events[0]["schema"], EVENT_SCHEMA);
        assert_eq!(events[0]["version"], EVENT_VERSION);
        assert_eq!(events[0]["source"], "claude-code");
        assert_eq!(events[0]["session_key"], "claude_code:s1");
        assert_eq!(events[0]["thread_id"], thread_id.to_string());
        assert_eq!(events[1]["event"], "session_completed");
        assert_eq!(events[1]["imported_count"], 7);
        assert_eq!(events[1]["success"], true);
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
