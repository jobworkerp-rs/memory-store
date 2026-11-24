//! How front treats its command line: `front --version`, which the release
//! runs on every archived binary and compares with the release tag, and any
//! other argument, which front ignores.

use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Runs front with `arg` where it cannot start the server: without GRPC_ADDR,
/// in an empty working directory so that a developer's .env cannot provide
/// it.
fn run_front(arg: impl AsRef<OsStr>) -> Output {
    let cwd = tempfile::tempdir().expect("create working directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_front"))
        .arg(arg)
        .current_dir(cwd.path())
        .env_remove("GRPC_ADDR")
        .stdout(Stdio::piped())
        .spawn()
        .expect("run front");
    // A server that did start would never exit on its own.
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().expect("wait for front").is_none() {
        if Instant::now() > deadline {
            child.kill().ok();
            panic!("front did not exit");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().expect("read front output")
}

#[test]
fn version_flag_reports_the_package_version() {
    // Starting the server instead fails, so a success also shows that
    // --version returns before any server setup.
    let output = run_front("--version");
    assert!(
        output.status.success(),
        "front --version failed: {output:?}"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("front {}\n", env!("CARGO_PKG_VERSION"))
    );
}

// An argument that is not UTF-8 must not make front panic: it reaches the
// server setup, which fails with its own startup error (exit code 1).
#[cfg(unix)]
#[test]
fn non_utf8_argument_is_ignored() {
    use std::os::unix::ffi::OsStrExt;
    let output = run_front(OsStr::from_bytes(b"\xff"));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}
