#![cfg(all(unix, feature = "build-measure"))]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn sigint_exits_130_and_removes_the_active_target() {
    let base =
        std::env::temp_dir().join(format!("resource-bench-signal-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir(&base).expect("create signal test base");

    let child = Command::new(env!("CARGO_BIN_EXE_resource-build-measure"))
        .env("RESOURCE_BUILD_SIGNAL_TEST_DIR", &base)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start signal cleanup helper");
    let mut child = ChildGuard(Some(child));
    let stdout = child
        .0
        .as_mut()
        .and_then(|child| child.stdout.take())
        .expect("capture helper stdout");
    let mut target = String::new();
    BufReader::new(stdout)
        .read_line(&mut target)
        .expect("read active target path");
    let target = PathBuf::from(target.trim());
    assert!(target.starts_with(&base));
    assert!(target.is_dir());

    let pid = child.0.as_ref().expect("child is running").id().to_string();
    let signal_status = Command::new("kill")
        .args(["-INT", &pid])
        .status()
        .expect("send SIGINT");
    assert!(signal_status.success());

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        let process = child.0.as_mut().expect("child is running");
        if let Some(status) = process.try_wait().expect("poll signal helper") {
            break status;
        }
        assert!(Instant::now() < deadline, "signal helper did not exit");
        std::thread::sleep(Duration::from_millis(20));
    };
    child.0.take();

    assert_eq!(status.code(), Some(130));
    assert!(!target.exists(), "active target survived SIGINT");
    fs::remove_dir(&base).expect("remove empty signal test base");
}
