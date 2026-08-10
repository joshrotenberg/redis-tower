#![cfg(all(unix, feature = "build-measure"))]

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::process::CommandExt;
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

struct ProcessGroupGuard(Option<Child>);

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let group = i32::try_from(child.id()).expect("child PID fits pid_t");
            // SAFETY: the child was spawned as the leader of this process group.
            let _ = unsafe { libc::kill(-group, libc::SIGKILL) };
            let _ = child.wait();
        }
    }
}

fn process_target_is_alive(target: i32) -> bool {
    // SAFETY: signal 0 performs an existence/permission check without sending
    // a signal. A negative target addresses a process group.
    let result = unsafe { libc::kill(target, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[test]
fn sigint_reaps_the_writer_without_touching_an_unrelated_group() {
    let base =
        std::env::temp_dir().join(format!("resource-bench-signal-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir(&base).expect("create signal test base");

    let mut sentinel_command = Command::new("sleep");
    sentinel_command
        .arg("30")
        .process_group(0)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let sentinel = sentinel_command.spawn().expect("start unrelated sentinel");
    let mut sentinel = ProcessGroupGuard(Some(sentinel));
    let sentinel_pid = i32::try_from(sentinel.0.as_ref().expect("sentinel is running").id())
        .expect("sentinel PID fits pid_t");

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
    let mut registration = String::new();
    BufReader::new(stdout)
        .read_line(&mut registration)
        .expect("read active target and writer PID");
    let mut registration = registration.trim_end().split('\t');
    let target = PathBuf::from(registration.next().expect("target path is present"));
    let writer_pid = registration
        .next()
        .expect("writer PID is present")
        .parse::<i32>()
        .expect("writer PID is numeric");
    assert!(registration.next().is_none());
    assert!(target.starts_with(&base));
    assert!(target.is_dir());

    let marker_deadline = Instant::now() + Duration::from_secs(5);
    while !target.join("writer-active").is_file() {
        assert!(
            child
                .0
                .as_mut()
                .expect("helper is running")
                .try_wait()
                .expect("poll helper before SIGINT")
                .is_none(),
            "signal helper exited before its writer became active"
        );
        assert!(
            Instant::now() < marker_deadline,
            "signal writer did not become active"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(process_target_is_alive(writer_pid));
    assert!(process_target_is_alive(-writer_pid));
    assert!(process_target_is_alive(sentinel_pid));
    assert!(process_target_is_alive(-sentinel_pid));

    let helper_pid = i32::try_from(child.0.as_ref().expect("helper is running").id())
        .expect("helper PID fits pid_t");
    // SAFETY: helper_pid identifies the live child process above.
    let signal_result = unsafe { libc::kill(helper_pid, libc::SIGINT) };
    assert_eq!(
        signal_result,
        0,
        "send SIGINT: {}",
        std::io::Error::last_os_error()
    );

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
    assert!(
        !process_target_is_alive(writer_pid),
        "writer process survived its parent's SIGINT cleanup"
    );
    assert!(
        !process_target_is_alive(-writer_pid),
        "writer process group survived its parent's SIGINT cleanup"
    );
    assert!(
        sentinel
            .0
            .as_mut()
            .expect("sentinel is running")
            .try_wait()
            .expect("poll unrelated sentinel")
            .is_none(),
        "unrelated sentinel process exited during cleanup"
    );
    assert!(process_target_is_alive(sentinel_pid));
    assert!(process_target_is_alive(-sentinel_pid));

    fs::remove_dir(&base).expect("remove empty signal test base");
}
