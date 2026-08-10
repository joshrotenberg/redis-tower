#![cfg(unix)]

use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
#[ignore = "requires redis-server and redis-cli on PATH"]
fn live_sigterm_cleans_managed_process() {
    let port = reserve_ephemeral_port();
    let child = Command::new(env!("CARGO_BIN_EXE_soak-bench"))
        .env("SOAK_MODE", "standalone")
        .env("SOAK_CHAOS", "none")
        .env("SOAK_DURATION_SECS", "60")
        .env("SOAK_WARMUP_SECS", "0")
        .env("SOAK_REPORT_INTERVAL_SECS", "60")
        .env("SOAK_CONCURRENCY", "2")
        .env("SOAK_STANDALONE_PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn soak-bench binary");
    let mut child = ChildGuard(Some(child));
    wait_for_port(port, true, Duration::from_secs(20));

    send_sigterm_and_wait(&mut child);
    wait_for_port(port, false, Duration::from_secs(5));
}

#[test]
#[ignore = "requires redis-server and redis-cli on PATH"]
fn live_sigterm_cleans_replacement_owned_by_chaos_task() {
    let port = reserve_ephemeral_port();
    let child = Command::new(env!("CARGO_BIN_EXE_soak-bench"))
        .env("SOAK_MODE", "standalone")
        .env("SOAK_CHAOS", "standalone-sigkill")
        .env("SOAK_DURATION_SECS", "60")
        .env("SOAK_WARMUP_SECS", "0")
        .env("SOAK_REPORT_INTERVAL_SECS", "60")
        .env("SOAK_CHAOS_AFTER_SECS", "1")
        .env("SOAK_RECOVERY_TIMEOUT_SECS", "10")
        .env("SOAK_OPERATION_TIMEOUT_MS", "1000")
        .env("SOAK_CONCURRENCY", "2")
        .env("SOAK_STANDALONE_PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn soak-bench binary");
    let mut child = ChildGuard(Some(child));
    wait_for_port(port, true, Duration::from_secs(20));

    let soak_pid = child.0.as_ref().expect("child remains owned").id();
    let initial_pid = wait_for_owned_pid(soak_pid, port, None, Duration::from_secs(20));
    let replacement_pid =
        wait_for_owned_pid(soak_pid, port, Some(initial_pid), Duration::from_secs(20));
    assert!(pid_is_alive(replacement_pid));
    wait_for_port(port, true, Duration::from_secs(20));

    send_sigterm_and_wait(&mut child);
    wait_for_port(port, false, Duration::from_secs(5));
    wait_for_pid(replacement_pid, false, Duration::from_secs(5));
}

fn reserve_ephemeral_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve loopback port");
    listener.local_addr().expect("read loopback port").port()
}

fn port_is_open(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(20),
    )
    .is_ok()
}

fn wait_for_port(port: u16, expected_open: bool, wait: Duration) {
    let deadline = Instant::now() + wait;
    while port_is_open(port) != expected_open {
        assert!(
            Instant::now() < deadline,
            "port {port} did not become {} within {wait:?}",
            if expected_open { "open" } else { "closed" }
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_owned_pid(soak_pid: u32, port: u16, excluded: Option<u32>, wait: Duration) -> u32 {
    let deadline = Instant::now() + wait;
    let prefix = format!("redis-tower-soak-{soak_pid}-{port}-");
    loop {
        let pid = fs::read_dir(std::env::temp_dir())
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
            .filter_map(|entry| {
                fs::read_to_string(entry.path().join(format!("node-{port}/redis.pid"))).ok()
            })
            .find_map(|contents| contents.trim().parse().ok())
            .filter(|pid| Some(*pid) != excluded);
        if let Some(pid) = pid {
            return pid;
        }
        assert!(
            Instant::now() < deadline,
            "owned Redis PID for soak pid={soak_pid} port={port} did not appear within {wait:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn pid_is_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn wait_for_pid(pid: u32, expected_alive: bool, wait: Duration) {
    let deadline = Instant::now() + wait;
    while pid_is_alive(pid) != expected_alive {
        assert!(
            Instant::now() < deadline,
            "PID {pid} did not become {} within {wait:?}",
            if expected_alive { "alive" } else { "dead" }
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn send_sigterm_and_wait(child: &mut ChildGuard) {
    let pid = child.0.as_ref().expect("child remains owned").id();
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM to soak-bench");
    assert!(status.success());

    let deadline = Instant::now() + Duration::from_secs(10);
    let exit = loop {
        if let Some(status) = child
            .0
            .as_mut()
            .expect("child remains owned")
            .try_wait()
            .expect("poll soak-bench")
        {
            break status;
        }
        assert!(Instant::now() < deadline, "soak-bench ignored SIGTERM");
        std::thread::sleep(Duration::from_millis(25));
    };
    child.0.take();
    assert_eq!(exit.code(), Some(130));
}
