//! The server must stop when it is asked to, on every transport.
//!
//! These drive the real binary and send it real signals, because the bug this
//! guards against is invisible from inside the process: the stdio transport
//! blocks on reading stdin, and a blocking read cannot be interrupted by a
//! signal handler. Racing the read against a shutdown signal is the fix, and
//! only an out-of-process test proves it.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use adb_core::{DatabaseName, RequestContext, TableName};
use adb_engine::{Engine, EngineConfig};
use tempfile::TempDir;

/// Long enough to be a real failure rather than a slow machine.
const PATIENCE: Duration = Duration::from_secs(15);

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_agedb")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("a local address")
        .port()
}

fn signal(child: &Child, name: &str) {
    let status = Command::new("kill")
        .args([&format!("-{name}"), &child.id().to_string()])
        .status()
        .expect("kill should run");
    assert!(status.success(), "could not send SIG{name}");
}

/// Wait for the process to exit, returning how long it took.
fn wait_for_exit(child: &mut Child) -> Option<Duration> {
    let started = Instant::now();
    while started.elapsed() < PATIENCE {
        match child.try_wait().expect("try_wait should work") {
            Some(_) => return Some(started.elapsed()),
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    None
}

fn wait_for_http(port: u16) {
    let started = Instant::now();
    while started.elapsed() < PATIENCE {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the server never started listening on {port}");
}

/// Spawn the server with stdin held open, which is what an MCP client does.
fn spawn(dir: &TempDir, args: &[&str]) -> Child {
    Command::new(binary())
        .args([
            "--data-dir",
            dir.path().to_str().expect("utf-8 path"),
            "--log",
            "warn",
            "serve",
        ])
        .args(args)
        .args(["--tenant", "acme"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the server binary should start")
}

fn assert_stops(mut child: Child, signal_name: &str, context: &str) {
    signal(&child, signal_name);
    match wait_for_exit(&mut child) {
        Some(elapsed) => assert!(elapsed < PATIENCE, "{context}: took {elapsed:?} to stop"),
        None => {
            let _ = child.kill();
            panic!("{context}: still running {PATIENCE:?} after SIG{signal_name}");
        }
    }
}

#[test]
fn sigint_stops_the_stdio_transport_even_though_it_blocks_on_stdin() {
    let dir = TempDir::new().unwrap();
    let child = spawn(&dir, &["--transport", "stdio"]);
    // Give it time to reach the blocking read on stdin, which is held open by
    // the piped handle we never close.
    std::thread::sleep(Duration::from_millis(500));
    assert_stops(child, "INT", "stdio transport");
}

#[test]
fn sigint_stops_both_transports_at_once() {
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let child = spawn(
        &dir,
        &[
            "--transport",
            "both",
            "--port",
            &port.to_string(),
            "--api-key",
            "k",
        ],
    );
    wait_for_http(port);
    assert_stops(child, "INT", "both transports");
}

#[test]
fn sigterm_stops_the_server_too() {
    // Docker, Kubernetes and systemd all send SIGTERM, not SIGINT.
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let child = spawn(
        &dir,
        &[
            "--transport",
            "http",
            "--port",
            &port.to_string(),
            "--api-key",
            "k",
        ],
    );
    wait_for_http(port);
    assert_stops(child, "TERM", "http transport");
}

#[test]
fn closing_stdin_stops_the_process() {
    // An MCP client that exits closes the pipe. The server should follow it out
    // rather than lingering.
    let dir = TempDir::new().unwrap();
    let mut child = spawn(&dir, &["--transport", "stdio"]);
    drop(child.stdin.take());
    match wait_for_exit(&mut child) {
        Some(_) => {}
        None => {
            let _ = child.kill();
            panic!("the server outlived its stdin");
        }
    }
}

#[test]
fn shutdown_flushes_buffered_rows_into_segments() {
    let dir = TempDir::new().unwrap();
    let mut child = spawn(&dir, &["--transport", "stdio"]);

    // Write some rows over MCP, then stop the server without ever asking it to
    // flush. A graceful stop should checkpoint them anyway.
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
    let session = [
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"database_create","arguments":{"database":"crm"}}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"table_create","arguments":{"database":"crm","table":"events","columns":[{"name":"at","type":"timestamp","nullable":false},{"name":"kind","type":"utf8"}]}}}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"data_insert","arguments":{"database":"crm","table":"events","rows":[{"at":"2026-01-01T00:00:00Z","kind":"click"},{"at":"2026-01-02T00:00:00Z","kind":"view"}]}}}"#,
    ];
    for line in session {
        writeln!(stdin, "{line}").expect("the server should accept input");
        stdin.flush().expect("flush");
        let mut response = String::new();
        stdout
            .read_line(&mut response)
            .expect("a response per request");
        assert!(
            response.contains("\"isError\": false") || response.contains("\"isError\":false"),
            "tool call failed: {response}"
        );
    }

    signal(&child, "INT");
    assert!(
        wait_for_exit(&mut child).is_some(),
        "the server did not stop"
    );

    // Reopening also proves the data directory lock was released.
    let engine = Engine::open(EngineConfig::new(dir.path())).expect("the lock should be free");
    let ctx = RequestContext::root("acme").with_database(DatabaseName::new("crm").unwrap());
    let stats = engine
        .table_stats(&ctx, &TableName::new("events").unwrap())
        .expect("the table should still be there");
    assert_eq!(stats.rows, 2, "rows must survive the shutdown");
    assert!(
        stats.segments > 0,
        "a graceful shutdown should have flushed the memtable into a segment, got {stats:?}"
    );
}
