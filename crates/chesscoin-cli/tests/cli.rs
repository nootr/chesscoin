use std::io::{BufRead, BufReader, Read};
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::Duration;

use chesscoin_core::application::ChainConfig;
use chesscoin_node::wire::chain_fingerprint;

fn chesscoin_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chesscoin")
}

#[test]
fn simulate_command_accepts_honest_trace() {
    let output = Command::new(chesscoin_bin())
        .args(["simulate", "--steps", "4", "--samples", "4"])
        .output()
        .expect("simulate command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("outcome              ACCEPT"));
}

#[test]
fn simulate_command_rejects_zero_samples() {
    let output = Command::new(chesscoin_bin())
        .args(["simulate", "--steps", "4", "--samples", "0"])
        .output()
        .expect("simulate command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--samples must be greater than zero"),
        "{stderr}"
    );
}

#[test]
fn node_command_mines_one_block() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--mine-once",
            "--run-ms",
            "200",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ChessCoin node v0.1"));
    assert!(stdout.contains("final height         1"));
    assert!(stdout.contains("known blocks         1"));
    assert!(stdout.contains("failed responses     0"));
}

#[test]
fn node_command_rejects_unmineable_difficulty() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--mine-once",
            "--run-ms",
            "200",
            "--difficulty",
            "257",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("difficulty_zero_bits"), "{stderr}");
}

#[test]
fn node_command_rejects_zero_write_timeout() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--run-ms",
            "200",
            "--write-timeout-ms",
            "0",
        ])
        .output()
        .expect("node command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("write_timeout"), "{stderr}");
}

#[test]
fn node_command_rejects_zero_max_inbound_connections() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--run-ms",
            "200",
            "--max-inbound-connections",
            "0",
        ])
        .output()
        .expect("node command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("max_inbound_connections"), "{stderr}");
}

#[test]
fn node_command_requires_advertise_for_unspecified_listener() {
    let output = Command::new(chesscoin_bin())
        .args(["node", "--listen", "0.0.0.0:0", "--run-ms", "1"])
        .output()
        .expect("node command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("advertise_addr is required"), "{stderr}");
}

#[test]
fn node_command_prints_configured_advertise_address() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--advertise",
            "127.0.0.1:39393",
            "--run-ms",
            "1",
        ])
        .output()
        .expect("node command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("advertising          127.0.0.1:39393"),
        "{stdout}"
    );
}

#[test]
fn two_cli_nodes_exchange_a_mined_block() {
    let _guard = multi_node_test_guard();
    let (node_a, addr_a) = spawn_node_and_wait_for_listening(&[
        "node",
        "--listen",
        "127.0.0.1:0",
        "--run-ms",
        "2500",
        "--difficulty",
        "0",
        "--steps",
        "4",
        "--samples",
        "4",
    ]);

    let output_b = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--peer",
            &addr_a,
            "--mine-once",
            "--run-ms",
            "800",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node b runs");

    let output_a = node_a.wait();

    assert!(output_b.status.success());
    let stdout_b = String::from_utf8_lossy(&output_b.stdout);
    assert!(stdout_b.contains("final height         1"), "{stdout_b}");
    assert!(stdout_b.contains("mined blocks         1"), "{stdout_b}");

    assert!(
        output_a.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        output_a.stdout,
        output_a.stderr
    );
    let stdout_a = output_a.stdout;

    assert!(stdout_a.contains("final height         1"), "{stdout_a}");
    assert!(stdout_a.contains("accepted blocks      1"), "{stdout_a}");
    assert!(stdout_a.contains("known peers          1"), "{stdout_a}");
}

#[test]
fn late_cli_node_syncs_existing_block_from_peer() {
    let _guard = multi_node_test_guard();
    let (node_a, addr_a) = spawn_node_and_wait_for_listening(&[
        "node",
        "--listen",
        "127.0.0.1:0",
        "--mine-once",
        "--run-ms",
        "1600",
        "--difficulty",
        "0",
        "--steps",
        "4",
        "--samples",
        "4",
    ]);

    thread::sleep(Duration::from_millis(300));

    let output_b = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--peer",
            &addr_a,
            "--sync-interval-ms",
            "50",
            "--run-ms",
            "700",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node b runs");

    let output_a = node_a.wait();

    let stdout_b = String::from_utf8_lossy(&output_b.stdout);
    let stderr_b = String::from_utf8_lossy(&output_b.stderr);
    assert!(
        output_b.status.success(),
        "stdout:\n{stdout_b}\nstderr:\n{stderr_b}"
    );
    assert!(stdout_b.contains("final height         1"), "{stdout_b}");
    assert!(stdout_b.contains("synced blocks        1"), "{stdout_b}");

    assert!(
        output_a.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        output_a.stdout,
        output_a.stderr
    );
}

#[test]
fn node_command_continuously_mines_blocks() {
    let output = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            "127.0.0.1:0",
            "--mine",
            "--mine-interval-ms",
            "50",
            "--run-ms",
            "220",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("miner                continuous"),
        "{stdout}"
    );
    assert!(stdout.contains("mined blocks         "), "{stdout}");
    assert!(stdout.contains("known blocks         "), "{stdout}");
    assert!(
        stdout.contains("final height         3")
            || stdout.contains("final height         4")
            || stdout.contains("final height         5"),
        "{stdout}"
    );
}

#[test]
fn node_command_reads_config_file() {
    let config_path = std::env::temp_dir().join(format!(
        "chesscoin-node-{}.conf",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::write(
        &config_path,
        "listen=127.0.0.1:0\nrun_ms=200\ndifficulty=0\nsteps=4\nsamples=4\nmine_once=true\nmax_message_bytes=4096\nmax_peers=8\nmax_inbound_connections=7\nsync_locator_hashes=16\n",
    )
    .expect("write config");

    let output = Command::new(chesscoin_bin())
        .args(["node", "--config", config_path.to_str().expect("path")])
        .output()
        .expect("node command runs");

    let _ = std::fs::remove_file(config_path);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("final height         1"), "{stdout}");
    assert!(
        stdout.contains(
            &format!(
                "network              id=chesscoin-local protocol=6 chain={} max_message_bytes=4096 max_peers=8 max_inbound_connections=7",
                chain_fingerprint(&ChainConfig {
                    steps_per_block: 4,
                    samples_per_block: 4,
                    difficulty_zero_bits: 0,
                })
            ),
        ),
        "{stdout}"
    );
}

fn multi_node_test_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct RunningCliNode {
    child: Child,
    stdout_reader: thread::JoinHandle<String>,
}

struct CliNodeOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

impl RunningCliNode {
    fn wait(mut self) -> CliNodeOutput {
        let status = self.child.wait().expect("node exits");
        let stdout = self.stdout_reader.join().expect("stdout reader exits");
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut stderr).expect("read stderr");
        }

        CliNodeOutput {
            status,
            stdout,
            stderr,
        }
    }
}

fn spawn_node_and_wait_for_listening(args: &[&str]) -> (RunningCliNode, String) {
    let mut child = Command::new(chesscoin_bin())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("node starts");
    let stdout = child.stdout.take().expect("node stdout is piped");
    let (addr_tx, addr_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        let mut captured = String::new();
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("read node stdout");
            if let Some(addr) = line.strip_prefix("listening") {
                let _ = addr_tx.send(addr.trim().to_string());
            }
            captured.push_str(&line);
            captured.push('\n');
        }
        captured
    });
    let addr = match addr_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(addr) => addr,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("node did not report listening address: {error}");
        }
    };

    (
        RunningCliNode {
            child,
            stdout_reader,
        },
        addr,
    )
}
