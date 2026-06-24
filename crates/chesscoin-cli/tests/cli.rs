use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;

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
}

#[test]
fn two_cli_nodes_exchange_a_mined_block() {
    let addr_a = reserve_local_addr();
    let addr_b = reserve_local_addr();

    let node_a = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            &addr_a,
            "--run-ms",
            "1500",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("node a starts");

    thread::sleep(Duration::from_millis(150));

    let output_b = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            &addr_b,
            "--peer",
            &addr_a,
            "--mine-once",
            "--run-ms",
            "400",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .output()
        .expect("node b runs");

    assert!(output_b.status.success());

    let output_a = node_a.wait_with_output().expect("node a exits");
    assert!(output_a.status.success());
    let stdout_a = String::from_utf8_lossy(&output_a.stdout);

    assert!(stdout_a.contains("final height         1"), "{stdout_a}");
    assert!(stdout_a.contains("accepted blocks      1"), "{stdout_a}");
}

#[test]
fn late_cli_node_syncs_existing_block_from_peer() {
    let addr_a = reserve_local_addr();
    let addr_b = reserve_local_addr();

    let node_a = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            &addr_a,
            "--mine-once",
            "--run-ms",
            "1600",
            "--difficulty",
            "0",
            "--steps",
            "4",
            "--samples",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("node a starts");

    thread::sleep(Duration::from_millis(300));

    let output_b = Command::new(chesscoin_bin())
        .args([
            "node",
            "--listen",
            &addr_b,
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

    let stdout_b = String::from_utf8_lossy(&output_b.stdout);
    let stderr_b = String::from_utf8_lossy(&output_b.stderr);
    assert!(
        output_b.status.success(),
        "stdout:\n{stdout_b}\nstderr:\n{stderr_b}"
    );
    assert!(stdout_b.contains("final height         1"), "{stdout_b}");
    assert!(stdout_b.contains("synced blocks        1"), "{stdout_b}");

    let output_a = node_a.wait_with_output().expect("node a exits");
    assert!(output_a.status.success());
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
        "listen=127.0.0.1:0\nrun_ms=200\ndifficulty=0\nsteps=4\nsamples=4\nmine_once=true\nmax_message_bytes=4096\nsync_locator_hashes=16\n",
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
        stdout
            .contains("network              id=chesscoin-local protocol=1 max_message_bytes=4096"),
        "{stdout}"
    );
}

fn reserve_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve local port");
    listener.local_addr().expect("local addr").to_string()
}
