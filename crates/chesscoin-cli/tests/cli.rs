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
            "--mine",
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
            "--mine",
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

fn reserve_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve local port");
    listener.local_addr().expect("local addr").to_string()
}
