use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

use chesscoin_core::application::{ChainConfig, ProtocolSimulator, SimulationRequest};
use chesscoin_core::domain::VerificationOutcome;
use chesscoin_node::adapters::{DeterministicSampler, ToyHash};
use chesscoin_node::runtime::{start_node, NodeCommand, NodeConfig};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!();
            print_usage();
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if matches!(
        args.first().map(String::as_str),
        Some("-h" | "--help" | "help")
    ) {
        print_usage();
        return Ok(());
    }

    match args.first().map(String::as_str) {
        Some("simulate") => {
            args.remove(0);
            run_simulate(&args)
        }
        Some("node") => {
            args.remove(0);
            run_node(&args)
        }
        Some(command) if !command.starts_with('-') => Err(format!("unknown command '{command}'")),
        _ => run_simulate(&args),
    }
}

fn run_simulate(args: &[String]) -> Result<(), String> {
    let request = parse_simulation_request(args)?;
    if request
        .tamper_step
        .is_some_and(|index| index >= request.steps)
    {
        return Err("--tamper-step must be less than --steps".to_string());
    }

    let simulator = ProtocolSimulator::new(ToyHash, DeterministicSampler);
    let report = simulator.run(request.clone());

    println!("ChessCoin local protocol simulator");
    println!("----------------------------------");
    println!(
        "M_t                  {}",
        report.committed_trace.initial_model
    );
    println!("training steps       {}", request.steps);
    println!("seed                 {}", request.seed);
    println!("trace root           {}", report.committed_trace.root);
    println!(
        "M_t+1                {}",
        report.committed_trace.candidate_model
    );
    println!("sampling entropy     {}", request.sampling_entropy);
    println!("sampled indices      {:?}", report.sampled_indices);
    if let Some(index) = report.tamper_applied {
        println!("tamper applied       step {index}");
    }
    println!();
    println!("sample verification");
    for sample in &report.samples {
        println!(
            "  step {:>3}: commitment={} transition={}",
            sample.index,
            verdict(sample.commitment_ok),
            verdict(sample.transition_ok)
        );
    }
    println!();

    match report.outcome {
        VerificationOutcome::Accepted => {
            println!("outcome              ACCEPT");
        }
        VerificationOutcome::Rejected { failed_indices } => {
            println!("outcome              REJECT");
            println!("failed indices       {:?}", failed_indices);
        }
    }

    Ok(())
}

fn run_node(args: &[String]) -> Result<(), String> {
    let request = parse_node_request(args)?;
    let node = start_node(request.config)?;
    let startup = node.snapshot();

    println!("ChessCoin node v0.1");
    println!("-------------------");
    println!("node id              {}", startup.node_id);
    println!("listening            {}", node.bound_addr());
    println!("peers                {:?}", startup.known_peers);
    println!("height               {}", startup.height);

    if request.mine {
        node.send(NodeCommand::MineOnce {
            training_seed: request.training_seed,
            sampling_entropy: request.sampling_entropy,
        })?;
        println!("mining               one block requested");
    }

    if request.run_ms == 0 {
        println!("mode                 running until process is stopped");
        loop {
            thread::sleep(Duration::from_secs(1));
            let snapshot = node.snapshot();
            println!(
                "status               height={} accepted={} rejected={} head={}",
                snapshot.height, snapshot.accepted_blocks, snapshot.rejected_blocks, snapshot.head
            );
        }
    }

    thread::sleep(Duration::from_millis(request.run_ms));
    let snapshot = node.stop();
    println!("final height         {}", snapshot.height);
    println!("accepted blocks      {}", snapshot.accepted_blocks);
    println!("rejected blocks      {}", snapshot.rejected_blocks);
    println!("head                 {}", snapshot.head);

    Ok(())
}

fn parse_simulation_request(args: &[String]) -> Result<SimulationRequest, String> {
    let mut request = SimulationRequest::default();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--steps" => {
                request.steps = parse_next(args, &mut index, "--steps")?;
            }
            "--samples" => {
                request.samples = parse_next(args, &mut index, "--samples")?;
            }
            "--seed" => {
                request.seed = parse_next(args, &mut index, "--seed")?;
            }
            "--entropy" => {
                request.sampling_entropy = parse_next(args, &mut index, "--entropy")?;
            }
            "--tamper-step" => {
                request.tamper_step = Some(parse_next(args, &mut index, "--tamper-step")?);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown option '{unknown}'")),
        }
        index += 1;
    }

    Ok(request)
}

struct NodeRequest {
    config: NodeConfig,
    mine: bool,
    run_ms: u64,
    training_seed: u64,
    sampling_entropy: u64,
}

fn parse_node_request(args: &[String]) -> Result<NodeRequest, String> {
    let mut request = NodeRequest {
        config: NodeConfig {
            node_id: "chesscoin-node".to_string(),
            listen_addr: parse_socket_addr("127.0.0.1:9333")?,
            peers: Vec::new(),
            chain: ChainConfig::default(),
            mine_on_start: false,
        },
        mine: false,
        run_ms: 0,
        training_seed: 42,
        sampling_entropy: 2_026,
    };
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--node-id" => {
                request.config.node_id = parse_next(args, &mut index, "--node-id")?;
            }
            "--listen" => {
                request.config.listen_addr =
                    parse_next::<SocketAddr>(args, &mut index, "--listen")?;
            }
            "--peer" => {
                request
                    .config
                    .peers
                    .push(parse_next::<SocketAddr>(args, &mut index, "--peer")?);
            }
            "--mine" => {
                request.mine = true;
            }
            "--run-ms" => {
                request.run_ms = parse_next(args, &mut index, "--run-ms")?;
            }
            "--steps" => {
                request.config.chain.steps_per_block = parse_next(args, &mut index, "--steps")?;
            }
            "--samples" => {
                request.config.chain.samples_per_block = parse_next(args, &mut index, "--samples")?;
            }
            "--difficulty" => {
                request.config.chain.difficulty_zero_bits =
                    parse_next(args, &mut index, "--difficulty")?;
            }
            "--seed" => {
                request.training_seed = parse_next(args, &mut index, "--seed")?;
            }
            "--entropy" => {
                request.sampling_entropy = parse_next(args, &mut index, "--entropy")?;
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown option '{unknown}'")),
        }
        index += 1;
    }

    if request.config.chain.samples_per_block > request.config.chain.steps_per_block {
        return Err("--samples must be less than or equal to --steps".to_string());
    }

    Ok(request)
}

fn parse_next<T>(args: &[String], index: &mut usize, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    *index += 1;
    args.get(*index)
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<T>()
        .map_err(|_| format!("{flag} received an invalid value"))
}

fn parse_socket_addr(input: &str) -> Result<SocketAddr, String> {
    input
        .parse::<SocketAddr>()
        .map_err(|_| format!("invalid socket address '{input}'"))
}

fn verdict(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "fail"
    }
}

fn print_usage() {
    println!(
        "Usage:
  chesscoin simulate [--steps N] [--samples N] [--seed N] [--entropy N] [--tamper-step N]
  chesscoin node [--listen ADDR] [--peer ADDR] [--mine] [--run-ms N]

Defaults:
  simulate: --steps 16 --samples 6 --seed 42 --entropy 2026
  node:     --listen 127.0.0.1:9333 --difficulty 8

Examples:
  cargo run -p chesscoin-cli -- simulate
  cargo run -p chesscoin-cli -- simulate --steps 8 --samples 8 --tamper-step 3
  cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine --run-ms 1000
  cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9334 --peer 127.0.0.1:9333"
    );
}
