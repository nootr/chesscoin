use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

use chesscoin_core::application::{ChainConfig, ProtocolSimulator, SimulationRequest};
use chesscoin_core::domain::VerificationOutcome;
use chesscoin_node::adapters::{DeterministicSampler, ToyHash};
use chesscoin_node::runtime::{
    start_node, validate_node_config, MinerConfig, NetworkConfig, NodeCommand, NodeConfig,
    StorageConfig, SyncConfig,
};
use chesscoin_node::wire::chain_fingerprint;

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
    validate_simulation_request(&request)?;
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
    println!("advertising          {}", startup.advertised_addr);
    println!("peers                {:?}", startup.known_peers);
    println!("height               {}", startup.height);
    println!(
        "network              id={} protocol={} chain={} max_message_bytes={} max_peers={} max_inbound_connections={}",
        request.network_id,
        request.protocol_version,
        request.chain_fingerprint,
        request.network_max_message_bytes,
        request.network_max_peers,
        request.network_max_inbound_connections
    );

    if request.mine_once {
        node.send(NodeCommand::MineOnce {
            training_seed: request.training_seed,
            sampling_entropy: request.sampling_entropy,
        })?;
        println!("mining               one block requested");
    }

    if request.mining_enabled {
        println!(
            "miner                continuous interval={}ms",
            request.mining_interval.as_millis()
        );
    }

    if request.run_ms == 0 {
        println!("mode                 running until process is stopped");
        loop {
            thread::sleep(Duration::from_secs(1));
            let snapshot = node.snapshot();
            println!(
                "status               height={} mined={} known={} accepted={} rejected={} reorgs={} malformed={} incompatible={} oversized={} outbound={} failed_broadcasts={} hellos={} failed_hellos={} dropped_inbound={} storage_failures={} peer_rejections={} peers={} head={}",
                snapshot.height,
                snapshot.mined_blocks,
                snapshot.known_blocks,
                snapshot.accepted_blocks,
                snapshot.rejected_blocks,
                snapshot.reorgs,
                snapshot.malformed_messages,
                snapshot.incompatible_messages,
                snapshot.oversized_messages,
                snapshot.outbound_blocks,
                snapshot.failed_broadcasts,
                snapshot.hello_announcements,
                snapshot.failed_hello_announcements,
                snapshot.dropped_inbound_connections,
                snapshot.storage_failures,
                snapshot.peer_rejections,
                snapshot.known_peers.len(),
                snapshot.head
            );
        }
    }

    thread::sleep(Duration::from_millis(request.run_ms));
    let snapshot = node.stop();
    println!("final height         {}", snapshot.height);
    println!("accepted blocks      {}", snapshot.accepted_blocks);
    println!("rejected blocks      {}", snapshot.rejected_blocks);
    println!("mined blocks         {}", snapshot.mined_blocks);
    println!("known blocks         {}", snapshot.known_blocks);
    println!("reorgs               {}", snapshot.reorgs);
    println!("synced blocks        {}", snapshot.synced_blocks);
    println!("failed syncs         {}", snapshot.failed_syncs);
    println!("malformed messages   {}", snapshot.malformed_messages);
    println!("incompatible messages {}", snapshot.incompatible_messages);
    println!("oversized messages   {}", snapshot.oversized_messages);
    println!("outbound blocks      {}", snapshot.outbound_blocks);
    println!("failed broadcasts    {}", snapshot.failed_broadcasts);
    println!("hello announcements  {}", snapshot.hello_announcements);
    println!(
        "failed hellos        {}",
        snapshot.failed_hello_announcements
    );
    println!(
        "dropped inbound      {}",
        snapshot.dropped_inbound_connections
    );
    println!("storage failures     {}", snapshot.storage_failures);
    println!("peer rejections      {}", snapshot.peer_rejections);
    println!("known peers          {}", snapshot.known_peers.len());
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

fn validate_simulation_request(request: &SimulationRequest) -> Result<(), String> {
    if request.steps == 0 {
        return Err("--steps must be greater than zero".to_string());
    }
    if request.samples == 0 {
        return Err("--samples must be greater than zero".to_string());
    }
    if request.samples > request.steps {
        return Err("--samples must be less than or equal to --steps".to_string());
    }

    Ok(())
}

struct NodeRequest {
    config: NodeConfig,
    mine_once: bool,
    mining_enabled: bool,
    mining_interval: Duration,
    network_max_message_bytes: usize,
    network_max_peers: usize,
    network_max_inbound_connections: usize,
    chain_fingerprint: String,
    network_id: String,
    protocol_version: u16,
    run_ms: u64,
    training_seed: u64,
    sampling_entropy: u64,
}

fn parse_node_request(args: &[String]) -> Result<NodeRequest, String> {
    let args = expand_config_args(args)?;
    let network_defaults = NetworkConfig::default();
    let mut request = NodeRequest {
        config: NodeConfig {
            node_id: "chesscoin-node".to_string(),
            listen_addr: parse_socket_addr("127.0.0.1:9333")?,
            advertise_addr: None,
            peers: Vec::new(),
            chain: ChainConfig::default(),
            mine_once_on_start: false,
            miner: None,
            storage: None,
            network: network_defaults.clone(),
            sync: SyncConfig::default(),
        },
        mine_once: false,
        mining_enabled: false,
        mining_interval: Duration::from_secs(5),
        network_max_message_bytes: network_defaults.max_message_bytes,
        network_max_peers: network_defaults.max_peers,
        network_max_inbound_connections: network_defaults.max_inbound_connections,
        chain_fingerprint: network_defaults.wire.chain_fingerprint.clone(),
        network_id: network_defaults.wire.network_id.clone(),
        protocol_version: network_defaults.wire.protocol_version,
        run_ms: 0,
        training_seed: 42,
        sampling_entropy: 2_026,
    };
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
            }
            "--node-id" => {
                request.config.node_id = parse_next(&args, &mut index, "--node-id")?;
            }
            "--listen" => {
                request.config.listen_addr =
                    parse_next::<SocketAddr>(&args, &mut index, "--listen")?;
            }
            "--advertise" => {
                request.config.advertise_addr =
                    Some(parse_next::<SocketAddr>(&args, &mut index, "--advertise")?);
            }
            "--peer" => {
                request
                    .config
                    .peers
                    .push(parse_next::<SocketAddr>(&args, &mut index, "--peer")?);
            }
            "--mine" => {
                request.mining_enabled = true;
                request.config.miner = Some(MinerConfig {
                    interval: request.mining_interval,
                });
            }
            "--mine-once" => {
                request.mine_once = true;
            }
            "--mine-interval-ms" => {
                let millis = parse_next(&args, &mut index, "--mine-interval-ms")?;
                request.mining_interval = Duration::from_millis(millis);
                if request.mining_enabled {
                    request.config.miner = Some(MinerConfig {
                        interval: request.mining_interval,
                    });
                }
            }
            "--run-ms" => {
                request.run_ms = parse_next(&args, &mut index, "--run-ms")?;
            }
            "--data-dir" => {
                let data_dir = parse_next::<PathBuf>(&args, &mut index, "--data-dir")?;
                request.config.storage = Some(StorageConfig { data_dir });
            }
            "--max-message-bytes" => {
                request.network_max_message_bytes =
                    parse_next(&args, &mut index, "--max-message-bytes")?;
                request.config.network.max_message_bytes = request.network_max_message_bytes;
            }
            "--max-peers" => {
                request.network_max_peers = parse_next(&args, &mut index, "--max-peers")?;
                request.config.network.max_peers = request.network_max_peers;
            }
            "--max-inbound-connections" => {
                request.network_max_inbound_connections =
                    parse_next(&args, &mut index, "--max-inbound-connections")?;
                request.config.network.max_inbound_connections =
                    request.network_max_inbound_connections;
            }
            "--network-id" => {
                request.network_id = parse_next(&args, &mut index, "--network-id")?;
                request.config.network.wire.network_id = request.network_id.clone();
            }
            "--protocol-version" => {
                request.protocol_version = parse_next(&args, &mut index, "--protocol-version")?;
                request.config.network.wire.protocol_version = request.protocol_version;
            }
            "--connect-timeout-ms" => {
                let millis = parse_next(&args, &mut index, "--connect-timeout-ms")?;
                request.config.network.connect_timeout = Duration::from_millis(millis);
            }
            "--read-timeout-ms" => {
                let millis = parse_next(&args, &mut index, "--read-timeout-ms")?;
                request.config.network.read_timeout = Duration::from_millis(millis);
            }
            "--write-timeout-ms" => {
                let millis = parse_next(&args, &mut index, "--write-timeout-ms")?;
                request.config.network.write_timeout = Duration::from_millis(millis);
            }
            "--sync-interval-ms" => {
                let millis = parse_next(&args, &mut index, "--sync-interval-ms")?;
                request.config.sync.interval = Duration::from_millis(millis);
            }
            "--sync-max-blocks" => {
                request.config.sync.max_blocks_per_response =
                    parse_next(&args, &mut index, "--sync-max-blocks")?;
            }
            "--sync-locator-hashes" => {
                request.config.sync.max_locator_hashes =
                    parse_next(&args, &mut index, "--sync-locator-hashes")?;
            }
            "--no-sync" => {
                request.config.sync.enabled = false;
            }
            "--steps" => {
                request.config.chain.steps_per_block = parse_next(&args, &mut index, "--steps")?;
            }
            "--samples" => {
                request.config.chain.samples_per_block =
                    parse_next(&args, &mut index, "--samples")?;
            }
            "--difficulty" => {
                request.config.chain.difficulty_zero_bits =
                    parse_next(&args, &mut index, "--difficulty")?;
            }
            "--seed" => {
                request.training_seed = parse_next(&args, &mut index, "--seed")?;
            }
            "--entropy" => {
                request.sampling_entropy = parse_next(&args, &mut index, "--entropy")?;
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown option '{unknown}'")),
        }
        index += 1;
    }

    validate_node_config(&request.config)?;
    request.chain_fingerprint = chain_fingerprint(&request.config.chain);
    request.config.network.wire.chain_fingerprint = request.chain_fingerprint.clone();

    Ok(request)
}

fn expand_config_args(args: &[String]) -> Result<Vec<String>, String> {
    let mut expanded = Vec::new();
    let mut index = 0;

    while index < args.len() {
        if args[index] == "--config" {
            let Some(path) = args.get(index + 1) else {
                return Err("--config requires a value".to_string());
            };
            expanded.extend(read_config_args(path)?);
            expanded.push(args[index].clone());
            expanded.push(path.clone());
            index += 2;
        } else {
            expanded.push(args[index].clone());
            index += 1;
        }
    }

    Ok(expanded)
}

fn read_config_args(path: &str) -> Result<Vec<String>, String> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read config {path}: {error}"))?;
    let mut args = Vec::new();

    for (line_number, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(format!(
                "invalid config line {}: expected key=value",
                line_number + 1
            ));
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return Err(format!(
                "invalid config line {}: empty key or value",
                line_number + 1
            ));
        }
        let flag = key.replace('_', "-");
        if matches!(flag.as_str(), "mine" | "mine-once") {
            match value {
                "true" | "yes" | "1" => args.push(format!("--{flag}")),
                "false" | "no" | "0" => {}
                _ => {
                    return Err(format!(
                        "invalid config line {}: boolean flag {key} must be true or false",
                        line_number + 1
                    ));
                }
            }
        } else {
            args.push(format!("--{flag}"));
            args.push(value.to_string());
        }
    }

    Ok(args)
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
  chesscoin node [--config PATH] [--listen ADDR] [--advertise ADDR] [--peer ADDR] [--mine] [--data-dir PATH] [--max-peers N] [--max-inbound-connections N] [--connect-timeout-ms N] [--read-timeout-ms N] [--write-timeout-ms N] [--sync-interval-ms N] [--sync-locator-hashes N]

Defaults:
  simulate: --steps 16 --samples 6 --seed 42 --entropy 2026
  node:     --listen 127.0.0.1:9333 --difficulty 8 --mine-interval-ms 5000

Examples:
  cargo run -p chesscoin-cli -- simulate
  cargo run -p chesscoin-cli -- simulate --steps 8 --samples 8 --tamper-step 3
  cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine --data-dir .chesscoin
  cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine-once --run-ms 1000
  cargo run -p chesscoin-cli -- node --config node.conf --peer 127.0.0.1:9333"
    );
}
