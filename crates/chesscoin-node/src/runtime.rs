use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chesscoin_core::application::{block_hash, ChainConfig, ChainState};
use chesscoin_core::domain::Digest;

use crate::adapters::{DeterministicSampler, ToyHash};
use crate::wire::{decode_message, encode_message, PeerMessage};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub chain: ChainConfig,
    pub mine_on_start: bool,
}

impl NodeConfig {
    pub fn localhost_ephemeral(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            peers: Vec::new(),
            chain: ChainConfig::default(),
            mine_on_start: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub bound_addr: SocketAddr,
    pub height: u64,
    pub head: Digest,
    pub accepted_blocks: usize,
    pub rejected_blocks: usize,
    pub known_peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug)]
pub enum NodeCommand {
    MineOnce {
        training_seed: u64,
        sampling_entropy: u64,
    },
    AddPeer(SocketAddr),
    Stop,
}

pub struct RunningNode {
    bound_addr: SocketAddr,
    state: Arc<Mutex<NodeState>>,
    stop: Arc<AtomicBool>,
    commands: mpsc::Sender<NodeCommand>,
    handle: Option<JoinHandle<()>>,
}

impl RunningNode {
    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn send(&self, command: NodeCommand) -> Result<(), String> {
        self.commands
            .send(command)
            .map_err(|error| format!("node command failed: {error}"))
    }

    pub fn snapshot(&self) -> NodeSnapshot {
        snapshot(&self.state)
    }

    pub fn stop(mut self) -> NodeSnapshot {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.commands.send(NodeCommand::Stop);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.snapshot()
    }
}

struct NodeState {
    node_id: String,
    bound_addr: SocketAddr,
    chain: ChainState,
    peers: Vec<SocketAddr>,
    accepted_blocks: usize,
    rejected_blocks: usize,
}

pub fn start_node(config: NodeConfig) -> Result<RunningNode, String> {
    let listener = TcpListener::bind(config.listen_addr)
        .map_err(|error| format!("failed to bind {}: {error}", config.listen_addr))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set listener nonblocking: {error}"))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read listener addr: {error}"))?;

    let (command_tx, command_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Mutex::new(NodeState {
        node_id: config.node_id,
        bound_addr,
        chain: ChainState::new(config.chain),
        peers: config.peers,
        accepted_blocks: 0,
        rejected_blocks: 0,
    }));

    let thread_state = Arc::clone(&state);
    let thread_stop = Arc::clone(&stop);
    let handle = thread::spawn(move || node_loop(listener, thread_state, thread_stop, command_rx));

    let node = RunningNode {
        bound_addr,
        state,
        stop,
        commands: command_tx,
        handle: Some(handle),
    };

    if config.mine_on_start {
        node.send(NodeCommand::MineOnce {
            training_seed: timestamp_seed(),
            sampling_entropy: timestamp_seed().rotate_left(7),
        })?;
    }

    Ok(node)
}

fn node_loop(
    listener: TcpListener,
    state: Arc<Mutex<NodeState>>,
    stop: Arc<AtomicBool>,
    command_rx: mpsc::Receiver<NodeCommand>,
) {
    while !stop.load(Ordering::SeqCst) {
        accept_ready_connections(&listener, &state);

        while let Ok(command) = command_rx.try_recv() {
            match command {
                NodeCommand::MineOnce {
                    training_seed,
                    sampling_entropy,
                } => mine_once(&state, training_seed, sampling_entropy),
                NodeCommand::AddPeer(peer) => add_peer(&state, peer),
                NodeCommand::Stop => {
                    stop.store(true, Ordering::SeqCst);
                }
            }
        }

        thread::sleep(Duration::from_millis(10));
    }
}

fn accept_ready_connections(listener: &TcpListener, state: &Arc<Mutex<NodeState>>) {
    loop {
        match listener.accept() {
            Ok((stream, _remote_addr)) => handle_stream(stream, state),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}

fn handle_stream(stream: TcpStream, state: &Arc<Mutex<NodeState>>) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
        return;
    }

    match decode_message(&line) {
        Ok(PeerMessage::Hello { .. }) => {}
        Ok(PeerMessage::Block(block)) => {
            let block = *block;
            let accepted = {
                let mut guard = state.lock().expect("node state lock poisoned");
                let hasher = ToyHash;
                let sampler = DeterministicSampler;
                match guard.chain.apply_block(&hasher, &sampler, block.clone()) {
                    Ok(_) => {
                        guard.accepted_blocks += 1;
                        true
                    }
                    Err(_) => {
                        guard.rejected_blocks += 1;
                        false
                    }
                }
            };

            if accepted {
                broadcast_block(state, &block);
            }
        }
        Err(_) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.rejected_blocks += 1;
        }
    }
}

fn mine_once(state: &Arc<Mutex<NodeState>>, training_seed: u64, sampling_entropy: u64) {
    let block = {
        let mut guard = state.lock().expect("node state lock poisoned");
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let block = guard
            .chain
            .mine_next_block(&hasher, &sampler, training_seed, sampling_entropy);
        guard
            .chain
            .apply_block(&hasher, &sampler, block.clone())
            .expect("locally mined block should validate");
        block
    };

    broadcast_block(state, &block);
}

fn add_peer(state: &Arc<Mutex<NodeState>>, peer: SocketAddr) {
    let mut guard = state.lock().expect("node state lock poisoned");
    if peer != guard.bound_addr && !guard.peers.contains(&peer) {
        guard.peers.push(peer);
    }
}

fn broadcast_block(state: &Arc<Mutex<NodeState>>, block: &chesscoin_core::domain::Block) {
    let peers = {
        let guard = state.lock().expect("node state lock poisoned");
        guard.peers.clone()
    };
    let message = encode_message(&PeerMessage::Block(Box::new(block.clone()))) + "\n";

    for peer in peers {
        if let Ok(mut stream) = TcpStream::connect_timeout(&peer, Duration::from_millis(250)) {
            let _ = stream.write_all(message.as_bytes());
            let _ = stream.flush();
        }
    }
}

fn snapshot(state: &Arc<Mutex<NodeState>>) -> NodeSnapshot {
    let guard = state.lock().expect("node state lock poisoned");
    let hasher = ToyHash;
    NodeSnapshot {
        node_id: guard.node_id.clone(),
        bound_addr: guard.bound_addr,
        height: guard.chain.height(),
        head: guard.chain.head_hash(&hasher),
        accepted_blocks: guard.accepted_blocks,
        rejected_blocks: guard.rejected_blocks,
        known_peers: guard.peers.clone(),
    }
}

fn timestamp_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(42)
}

pub fn block_id(block: &chesscoin_core::domain::Block) -> Digest {
    block_hash(&ToyHash, block)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_for_height(node: &RunningNode, height: u64) -> NodeSnapshot {
        for _ in 0..100 {
            let snapshot = node.snapshot();
            if snapshot.height >= height {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    #[test]
    fn two_nodes_share_a_mined_block() {
        let mut config_a = NodeConfig::localhost_ephemeral("node-a");
        config_a.chain.difficulty_zero_bits = 0;
        config_a.chain.steps_per_block = 4;
        config_a.chain.samples_per_block = 4;
        let node_a = start_node(config_a).expect("node a starts");

        let mut config_b = NodeConfig::localhost_ephemeral("node-b");
        config_b.chain.difficulty_zero_bits = 0;
        config_b.chain.steps_per_block = 4;
        config_b.chain.samples_per_block = 4;
        config_b.peers.push(node_a.bound_addr());
        let node_b = start_node(config_b).expect("node b starts");

        node_b
            .send(NodeCommand::MineOnce {
                training_seed: 123,
                sampling_entropy: 456,
            })
            .expect("mine command sends");

        let snapshot_a = wait_for_height(&node_a, 1);
        let snapshot_b = wait_for_height(&node_b, 1);

        assert_eq!(snapshot_a.height, 1);
        assert_eq!(snapshot_b.height, 1);
        assert_eq!(snapshot_a.head, snapshot_b.head);
        assert_eq!(snapshot_a.accepted_blocks, 1);

        node_b.stop();
        node_a.stop();
    }
}
