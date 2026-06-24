use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chesscoin_core::application::{block_hash, ChainConfig, ChainState};
use chesscoin_core::domain::Digest;

use crate::adapters::{DeterministicSampler, ToyHash};
use crate::wire::{
    decode_block_message, decode_network_message, encode_block_message, encode_network_message,
    PeerMessage, WireConfig,
};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub chain: ChainConfig,
    pub mine_once_on_start: bool,
    pub miner: Option<MinerConfig>,
    pub storage: Option<StorageConfig>,
    pub network: NetworkConfig,
    pub sync: SyncConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinerConfig {
    pub interval: Duration,
}

impl Default for MinerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkConfig {
    pub wire: WireConfig,
    pub max_message_bytes: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub max_blocks_per_response: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            max_blocks_per_response: 128,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            wire: WireConfig::default(),
            max_message_bytes: 1024 * 1024,
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_secs(5),
        }
    }
}

impl NodeConfig {
    pub fn localhost_ephemeral(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            peers: Vec::new(),
            chain: ChainConfig::default(),
            mine_once_on_start: false,
            miner: None,
            storage: None,
            network: NetworkConfig::default(),
            sync: SyncConfig::default(),
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
    pub mined_blocks: usize,
    pub malformed_messages: usize,
    pub incompatible_messages: usize,
    pub oversized_messages: usize,
    pub outbound_blocks: usize,
    pub failed_broadcasts: usize,
    pub synced_blocks: usize,
    pub failed_syncs: usize,
    pub known_peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug)]
pub enum NodeCommand {
    MineOnce {
        training_seed: u64,
        sampling_entropy: u64,
    },
    AddPeer(SocketAddr),
    StartMining(MinerConfig),
    StopMining,
    SyncOnce,
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
    mined_blocks: usize,
    malformed_messages: usize,
    incompatible_messages: usize,
    oversized_messages: usize,
    outbound_blocks: usize,
    failed_broadcasts: usize,
    synced_blocks: usize,
    failed_syncs: usize,
    storage_path: Option<PathBuf>,
    network: NetworkConfig,
    sync: SyncConfig,
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
    let storage_path = config
        .storage
        .as_ref()
        .map(|storage| storage.data_dir.join("blocks.log"));
    let chain = if let Some(path) = storage_path.as_deref() {
        load_chain(path, config.chain.clone())?
    } else {
        ChainState::new(config.chain)
    };
    let initial_sync = config.sync.clone();
    let state = Arc::new(Mutex::new(NodeState {
        node_id: config.node_id,
        bound_addr,
        chain,
        peers: config.peers,
        accepted_blocks: 0,
        rejected_blocks: 0,
        mined_blocks: 0,
        malformed_messages: 0,
        incompatible_messages: 0,
        oversized_messages: 0,
        outbound_blocks: 0,
        failed_broadcasts: 0,
        synced_blocks: 0,
        failed_syncs: 0,
        storage_path,
        network: config.network,
        sync: config.sync,
    }));

    let thread_state = Arc::clone(&state);
    let thread_stop = Arc::clone(&stop);
    let initial_miner = config.miner.clone();
    let handle = thread::spawn(move || {
        node_loop(
            listener,
            thread_state,
            thread_stop,
            command_rx,
            initial_miner,
            initial_sync,
        )
    });

    let node = RunningNode {
        bound_addr,
        state,
        stop,
        commands: command_tx,
        handle: Some(handle),
    };

    if config.mine_once_on_start {
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
    initial_miner: Option<MinerConfig>,
    initial_sync: SyncConfig,
) {
    let mut miner = initial_miner;
    let mut next_mine_at = Instant::now();
    let sync = initial_sync;
    let mut next_sync_at = Instant::now();

    while !stop.load(Ordering::SeqCst) {
        accept_ready_connections(&listener, &state);

        while let Ok(command) = command_rx.try_recv() {
            match command {
                NodeCommand::MineOnce {
                    training_seed,
                    sampling_entropy,
                } => mine_once(&state, training_seed, sampling_entropy),
                NodeCommand::AddPeer(peer) => add_peer(&state, peer),
                NodeCommand::StartMining(config) => {
                    miner = Some(config);
                    next_mine_at = Instant::now();
                }
                NodeCommand::StopMining => {
                    miner = None;
                }
                NodeCommand::SyncOnce => {
                    sync_once(&state);
                    next_sync_at = Instant::now() + sync.interval;
                }
                NodeCommand::Stop => {
                    stop.store(true, Ordering::SeqCst);
                }
            }
        }

        if let Some(config) = miner.as_ref() {
            if Instant::now() >= next_mine_at {
                let seed = timestamp_seed();
                mine_once(&state, seed, seed.rotate_left(7));
                next_mine_at = Instant::now() + config.interval;
            }
        }

        if sync.enabled && Instant::now() >= next_sync_at {
            sync_once(&state);
            next_sync_at = Instant::now() + sync.interval;
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

fn handle_stream(mut stream: TcpStream, state: &Arc<Mutex<NodeState>>) {
    let network = {
        let guard = state.lock().expect("node state lock poisoned");
        guard.network.clone()
    };

    let line = match read_limited_line(&mut stream, &network) {
        Ok(Some(line)) => line,
        Ok(None) => return,
        Err(IncomingReadError::Oversized) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.rejected_blocks += 1;
            guard.oversized_messages += 1;
            return;
        }
        Err(IncomingReadError::Io) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.malformed_messages += 1;
            return;
        }
    };

    match decode_network_message(&line, &network.wire) {
        Ok(PeerMessage::Hello { .. }) => {}
        Ok(PeerMessage::GetBlocks { from_height, limit }) => {
            serve_blocks(stream, state, from_height, limit);
        }
        Ok(PeerMessage::Block(block)) => {
            let block = *block;
            let accepted = {
                let mut guard = state.lock().expect("node state lock poisoned");
                apply_incoming_block(&mut guard, block.clone(), BlockSource::Gossip)
            };

            if accepted {
                broadcast_block(state, &block);
            }
        }
        Ok(PeerMessage::EndBlocks) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.malformed_messages += 1;
        }
        Err(_) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.rejected_blocks += 1;
            if line.starts_with(crate::wire::WIRE_MAGIC) {
                guard.incompatible_messages += 1;
            } else {
                guard.malformed_messages += 1;
            }
        }
    }
}

fn serve_blocks(
    mut stream: TcpStream,
    state: &Arc<Mutex<NodeState>>,
    from_height: u64,
    requested_limit: usize,
) {
    let (blocks, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        let limit = requested_limit.min(guard.sync.max_blocks_per_response);
        let start = from_height as usize;
        let blocks = guard
            .chain
            .blocks()
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        (blocks, guard.network.clone())
    };

    let _ = stream.set_write_timeout(Some(network.connect_timeout));
    for block in blocks {
        let line =
            encode_network_message(&network.wire, &PeerMessage::Block(Box::new(block))) + "\n";
        if stream.write_all(line.as_bytes()).is_err() {
            return;
        }
    }
    let _ = stream.write_all(
        (encode_network_message(&network.wire, &PeerMessage::EndBlocks) + "\n").as_bytes(),
    );
    let _ = stream.flush();
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
        if let Some(path) = guard.storage_path.as_deref() {
            append_block(path, &block).expect("persist locally mined block");
        }
        guard.mined_blocks += 1;
        block
    };

    broadcast_block(state, &block);
}

#[derive(Clone, Copy)]
enum BlockSource {
    Gossip,
    Sync,
}

fn apply_incoming_block(
    guard: &mut NodeState,
    block: chesscoin_core::domain::Block,
    source: BlockSource,
) -> bool {
    let hasher = ToyHash;
    let sampler = DeterministicSampler;
    match guard.chain.apply_block(&hasher, &sampler, block.clone()) {
        Ok(_) => {
            if let Some(path) = guard.storage_path.as_deref() {
                let _ = append_block(path, &block);
            }
            match source {
                BlockSource::Gossip => guard.accepted_blocks += 1,
                BlockSource::Sync => guard.synced_blocks += 1,
            }
            true
        }
        Err(_) => {
            match source {
                BlockSource::Gossip => guard.rejected_blocks += 1,
                BlockSource::Sync => guard.failed_syncs += 1,
            }
            false
        }
    }
}

fn sync_once(state: &Arc<Mutex<NodeState>>) {
    let (peers, from_height, network, limit) = {
        let guard = state.lock().expect("node state lock poisoned");
        (
            guard.peers.clone(),
            guard.chain.height(),
            guard.network.clone(),
            guard.sync.max_blocks_per_response,
        )
    };

    if peers.is_empty() {
        return;
    }

    for peer in peers {
        if sync_from_peer(state, peer, from_height, limit, &network).is_err() {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.failed_syncs += 1;
        }
    }
}

fn sync_from_peer(
    state: &Arc<Mutex<NodeState>>,
    peer: SocketAddr,
    from_height: u64,
    limit: usize,
    network: &NetworkConfig,
) -> Result<(), ()> {
    let mut stream = TcpStream::connect_timeout(&peer, network.connect_timeout).map_err(|_| ())?;
    stream
        .set_read_timeout(Some(network.read_timeout))
        .map_err(|_| ())?;
    let request = encode_network_message(
        &network.wire,
        &PeerMessage::GetBlocks { from_height, limit },
    ) + "\n";
    stream.write_all(request.as_bytes()).map_err(|_| ())?;
    stream.flush().map_err(|_| ())?;

    loop {
        let line = match read_limited_line_from(&mut stream, network.max_message_bytes) {
            Ok(Some(line)) => line,
            Ok(None) => return Err(()),
            Err(IncomingReadError::Oversized) => {
                let mut guard = state.lock().expect("node state lock poisoned");
                guard.oversized_messages += 1;
                return Err(());
            }
            Err(IncomingReadError::Io) => return Err(()),
        };

        match decode_network_message(&line, &network.wire).map_err(|_| ())? {
            PeerMessage::Block(block) => {
                let mut guard = state.lock().expect("node state lock poisoned");
                if !apply_incoming_block(&mut guard, *block, BlockSource::Sync) {
                    return Err(());
                }
            }
            PeerMessage::EndBlocks => return Ok(()),
            _ => return Err(()),
        }
    }
}

fn add_peer(state: &Arc<Mutex<NodeState>>, peer: SocketAddr) {
    let mut guard = state.lock().expect("node state lock poisoned");
    if peer != guard.bound_addr && !guard.peers.contains(&peer) {
        guard.peers.push(peer);
    }
}

fn broadcast_block(state: &Arc<Mutex<NodeState>>, block: &chesscoin_core::domain::Block) {
    let (peers, timeout) = {
        let guard = state.lock().expect("node state lock poisoned");
        (guard.peers.clone(), guard.network.connect_timeout)
    };
    let message = {
        let guard = state.lock().expect("node state lock poisoned");
        encode_network_message(
            &guard.network.wire,
            &PeerMessage::Block(Box::new(block.clone())),
        ) + "\n"
    };
    let mut sent = 0;
    let mut failed = 0;

    for peer in peers {
        match TcpStream::connect_timeout(&peer, timeout) {
            Ok(mut stream) => {
                if stream
                    .write_all(message.as_bytes())
                    .and_then(|_| stream.flush())
                    .is_ok()
                {
                    sent += 1;
                } else {
                    failed += 1;
                }
            }
            Err(_) => {
                failed += 1;
            }
        }
    }

    let mut guard = state.lock().expect("node state lock poisoned");
    guard.outbound_blocks += sent;
    guard.failed_broadcasts += failed;
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
        mined_blocks: guard.mined_blocks,
        malformed_messages: guard.malformed_messages,
        incompatible_messages: guard.incompatible_messages,
        oversized_messages: guard.oversized_messages,
        outbound_blocks: guard.outbound_blocks,
        failed_broadcasts: guard.failed_broadcasts,
        synced_blocks: guard.synced_blocks,
        failed_syncs: guard.failed_syncs,
        known_peers: guard.peers.clone(),
    }
}

enum IncomingReadError {
    Oversized,
    Io,
}

fn read_limited_line(
    stream: &mut TcpStream,
    network: &NetworkConfig,
) -> Result<Option<String>, IncomingReadError> {
    stream
        .set_read_timeout(Some(network.read_timeout))
        .map_err(|_| IncomingReadError::Io)?;

    read_limited_line_from(stream, network.max_message_bytes)
}

fn read_limited_line_from<R: Read>(
    reader: &mut R,
    max_message_bytes: usize,
) -> Result<Option<String>, IncomingReadError> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1];

    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {
                bytes.push(buffer[0]);
                if bytes.len() > max_message_bytes {
                    return Err(IncomingReadError::Oversized);
                }
                if buffer[0] == b'\n' {
                    break;
                }
            }
            Err(_) => return Err(IncomingReadError::Io),
        }
    }

    if bytes.is_empty() {
        return Ok(None);
    }

    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| IncomingReadError::Io)
}

fn load_chain(path: &Path, config: ChainConfig) -> Result<ChainState, String> {
    let mut chain = ChainState::new(config);
    if !path.exists() {
        return Ok(chain);
    }

    let file = fs::File::open(path)
        .map_err(|error| format!("failed to open chain storage {}: {error}", path.display()))?;
    let reader = BufReader::new(file);
    let hasher = ToyHash;
    let sampler = DeterministicSampler;

    for (line_number, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| {
            format!(
                "failed to read chain storage {} line {}: {error}",
                path.display(),
                line_number + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let block = decode_block_message(&line).map_err(|error| {
            format!(
                "invalid chain storage {} line {}: {error}",
                path.display(),
                line_number + 1
            )
        })?;
        chain
            .apply_block(&hasher, &sampler, block)
            .map_err(|error| {
                format!(
                    "stored block failed validation in {} line {}: {error:?}",
                    path.display(),
                    line_number + 1
                )
            })?;
    }

    Ok(chain)
}

fn append_block(path: &Path, block: &chesscoin_core::domain::Block) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create chain storage directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open chain storage {}: {error}", path.display()))?;
    file.write_all(encode_block_message(block).as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|error| format!("failed to write chain storage {}: {error}", path.display()))
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
    use std::fs;

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

    fn wait_for_rejections(node: &RunningNode, rejected: usize) -> NodeSnapshot {
        for _ in 0..100 {
            let snapshot = node.snapshot();
            if snapshot.rejected_blocks >= rejected {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn wait_for_incompatible(node: &RunningNode, incompatible: usize) -> NodeSnapshot {
        for _ in 0..100 {
            let snapshot = node.snapshot();
            if snapshot.incompatible_messages >= incompatible {
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

    #[test]
    fn continuous_miner_produces_multiple_blocks() {
        let mut config = NodeConfig::localhost_ephemeral("miner");
        config.chain.difficulty_zero_bits = 0;
        config.chain.steps_per_block = 4;
        config.chain.samples_per_block = 4;
        config.miner = Some(MinerConfig {
            interval: Duration::from_millis(40),
        });
        let node = start_node(config).expect("node starts");

        let snapshot = wait_for_height(&node, 3);

        assert!(snapshot.height >= 3, "{snapshot:?}");
        assert!(snapshot.mined_blocks >= 3, "{snapshot:?}");

        node.stop();
    }

    #[test]
    fn late_joining_peer_syncs_existing_blocks() {
        let mut config_a = NodeConfig::localhost_ephemeral("sync-a");
        config_a.chain.difficulty_zero_bits = 0;
        config_a.chain.steps_per_block = 4;
        config_a.chain.samples_per_block = 4;
        let node_a = start_node(config_a).expect("node a starts");

        node_a
            .send(NodeCommand::MineOnce {
                training_seed: 1,
                sampling_entropy: 2,
            })
            .expect("mine command sends");
        node_a
            .send(NodeCommand::MineOnce {
                training_seed: 3,
                sampling_entropy: 4,
            })
            .expect("mine command sends");
        let snapshot_a = wait_for_height(&node_a, 2);
        assert_eq!(snapshot_a.height, 2);

        let mut config_b = NodeConfig::localhost_ephemeral("sync-b");
        config_b.chain.difficulty_zero_bits = 0;
        config_b.chain.steps_per_block = 4;
        config_b.chain.samples_per_block = 4;
        config_b.peers.push(node_a.bound_addr());
        config_b.sync.interval = Duration::from_millis(20);
        let node_b = start_node(config_b).expect("node b starts");

        let snapshot_b = wait_for_height(&node_b, 2);

        assert_eq!(snapshot_b.height, 2);
        assert_eq!(snapshot_b.head, snapshot_a.head);
        assert_eq!(snapshot_b.synced_blocks, 2);

        node_b.stop();
        node_a.stop();
    }

    #[test]
    fn node_reloads_persisted_blocks() {
        let data_dir = unique_test_data_dir("reload");
        let mut config = NodeConfig::localhost_ephemeral("persist-a");
        config.chain.difficulty_zero_bits = 0;
        config.chain.steps_per_block = 4;
        config.chain.samples_per_block = 4;
        config.storage = Some(StorageConfig {
            data_dir: data_dir.clone(),
        });
        let node = start_node(config.clone()).expect("node starts");
        node.send(NodeCommand::MineOnce {
            training_seed: 77,
            sampling_entropy: 88,
        })
        .expect("mine command sends");
        let first = wait_for_height(&node, 1);
        assert_eq!(first.height, 1);
        node.stop();

        config.node_id = "persist-b".to_string();
        let restarted = start_node(config).expect("node restarts");
        let second = restarted.snapshot();

        assert_eq!(second.height, 1);
        assert_eq!(second.head, first.head);

        restarted.stop();
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn malformed_peer_message_is_counted_and_rejected() {
        let node = start_node(NodeConfig::localhost_ephemeral("malformed")).expect("node starts");
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(b"NOT_A_MESSAGE\n").expect("write");
        stream.flush().expect("flush");

        let snapshot = wait_for_rejections(&node, 1);

        assert_eq!(snapshot.rejected_blocks, 1);
        assert_eq!(snapshot.malformed_messages, 1);

        node.stop();
    }

    #[test]
    fn oversized_peer_message_is_counted_and_rejected_before_decode() {
        let mut config = NodeConfig::localhost_ephemeral("oversized");
        config.network.max_message_bytes = 8;
        let node = start_node(config).expect("node starts");
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(b"BLOCK_TOO_LARGE\n").expect("write");
        stream.flush().expect("flush");

        let snapshot = wait_for_rejections(&node, 1);

        assert_eq!(snapshot.rejected_blocks, 1);
        assert_eq!(snapshot.oversized_messages, 1);
        assert_eq!(snapshot.malformed_messages, 0);

        node.stop();
    }

    #[test]
    fn incompatible_network_message_is_counted() {
        let node = start_node(NodeConfig::localhost_ephemeral("network-a")).expect("node starts");
        let wrong_network = crate::wire::WireConfig {
            network_id: "other-network".to_string(),
            protocol_version: crate::wire::PROTOCOL_VERSION,
        };
        let message = crate::wire::encode_network_message(
            &wrong_network,
            &PeerMessage::GetBlocks {
                from_height: 0,
                limit: 1,
            },
        ) + "\n";
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(message.as_bytes()).expect("write");
        stream.flush().expect("flush");

        let snapshot = wait_for_incompatible(&node, 1);

        assert_eq!(snapshot.incompatible_messages, 1);
        assert_eq!(snapshot.malformed_messages, 0);

        node.stop();
    }

    fn unique_test_data_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chesscoin-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }
}
