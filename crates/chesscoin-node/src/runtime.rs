use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chesscoin_core::application::{
    block_hash, block_header_hash, ChainConfig, ChainState, ForkChoiceState,
};
use chesscoin_core::domain::{BlockHeader, Digest};
use chesscoin_core::ports::HashPort;

use crate::adapters::{DeterministicSampler, ToyHash};
use crate::wire::{
    chain_fingerprint, decode_block_message, decode_network_message, encode_block_message,
    encode_network_message, PeerMessage, WireConfig,
};

const STORAGE_RECORD_VERSION: &str = "CCBLK1";
const STORAGE_HEADER_VERSION: &str = "CCCFG1";
const MIN_MAX_MESSAGE_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    pub advertise_addr: Option<SocketAddr>,
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
    pub max_peers: usize,
    pub max_inbound_connections: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub max_blocks_per_response: usize,
    pub max_locator_hashes: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            max_blocks_per_response: 128,
            max_locator_hashes: 32,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            wire: WireConfig::default(),
            max_message_bytes: 1024 * 1024,
            max_peers: 64,
            max_inbound_connections: 64,
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        }
    }
}

impl NodeConfig {
    pub fn localhost_ephemeral(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_addr: None,
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
    pub advertised_addr: SocketAddr,
    pub height: u64,
    pub head: Digest,
    pub accepted_blocks: usize,
    pub rejected_blocks: usize,
    pub mined_blocks: usize,
    pub known_blocks: usize,
    pub reorgs: usize,
    pub malformed_messages: usize,
    pub incompatible_messages: usize,
    pub oversized_messages: usize,
    pub outbound_blocks: usize,
    pub failed_broadcasts: usize,
    pub dropped_inbound_connections: usize,
    pub peer_rejections: usize,
    pub synced_blocks: usize,
    pub failed_syncs: usize,
    pub storage_failures: usize,
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
    advertised_addr: SocketAddr,
    chain: ChainState,
    fork_choice: ForkChoiceState,
    peers: Vec<SocketAddr>,
    accepted_blocks: usize,
    rejected_blocks: usize,
    mined_blocks: usize,
    reorgs: usize,
    malformed_messages: usize,
    incompatible_messages: usize,
    oversized_messages: usize,
    outbound_blocks: usize,
    failed_broadcasts: usize,
    dropped_inbound_connections: usize,
    peer_rejections: usize,
    synced_blocks: usize,
    failed_syncs: usize,
    storage_failures: usize,
    _storage_lock: Option<StorageLock>,
    storage_path: Option<PathBuf>,
    network: NetworkConfig,
    sync: SyncConfig,
}

#[derive(Debug)]
struct StorageLock {
    path: PathBuf,
    _file: File,
}

impl Drop for StorageLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn start_node(mut config: NodeConfig) -> Result<RunningNode, String> {
    validate_node_config(&config)?;
    config.network.wire.chain_fingerprint = chain_fingerprint(&config.chain);
    let listener = TcpListener::bind(config.listen_addr)
        .map_err(|error| format!("failed to bind {}: {error}", config.listen_addr))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set listener nonblocking: {error}"))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read listener addr: {error}"))?;
    let advertised_addr = config.advertise_addr.unwrap_or(bound_addr);
    if !peer_address_is_dialable(advertised_addr) {
        return Err(format!(
            "advertise_addr must be dialable, got {advertised_addr}"
        ));
    }

    let (command_tx, command_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let storage_path = config
        .storage
        .as_ref()
        .map(|storage| storage.data_dir.join("blocks.log"));
    let storage_lock = if let Some(storage) = config.storage.as_ref() {
        Some(acquire_storage_lock(&storage.data_dir)?)
    } else {
        None
    };
    let (chain, fork_choice) = if let Some(path) = storage_path.as_deref() {
        load_chains(path, config.chain.clone())?
    } else {
        (
            ChainState::new(config.chain.clone()),
            ForkChoiceState::new(config.chain.clone()),
        )
    };
    let initial_sync = config.sync.clone();
    let (peers, peer_rejections) = normalize_initial_peers(
        bound_addr,
        advertised_addr,
        config.peers,
        config.network.max_peers,
    );
    let state = Arc::new(Mutex::new(NodeState {
        node_id: config.node_id,
        bound_addr,
        advertised_addr,
        chain,
        fork_choice,
        peers,
        accepted_blocks: 0,
        rejected_blocks: 0,
        mined_blocks: 0,
        reorgs: 0,
        malformed_messages: 0,
        incompatible_messages: 0,
        oversized_messages: 0,
        outbound_blocks: 0,
        failed_broadcasts: 0,
        dropped_inbound_connections: 0,
        peer_rejections,
        synced_blocks: 0,
        failed_syncs: 0,
        storage_failures: 0,
        _storage_lock: storage_lock,
        storage_path,
        network: config.network,
        sync: config.sync,
    }));

    let thread_state = Arc::clone(&state);
    let thread_stop = Arc::clone(&stop);
    let active_inbound_connections = Arc::new(AtomicUsize::new(0));
    let initial_miner = config.miner.clone();
    let handle = thread::spawn(move || {
        node_loop(
            listener,
            thread_state,
            thread_stop,
            active_inbound_connections,
            command_rx,
            initial_miner,
            initial_sync,
        )
    });

    announce_to_peers(&state);

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

pub fn validate_node_config(config: &NodeConfig) -> Result<(), String> {
    if config.node_id.trim().is_empty() {
        return Err("node_id must not be empty".to_string());
    }
    config
        .chain
        .validate()
        .map_err(|error| format!("invalid chain config: {error}"))?;
    if config.network.wire.network_id.trim().is_empty() {
        return Err("network_id must not be empty".to_string());
    }
    if config.network.wire.protocol_version == 0 {
        return Err("protocol_version must be greater than zero".to_string());
    }
    if config.advertise_addr.is_none() && ip_is_unspecified(config.listen_addr.ip()) {
        return Err(
            "advertise_addr is required when listen_addr uses an unspecified IP".to_string(),
        );
    }
    if let Some(advertise_addr) = config.advertise_addr {
        if !peer_address_is_dialable(advertise_addr) {
            return Err(format!(
                "advertise_addr must be dialable, got {advertise_addr}"
            ));
        }
    }
    if config.network.max_message_bytes < MIN_MAX_MESSAGE_BYTES {
        return Err(format!(
            "max_message_bytes must be at least {MIN_MAX_MESSAGE_BYTES}"
        ));
    }
    if config.network.max_peers == 0 {
        return Err("max_peers must be greater than zero".to_string());
    }
    if config.network.max_inbound_connections == 0 {
        return Err("max_inbound_connections must be greater than zero".to_string());
    }
    if config.network.connect_timeout.is_zero() {
        return Err("connect_timeout must be greater than zero".to_string());
    }
    if config.network.read_timeout.is_zero() {
        return Err("read_timeout must be greater than zero".to_string());
    }
    if config.network.write_timeout.is_zero() {
        return Err("write_timeout must be greater than zero".to_string());
    }
    if config.sync.enabled {
        if config.sync.interval.is_zero() {
            return Err("sync interval must be greater than zero".to_string());
        }
        if config.sync.max_blocks_per_response == 0 {
            return Err("sync max_blocks_per_response must be greater than zero".to_string());
        }
        if config.sync.max_locator_hashes == 0 {
            return Err("sync max_locator_hashes must be greater than zero".to_string());
        }
    }
    if let Some(miner) = config.miner.as_ref() {
        validate_miner_config(miner)?;
    }
    if let Some(storage) = config.storage.as_ref() {
        if storage.data_dir.as_os_str().is_empty() {
            return Err("storage data_dir must not be empty".to_string());
        }
    }

    Ok(())
}

fn validate_miner_config(config: &MinerConfig) -> Result<(), String> {
    if config.interval.is_zero() {
        return Err("miner interval must be greater than zero".to_string());
    }

    Ok(())
}

fn node_loop(
    listener: TcpListener,
    state: Arc<Mutex<NodeState>>,
    stop: Arc<AtomicBool>,
    active_inbound_connections: Arc<AtomicUsize>,
    command_rx: mpsc::Receiver<NodeCommand>,
    initial_miner: Option<MinerConfig>,
    initial_sync: SyncConfig,
) {
    let mut miner = initial_miner;
    let mut next_mine_at = Instant::now();
    let sync = initial_sync;
    let mut next_sync_at = Instant::now();
    let mut inbound_handlers = Vec::new();

    while !stop.load(Ordering::SeqCst) {
        accept_ready_connections(
            &listener,
            &state,
            &active_inbound_connections,
            &mut inbound_handlers,
        );
        join_finished_inbound_handlers(&mut inbound_handlers);

        while let Ok(command) = command_rx.try_recv() {
            match command {
                NodeCommand::MineOnce {
                    training_seed,
                    sampling_entropy,
                } => mine_once(&state, training_seed, sampling_entropy),
                NodeCommand::AddPeer(peer) => {
                    if add_peer(&state, peer) {
                        announce_to_peer(state.as_ref(), peer);
                    }
                }
                NodeCommand::StartMining(config) => {
                    if validate_miner_config(&config).is_ok() {
                        miner = Some(config);
                        next_mine_at = Instant::now();
                    } else {
                        let mut guard = state.lock().expect("node state lock poisoned");
                        guard.malformed_messages += 1;
                    }
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

    join_all_inbound_handlers(inbound_handlers);
}

fn accept_ready_connections(
    listener: &TcpListener,
    state: &Arc<Mutex<NodeState>>,
    active_inbound_connections: &Arc<AtomicUsize>,
    inbound_handlers: &mut Vec<JoinHandle<()>>,
) {
    loop {
        match listener.accept() {
            Ok((stream, _remote_addr)) => {
                let max_inbound_connections = {
                    let guard = state.lock().expect("node state lock poisoned");
                    guard.network.max_inbound_connections
                };
                let Some(permit) = reserve_inbound_connection(
                    Arc::clone(active_inbound_connections),
                    max_inbound_connections,
                ) else {
                    let mut guard = state.lock().expect("node state lock poisoned");
                    guard.dropped_inbound_connections += 1;
                    continue;
                };

                let handler_state = Arc::clone(state);
                let handle = thread::spawn(move || {
                    let _permit = permit;
                    handle_stream(stream, &handler_state);
                });
                inbound_handlers.push(handle);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}

fn join_finished_inbound_handlers(inbound_handlers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < inbound_handlers.len() {
        if inbound_handlers[index].is_finished() {
            let handle = inbound_handlers.swap_remove(index);
            let _ = handle.join();
        } else {
            index += 1;
        }
    }
}

fn join_all_inbound_handlers(inbound_handlers: Vec<JoinHandle<()>>) {
    for handle in inbound_handlers {
        let _ = handle.join();
    }
}

struct InboundConnectionPermit {
    active_connections: Arc<AtomicUsize>,
}

impl Drop for InboundConnectionPermit {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

fn reserve_inbound_connection(
    active_connections: Arc<AtomicUsize>,
    limit: usize,
) -> Option<InboundConnectionPermit> {
    loop {
        let current = active_connections.load(Ordering::SeqCst);
        if current >= limit {
            return None;
        }
        if active_connections
            .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Some(InboundConnectionPermit { active_connections });
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
        Ok(PeerMessage::Hello { listen_addr, .. }) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            if !add_peer_to_state(&mut guard, listen_addr) {
                guard.peer_rejections += 1;
            }
        }
        Ok(PeerMessage::GetBlocks { from_height, limit }) => {
            serve_blocks(stream, state, from_height, limit);
        }
        Ok(PeerMessage::GetBlocksByLocator { locator, limit }) => {
            if !locator_is_within_limit(state, locator.len()) {
                return;
            }
            serve_blocks_by_locator(stream, state, &locator, limit);
        }
        Ok(PeerMessage::GetInventory { locator, limit }) => {
            if !locator_is_within_limit(state, locator.len()) {
                return;
            }
            serve_inventory(stream, state, &locator, limit);
        }
        Ok(PeerMessage::GetHeaders { locator, limit }) => {
            if !locator_is_within_limit(state, locator.len()) {
                return;
            }
            serve_headers(stream, state, &locator, limit);
        }
        Ok(PeerMessage::GetPeers { limit }) => {
            serve_peers(stream, state, limit);
        }
        Ok(PeerMessage::Block(block)) => {
            let block = *block;
            let application = {
                let mut guard = state.lock().expect("node state lock poisoned");
                apply_incoming_block(&mut guard, block.clone(), BlockSource::Gossip)
            };

            if application == BlockApplication::Accepted {
                broadcast_block(state, &block);
            }
        }
        Ok(PeerMessage::EndBlocks) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.malformed_messages += 1;
        }
        Ok(PeerMessage::Inventory { .. }) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.malformed_messages += 1;
        }
        Ok(PeerMessage::Headers { .. }) => {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.malformed_messages += 1;
        }
        Ok(PeerMessage::Peers { .. }) => {
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

fn locator_is_within_limit(state: &Arc<Mutex<NodeState>>, locator_len: usize) -> bool {
    let mut guard = state.lock().expect("node state lock poisoned");
    if locator_len > guard.sync.max_locator_hashes {
        guard.malformed_messages += 1;
        return false;
    }
    true
}

fn serve_inventory(
    mut stream: TcpStream,
    state: &Arc<Mutex<NodeState>>,
    locator: &[Digest],
    requested_limit: usize,
) {
    let (hashes, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        let limit = requested_limit.min(guard.sync.max_blocks_per_response);
        (
            inventory_after_locator(&guard.chain, locator, limit),
            guard.network.clone(),
        )
    };

    let _ = stream.set_write_timeout(Some(network.write_timeout));
    let line = encode_network_message(&network.wire, &PeerMessage::Inventory { hashes }) + "\n";
    let _ = stream
        .write_all(line.as_bytes())
        .and_then(|_| stream.flush());
}

fn serve_headers(
    mut stream: TcpStream,
    state: &Arc<Mutex<NodeState>>,
    locator: &[Digest],
    requested_limit: usize,
) {
    let (headers, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        let limit = requested_limit.min(guard.sync.max_blocks_per_response);
        (
            headers_after_locator(&guard.chain, locator, limit),
            guard.network.clone(),
        )
    };

    let _ = stream.set_write_timeout(Some(network.write_timeout));
    let line = encode_network_message(&network.wire, &PeerMessage::Headers { headers }) + "\n";
    let _ = stream
        .write_all(line.as_bytes())
        .and_then(|_| stream.flush());
}

fn serve_peers(mut stream: TcpStream, state: &Arc<Mutex<NodeState>>, requested_limit: usize) {
    let (peers, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        let limit = requested_limit.min(guard.network.max_peers);
        (peer_advertisement(&guard, limit), guard.network.clone())
    };

    let _ = stream.set_write_timeout(Some(network.write_timeout));
    let line = encode_network_message(&network.wire, &PeerMessage::Peers { peers }) + "\n";
    let _ = stream
        .write_all(line.as_bytes())
        .and_then(|_| stream.flush());
}

fn peer_advertisement(guard: &NodeState, limit: usize) -> Vec<SocketAddr> {
    let mut peers = Vec::new();
    for peer in std::iter::once(guard.advertised_addr).chain(guard.peers.iter().copied()) {
        if peers.len() >= limit {
            break;
        }
        if !peers.contains(&peer) {
            peers.push(peer);
        }
    }
    peers
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

    let _ = stream.set_write_timeout(Some(network.write_timeout));
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

fn serve_blocks_by_locator(
    mut stream: TcpStream,
    state: &Arc<Mutex<NodeState>>,
    locator: &[Digest],
    requested_limit: usize,
) {
    let (blocks, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        let limit = requested_limit.min(guard.sync.max_blocks_per_response);
        (
            blocks_after_locator(&guard.chain, locator, limit),
            guard.network.clone(),
        )
    };

    let _ = stream.set_write_timeout(Some(network.write_timeout));
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
        let block =
            guard
                .fork_choice
                .mine_next_block(&hasher, &sampler, training_seed, sampling_entropy);
        guard
            .fork_choice
            .insert_block(&hasher, &sampler, block.clone())
            .expect("locally mined block should validate");
        activate_best_chain(&mut guard).expect("locally mined chain should activate");
        persist_block(&mut guard, &block);
        guard.mined_blocks += 1;
        block
    };

    announce_to_peers(state);
    broadcast_block(state, &block);
}

#[derive(Clone, Copy)]
enum BlockSource {
    Gossip,
    Sync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockApplication {
    Accepted,
    AlreadyKnown,
    Rejected,
}

fn apply_incoming_block(
    guard: &mut NodeState,
    block: chesscoin_core::domain::Block,
    source: BlockSource,
) -> BlockApplication {
    let hasher = ToyHash;
    let sampler = DeterministicSampler;
    let hash = block_hash(&hasher, &block);
    if guard.fork_choice.contains_block(hash) {
        return BlockApplication::AlreadyKnown;
    }

    let old_head = guard.chain.head_hash(&hasher);
    match guard
        .fork_choice
        .insert_block(&hasher, &sampler, block.clone())
    {
        Ok(_) => {
            persist_block(guard, &block);
            if activate_best_chain(guard).is_ok()
                && old_head != Digest::zero()
                && block.header.previous_block != old_head
                && guard.chain.head_hash(&hasher) == hash
            {
                guard.reorgs += 1;
            }
            match source {
                BlockSource::Gossip => guard.accepted_blocks += 1,
                BlockSource::Sync => guard.synced_blocks += 1,
            }
            BlockApplication::Accepted
        }
        Err(_) => {
            match source {
                BlockSource::Gossip => guard.rejected_blocks += 1,
                BlockSource::Sync => guard.failed_syncs += 1,
            }
            BlockApplication::Rejected
        }
    }
}

fn persist_block(guard: &mut NodeState, block: &chesscoin_core::domain::Block) {
    let Some(path) = guard.storage_path.clone() else {
        return;
    };

    if append_block(&path, guard.chain.config(), block).is_err() {
        guard.storage_failures += 1;
    }
}

fn sync_once(state: &Arc<Mutex<NodeState>>) {
    let (peers, locator, network, block_limit, peer_limit) = {
        let guard = state.lock().expect("node state lock poisoned");
        (
            guard.peers.clone(),
            block_locator(&guard.chain, guard.sync.max_locator_hashes),
            guard.network.clone(),
            guard.sync.max_blocks_per_response,
            guard.network.max_peers,
        )
    };

    if peers.is_empty() {
        return;
    }

    for peer in &peers {
        if exchange_peers_with(state, *peer, peer_limit, &network).is_err() {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.failed_syncs += 1;
        }
    }

    let peers = {
        let guard = state.lock().expect("node state lock poisoned");
        guard.peers.clone()
    };

    for peer in peers {
        if sync_from_peer(state, peer, locator.clone(), block_limit, &network).is_err() {
            let mut guard = state.lock().expect("node state lock poisoned");
            guard.failed_syncs += 1;
        }
    }
}

fn exchange_peers_with(
    state: &Arc<Mutex<NodeState>>,
    peer: SocketAddr,
    limit: usize,
    network: &NetworkConfig,
) -> Result<(), ()> {
    let advertised = peers_from_peer(peer, limit, network)?;
    let mut guard = state.lock().expect("node state lock poisoned");
    for candidate in advertised {
        if !add_peer_to_state(&mut guard, candidate) {
            guard.peer_rejections += 1;
        }
    }
    Ok(())
}

fn peers_from_peer(
    peer: SocketAddr,
    limit: usize,
    network: &NetworkConfig,
) -> Result<Vec<SocketAddr>, ()> {
    let mut stream = connect_outbound(peer, network)?;
    let request = encode_network_message(&network.wire, &PeerMessage::GetPeers { limit }) + "\n";
    stream.write_all(request.as_bytes()).map_err(|_| ())?;
    stream.flush().map_err(|_| ())?;

    let line = match read_limited_line_from(&mut stream, network.max_message_bytes) {
        Ok(Some(line)) => line,
        Ok(None) => return Err(()),
        Err(IncomingReadError::Oversized) | Err(IncomingReadError::Io) => return Err(()),
    };

    match decode_network_message(&line, &network.wire).map_err(|_| ())? {
        PeerMessage::Peers { peers } if peers.len() <= limit => Ok(peers),
        _ => Err(()),
    }
}

fn connect_outbound(peer: SocketAddr, network: &NetworkConfig) -> Result<TcpStream, ()> {
    let stream = TcpStream::connect_timeout(&peer, network.connect_timeout).map_err(|_| ())?;
    stream
        .set_read_timeout(Some(network.read_timeout))
        .map_err(|_| ())?;
    stream
        .set_write_timeout(Some(network.write_timeout))
        .map_err(|_| ())?;
    Ok(stream)
}

fn sync_from_peer(
    state: &Arc<Mutex<NodeState>>,
    peer: SocketAddr,
    locator: Vec<Digest>,
    limit: usize,
    network: &NetworkConfig,
) -> Result<(), ()> {
    let headers = headers_from_peer(peer, locator.clone(), limit, network)?;
    let has_unknown_block = {
        let guard = state.lock().expect("node state lock poisoned");
        validate_headers_after_locator(&guard.chain, &locator, &headers)?;
        headers
            .iter()
            .map(|header| block_header_hash(&ToyHash, header))
            .any(|hash| !guard.fork_choice.contains_block(hash))
    };

    if !has_unknown_block {
        return Ok(());
    }

    blocks_from_peer(state, peer, locator, limit, network)
}

fn validate_headers_after_locator(
    chain: &ChainState,
    locator: &[Digest],
    headers: &[BlockHeader],
) -> Result<(), ()> {
    if headers.is_empty() {
        return Ok(());
    }

    let hasher = ToyHash;
    let chain_hashes = chain
        .blocks()
        .iter()
        .map(|block| block_hash(&hasher, block))
        .collect::<Vec<_>>();
    let first_parent = headers[0].previous_block;
    let mut expected_height = if first_parent == Digest::zero() {
        if !chain.blocks().is_empty() && !locator.contains(&Digest::zero()) {
            return Err(());
        }
        0
    } else {
        if !locator.contains(&first_parent) {
            return Err(());
        }
        let Some(parent_index) = chain_hashes.iter().position(|hash| *hash == first_parent) else {
            return Err(());
        };
        parent_index as u64 + 1
    };
    let mut expected_previous = first_parent;

    for header in headers {
        if header.previous_block != expected_previous {
            return Err(());
        }
        if header.height != expected_height {
            return Err(());
        }
        if header.sample_count != chain.config().samples_per_block {
            return Err(());
        }
        let hash = block_header_hash(&hasher, header);
        if hash.leading_zero_bits() < chain.config().difficulty_zero_bits {
            return Err(());
        }
        expected_previous = hash;
        expected_height += 1;
    }

    Ok(())
}

fn headers_from_peer(
    peer: SocketAddr,
    locator: Vec<Digest>,
    limit: usize,
    network: &NetworkConfig,
) -> Result<Vec<BlockHeader>, ()> {
    let mut stream = connect_outbound(peer, network)?;
    let request =
        encode_network_message(&network.wire, &PeerMessage::GetHeaders { locator, limit }) + "\n";
    stream.write_all(request.as_bytes()).map_err(|_| ())?;
    stream.flush().map_err(|_| ())?;

    let line = match read_limited_line_from(&mut stream, network.max_message_bytes) {
        Ok(Some(line)) => line,
        Ok(None) => return Err(()),
        Err(IncomingReadError::Oversized) | Err(IncomingReadError::Io) => return Err(()),
    };

    match decode_network_message(&line, &network.wire).map_err(|_| ())? {
        PeerMessage::Headers { headers } if headers.len() <= limit => Ok(headers),
        _ => Err(()),
    }
}

#[cfg(test)]
fn inventory_from_peer(
    peer: SocketAddr,
    locator: Vec<Digest>,
    limit: usize,
    network: &NetworkConfig,
) -> Result<Vec<Digest>, ()> {
    let mut stream = connect_outbound(peer, network)?;
    let request =
        encode_network_message(&network.wire, &PeerMessage::GetInventory { locator, limit }) + "\n";
    stream.write_all(request.as_bytes()).map_err(|_| ())?;
    stream.flush().map_err(|_| ())?;

    let line = match read_limited_line_from(&mut stream, network.max_message_bytes) {
        Ok(Some(line)) => line,
        Ok(None) => return Err(()),
        Err(IncomingReadError::Oversized) | Err(IncomingReadError::Io) => return Err(()),
    };

    match decode_network_message(&line, &network.wire).map_err(|_| ())? {
        PeerMessage::Inventory { hashes } if hashes.len() <= limit => Ok(hashes),
        _ => Err(()),
    }
}

fn blocks_from_peer(
    state: &Arc<Mutex<NodeState>>,
    peer: SocketAddr,
    locator: Vec<Digest>,
    limit: usize,
    network: &NetworkConfig,
) -> Result<(), ()> {
    let mut stream = connect_outbound(peer, network)?;
    let request = encode_network_message(
        &network.wire,
        &PeerMessage::GetBlocksByLocator { locator, limit },
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
                if apply_incoming_block(&mut guard, *block, BlockSource::Sync)
                    == BlockApplication::Rejected
                {
                    return Err(());
                }
            }
            PeerMessage::EndBlocks => return Ok(()),
            PeerMessage::Inventory { .. } => return Err(()),
            _ => return Err(()),
        }
    }
}

fn add_peer(state: &Arc<Mutex<NodeState>>, peer: SocketAddr) -> bool {
    let mut guard = state.lock().expect("node state lock poisoned");
    if !add_peer_to_state(&mut guard, peer) {
        guard.peer_rejections += 1;
        false
    } else {
        true
    }
}

fn normalize_initial_peers(
    bound_addr: SocketAddr,
    advertised_addr: SocketAddr,
    peers: Vec<SocketAddr>,
    max_peers: usize,
) -> (Vec<SocketAddr>, usize) {
    let mut accepted = Vec::new();
    let mut rejected = 0;

    for peer in peers {
        if !peer_address_is_dialable(peer)
            || peer == bound_addr
            || peer == advertised_addr
            || accepted.contains(&peer)
            || accepted.len() >= max_peers
        {
            rejected += 1;
        } else {
            accepted.push(peer);
        }
    }

    (accepted, rejected)
}

fn add_peer_to_state(guard: &mut NodeState, peer: SocketAddr) -> bool {
    if !peer_address_is_dialable(peer)
        || peer == guard.bound_addr
        || peer == guard.advertised_addr
        || guard.peers.contains(&peer)
        || guard.peers.len() >= guard.network.max_peers
    {
        return false;
    }

    guard.peers.push(peer);
    true
}

fn peer_address_is_dialable(peer: SocketAddr) -> bool {
    if peer.port() == 0 {
        return false;
    }

    match peer.ip() {
        IpAddr::V4(addr) => {
            !addr.is_unspecified() && !addr.is_multicast() && addr != Ipv4Addr::BROADCAST
        }
        IpAddr::V6(addr) => !addr.is_unspecified() && !addr.is_multicast(),
    }
}

fn ip_is_unspecified(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => addr.is_unspecified(),
        IpAddr::V6(addr) => addr.is_unspecified(),
    }
}

fn announce_to_peers(state: &Arc<Mutex<NodeState>>) {
    let peers = {
        let guard = state.lock().expect("node state lock poisoned");
        guard.peers.clone()
    };

    for peer in peers {
        announce_to_peer(state.as_ref(), peer);
    }
}

fn announce_to_peer(state: &Mutex<NodeState>, peer: SocketAddr) {
    let (network, message) = {
        let guard = state.lock().expect("node state lock poisoned");
        let hasher = ToyHash;
        (
            guard.network.clone(),
            PeerMessage::Hello {
                node_id: guard.node_id.clone(),
                height: guard.chain.height(),
                head: guard.chain.head_hash(&hasher),
                listen_addr: guard.advertised_addr,
            },
        )
    };
    let line = encode_network_message(&network.wire, &message) + "\n";

    if let Ok(mut stream) = TcpStream::connect_timeout(&peer, network.connect_timeout) {
        let _ = stream
            .set_write_timeout(Some(network.write_timeout))
            .and_then(|_| stream.write_all(line.as_bytes()))
            .and_then(|_| stream.flush());
    }
}

fn broadcast_block(state: &Arc<Mutex<NodeState>>, block: &chesscoin_core::domain::Block) {
    let (peers, network) = {
        let guard = state.lock().expect("node state lock poisoned");
        (guard.peers.clone(), guard.network.clone())
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
        match TcpStream::connect_timeout(&peer, network.connect_timeout) {
            Ok(mut stream) => {
                if stream
                    .set_write_timeout(Some(network.write_timeout))
                    .and_then(|_| stream.write_all(message.as_bytes()))
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
        advertised_addr: guard.advertised_addr,
        height: guard.chain.height(),
        head: guard.chain.head_hash(&hasher),
        accepted_blocks: guard.accepted_blocks,
        rejected_blocks: guard.rejected_blocks,
        mined_blocks: guard.mined_blocks,
        known_blocks: guard.fork_choice.known_block_count(),
        reorgs: guard.reorgs,
        malformed_messages: guard.malformed_messages,
        incompatible_messages: guard.incompatible_messages,
        oversized_messages: guard.oversized_messages,
        outbound_blocks: guard.outbound_blocks,
        failed_broadcasts: guard.failed_broadcasts,
        dropped_inbound_connections: guard.dropped_inbound_connections,
        peer_rejections: guard.peer_rejections,
        synced_blocks: guard.synced_blocks,
        failed_syncs: guard.failed_syncs,
        storage_failures: guard.storage_failures,
        known_peers: guard.peers.clone(),
    }
}

fn block_locator(chain: &ChainState, max_hashes: usize) -> Vec<Digest> {
    if max_hashes == 0 {
        return Vec::new();
    }

    let hasher = ToyHash;
    let mut hashes = chain
        .blocks()
        .iter()
        .rev()
        .take(max_hashes)
        .map(|block| block_hash(&hasher, block))
        .collect::<Vec<_>>();

    if hashes.len() < max_hashes && !chain.blocks().is_empty() {
        hashes.push(Digest::zero());
    }

    hashes
}

fn blocks_after_locator(
    chain: &ChainState,
    locator: &[Digest],
    limit: usize,
) -> Vec<chesscoin_core::domain::Block> {
    let hasher = ToyHash;
    let chain_hashes = chain
        .blocks()
        .iter()
        .map(|block| block_hash(&hasher, block))
        .collect::<Vec<_>>();
    let start = locator
        .iter()
        .find_map(|hash| {
            if *hash == Digest::zero() {
                Some(0)
            } else {
                chain_hashes
                    .iter()
                    .position(|candidate| candidate == hash)
                    .map(|index| index + 1)
            }
        })
        .unwrap_or(0);

    chain
        .blocks()
        .iter()
        .skip(start)
        .take(limit)
        .cloned()
        .collect()
}

fn inventory_after_locator(chain: &ChainState, locator: &[Digest], limit: usize) -> Vec<Digest> {
    let hasher = ToyHash;
    blocks_after_locator(chain, locator, limit)
        .iter()
        .map(|block| block_hash(&hasher, block))
        .collect()
}

fn headers_after_locator(
    chain: &ChainState,
    locator: &[Digest],
    limit: usize,
) -> Vec<chesscoin_core::domain::BlockHeader> {
    blocks_after_locator(chain, locator, limit)
        .iter()
        .map(|block| block.header.clone())
        .collect()
}

fn activate_best_chain(guard: &mut NodeState) -> Result<bool, String> {
    let hasher = ToyHash;
    let old_head = guard.chain.head_hash(&hasher);
    let new_head = guard.fork_choice.best_hash();
    if old_head == new_head {
        return Ok(false);
    }

    guard.chain = chain_from_blocks(
        guard.fork_choice.config().clone(),
        guard.fork_choice.best_chain(),
    )?;
    Ok(true)
}

fn chain_from_blocks(
    config: ChainConfig,
    blocks: Vec<chesscoin_core::domain::Block>,
) -> Result<ChainState, String> {
    let mut chain = ChainState::new(config);
    let hasher = ToyHash;
    let sampler = DeterministicSampler;

    for block in blocks {
        chain
            .apply_block(&hasher, &sampler, block)
            .map_err(|error| format!("best chain failed validation: {error:?}"))?;
    }

    Ok(chain)
}

#[derive(Debug)]
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

fn load_chains(path: &Path, config: ChainConfig) -> Result<(ChainState, ForkChoiceState), String> {
    let mut fork_choice = ForkChoiceState::new(config.clone());
    if !path.exists() {
        return Ok((ChainState::new(config), fork_choice));
    }

    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read chain storage {}: {error}", path.display()))?;
    let has_complete_tail = content.ends_with('\n') || content.is_empty();
    let lines = content.lines().collect::<Vec<_>>();
    let hasher = ToyHash;
    let sampler = DeterministicSampler;
    let expected_fingerprint = chain_fingerprint(&config);
    let mut seen_header = false;
    let mut seen_block = false;

    for (index, line) in lines.iter().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let block = match decode_storage_line(line) {
            Ok(StorageLine::Header(fingerprint)) => {
                if seen_header {
                    return Err(format!(
                        "invalid chain storage {} line {}: duplicate storage header",
                        path.display(),
                        line_number
                    ));
                }
                if seen_block {
                    return Err(format!(
                        "invalid chain storage {} line {}: storage header must precede blocks",
                        path.display(),
                        line_number
                    ));
                }
                if fingerprint != expected_fingerprint {
                    return Err(format!(
                        "chain storage {} fingerprint mismatch: stored {fingerprint}, configured {expected_fingerprint}",
                        path.display()
                    ));
                }
                seen_header = true;
                continue;
            }
            Ok(StorageLine::Block(block)) => *block,
            Err(_) if !has_complete_tail && line_number == lines.len() => break,
            Err(error) => {
                return Err(format!(
                    "invalid chain storage {} line {}: {error}",
                    path.display(),
                    line_number
                ));
            }
        };
        fork_choice
            .insert_block(&hasher, &sampler, block)
            .map_err(|error| {
                format!(
                    "stored block failed validation in {} line {}: {error:?}",
                    path.display(),
                    line_number
                )
            })?;
        seen_block = true;
    }

    let chain = chain_from_blocks(config, fork_choice.best_chain())?;
    Ok((chain, fork_choice))
}

enum StorageLine {
    Header(String),
    Block(Box<chesscoin_core::domain::Block>),
}

fn decode_storage_line(line: &str) -> Result<StorageLine, String> {
    if line.starts_with(STORAGE_HEADER_VERSION) {
        decode_storage_header(line).map(StorageLine::Header)
    } else {
        decode_storage_record(line)
            .map(Box::new)
            .map(StorageLine::Block)
    }
}

fn encode_storage_header(config: &ChainConfig) -> String {
    let fingerprint = chain_fingerprint(config);
    let checksum = storage_record_checksum(&fingerprint);
    format!("{STORAGE_HEADER_VERSION}|{checksum}|{fingerprint}")
}

fn decode_storage_header(line: &str) -> Result<String, String> {
    let fields = line.splitn(3, '|').collect::<Vec<_>>();
    if fields.first().copied() != Some(STORAGE_HEADER_VERSION) {
        return Err("storage header version mismatch".to_string());
    }
    if fields.len() != 3 {
        return Err("storage header requires version, checksum, and fingerprint".to_string());
    }

    let expected = Digest::from_hex(fields[1])?;
    let actual = storage_record_checksum(fields[2]);
    if expected != actual {
        return Err("storage header checksum mismatch".to_string());
    }

    Ok(fields[2].to_string())
}

fn encode_storage_record(block: &chesscoin_core::domain::Block) -> String {
    let payload = encode_block_message(block);
    let checksum = storage_record_checksum(&payload);
    format!("{STORAGE_RECORD_VERSION}|{checksum}|{payload}")
}

fn decode_storage_record(line: &str) -> Result<chesscoin_core::domain::Block, String> {
    let fields = line.splitn(3, '|').collect::<Vec<_>>();
    if fields.first().copied() != Some(STORAGE_RECORD_VERSION) {
        return decode_block_message(line);
    }
    if fields.len() != 3 {
        return Err("storage record requires version, checksum, and payload".to_string());
    }

    let expected = Digest::from_hex(fields[1])?;
    let actual = storage_record_checksum(fields[2]);
    if expected != actual {
        return Err("storage record checksum mismatch".to_string());
    }

    decode_block_message(fields[2])
}

fn storage_record_checksum(payload: &str) -> Digest {
    ToyHash.digest(payload.as_bytes())
}

fn acquire_storage_lock(data_dir: &Path) -> Result<StorageLock, String> {
    fs::create_dir_all(data_dir).map_err(|error| {
        format!(
            "failed to create chain storage directory {}: {error}",
            data_dir.display()
        )
    })?;
    let path = data_dir.join("node.lock");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            format!(
                "failed to lock chain storage {}: {error}; another node may be using this data-dir",
                path.display()
            )
        })?;
    Ok(StorageLock { path, _file: file })
}

fn append_block(
    path: &Path,
    config: &ChainConfig,
    block: &chesscoin_core::domain::Block,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create chain storage directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let should_write_header = match fs::metadata(path) {
        Ok(metadata) => metadata.len() == 0,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => false,
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open chain storage {}: {error}", path.display()))?;
    if should_write_header {
        file.write_all(encode_storage_header(config).as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|error| {
                format!("failed to write chain storage {}: {error}", path.display())
            })?;
    }
    file.write_all(encode_storage_record(block).as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.flush())
        .and_then(|_| file.sync_data())
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
    use chesscoin_core::application::MAX_DIGEST_ZERO_BITS;
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

    fn wait_for_oversized(node: &RunningNode, oversized: usize) -> NodeSnapshot {
        for _ in 0..250 {
            let snapshot = node.snapshot();
            if snapshot.oversized_messages >= oversized {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn wait_for_malformed(node: &RunningNode, malformed: usize) -> NodeSnapshot {
        for _ in 0..250 {
            let snapshot = node.snapshot();
            if snapshot.malformed_messages >= malformed {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn wait_for_incompatible(node: &RunningNode, incompatible: usize) -> NodeSnapshot {
        for _ in 0..250 {
            let snapshot = node.snapshot();
            if snapshot.incompatible_messages >= incompatible {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn wait_for_dropped_inbound(node: &RunningNode, dropped_inbound: usize) -> NodeSnapshot {
        for _ in 0..250 {
            let snapshot = node.snapshot();
            if snapshot.dropped_inbound_connections >= dropped_inbound {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn wait_for_known_peer(node: &RunningNode, peer: SocketAddr) -> NodeSnapshot {
        for _ in 0..250 {
            let snapshot = node.snapshot();
            if snapshot.known_peers.contains(&peer) {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        node.snapshot()
    }

    fn small_chain_config() -> ChainConfig {
        ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        }
    }

    fn test_state(config: ChainConfig, storage_path: Option<PathBuf>) -> NodeState {
        NodeState {
            node_id: "test-node".to_string(),
            bound_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertised_addr: SocketAddr::from(([127, 0, 0, 1], 9333)),
            chain: ChainState::new(config.clone()),
            fork_choice: ForkChoiceState::new(config),
            peers: Vec::new(),
            accepted_blocks: 0,
            rejected_blocks: 0,
            mined_blocks: 0,
            reorgs: 0,
            malformed_messages: 0,
            incompatible_messages: 0,
            oversized_messages: 0,
            outbound_blocks: 0,
            failed_broadcasts: 0,
            dropped_inbound_connections: 0,
            peer_rejections: 0,
            synced_blocks: 0,
            failed_syncs: 0,
            storage_failures: 0,
            _storage_lock: None,
            storage_path,
            network: NetworkConfig::default(),
            sync: SyncConfig::default(),
        }
    }

    fn competing_branch_blocks() -> Vec<chesscoin_core::domain::Block> {
        let config = small_chain_config();
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let mut base_chain = ChainState::new(config);
        let block0 = base_chain.mine_next_block(&hasher, &sampler, 1, 2);
        base_chain
            .apply_block(&hasher, &sampler, block0.clone())
            .expect("block0 applies");

        let branch_a1 = base_chain.mine_next_block(&hasher, &sampler, 3, 4);
        let branch_b1 = base_chain.mine_next_block(&hasher, &sampler, 5, 6);
        let mut branch_a_chain = base_chain.clone();
        branch_a_chain
            .apply_block(&hasher, &sampler, branch_a1.clone())
            .expect("branch a1 applies");
        let branch_a2 = branch_a_chain.mine_next_block(&hasher, &sampler, 7, 8);

        vec![block0, branch_b1, branch_a1, branch_a2]
    }

    #[test]
    fn initial_peers_are_deduplicated_and_bounded() {
        let bound = SocketAddr::from(([127, 0, 0, 1], 9333));
        let peer_a = SocketAddr::from(([127, 0, 0, 1], 9334));
        let peer_b = SocketAddr::from(([127, 0, 0, 1], 9335));
        let peer_c = SocketAddr::from(([127, 0, 0, 1], 9336));
        let invalid = SocketAddr::from(([0, 0, 0, 0], 9337));

        let advertised = SocketAddr::from(([127, 0, 0, 1], 9338));
        let (peers, rejected) = normalize_initial_peers(
            bound,
            advertised,
            vec![bound, advertised, invalid, peer_a, peer_a, peer_b, peer_c],
            2,
        );

        assert_eq!(peers, vec![peer_a, peer_b]);
        assert_eq!(rejected, 5);
    }

    #[test]
    fn add_peer_rejects_self_duplicates_and_capacity_overflow() {
        let mut state = test_state(small_chain_config(), None);
        state.network.max_peers = 1;
        let peer_a = SocketAddr::from(([127, 0, 0, 1], 9334));
        let peer_b = SocketAddr::from(([127, 0, 0, 1], 9335));

        assert!(!add_peer_to_state(
            &mut state,
            SocketAddr::from(([127, 0, 0, 1], 0))
        ));
        assert!(!add_peer_to_state(
            &mut state,
            SocketAddr::from(([0, 0, 0, 0], 9333))
        ));
        let advertised = SocketAddr::from(([127, 0, 0, 1], 9336));
        state.advertised_addr = advertised;
        assert!(!add_peer_to_state(&mut state, advertised));
        assert_eq!(state.peers, Vec::<SocketAddr>::new());
        assert!(add_peer_to_state(&mut state, peer_a));
        assert!(!add_peer_to_state(&mut state, peer_a));
        assert!(!add_peer_to_state(&mut state, peer_b));
        assert_eq!(state.peers, vec![peer_a]);
    }

    #[test]
    fn peer_address_validation_rejects_non_dialable_addresses() {
        assert!(peer_address_is_dialable(SocketAddr::from((
            [127, 0, 0, 1],
            9333
        ))));
        assert!(!peer_address_is_dialable(SocketAddr::from((
            [127, 0, 0, 1],
            0
        ))));
        assert!(!peer_address_is_dialable(SocketAddr::from((
            [0, 0, 0, 0],
            9333
        ))));
        assert!(!peer_address_is_dialable(SocketAddr::from((
            [224, 0, 0, 1],
            9333
        ))));
        assert!(!peer_address_is_dialable(SocketAddr::from((
            [255, 255, 255, 255],
            9333
        ))));
        assert!(!peer_address_is_dialable(
            "[::]:9333".parse().expect("valid addr")
        ));
        assert!(!peer_address_is_dialable(
            "[ff02::1]:9333".parse().expect("valid addr")
        ));
    }

    #[test]
    fn add_peer_command_path_counts_rejections() {
        let mut raw_state = test_state(small_chain_config(), None);
        raw_state.network.max_peers = 1;
        let state = Arc::new(Mutex::new(raw_state));
        let peer_a = SocketAddr::from(([127, 0, 0, 1], 9334));
        let peer_b = SocketAddr::from(([127, 0, 0, 1], 9335));

        add_peer(&state, peer_a);
        add_peer(&state, peer_a);
        add_peer(&state, peer_b);
        add_peer(&state, SocketAddr::from(([0, 0, 0, 0], 9336)));

        let guard = state.lock().expect("state lock");
        assert_eq!(guard.peers, vec![peer_a]);
        assert_eq!(guard.peer_rejections, 3);
    }

    #[test]
    fn start_node_rejects_invalid_config_before_runtime_starts() {
        let mut config = NodeConfig::localhost_ephemeral("bad-difficulty");
        config.chain.difficulty_zero_bits = MAX_DIGEST_ZERO_BITS + 1;
        let error = start_node_error(config);
        assert!(error.contains("difficulty_zero_bits"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("bad-message-limit");
        config.network.max_message_bytes = MIN_MAX_MESSAGE_BYTES - 1;
        let error = start_node_error(config);
        assert!(error.contains("max_message_bytes"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("bad-inbound-limit");
        config.network.max_inbound_connections = 0;
        let error = start_node_error(config);
        assert!(error.contains("max_inbound_connections"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("bad-sync-limit");
        config.sync.max_blocks_per_response = 0;
        let error = start_node_error(config);
        assert!(error.contains("max_blocks_per_response"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("bad-write-timeout");
        config.network.write_timeout = Duration::ZERO;
        let error = start_node_error(config);
        assert!(error.contains("write_timeout"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("missing-advertise");
        config.listen_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let error = start_node_error(config);
        assert!(error.contains("advertise_addr is required"), "{error}");

        let mut config = NodeConfig::localhost_ephemeral("bad-advertise");
        config.advertise_addr = Some(SocketAddr::from(([0, 0, 0, 0], 9333)));
        let error = start_node_error(config);
        assert!(error.contains("advertise_addr must be dialable"), "{error}");
    }

    #[test]
    fn inbound_connection_limit_drops_excess_connections() {
        let mut config = NodeConfig::localhost_ephemeral("inbound-limit");
        config.chain = small_chain_config();
        config.network.max_inbound_connections = 1;
        config.network.read_timeout = Duration::from_secs(10);
        let node = start_node(config).expect("node starts");

        let mut held_streams = Vec::new();
        let mut first = TcpStream::connect(node.bound_addr()).expect("first connect");
        first.write_all(b"HOLD").expect("hold first connection");
        held_streams.push(first);

        let mut snapshot = node.snapshot();
        for _ in 0..8 {
            held_streams.push(TcpStream::connect(node.bound_addr()).expect("extra connect"));
            snapshot = wait_for_dropped_inbound(&node, 1);
            if snapshot.dropped_inbound_connections >= 1 {
                break;
            }
        }

        assert!(snapshot.dropped_inbound_connections >= 1);

        drop(held_streams);
        node.stop();
    }

    #[test]
    fn start_mining_rejects_zero_interval_command() {
        let mut config = NodeConfig::localhost_ephemeral("bad-miner-command");
        config.chain = small_chain_config();
        let node = start_node(config).expect("node starts");

        node.send(NodeCommand::StartMining(MinerConfig {
            interval: Duration::ZERO,
        }))
        .expect("start mining command sends");
        let snapshot = wait_for_malformed(&node, 1);

        assert_eq!(snapshot.malformed_messages, 1);
        assert_eq!(snapshot.mined_blocks, 0);

        node.stop();
    }

    #[test]
    fn outbound_connections_apply_read_and_write_timeouts() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake peer");
        let peer = listener.local_addr().expect("fake peer addr");
        let handle = thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept outbound connection");
        });
        let network = NetworkConfig {
            read_timeout: Duration::from_millis(1234),
            write_timeout: Duration::from_millis(2345),
            ..NetworkConfig::default()
        };

        let stream = connect_outbound(peer, &network).expect("connect outbound");

        assert_eq!(
            stream.read_timeout().expect("read timeout"),
            Some(network.read_timeout)
        );
        assert_eq!(
            stream.write_timeout().expect("write timeout"),
            Some(network.write_timeout)
        );

        drop(stream);
        handle.join().expect("fake peer exits");
    }

    #[test]
    fn peer_advertisement_includes_self_deduplicates_and_respects_limit() {
        let mut state = test_state(small_chain_config(), None);
        let peer_a = SocketAddr::from(([127, 0, 0, 1], 9334));
        let peer_b = SocketAddr::from(([127, 0, 0, 1], 9335));
        state.peers = vec![peer_a, peer_a, peer_b];

        assert_eq!(
            peer_advertisement(&state, 2),
            vec![state.advertised_addr, peer_a]
        );
    }

    #[test]
    fn configured_advertise_address_is_announced_to_peers() {
        let mut config_a = NodeConfig::localhost_ephemeral("advertise-a");
        config_a.chain = small_chain_config();
        let node_a = start_node(config_a).expect("node a starts");

        let advertised = SocketAddr::from(([127, 0, 0, 1], 39_393));
        let mut config_b = NodeConfig::localhost_ephemeral("advertise-b");
        config_b.chain = small_chain_config();
        config_b.advertise_addr = Some(advertised);
        config_b.peers.push(node_a.bound_addr());
        let node_b = start_node(config_b).expect("node b starts");

        let snapshot_a = wait_for_known_peer(&node_a, advertised);

        assert!(snapshot_a.known_peers.contains(&advertised));
        assert!(!snapshot_a.known_peers.contains(&node_b.bound_addr()));

        node_b.stop();
        node_a.stop();
    }

    #[test]
    fn startup_hello_announces_configured_peer_listen_address() {
        let mut config_a = NodeConfig::localhost_ephemeral("node-a");
        config_a.chain = small_chain_config();
        let node_a = start_node(config_a).expect("node a starts");

        let mut config_b = NodeConfig::localhost_ephemeral("node-b");
        config_b.chain = small_chain_config();
        config_b.peers.push(node_a.bound_addr());
        let node_b = start_node(config_b).expect("node b starts");

        let snapshot_a = wait_for_known_peer(&node_a, node_b.bound_addr());

        assert!(snapshot_a.known_peers.contains(&node_b.bound_addr()));

        node_b.stop();
        node_a.stop();
    }

    #[test]
    fn peer_exchange_discovers_listen_addresses_from_known_peers() {
        let mut config_a = NodeConfig::localhost_ephemeral("peer-a");
        config_a.chain = small_chain_config();
        let node_a = start_node(config_a).expect("node a starts");

        let mut config_c = NodeConfig::localhost_ephemeral("peer-c");
        config_c.chain = small_chain_config();
        config_c.peers.push(node_a.bound_addr());
        let node_c = start_node(config_c).expect("node c starts");
        let snapshot_a = wait_for_known_peer(&node_a, node_c.bound_addr());
        assert!(snapshot_a.known_peers.contains(&node_c.bound_addr()));

        let mut config_b = NodeConfig::localhost_ephemeral("peer-b");
        config_b.chain = small_chain_config();
        config_b.peers.push(node_a.bound_addr());
        let node_b = start_node(config_b).expect("node b starts");

        node_b
            .send(NodeCommand::SyncOnce)
            .expect("sync command sends");
        let snapshot_b = wait_for_known_peer(&node_b, node_c.bound_addr());

        assert!(snapshot_b.known_peers.contains(&node_c.bound_addr()));

        node_b.stop();
        node_c.stop();
        node_a.stop();
    }

    #[test]
    fn block_locator_lists_best_chain_hashes_from_tip_to_genesis() {
        let blocks = competing_branch_blocks();
        let mut state = test_state(small_chain_config(), None);
        for block in [blocks[0].clone(), blocks[2].clone(), blocks[3].clone()] {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        let locator = block_locator(&state.chain, 8);

        assert_eq!(locator[0], block_id(&blocks[3]));
        assert_eq!(locator[1], block_id(&blocks[2]));
        assert_eq!(locator[2], block_id(&blocks[0]));
        assert_eq!(locator[3], Digest::zero());
    }

    #[test]
    fn blocks_after_locator_returns_blocks_after_first_common_hash() {
        let blocks = competing_branch_blocks();
        let mut state = test_state(small_chain_config(), None);
        for block in blocks.clone() {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        let locator = vec![block_id(&blocks[1]), block_id(&blocks[0])];
        let response = blocks_after_locator(&state.chain, &locator, 10);

        assert_eq!(response, vec![blocks[2].clone(), blocks[3].clone()]);
    }

    #[test]
    fn inventory_after_locator_returns_hashes_after_first_common_hash() {
        let blocks = competing_branch_blocks();
        let mut state = test_state(small_chain_config(), None);
        for block in blocks.clone() {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        let locator = vec![block_id(&blocks[1]), block_id(&blocks[0])];
        let response = inventory_after_locator(&state.chain, &locator, 10);

        assert_eq!(response, vec![block_id(&blocks[2]), block_id(&blocks[3])]);
    }

    #[test]
    fn header_validation_accepts_contiguous_locator_extension() {
        let config = small_chain_config();
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let mut chain = ChainState::new(config);
        let block0 = chain.mine_next_block(&hasher, &sampler, 1, 2);
        chain
            .apply_block(&hasher, &sampler, block0.clone())
            .expect("block0 applies");
        let block1 = chain.mine_next_block(&hasher, &sampler, 3, 4);
        let locator = vec![block_id(&block0), Digest::zero()];

        assert_eq!(
            validate_headers_after_locator(&chain, &locator, &[block1.header]),
            Ok(())
        );
    }

    #[test]
    fn header_validation_rejects_disconnected_or_invalid_headers() {
        let config = small_chain_config();
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let mut chain = ChainState::new(config);
        let block0 = chain.mine_next_block(&hasher, &sampler, 1, 2);
        chain
            .apply_block(&hasher, &sampler, block0.clone())
            .expect("block0 applies");
        let block1 = chain.mine_next_block(&hasher, &sampler, 3, 4);
        let locator = vec![block_id(&block0), Digest::zero()];

        let mut bad_parent = block1.header.clone();
        bad_parent.previous_block = Digest::from_bytes([9; 32]);
        assert_eq!(
            validate_headers_after_locator(&chain, &locator, &[bad_parent]),
            Err(())
        );

        let mut bad_height = block1.header.clone();
        bad_height.height += 1;
        assert_eq!(
            validate_headers_after_locator(&chain, &locator, &[bad_height]),
            Err(())
        );

        let mut bad_sample_count = block1.header.clone();
        bad_sample_count.sample_count -= 1;
        assert_eq!(
            validate_headers_after_locator(&chain, &locator, &[bad_sample_count]),
            Err(())
        );
    }

    #[test]
    fn header_validation_rejects_insufficient_work() {
        let easy_config = small_chain_config();
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let easy_chain = ChainState::new(easy_config.clone());
        let block = easy_chain.mine_next_block(&hasher, &sampler, 1, 2);
        let actual_work = block_header_hash(&hasher, &block.header).leading_zero_bits();
        let mut strict_config = easy_config;
        strict_config.difficulty_zero_bits = actual_work + 1;
        let strict_chain = ChainState::new(strict_config);

        assert_eq!(
            validate_headers_after_locator(&strict_chain, &[], &[block.header]),
            Err(())
        );
    }

    #[test]
    fn header_validation_rejects_unrequested_genesis_replay() {
        let config = small_chain_config();
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let mut chain = ChainState::new(config);
        let block0 = chain.mine_next_block(&hasher, &sampler, 1, 2);
        chain
            .apply_block(&hasher, &sampler, block0.clone())
            .expect("block0 applies");
        let locator = vec![block_id(&block0)];

        assert_eq!(
            validate_headers_after_locator(&chain, &locator, &[block0.header]),
            Err(())
        );
    }

    #[test]
    fn blocks_after_unknown_locator_starts_from_genesis() {
        let blocks = competing_branch_blocks();
        let mut state = test_state(small_chain_config(), None);
        for block in blocks.iter().take(2).cloned() {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        let response = blocks_after_locator(&state.chain, &[Digest::from_bytes([9; 32])], 1);

        assert_eq!(response, vec![blocks[0].clone()]);
    }

    #[test]
    fn runtime_tracks_competing_branches_and_activates_best_tip() {
        let blocks = competing_branch_blocks();
        let best_hash = block_id(&blocks[3]);
        let mut state = test_state(small_chain_config(), None);

        for block in blocks {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        assert_eq!(state.chain.height(), 3);
        assert_eq!(state.chain.head_hash(&ToyHash), best_hash);
        assert_eq!(state.fork_choice.known_block_count(), 4);
        assert_eq!(state.accepted_blocks, 4);
        assert_eq!(state.rejected_blocks, 0);
        assert!(state.reorgs >= 1, "{:?}", state.reorgs);
    }

    #[test]
    fn duplicate_known_block_does_not_count_as_rejected() {
        let blocks = competing_branch_blocks();
        let mut state = test_state(small_chain_config(), None);

        assert_eq!(
            apply_incoming_block(&mut state, blocks[0].clone(), BlockSource::Gossip),
            BlockApplication::Accepted
        );
        assert_eq!(
            apply_incoming_block(&mut state, blocks[0].clone(), BlockSource::Gossip),
            BlockApplication::AlreadyKnown
        );

        assert_eq!(state.chain.height(), 1);
        assert_eq!(state.fork_choice.known_block_count(), 1);
        assert_eq!(state.accepted_blocks, 1);
        assert_eq!(state.rejected_blocks, 0);
    }

    #[test]
    fn persisted_fork_choice_reloads_best_branch() {
        let data_dir = unique_test_data_dir("fork-reload");
        let storage_path = data_dir.join("blocks.log");
        let blocks = competing_branch_blocks();
        let best_hash = block_id(&blocks[3]);
        let mut state = test_state(small_chain_config(), Some(storage_path.clone()));

        for block in blocks {
            assert_eq!(
                apply_incoming_block(&mut state, block, BlockSource::Gossip),
                BlockApplication::Accepted
            );
        }

        let (chain, fork_choice) =
            load_chains(&storage_path, small_chain_config()).expect("fork choice reloads");

        assert_eq!(chain.height(), 3);
        assert_eq!(chain.head_hash(&ToyHash), best_hash);
        assert_eq!(fork_choice.known_block_count(), 4);

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn storage_records_round_trip_with_checksum() {
        let block = competing_branch_blocks().remove(0);
        let record = encode_storage_record(&block);

        assert_eq!(decode_storage_record(&record).expect("valid record"), block);
    }

    #[test]
    fn storage_header_round_trips_with_checksum() {
        let config = small_chain_config();
        let header = encode_storage_header(&config);

        assert_eq!(
            decode_storage_header(&header).expect("valid header"),
            chain_fingerprint(&config)
        );
    }

    #[test]
    fn append_block_writes_chain_fingerprint_header_for_new_logs() {
        let data_dir = unique_test_data_dir("header-log");
        let storage_path = data_dir.join("blocks.log");
        let config = small_chain_config();
        let block = competing_branch_blocks().remove(0);

        append_block(&storage_path, &config, &block).expect("append good block");
        let content = fs::read_to_string(&storage_path).expect("read log");
        let lines = content.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 2);
        assert_eq!(
            decode_storage_header(lines[0]).expect("valid header"),
            chain_fingerprint(&config)
        );
        assert_eq!(
            decode_storage_record(lines[1]).expect("valid block record"),
            block
        );

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn storage_rejects_checksum_mismatch() {
        let block = competing_branch_blocks().remove(0);
        let payload = encode_block_message(&block);
        let record = format!("{}|{}|{}", STORAGE_RECORD_VERSION, Digest::zero(), payload);

        assert_eq!(
            decode_storage_record(&record),
            Err("storage record checksum mismatch".to_string())
        );
    }

    #[test]
    fn legacy_plain_block_log_still_loads() {
        let data_dir = unique_test_data_dir("legacy-log");
        let storage_path = data_dir.join("blocks.log");
        let block = competing_branch_blocks().remove(0);
        fs::create_dir_all(&data_dir).expect("create data dir");
        fs::write(&storage_path, format!("{}\n", encode_block_message(&block))).expect("write log");

        let (chain, fork_choice) =
            load_chains(&storage_path, small_chain_config()).expect("legacy log loads");

        assert_eq!(chain.height(), 1);
        assert_eq!(chain.head_hash(&ToyHash), block_id(&block));
        assert_eq!(fork_choice.known_block_count(), 1);

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn storage_rejects_incompatible_chain_fingerprint_header() {
        let data_dir = unique_test_data_dir("fingerprint-log");
        let storage_path = data_dir.join("blocks.log");
        let config = small_chain_config();
        let block = competing_branch_blocks().remove(0);
        append_block(&storage_path, &config, &block).expect("append good block");

        let mut incompatible = config;
        incompatible.samples_per_block += 1;
        let error = load_chains(&storage_path, incompatible).expect_err("fingerprint mismatch");

        assert!(error.contains("fingerprint mismatch"), "{error}");

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn interrupted_tail_record_is_ignored_on_reload() {
        let data_dir = unique_test_data_dir("tail-log");
        let storage_path = data_dir.join("blocks.log");
        let block = competing_branch_blocks().remove(0);
        append_block(&storage_path, &small_chain_config(), &block).expect("append good block");
        let mut file = OpenOptions::new()
            .append(true)
            .open(&storage_path)
            .expect("open log");
        file.write_all(b"CCBLK1|partial").expect("write partial");
        file.flush().expect("flush partial");

        let (chain, fork_choice) =
            load_chains(&storage_path, small_chain_config()).expect("tail recovers");

        assert_eq!(chain.height(), 1);
        assert_eq!(chain.head_hash(&ToyHash), block_id(&block));
        assert_eq!(fork_choice.known_block_count(), 1);

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn completed_corrupt_record_is_fatal_on_reload() {
        let data_dir = unique_test_data_dir("corrupt-log");
        let storage_path = data_dir.join("blocks.log");
        let block = competing_branch_blocks().remove(0);
        append_block(&storage_path, &small_chain_config(), &block).expect("append good block");
        let mut file = OpenOptions::new()
            .append(true)
            .open(&storage_path)
            .expect("open log");
        file.write_all(b"CCBLK1|partial\n")
            .expect("write corrupt complete record");
        file.flush().expect("flush corrupt record");

        let error =
            load_chains(&storage_path, small_chain_config()).expect_err("corrupt log fails");

        assert!(error.contains("invalid chain storage"), "{error}");

        let _ = fs::remove_dir_all(data_dir);
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
    fn node_serves_inventory_after_locator() {
        let mut config = NodeConfig::localhost_ephemeral("inventory");
        config.chain.difficulty_zero_bits = 0;
        config.chain.steps_per_block = 4;
        config.chain.samples_per_block = 4;
        let node = start_node(config).expect("node starts");

        node.send(NodeCommand::MineOnce {
            training_seed: 123,
            sampling_entropy: 456,
        })
        .expect("mine command sends");
        let snapshot = wait_for_height(&node, 1);

        let network = {
            let guard = node.state.lock().expect("node state lock");
            guard.network.clone()
        };
        let request = encode_network_message(
            &network.wire,
            &PeerMessage::GetInventory {
                locator: vec![Digest::zero()],
                limit: 8,
            },
        ) + "\n";
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(request.as_bytes()).expect("write");
        stream.flush().expect("flush");

        let line = read_limited_line_from(&mut stream, network.max_message_bytes)
            .expect("read inventory")
            .expect("inventory line");
        let response = decode_network_message(&line, &network.wire).expect("valid inventory");

        assert_eq!(
            response,
            PeerMessage::Inventory {
                hashes: vec![snapshot.head]
            }
        );

        node.stop();
    }

    #[test]
    fn node_serves_headers_after_locator() {
        let mut config = NodeConfig::localhost_ephemeral("headers");
        config.chain.difficulty_zero_bits = 0;
        config.chain.steps_per_block = 4;
        config.chain.samples_per_block = 4;
        let node = start_node(config).expect("node starts");

        node.send(NodeCommand::MineOnce {
            training_seed: 123,
            sampling_entropy: 456,
        })
        .expect("mine command sends");
        let snapshot = wait_for_height(&node, 1);

        let network = {
            let guard = node.state.lock().expect("node state lock");
            guard.network.clone()
        };
        let request = encode_network_message(
            &network.wire,
            &PeerMessage::GetHeaders {
                locator: vec![Digest::zero()],
                limit: 8,
            },
        ) + "\n";
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(request.as_bytes()).expect("write");
        stream.flush().expect("flush");

        let line = read_limited_line_from(&mut stream, network.max_message_bytes)
            .expect("read headers")
            .expect("headers line");
        let response = decode_network_message(&line, &network.wire).expect("valid headers");

        let PeerMessage::Headers { headers } = response else {
            panic!("expected headers response");
        };
        assert_eq!(headers.len(), 1);
        assert_eq!(block_header_hash(&ToyHash, &headers[0]), snapshot.head);

        node.stop();
    }

    #[test]
    fn node_serves_bounded_peers() {
        let mut config = NodeConfig::localhost_ephemeral("peers");
        config.chain = small_chain_config();
        config.network.max_peers = 4;
        let node = start_node(config).expect("node starts");
        let network = {
            let guard = node.state.lock().expect("node state lock");
            guard.network.clone()
        };
        let request =
            encode_network_message(&network.wire, &PeerMessage::GetPeers { limit: 1 }) + "\n";
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(request.as_bytes()).expect("write");
        stream.flush().expect("flush");

        let line = read_limited_line_from(&mut stream, network.max_message_bytes)
            .expect("read peers")
            .expect("peers line");
        let response = decode_network_message(&line, &network.wire).expect("valid peers");

        assert_eq!(
            response,
            PeerMessage::Peers {
                peers: vec![node.bound_addr()]
            }
        );

        node.stop();
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
    fn second_node_rejects_locked_data_dir_until_first_stops() {
        let data_dir = unique_test_data_dir("storage-lock");
        let mut config = NodeConfig::localhost_ephemeral("storage-lock-a");
        config.chain.difficulty_zero_bits = 0;
        config.chain.steps_per_block = 4;
        config.chain.samples_per_block = 4;
        config.storage = Some(StorageConfig {
            data_dir: data_dir.clone(),
        });
        let node = start_node(config.clone()).expect("first node starts");

        let error = match start_node(config.clone()) {
            Ok(node) => {
                node.stop();
                panic!("second node should fail while lock is held");
            }
            Err(error) => error,
        };
        assert!(error.contains("failed to lock chain storage"), "{error}");

        node.stop();

        config.node_id = "storage-lock-b".to_string();
        let restarted = start_node(config).expect("lock is released after stop");
        restarted.stop();

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn node_stop_waits_for_inbound_handlers_before_releasing_storage_lock() {
        let data_dir = unique_test_data_dir("storage-lock-inbound");
        let mut config = NodeConfig::localhost_ephemeral("storage-lock-inbound-a");
        config.chain = small_chain_config();
        config.storage = Some(StorageConfig {
            data_dir: data_dir.clone(),
        });
        config.network.max_inbound_connections = 1;
        config.network.read_timeout = Duration::from_millis(100);
        let node = start_node(config.clone()).expect("node starts");

        let mut held_streams = Vec::new();
        let mut first = TcpStream::connect(node.bound_addr()).expect("first connect");
        first.write_all(b"HOLD").expect("hold first connection");
        held_streams.push(first);

        for _ in 0..8 {
            held_streams.push(TcpStream::connect(node.bound_addr()).expect("extra connect"));
            if wait_for_dropped_inbound(&node, 1).dropped_inbound_connections >= 1 {
                break;
            }
        }
        assert!(node.snapshot().dropped_inbound_connections >= 1);

        node.stop();

        config.node_id = "storage-lock-inbound-b".to_string();
        let restarted = start_node(config).expect("lock is released after inbound handlers stop");
        restarted.stop();

        drop(held_streams);
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn persist_block_counts_storage_failures_without_panicking() {
        let data_dir = unique_test_data_dir("storage-failure");
        fs::write(&data_dir, b"not a directory").expect("write file in data-dir position");
        let storage_path = data_dir.join("blocks.log");
        let mut state = test_state(small_chain_config(), Some(storage_path));
        let block = state
            .chain
            .mine_next_block(&ToyHash, &DeterministicSampler, 77, 88);

        persist_block(&mut state, &block);

        assert_eq!(state.storage_failures, 1);

        let _ = fs::remove_file(data_dir);
    }

    #[test]
    fn malformed_peer_message_is_counted_and_rejected() {
        let node = start_node(NodeConfig::localhost_ephemeral("malformed")).expect("node starts");
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(b"NOT_A_MESSAGE\n").expect("write");
        stream.flush().expect("flush");
        drop(stream);

        let snapshot = wait_for_malformed(&node, 1);

        assert_eq!(snapshot.rejected_blocks, 1);
        assert_eq!(snapshot.malformed_messages, 1);

        node.stop();
    }

    #[test]
    fn locator_request_over_limit_is_counted_as_malformed() {
        let mut config = NodeConfig::localhost_ephemeral("locator-limit");
        config.sync.max_locator_hashes = 1;
        let node = start_node(config).expect("node starts");
        let network = {
            let guard = node.state.lock().expect("node state lock");
            guard.network.clone()
        };
        let request = encode_network_message(
            &network.wire,
            &PeerMessage::GetInventory {
                locator: vec![Digest::from_bytes([1; 32]), Digest::from_bytes([2; 32])],
                limit: 8,
            },
        ) + "\n";
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        stream.write_all(request.as_bytes()).expect("write");
        stream.flush().expect("flush");
        drop(stream);

        let snapshot = wait_for_malformed(&node, 1);

        assert_eq!(snapshot.malformed_messages, 1);

        node.stop();
    }

    #[test]
    fn inventory_response_over_requested_limit_fails_sync() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake peer");
        let peer = listener.local_addr().expect("fake peer addr");
        let network = NetworkConfig::default();
        let response_network = network.clone();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept inventory request");
            let _ = read_limited_line_from(&mut stream, response_network.max_message_bytes)
                .expect("read request");
            let response = encode_network_message(
                &response_network.wire,
                &PeerMessage::Inventory {
                    hashes: vec![Digest::from_bytes([1; 32]), Digest::from_bytes([2; 32])],
                },
            ) + "\n";
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        });

        assert_eq!(inventory_from_peer(peer, Vec::new(), 1, &network), Err(()));

        handle.join().expect("fake peer exits");
    }

    #[test]
    fn peers_response_over_requested_limit_fails_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake peer");
        let peer = listener.local_addr().expect("fake peer addr");
        let network = NetworkConfig::default();
        let response_network = network.clone();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept peers request");
            let _ = read_limited_line_from(&mut stream, response_network.max_message_bytes)
                .expect("read request");
            let response = encode_network_message(
                &response_network.wire,
                &PeerMessage::Peers {
                    peers: vec![
                        SocketAddr::from(([127, 0, 0, 1], 9334)),
                        SocketAddr::from(([127, 0, 0, 1], 9335)),
                    ],
                },
            ) + "\n";
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        });

        assert_eq!(peers_from_peer(peer, 1, &network), Err(()));

        handle.join().expect("fake peer exits");
    }

    #[test]
    fn peer_exchange_rejects_non_dialable_advertised_peers() {
        let mut state = test_state(small_chain_config(), None);
        let valid_peer = SocketAddr::from(([127, 0, 0, 1], 9334));
        let invalid_peer = SocketAddr::from(([0, 0, 0, 0], 9335));

        for candidate in [valid_peer, invalid_peer] {
            if !add_peer_to_state(&mut state, candidate) {
                state.peer_rejections += 1;
            }
        }

        assert_eq!(state.peers, vec![valid_peer]);
        assert_eq!(state.peer_rejections, 1);
    }

    #[test]
    fn headers_response_over_requested_limit_fails_sync() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake peer");
        let peer = listener.local_addr().expect("fake peer addr");
        let network = NetworkConfig::default();
        let response_network = network.clone();
        let headers = competing_branch_blocks()
            .into_iter()
            .take(2)
            .map(|block| block.header)
            .collect::<Vec<_>>();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept headers request");
            let _ = read_limited_line_from(&mut stream, response_network.max_message_bytes)
                .expect("read request");
            let response =
                encode_network_message(&response_network.wire, &PeerMessage::Headers { headers })
                    + "\n";
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        });

        assert_eq!(headers_from_peer(peer, Vec::new(), 1, &network), Err(()));

        handle.join().expect("fake peer exits");
    }

    #[test]
    fn disconnected_headers_fail_sync_before_block_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake peer");
        let peer = listener.local_addr().expect("fake peer addr");
        let network = NetworkConfig::default();
        let response_network = network.clone();
        let mut bad_header = competing_branch_blocks().remove(0).header;
        bad_header.previous_block = Digest::from_bytes([9; 32]);
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept headers request");
            let _ = read_limited_line_from(&mut stream, response_network.max_message_bytes)
                .expect("read request");
            let response = encode_network_message(
                &response_network.wire,
                &PeerMessage::Headers {
                    headers: vec![bad_header],
                },
            ) + "\n";
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        });
        let state = Arc::new(Mutex::new(test_state(small_chain_config(), None)));

        assert_eq!(
            sync_from_peer(&state, peer, Vec::new(), 1, &network),
            Err(())
        );

        handle.join().expect("fake peer exits");
    }

    #[test]
    fn oversized_peer_message_is_counted_and_rejected_before_decode() {
        let mut config = NodeConfig::localhost_ephemeral("oversized");
        config.network.max_message_bytes = MIN_MAX_MESSAGE_BYTES;
        let node = start_node(config).expect("node starts");
        let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
        let payload = vec![b'X'; MIN_MAX_MESSAGE_BYTES + 1];
        stream.write_all(&payload).expect("write");
        stream.flush().expect("flush");
        drop(stream);

        let snapshot = wait_for_oversized(&node, 1);

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
            chain_fingerprint: crate::wire::chain_fingerprint(&ChainConfig::default()),
            protocol_version: crate::wire::PROTOCOL_VERSION,
        };
        let message = crate::wire::encode_network_message(
            &wrong_network,
            &PeerMessage::GetBlocks {
                from_height: 0,
                limit: 1,
            },
        ) + "\n";
        let snapshot = send_until_incompatible(&node, &message);

        assert!(snapshot.incompatible_messages >= 1, "{snapshot:?}");

        node.stop();
    }

    #[test]
    fn incompatible_chain_fingerprint_message_is_counted() {
        let node = start_node(NodeConfig::localhost_ephemeral("chain-a")).expect("node starts");
        let wrong_chain = crate::wire::WireConfig {
            network_id: crate::wire::WireConfig::default().network_id,
            chain_fingerprint: "steps=99;samples=4;difficulty=0".to_string(),
            protocol_version: crate::wire::PROTOCOL_VERSION,
        };
        let message = crate::wire::encode_network_message(
            &wrong_chain,
            &PeerMessage::GetBlocks {
                from_height: 0,
                limit: 1,
            },
        ) + "\n";
        let snapshot = send_until_incompatible(&node, &message);

        assert!(snapshot.incompatible_messages >= 1, "{snapshot:?}");

        node.stop();
    }

    fn send_until_incompatible(node: &RunningNode, message: &str) -> NodeSnapshot {
        for _ in 0..5 {
            let mut stream = TcpStream::connect(node.bound_addr()).expect("connect");
            stream.write_all(message.as_bytes()).expect("write");
            stream.flush().expect("flush");
            drop(stream);

            let snapshot = wait_for_incompatible(node, 1);
            if snapshot.incompatible_messages >= 1 {
                return snapshot;
            }
        }

        node.snapshot()
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

    fn start_node_error(config: NodeConfig) -> String {
        match start_node(config) {
            Ok(node) => {
                node.stop();
                panic!("node should reject invalid config");
            }
            Err(error) => error,
        }
    }
}
