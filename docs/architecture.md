# ChessCoin Architecture

ChessCoin v0.1 is intentionally small, but it is structured as a hexagonal Rust workspace so the research protocol can grow without coupling consensus rules to networking or command-line details.

## Workspace

- `crates/chesscoin-core` contains domain objects, protocol use cases, and ports. It has no TCP, filesystem, process, or CLI dependencies.
- `crates/chesscoin-node` contains infrastructure adapters: the current toy hash, deterministic sampler, line-based wire protocol, and TCP runtime.
- `crates/chesscoin-cli` is the delivery layer. It parses commands, starts the node runtime, and renders simulator output.
- `www/` remains the GitHub Pages site and canonical whitepaper.

## Hexagonal Boundaries

The core crate owns:

- deterministic model-state transitions;
- trace entries and commitment-chain rules;
- sampled verification;
- block validation and chain application;
- ports for hashing and sampling.

The core crate does not own:

- concrete hash implementations;
- TCP sockets;
- wire encoding;
- process lifecycle;
- stdout formatting.

That separation is deliberate. Tests can exercise protocol behavior through fake ports, while node and CLI tests exercise real adapters and TCP behavior.

## Node v0.1 Flow

1. A node maintains a core `ForkChoiceState` plus an active `ChainState` view of the best branch.
2. Mining creates the next block from the current best model state.
3. The block includes a deterministic training trace and trace commitment root.
4. The node checks toy proof-of-work difficulty over the block header.
5. Peers receive full v0.1 blocks over TCP.
6. Receivers validate height, previous hash, model transition metadata, configured sample count, trace-state continuity, trace root, commitment chain, proof-of-work, and sampled deterministic transitions.
7. Accepted blocks are inserted into fork choice, persisted as checksummed block-log records under a chain-fingerprint header when storage is configured, and gossiped to known peers. A storage lock rejects concurrent writers for the same data directory, and persistence write failures are counted in snapshots instead of being ignored or crashing the miner.
8. Startup HELLO messages announce a node's dialable advertised listen address to configured peers so gossip can become bidirectional without manual peer entries on both sides. Successful and failed HELLO announcements are counted for operator diagnostics. Nodes that bind to an unspecified IP must configure an explicit advertised address.
9. Sync first requests bounded peer advertisements from known peers, then applies the same self, duplicate, and capacity checks used by manual peer configuration.
10. Peer messages, active inbound handlers, and known peers are bounded by configured limits, inbound reads and outbound writes use configured timeouts, non-dialable peer addresses are rejected before retention, and malformed, oversized, incompatible, failed response, dropped inbound, and rejected peer additions are counted in node snapshots.
11. Peer messages carry an explicit protocol version, network id, and chain fingerprint, so incompatible networks or chain parameters are rejected before block handling.
12. Late peers request bounded best-chain headers after a block locator, screen those headers for locator continuity, height sequence, configured sample count, and toy proof-of-work, then fetch and validate missing blocks through the normal fork-choice insertion path. Full-block sync responses must match the screened header hashes exactly, and malformed, oversized, or over-limit peer lists, locators, headers, blocks, and inventory responses are rejected before mutating fork choice.
13. Node startup validates chain, network, sync, storage, and miner settings before binding sockets, acquiring storage locks, or starting a miner.
14. Node shutdown joins active inbound peer handlers before returning, which prevents detached socket handlers from outliving node state or delaying storage-lock release invisibly.

## Current Research Limits

This is not a production cryptocurrency. RandomX is not integrated yet, the hash adapter is a labeled toy hash, and toy mining exists only to make local research blocks executable. There is no wallet, no mempool, and networking hardening is still basic. The core crate has a tested fork-choice index and the v0.1 runtime follows the best known branch. Sync now uses bounded peer exchange, block locators, and headers before block fetch instead of a height-only cursor, and configured peers exchange HELLO listen-address announcements, but it is not yet a full production sync, peer discovery, or historical reconciliation protocol. Persistence is a checksummed append-only block log with interrupted-tail recovery and chain-fingerprint headers for new logs, not a robust database or recovery subsystem. The purpose of v0.1 is to make the core protocol loop executable and testable across actual local peers.
