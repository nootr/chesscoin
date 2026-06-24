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

1. A node maintains a local `ChainState`.
2. Mining creates the next block from the current model state.
3. The block includes a deterministic training trace and trace commitment root.
4. The node checks toy proof-of-work difficulty over the block header.
5. Peers receive full v0.1 blocks over TCP.
6. Receivers validate height, previous hash, model transition metadata, trace root, commitment chain, proof-of-work, and sampled deterministic transitions.
7. Accepted blocks are applied locally, persisted to the node block log when storage is configured, and gossiped to known peers.
8. Peer messages are bounded by configured size and timeout limits, with malformed and oversized messages counted in node snapshots.
9. Peer messages carry an explicit protocol version and network id, so incompatible networks are rejected before block handling.
10. Late peers can request missing blocks from a given height and validate each synced block through the normal chain application path.

## Current Research Limits

This is not a production cryptocurrency. RandomX is not integrated yet, the hash adapter is a labeled toy hash, there is no wallet, no mempool, and networking hardening is still basic. The core crate has a tested fork-choice index, but the v0.1 runtime still applies blocks linearly and does not perform reorgs. Sync is height-based catch-up, not a full fork-aware historical reconciliation protocol. Persistence is currently a simple append-only block log, not a robust database or recovery subsystem. The purpose of v0.1 is to make the core protocol loop executable and testable across actual local peers.
