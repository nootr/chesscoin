# ChessCoin

ChessCoin is an experimental protocol research project exploring deterministic proof-of-training for chess AI on top of a proof-of-work blockchain design.

The repository now contains the public GitHub Pages whitepaper, a local simulator, and a v0.1 node that can mine and exchange blocks with peers over TCP. The whitepaper is published from `www/index.html`, with supporting assets in `www/assets/`.

GitHub Pages is deployed by the workflow in `.github/workflows/pages.yml`, which publishes the `www/` folder.

## Status

Research MVP. No token, wallet, persistence, RandomX integration, or production network exists yet.

The implemented v0.1 loop is:

```text
M_t -> deterministic training trace -> trace commitment root -> sampled verification -> block accept/reject -> TCP gossip
```

## Repository Layout

- `www/` - GitHub Pages website and canonical whitepaper HTML.
- `crates/chesscoin-core/` - Pure protocol domain, application use case, and ports.
- `crates/chesscoin-node/` - Node-side adapters, wire protocol, and TCP runtime.
- `crates/chesscoin-cli/` - CLI delivery layer for the simulator and node.
- `docs/` - Developer architecture and node operation notes.

The Rust code follows strict hexagonal architecture principles. The core crate depends only on domain logic and ports. Concrete hashing, sampling, wire encoding, TCP sockets, and process lifecycle live outside the core crate. This keeps protocol tests focused and leaves room to replace research adapters with production-grade cryptography, persistence, networking, or RandomX integration later.

## Node v0.1

Tagged releases publish prebuilt `chesscoin` binaries for Linux, macOS, and Windows on the [GitHub Releases](https://github.com/nootr/chesscoin/releases) page. Each release includes `SHA256SUMS.txt` for artifact verification.

Start a node that mines one block and exits after one second:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine --run-ms 1000
```

Start another node and connect it to the first:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9334 --peer 127.0.0.1:9333
```

For quick local demos, remove toy proof-of-work waiting:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:0 --mine --run-ms 500 --difficulty 0 --steps 4 --samples 4
```

Node v0.1 validates incoming blocks by checking height, previous block hash, model transition metadata, trace root, commitment-chain structure, toy proof-of-work, and sampled deterministic training transitions. Accepted blocks are applied locally and gossiped to known peers.

## Local Simulator

Run the honest path:

```sh
cargo run -p chesscoin-cli -- simulate
```

Run a tampered trace demonstration where all steps are sampled, so the invalid opening is rejected:

```sh
cargo run -p chesscoin-cli -- simulate --steps 8 --samples 8 --tamper-step 3
```

Useful options:

```text
--steps N        Number of deterministic training steps
--samples N      Number of trace steps to sample for verification
--seed N         Training seed
--entropy N      Post-commitment sampling entropy
--tamper-step N  Mutate one opened trace step to demonstrate rejection
```

The current simulator intentionally uses a labeled toy 256-bit hash adapter. It is suitable for deterministic local research demos only, not for consensus or security claims.

## Developer Docs

- [Architecture](docs/architecture.md)
- [Running a node](docs/node-v0.1.md)

## Development

```sh
cargo fmt --all --check
cargo test --workspace
```

The P2P tests bind localhost TCP sockets. Some restricted sandboxes block that; normal local shells and CI runners should allow it.

## Next Milestones

- Refine the public whitepaper.
- Add persistent chain storage and restart recovery.
- Add peer discovery, chain sync, fork choice, and better gossip controls.
- Replace the toy proof-of-work/hash adapter with RandomX-oriented integration.
- Add richer trace-opening data and verifier protocol notes.
- Decide later whether to integrate with or fork an existing RandomX-based chain.
