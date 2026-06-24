# Running a ChessCoin Node v0.1

Download a tagged release from the [GitHub Releases](https://github.com/nootr/chesscoin/releases) page, then verify the archive with `SHA256SUMS.txt`. Releases include native archives for Linux, macOS, and Windows.

Start a node that mines one local block and exits:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine --run-ms 1000
```

Start a second node that connects to the first:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9334 --peer 127.0.0.1:9333
```

Useful options:

```text
--node-id ID       Human-readable local node label
--listen ADDR     TCP address to bind, for example 127.0.0.1:9333
--peer ADDR       Peer TCP address; may be repeated
--mine            Mine one block after startup
--run-ms N        Run for N milliseconds, then stop; 0 means run forever
--steps N         Deterministic training steps per block
--samples N       Sampled transition checks per block
--difficulty N    Required leading zero bits for toy proof-of-work
--seed N          Training seed for --mine
--entropy N       Sampling entropy for --mine
```

For local development, use `--difficulty 0` to avoid waiting for toy proof-of-work:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:0 --mine --run-ms 500 --difficulty 0 --steps 4 --samples 4
```

## Verification

Run domain and adapter tests:

```sh
cargo fmt --all --check
cargo test --workspace
```

The P2P tests bind localhost TCP sockets. Some sandboxes block that; in a normal local shell or CI runner the tests should run without special flags.
