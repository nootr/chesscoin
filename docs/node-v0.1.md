# Running a ChessCoin Node v0.1

Download a tagged release from the [GitHub Releases](https://github.com/nootr/chesscoin/releases) page, then verify the archive with `SHA256SUMS.txt`. Releases include native archives for Linux, macOS, and Windows.

Start a node with continuous mining and local block-log persistence:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9333 --mine --data-dir .chesscoin
```

Start a second node that connects to the first:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:9334 --peer 127.0.0.1:9333
```

You can also put stable operator settings in a simple `key=value` config file:

```text
node_id=operator-a
listen=127.0.0.1:9333
network_id=chesscoin-local
data_dir=.chesscoin
mine=true
mine_interval_ms=5000
difficulty=8
max_message_bytes=1048576
sync_interval_ms=5000
```

Run it with:

```sh
cargo run -p chesscoin-cli -- node --config node.conf
```

Useful options:

```text
--node-id ID       Human-readable local node label
--config PATH     Load key=value operator settings before CLI overrides
--network-id ID    Network identifier required in peer message envelopes
--protocol-version N
                  Wire protocol version required from peers
--listen ADDR     TCP address to bind, for example 127.0.0.1:9333
--peer ADDR       Peer TCP address; may be repeated
--mine            Continuously mine blocks
--mine-once       Mine one block after startup
--mine-interval-ms N
                  Delay between continuous mining attempts
--data-dir PATH   Persist accepted/mined blocks in PATH/blocks.log
--run-ms N        Run for N milliseconds, then stop; 0 means run forever
--steps N         Deterministic training steps per block
--samples N       Sampled transition checks per block
--difficulty N    Required leading zero bits for toy proof-of-work
--max-message-bytes N
                  Reject inbound peer messages larger than N bytes
--connect-timeout-ms N
                  Outbound peer connect timeout
--read-timeout-ms N
                  Inbound peer read timeout
--sync-interval-ms N
                  Delay between peer catch-up requests
--sync-max-blocks N
                  Maximum blocks returned by a sync response
--no-sync         Disable peer catch-up requests
--seed N          Training seed for --mine-once
--entropy N       Sampling entropy for --mine-once
```

For local development, use `--difficulty 0` to avoid waiting for toy proof-of-work:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:0 --mine --run-ms 500 --difficulty 0 --steps 4 --samples 4
```

Use `--mine-once` for deterministic smoke tests:

```sh
cargo run -p chesscoin-cli -- node --listen 127.0.0.1:0 --mine-once --run-ms 500 --difficulty 0 --steps 4 --samples 4
```

Node output includes the active `height` and `head`, plus counters for `mined blocks`, `known blocks`, `accepted blocks`, `rejected blocks`, `synced blocks`, and `reorgs`. `known blocks` counts valid blocks retained by fork choice, including valid side branches. `reorgs` increments when the active best branch changes away from the previous head.

## Verification

Run domain and adapter tests:

```sh
cargo fmt --all --check
cargo test --workspace
```

The P2P tests bind localhost TCP sockets. Some sandboxes block that; in a normal local shell or CI runner the tests should run without special flags.
