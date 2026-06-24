# ChessCoin Release Checklist

ChessCoin releases are research-node releases, not production-network launches.

Before pushing a tag:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo build --release --locked -p chesscoin-cli
```

Create a signed tag when possible:

```sh
git tag -s v0.1.0 -m "ChessCoin v0.1.0"
git push origin v0.1.0
```

The release workflow repeats the formatter, clippy, workspace test, and release-build gates before it builds Linux, macOS, and Windows archives. It publishes `SHA256SUMS.txt` and creates GitHub artifact attestations from that checksum file.

After the workflow finishes, verify one archive before announcing the release:

```sh
shasum -a 256 -c SHA256SUMS.txt
gh attestation verify chesscoin-v0.1.0-linux-x86_64.tar.gz --repo nootr/chesscoin
```
