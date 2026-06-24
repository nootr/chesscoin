use chesscoin_core::domain::{
    Block, BlockHeader, Digest, ModelState, TraceEntry, TrainingStep, TrainingTrace, MODEL_WIDTH,
};

pub const WIRE_MAGIC: &str = "CHESSCOIN";
pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireConfig {
    pub network_id: String,
    pub protocol_version: u16,
}

impl Default for WireConfig {
    fn default() -> Self {
        Self {
            network_id: "chesscoin-local".to_string(),
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerMessage {
    Hello {
        node_id: String,
        height: u64,
        head: Digest,
    },
    GetBlocks {
        from_height: u64,
        limit: usize,
    },
    Block(Box<Block>),
    EndBlocks,
}

pub fn encode_network_message(config: &WireConfig, message: &PeerMessage) -> String {
    format!(
        "{}|{}|{}|{}",
        WIRE_MAGIC,
        config.protocol_version,
        escape(&config.network_id),
        encode_message(message)
    )
}

pub fn decode_network_message(line: &str, config: &WireConfig) -> Result<PeerMessage, String> {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let fields = trimmed.splitn(4, '|').collect::<Vec<_>>();
    if fields.len() != 4 {
        return Err("network message requires envelope".to_string());
    }
    if fields[0] != WIRE_MAGIC {
        return Err("invalid network magic".to_string());
    }
    let protocol_version = parse_field::<u16>(fields[1], "protocol_version")?;
    if protocol_version != config.protocol_version {
        return Err("incompatible protocol version".to_string());
    }
    let network_id = unescape(fields[2])?;
    if network_id != config.network_id {
        return Err("incompatible network id".to_string());
    }
    decode_message(fields[3])
}

pub fn encode_message(message: &PeerMessage) -> String {
    match message {
        PeerMessage::Hello {
            node_id,
            height,
            head,
        } => {
            format!("HELLO|{}|{}|{}", escape(node_id), height, head)
        }
        PeerMessage::GetBlocks { from_height, limit } => {
            format!("GET_BLOCKS|{from_height}|{limit}")
        }
        PeerMessage::Block(block) => encode_block(block),
        PeerMessage::EndBlocks => "END_BLOCKS".to_string(),
    }
}

pub fn encode_block_message(block: &Block) -> String {
    encode_block(block)
}

pub fn decode_block_message(line: &str) -> Result<Block, String> {
    match decode_message(line)? {
        PeerMessage::Block(block) => Ok(*block),
        PeerMessage::Hello { .. } => Err("expected BLOCK message".to_string()),
        PeerMessage::GetBlocks { .. } => Err("expected BLOCK message".to_string()),
        PeerMessage::EndBlocks => Err("expected BLOCK message".to_string()),
    }
}

pub fn decode_message(line: &str) -> Result<PeerMessage, String> {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let fields = trimmed.split('|').collect::<Vec<_>>();
    match fields.first().copied() {
        Some("HELLO") => decode_hello(&fields),
        Some("GET_BLOCKS") => decode_get_blocks(&fields),
        Some("BLOCK") => decode_block(&fields),
        Some("END_BLOCKS") => {
            if fields.len() == 1 {
                Ok(PeerMessage::EndBlocks)
            } else {
                Err("END_BLOCKS requires no fields".to_string())
            }
        }
        Some(other) => Err(format!("unknown message type '{other}'")),
        None => Err("empty message".to_string()),
    }
}

fn decode_get_blocks(fields: &[&str]) -> Result<PeerMessage, String> {
    if fields.len() != 3 {
        return Err("GET_BLOCKS requires 2 fields".to_string());
    }

    Ok(PeerMessage::GetBlocks {
        from_height: parse_field(fields[1], "from_height")?,
        limit: parse_field(fields[2], "limit")?,
    })
}

fn decode_hello(fields: &[&str]) -> Result<PeerMessage, String> {
    if fields.len() != 4 {
        return Err("HELLO requires 3 fields".to_string());
    }

    Ok(PeerMessage::Hello {
        node_id: unescape(fields[1])?,
        height: parse_field(fields[2], "height")?,
        head: Digest::from_hex(fields[3])?,
    })
}

fn encode_block(block: &Block) -> String {
    let header = &block.header;
    let entries = block
        .trace
        .entries
        .iter()
        .map(encode_entry)
        .collect::<Vec<_>>()
        .join(";");

    [
        "BLOCK".to_string(),
        header.height.to_string(),
        header.previous_block.to_string(),
        encode_model(&header.model_before),
        encode_model(&header.model_after),
        header.trace_root.to_string(),
        header.training_seed.to_string(),
        header.sampling_entropy.to_string(),
        header.sample_count.to_string(),
        header.nonce.to_string(),
        encode_model(&block.trace.initial_model),
        encode_model(&block.trace.candidate_model),
        block.trace.root.to_string(),
        entries,
    ]
    .join("|")
}

fn decode_block(fields: &[&str]) -> Result<PeerMessage, String> {
    if fields.len() != 14 {
        return Err("BLOCK requires 13 fields".to_string());
    }

    let header = BlockHeader {
        height: parse_field(fields[1], "height")?,
        previous_block: Digest::from_hex(fields[2])?,
        model_before: decode_model(fields[3])?,
        model_after: decode_model(fields[4])?,
        trace_root: Digest::from_hex(fields[5])?,
        training_seed: parse_field(fields[6], "training_seed")?,
        sampling_entropy: parse_field(fields[7], "sampling_entropy")?,
        sample_count: parse_field(fields[8], "sample_count")?,
        nonce: parse_field(fields[9], "nonce")?,
    };

    let entries = if fields[13].is_empty() {
        Vec::new()
    } else {
        fields[13]
            .split(';')
            .map(decode_entry)
            .collect::<Result<Vec<_>, _>>()?
    };

    Ok(PeerMessage::Block(Box::new(Block {
        trace: TrainingTrace {
            initial_model: decode_model(fields[10])?,
            candidate_model: decode_model(fields[11])?,
            root: Digest::from_hex(fields[12])?,
            entries,
        },
        header,
    })))
}

fn encode_entry(entry: &TraceEntry) -> String {
    let step = &entry.step;
    [
        entry.previous_commitment.to_string(),
        entry.commitment.to_string(),
        step.index.to_string(),
        step.seed.to_string(),
        step.batch_id.to_string(),
        step.weight_index.to_string(),
        step.gradient.to_string(),
        step.learning_rate_ppm.to_string(),
        encode_model(&step.state_before),
        encode_model(&step.state_after),
    ]
    .join(",")
}

fn decode_entry(input: &str) -> Result<TraceEntry, String> {
    let fields = input.split(',').collect::<Vec<_>>();
    if fields.len() != 10 {
        return Err("trace entry requires 10 fields".to_string());
    }

    Ok(TraceEntry {
        previous_commitment: Digest::from_hex(fields[0])?,
        commitment: Digest::from_hex(fields[1])?,
        step: TrainingStep {
            index: parse_field(fields[2], "step.index")?,
            seed: parse_field(fields[3], "step.seed")?,
            batch_id: parse_field(fields[4], "step.batch_id")?,
            weight_index: parse_field(fields[5], "step.weight_index")?,
            gradient: parse_field(fields[6], "step.gradient")?,
            learning_rate_ppm: parse_field(fields[7], "step.learning_rate_ppm")?,
            state_before: decode_model(fields[8])?,
            state_after: decode_model(fields[9])?,
        },
    })
}

fn encode_model(model: &ModelState) -> String {
    model
        .weights()
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join("_")
}

fn decode_model(input: &str) -> Result<ModelState, String> {
    let fields = input.split('_').collect::<Vec<_>>();
    if fields.len() != MODEL_WIDTH {
        return Err(format!("model requires {MODEL_WIDTH} weights"));
    }

    let mut weights = [0_i64; MODEL_WIDTH];
    for (index, field) in fields.iter().enumerate() {
        weights[index] = parse_field(field, "model weight")?;
    }
    Ok(ModelState::new(weights))
}

fn parse_field<T>(input: &str, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    input
        .parse::<T>()
        .map_err(|_| format!("invalid {name}: '{input}'"))
}

fn escape(input: &str) -> String {
    input.replace('%', "%25").replace('|', "%7C")
}

fn unescape(input: &str) -> Result<String, String> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.as_bytes().iter().copied().peekable();

    while let Some(byte) = chars.next() {
        if byte != b'%' {
            out.push(byte as char);
            continue;
        }

        let high = chars.next().ok_or_else(|| "truncated escape".to_string())?;
        let low = chars.next().ok_or_else(|| "truncated escape".to_string())?;
        let hex = [high, low];
        let text = std::str::from_utf8(&hex).map_err(|_| "invalid escape".to_string())?;
        let value = u8::from_str_radix(text, 16).map_err(|_| "invalid escape".to_string())?;
        out.push(value as char);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::{DeterministicSampler, ToyHash};
    use chesscoin_core::application::{ChainConfig, ChainState};

    #[test]
    fn block_round_trips() {
        let hasher = ToyHash;
        let sampler = DeterministicSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 3,
            samples_per_block: 2,
            difficulty_zero_bits: 0,
        });
        let block = chain.mine_next_block(&hasher, &sampler, 7, 9);

        let encoded = encode_message(&PeerMessage::Block(Box::new(block.clone())));
        let decoded = decode_message(&encoded).expect("valid wire message");

        assert_eq!(decoded, PeerMessage::Block(Box::new(block)));
    }

    #[test]
    fn hello_round_trips() {
        let message = PeerMessage::Hello {
            node_id: "node|one".to_string(),
            height: 2,
            head: Digest::from_bytes([1; 32]),
        };

        let encoded = encode_message(&message);
        let decoded = decode_message(&encoded).expect("valid hello");

        assert_eq!(decoded, message);
    }

    #[test]
    fn get_blocks_round_trips() {
        let message = PeerMessage::GetBlocks {
            from_height: 12,
            limit: 128,
        };

        let encoded = encode_message(&message);
        let decoded = decode_message(&encoded).expect("valid get blocks");

        assert_eq!(decoded, message);
    }

    #[test]
    fn end_blocks_round_trips() {
        let encoded = encode_message(&PeerMessage::EndBlocks);
        let decoded = decode_message(&encoded).expect("valid end blocks");

        assert_eq!(decoded, PeerMessage::EndBlocks);
    }

    #[test]
    fn network_envelope_round_trips() {
        let config = WireConfig::default();
        let message = PeerMessage::GetBlocks {
            from_height: 4,
            limit: 9,
        };

        let encoded = encode_network_message(&config, &message);
        let decoded = decode_network_message(&encoded, &config).expect("valid envelope");

        assert_eq!(decoded, message);
    }

    #[test]
    fn network_envelope_rejects_wrong_network() {
        let encoded = encode_network_message(
            &WireConfig {
                network_id: "one".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            &PeerMessage::EndBlocks,
        );

        assert!(decode_network_message(
            &encoded,
            &WireConfig {
                network_id: "two".to_string(),
                protocol_version: PROTOCOL_VERSION,
            }
        )
        .is_err());
    }
}
