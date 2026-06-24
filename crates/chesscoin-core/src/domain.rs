use std::fmt;

pub const MODEL_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const fn zero() -> Self {
        Self([0; 32])
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            out.push(hex_digit(byte >> 4));
            out.push(hex_digit(byte & 0x0f));
        }
        out
    }

    pub fn from_hex(input: &str) -> Result<Self, String> {
        if input.len() != 64 {
            return Err("digest hex must be 64 characters".to_string());
        }

        let mut bytes = [0_u8; 32];
        let chars = input.as_bytes();
        for index in 0..32 {
            let high = decode_hex_digit(chars[index * 2])?;
            let low = decode_hex_digit(chars[index * 2 + 1])?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    pub fn leading_zero_bits(&self) -> u32 {
        let mut total = 0;
        for byte in self.0 {
            if byte == 0 {
                total += 8;
            } else {
                total += byte.leading_zeros();
                break;
            }
        }
        total
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelState {
    weights: [i64; MODEL_WIDTH],
}

impl ModelState {
    pub const fn new(weights: [i64; MODEL_WIDTH]) -> Self {
        Self { weights }
    }

    pub const fn genesis() -> Self {
        Self {
            weights: [120, -80, 45, 210, -135, 65, 15, -30],
        }
    }

    pub const fn weights(&self) -> &[i64; MODEL_WIDTH] {
        &self.weights
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        for weight in self.weights {
            out.extend_from_slice(&weight.to_le_bytes());
        }
    }
}

impl fmt::Display for ModelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (index, weight) in self.weights.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{weight}")?;
        }
        write!(f, "]")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrainingStep {
    pub index: u64,
    pub seed: u64,
    pub batch_id: u64,
    pub weight_index: usize,
    pub gradient: i64,
    pub learning_rate_ppm: i64,
    pub state_before: ModelState,
    pub state_after: ModelState,
}

impl TrainingStep {
    pub fn deterministic(seed: u64, index: u64, state_before: &ModelState) -> Self {
        let mixed = mix64(seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let weight_index = (mixed as usize) % MODEL_WIDTH;
        let batch_id = mix64(mixed ^ 0xa076_1d64_78bd_642f);
        let gradient = deterministic_gradient(seed, index, state_before, mixed);
        let learning_rate_ppm = 125_000;

        let mut next_weights = *state_before.weights();
        let delta = (gradient * learning_rate_ppm) / 1_000_000;
        next_weights[weight_index] = next_weights[weight_index].saturating_sub(delta);

        Self {
            index,
            seed,
            batch_id,
            weight_index,
            gradient,
            learning_rate_ppm,
            state_before: state_before.clone(),
            state_after: ModelState::new(next_weights),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(b"chesscoin.trace-step.v1");
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.batch_id.to_le_bytes());
        out.extend_from_slice(&(self.weight_index as u64).to_le_bytes());
        out.extend_from_slice(&self.gradient.to_le_bytes());
        out.extend_from_slice(&self.learning_rate_ppm.to_le_bytes());
        self.state_before.encode(&mut out);
        self.state_after.encode(&mut out);
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub height: u64,
    pub previous_block: Digest,
    pub model_before: ModelState,
    pub model_after: ModelState,
    pub trace_root: Digest,
    pub training_seed: u64,
    pub sampling_entropy: u64,
    pub sample_count: usize,
    pub nonce: u64,
}

impl BlockHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(b"chesscoin.block-header.v1");
        out.extend_from_slice(&self.height.to_le_bytes());
        out.extend_from_slice(self.previous_block.as_bytes());
        self.model_before.encode(&mut out);
        self.model_after.encode(&mut out);
        out.extend_from_slice(self.trace_root.as_bytes());
        out.extend_from_slice(&self.training_seed.to_le_bytes());
        out.extend_from_slice(&self.sampling_entropy.to_le_bytes());
        out.extend_from_slice(&(self.sample_count as u64).to_le_bytes());
        out.extend_from_slice(&self.nonce.to_le_bytes());
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub header: BlockHeader,
    pub trace: TrainingTrace,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub previous_commitment: Digest,
    pub step: TrainingStep,
    pub commitment: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrainingTrace {
    pub initial_model: ModelState,
    pub candidate_model: ModelState,
    pub entries: Vec<TraceEntry>,
    pub root: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationSample {
    pub index: usize,
    pub commitment_ok: bool,
    pub transition_ok: bool,
}

impl VerificationSample {
    pub const fn accepted(&self) -> bool {
        self.commitment_ok && self.transition_ok
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerificationOutcome {
    Accepted,
    Rejected { failed_indices: Vec<usize> },
}

impl VerificationOutcome {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

fn deterministic_gradient(seed: u64, index: u64, state: &ModelState, mixed: u64) -> i64 {
    let mut accumulator = seed.wrapping_add(index.rotate_left(17));
    for (offset, weight) in state.weights().iter().enumerate() {
        let lane = (*weight as u64).wrapping_mul((offset as u64) + 3);
        accumulator = mix64(accumulator ^ lane ^ mixed.rotate_left(offset as u32));
    }

    ((accumulator % 41) as i64) - 20
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!(),
    }
}

fn decode_hex_digit(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex digit".to_string()),
    }
}
