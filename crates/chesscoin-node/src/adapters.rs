use chesscoin_core::domain::Digest;
use chesscoin_core::ports::{HashPort, SamplingPort};

#[derive(Clone, Copy, Debug, Default)]
pub struct ToyHash;

impl HashPort for ToyHash {
    fn digest(&self, bytes: &[u8]) -> Digest {
        toy_digest(bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeterministicSampler;

impl SamplingPort for DeterministicSampler {
    fn sample_indices(
        &self,
        trace_len: usize,
        sample_count: usize,
        trace_root: Digest,
        entropy: u64,
    ) -> Vec<usize> {
        if trace_len == 0 || sample_count == 0 {
            return Vec::new();
        }

        if sample_count >= trace_len {
            return (0..trace_len).collect();
        }

        let mut selected = Vec::with_capacity(sample_count);
        let mut nonce = 0_u64;
        while selected.len() < sample_count {
            let mut bytes = Vec::with_capacity(48);
            bytes.extend_from_slice(b"chesscoin.sample.v1");
            bytes.extend_from_slice(trace_root.as_bytes());
            bytes.extend_from_slice(&entropy.to_le_bytes());
            bytes.extend_from_slice(&nonce.to_le_bytes());
            let digest = toy_digest(&bytes);
            let candidate = u64::from_le_bytes(digest.as_bytes()[0..8].try_into().expect("slice"));
            let index = (candidate as usize) % trace_len;
            if !selected.contains(&index) {
                selected.push(index);
            }
            nonce = nonce.wrapping_add(1);
        }
        selected.sort_unstable();
        selected
    }
}

fn toy_digest(bytes: &[u8]) -> Digest {
    let mut lanes = [
        0x243f_6a88_85a3_08d3_u64,
        0x1319_8a2e_0370_7344_u64,
        0xa409_3822_299f_31d0_u64,
        0x082e_fa98_ec4e_6c89_u64,
    ];

    for (index, byte) in bytes.iter().enumerate() {
        let lane = index % lanes.len();
        lanes[lane] ^= (*byte as u64).wrapping_add((index as u64) << 8);
        lanes[lane] = mix64(
            lanes[lane]
                .rotate_left(13)
                .wrapping_add(lanes[(lane + 1) % 4]),
        );
    }

    lanes[0] ^= bytes.len() as u64;
    lanes[1] ^= (bytes.len() as u64).rotate_left(17);
    lanes[2] ^= lanes[0].wrapping_add(lanes[1]);
    lanes[3] ^= lanes[2].rotate_right(7);

    let mut out = [0_u8; 32];
    for (index, lane) in lanes.into_iter().enumerate() {
        out[index * 8..(index + 1) * 8].copy_from_slice(&mix64(lane).to_le_bytes());
    }
    Digest::from_bytes(out)
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampler_is_deterministic_and_unique() {
        let sampler = DeterministicSampler;
        let root = Digest::from_bytes([7; 32]);

        let first = sampler.sample_indices(16, 6, root, 99);
        let second = sampler.sample_indices(16, 6, root, 99);

        assert_eq!(first, second);
        assert_eq!(first.len(), 6);
        let mut unique = first.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), first.len());
    }

    #[test]
    fn sampler_returns_all_indices_when_sample_count_exceeds_trace() {
        let sampler = DeterministicSampler;

        assert_eq!(
            sampler.sample_indices(4, 99, Digest::from_bytes([1; 32]), 42),
            vec![0, 1, 2, 3]
        );
    }
}
