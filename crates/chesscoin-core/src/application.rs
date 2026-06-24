use std::collections::HashMap;

use crate::domain::{
    Block, BlockHeader, Digest, ModelState, TraceEntry, TrainingStep, TrainingTrace,
    VerificationOutcome, VerificationSample,
};
use crate::ports::{HashPort, SamplingPort};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationRequest {
    pub steps: usize,
    pub samples: usize,
    pub seed: u64,
    pub sampling_entropy: u64,
    pub tamper_step: Option<usize>,
}

impl Default for SimulationRequest {
    fn default() -> Self {
        Self {
            steps: 16,
            samples: 6,
            seed: 42,
            sampling_entropy: 2_026,
            tamper_step: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationReport {
    pub committed_trace: TrainingTrace,
    pub opened_trace: TrainingTrace,
    pub sampled_indices: Vec<usize>,
    pub samples: Vec<VerificationSample>,
    pub outcome: VerificationOutcome,
    pub tamper_applied: Option<usize>,
}

pub struct ProtocolSimulator<H, S> {
    hasher: H,
    sampler: S,
}

impl<H, S> ProtocolSimulator<H, S>
where
    H: HashPort,
    S: SamplingPort,
{
    pub const fn new(hasher: H, sampler: S) -> Self {
        Self { hasher, sampler }
    }

    pub fn run(&self, request: SimulationRequest) -> SimulationReport {
        let committed_trace = self.build_trace(request.seed, request.steps);
        let mut opened_trace = committed_trace.clone();
        let tamper_applied = request
            .tamper_step
            .filter(|index| *index < opened_trace.entries.len());

        if let Some(index) = tamper_applied {
            tamper_trace_step(&mut opened_trace.entries[index]);
        }

        let sampled_indices = self.sampler.sample_indices(
            committed_trace.entries.len(),
            request.samples,
            committed_trace.root,
            request.sampling_entropy,
        );

        let samples = sampled_indices
            .iter()
            .map(|index| {
                self.verify_sample(
                    request.seed,
                    *index,
                    &committed_trace.entries[*index],
                    &opened_trace.entries[*index],
                )
            })
            .collect::<Vec<_>>();

        let failed_indices = samples
            .iter()
            .filter(|sample| !sample.accepted())
            .map(|sample| sample.index)
            .collect::<Vec<_>>();

        let outcome = if failed_indices.is_empty() {
            VerificationOutcome::Accepted
        } else {
            VerificationOutcome::Rejected { failed_indices }
        };

        SimulationReport {
            committed_trace,
            opened_trace,
            sampled_indices,
            samples,
            outcome,
            tamper_applied,
        }
    }

    pub fn build_trace(&self, seed: u64, steps: usize) -> TrainingTrace {
        self.build_trace_from(ModelState::genesis(), seed, steps)
    }

    pub fn build_trace_from(
        &self,
        initial_model: ModelState,
        seed: u64,
        steps: usize,
    ) -> TrainingTrace {
        let mut state = initial_model.clone();
        let mut previous_commitment = Digest::zero();
        let mut entries = Vec::with_capacity(steps);

        for index in 0..steps {
            let step = TrainingStep::deterministic(seed, index as u64, &state);
            let commitment = self.commit_step(previous_commitment, &step);
            state = step.state_after.clone();
            entries.push(TraceEntry {
                previous_commitment,
                step,
                commitment,
            });
            previous_commitment = commitment;
        }

        TrainingTrace {
            initial_model,
            candidate_model: state,
            entries,
            root: previous_commitment,
        }
    }

    pub fn verify_sample(
        &self,
        seed: u64,
        index: usize,
        committed_entry: &TraceEntry,
        opened_entry: &TraceEntry,
    ) -> VerificationSample {
        let expected_step = TrainingStep::deterministic(
            seed,
            opened_entry.step.index,
            &opened_entry.step.state_before,
        );
        let expected_commitment =
            self.commit_step(committed_entry.previous_commitment, &opened_entry.step);

        VerificationSample {
            index,
            commitment_ok: opened_entry.previous_commitment == committed_entry.previous_commitment
                && opened_entry.commitment == committed_entry.commitment
                && expected_commitment == committed_entry.commitment,
            transition_ok: opened_entry.step.index as usize == index
                && expected_step == opened_entry.step,
        }
    }

    pub fn commit_step(&self, previous_commitment: Digest, step: &TrainingStep) -> Digest {
        let encoded_step = step.encode();
        let mut bytes = Vec::with_capacity(32 + encoded_step.len() + 24);
        bytes.extend_from_slice(b"chesscoin.commit.v1");
        bytes.extend_from_slice(previous_commitment.as_bytes());
        bytes.extend_from_slice(&encoded_step);
        self.hasher.digest(&bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainConfig {
    pub steps_per_block: usize,
    pub samples_per_block: usize,
    pub difficulty_zero_bits: u32,
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self {
            steps_per_block: 16,
            samples_per_block: 6,
            difficulty_zero_bits: 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockValidationError {
    WrongHeight {
        expected: u64,
        actual: u64,
    },
    WrongPreviousBlock,
    WrongModelBefore,
    WrongModelAfter,
    WrongTraceLength {
        expected: usize,
        actual: usize,
    },
    WrongSampleCount {
        expected: usize,
        actual: usize,
    },
    WrongTraceContinuity {
        index: usize,
    },
    WrongTraceRoot,
    InvalidSample {
        index: usize,
    },
    InsufficientWork {
        required_zero_bits: u32,
        actual_zero_bits: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkChoiceError {
    UnknownParent { previous_block: Digest },
    InvalidBlock(BlockValidationError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentContext {
    pub height: u64,
    pub hash: Digest,
    pub model: ModelState,
}

impl ParentContext {
    pub fn genesis() -> Self {
        Self {
            height: 0,
            hash: Digest::zero(),
            model: ModelState::genesis(),
        }
    }

    pub fn next_height(&self) -> u64 {
        self.height
    }
}

#[derive(Clone, Debug)]
pub struct ChainState {
    config: ChainConfig,
    blocks: Vec<Block>,
}

impl ChainState {
    pub fn new(config: ChainConfig) -> Self {
        Self {
            config,
            blocks: Vec::new(),
        }
    }

    pub const fn config(&self) -> &ChainConfig {
        &self.config
    }

    pub fn height(&self) -> u64 {
        self.blocks.len() as u64
    }

    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn current_model(&self) -> ModelState {
        self.blocks
            .last()
            .map(|block| block.header.model_after.clone())
            .unwrap_or_else(ModelState::genesis)
    }

    pub fn head_hash<H: HashPort>(&self, hasher: &H) -> Digest {
        self.blocks
            .last()
            .map(|block| block_hash(hasher, block))
            .unwrap_or_else(Digest::zero)
    }

    pub fn mine_next_block<H, S>(
        &self,
        hasher: &H,
        sampler: &S,
        training_seed: u64,
        sampling_entropy: u64,
    ) -> Block
    where
        H: HashPort,
        S: SamplingPort,
    {
        let simulator = ProtocolSimulator::new(hasher, sampler);
        let trace = simulator.build_trace_from(
            self.current_model(),
            training_seed,
            self.config.steps_per_block,
        );
        let mut header = BlockHeader {
            height: self.height(),
            previous_block: self.head_hash(hasher),
            model_before: trace.initial_model.clone(),
            model_after: trace.candidate_model.clone(),
            trace_root: trace.root,
            training_seed,
            sampling_entropy,
            sample_count: self.config.samples_per_block,
            nonce: 0,
        };

        loop {
            let candidate = Block {
                header: header.clone(),
                trace: trace.clone(),
            };
            if block_hash(hasher, &candidate).leading_zero_bits()
                >= self.config.difficulty_zero_bits
            {
                return candidate;
            }
            header.nonce = header.nonce.wrapping_add(1);
        }
    }

    pub fn validate_next_block<H, S>(
        &self,
        hasher: &H,
        sampler: &S,
        block: &Block,
    ) -> Result<(), BlockValidationError>
    where
        H: HashPort,
        S: SamplingPort,
    {
        let parent = ParentContext {
            height: self.height(),
            hash: self.head_hash(hasher),
            model: self.current_model(),
        };
        validate_block_after(&self.config, hasher, sampler, &parent, block)
    }

    pub fn apply_block<H, S>(
        &mut self,
        hasher: &H,
        sampler: &S,
        block: Block,
    ) -> Result<Digest, BlockValidationError>
    where
        H: HashPort,
        S: SamplingPort,
    {
        self.validate_next_block(hasher, sampler, &block)?;
        let hash = block_hash(hasher, &block);
        self.blocks.push(block);
        Ok(hash)
    }
}

pub fn validate_block_after<H, S>(
    config: &ChainConfig,
    hasher: &H,
    sampler: &S,
    parent: &ParentContext,
    block: &Block,
) -> Result<(), BlockValidationError>
where
    H: HashPort,
    S: SamplingPort,
{
    if block.header.height != parent.next_height() {
        return Err(BlockValidationError::WrongHeight {
            expected: parent.next_height(),
            actual: block.header.height,
        });
    }

    if block.header.previous_block != parent.hash {
        return Err(BlockValidationError::WrongPreviousBlock);
    }

    if block.header.model_before != parent.model
        || block.trace.initial_model != block.header.model_before
    {
        return Err(BlockValidationError::WrongModelBefore);
    }

    if block.trace.candidate_model != block.header.model_after {
        return Err(BlockValidationError::WrongModelAfter);
    }

    if block.trace.entries.len() != config.steps_per_block {
        return Err(BlockValidationError::WrongTraceLength {
            expected: config.steps_per_block,
            actual: block.trace.entries.len(),
        });
    }

    if block.header.sample_count != config.samples_per_block {
        return Err(BlockValidationError::WrongSampleCount {
            expected: config.samples_per_block,
            actual: block.header.sample_count,
        });
    }

    if block.trace.root != block.header.trace_root {
        return Err(BlockValidationError::WrongTraceRoot);
    }

    validate_commitment_chain(hasher, block)?;
    validate_trace_continuity(block)?;

    let actual_work = block_hash(hasher, block).leading_zero_bits();
    if actual_work < config.difficulty_zero_bits {
        return Err(BlockValidationError::InsufficientWork {
            required_zero_bits: config.difficulty_zero_bits,
            actual_zero_bits: actual_work,
        });
    }

    let simulator = ProtocolSimulator::new(hasher, sampler);
    let sampled_indices = sampler.sample_indices(
        block.trace.entries.len(),
        block.header.sample_count,
        block.trace.root,
        block.header.sampling_entropy,
    );

    for index in sampled_indices {
        let Some(entry) = block.trace.entries.get(index) else {
            return Err(BlockValidationError::InvalidSample { index });
        };
        let sample = simulator.verify_sample(block.header.training_seed, index, entry, entry);
        if !sample.accepted() {
            return Err(BlockValidationError::InvalidSample { index });
        }
    }

    Ok(())
}

fn validate_trace_continuity(block: &Block) -> Result<(), BlockValidationError> {
    let mut expected_state = block.trace.initial_model.clone();

    for (index, entry) in block.trace.entries.iter().enumerate() {
        if entry.step.state_before != expected_state {
            return Err(BlockValidationError::WrongTraceContinuity { index });
        }
        expected_state = entry.step.state_after.clone();
    }

    if block.trace.candidate_model != expected_state {
        return Err(BlockValidationError::WrongTraceContinuity {
            index: block.trace.entries.len(),
        });
    }

    Ok(())
}

fn validate_commitment_chain<H: HashPort>(
    hasher: &H,
    block: &Block,
) -> Result<(), BlockValidationError> {
    let simulator = ProtocolSimulator::new(hasher, DeterministicNoopSampler);
    let mut previous_commitment = Digest::zero();

    for entry in &block.trace.entries {
        if entry.previous_commitment != previous_commitment {
            return Err(BlockValidationError::WrongTraceRoot);
        }
        let expected_commitment = simulator.commit_step(previous_commitment, &entry.step);
        if entry.commitment != expected_commitment {
            return Err(BlockValidationError::WrongTraceRoot);
        }
        previous_commitment = entry.commitment;
    }

    if previous_commitment != block.trace.root {
        return Err(BlockValidationError::WrongTraceRoot);
    }

    Ok(())
}

pub fn block_hash<H: HashPort>(hasher: &H, block: &Block) -> Digest {
    block_header_hash(hasher, &block.header)
}

pub fn block_header_hash<H: HashPort>(hasher: &H, header: &BlockHeader) -> Digest {
    let mut bytes = header.encode();
    bytes.extend_from_slice(header.trace_root.as_bytes());
    hasher.digest(&bytes)
}

#[derive(Clone, Debug)]
pub struct ForkChoiceState {
    config: ChainConfig,
    nodes: HashMap<Digest, ForkNode>,
    best_tip: Option<Digest>,
}

#[derive(Clone, Debug)]
struct ForkNode {
    hash: Digest,
    parent: Digest,
    block: Block,
    next_height: u64,
    work_score: u128,
}

impl ForkChoiceState {
    pub fn new(config: ChainConfig) -> Self {
        Self {
            config,
            nodes: HashMap::new(),
            best_tip: None,
        }
    }

    pub const fn config(&self) -> &ChainConfig {
        &self.config
    }

    pub fn best_height(&self) -> u64 {
        self.best_tip
            .and_then(|hash| self.nodes.get(&hash).map(|node| node.next_height))
            .unwrap_or(0)
    }

    pub fn best_hash(&self) -> Digest {
        self.best_tip.unwrap_or_else(Digest::zero)
    }

    pub fn known_block_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn best_model(&self) -> ModelState {
        self.best_tip
            .and_then(|hash| {
                self.nodes
                    .get(&hash)
                    .map(|node| node.block.header.model_after.clone())
            })
            .unwrap_or_else(ModelState::genesis)
    }

    pub fn best_chain(&self) -> Vec<Block> {
        let mut chain = Vec::new();
        let mut cursor = self.best_tip;

        while let Some(hash) = cursor {
            let Some(node) = self.nodes.get(&hash) else {
                break;
            };
            chain.push(node.block.clone());
            cursor = if node.parent == Digest::zero() {
                None
            } else {
                Some(node.parent)
            };
        }

        chain.reverse();
        chain
    }

    pub fn contains_block(&self, hash: Digest) -> bool {
        self.nodes.contains_key(&hash)
    }

    pub fn mine_next_block<H, S>(
        &self,
        hasher: &H,
        sampler: &S,
        training_seed: u64,
        sampling_entropy: u64,
    ) -> Block
    where
        H: HashPort,
        S: SamplingPort,
    {
        let simulator = ProtocolSimulator::new(hasher, sampler);
        let trace = simulator.build_trace_from(
            self.best_model(),
            training_seed,
            self.config.steps_per_block,
        );
        let mut header = BlockHeader {
            height: self.best_height(),
            previous_block: self.best_hash(),
            model_before: trace.initial_model.clone(),
            model_after: trace.candidate_model.clone(),
            trace_root: trace.root,
            training_seed,
            sampling_entropy,
            sample_count: self.config.samples_per_block,
            nonce: 0,
        };

        loop {
            let candidate = Block {
                header: header.clone(),
                trace: trace.clone(),
            };
            if block_hash(hasher, &candidate).leading_zero_bits()
                >= self.config.difficulty_zero_bits
            {
                return candidate;
            }
            header.nonce = header.nonce.wrapping_add(1);
        }
    }

    pub fn insert_block<H, S>(
        &mut self,
        hasher: &H,
        sampler: &S,
        block: Block,
    ) -> Result<Digest, ForkChoiceError>
    where
        H: HashPort,
        S: SamplingPort,
    {
        let hash = block_hash(hasher, &block);
        if self.nodes.contains_key(&hash) {
            return Ok(hash);
        }

        let (parent_context, parent_score) = if block.header.previous_block == Digest::zero() {
            (ParentContext::genesis(), 0)
        } else {
            let Some(parent) = self.nodes.get(&block.header.previous_block) else {
                return Err(ForkChoiceError::UnknownParent {
                    previous_block: block.header.previous_block,
                });
            };
            (
                ParentContext {
                    height: parent.next_height,
                    hash: parent.hash,
                    model: parent.block.header.model_after.clone(),
                },
                parent.work_score,
            )
        };

        validate_block_after(&self.config, hasher, sampler, &parent_context, &block)
            .map_err(ForkChoiceError::InvalidBlock)?;

        let next_height = block.header.height + 1;
        let work_score =
            parent_score + configured_block_work_score(self.config.difficulty_zero_bits);
        let node = ForkNode {
            hash,
            parent: block.header.previous_block,
            block,
            next_height,
            work_score,
        };
        self.nodes.insert(hash, node);
        self.maybe_update_best(hash);
        Ok(hash)
    }

    fn maybe_update_best(&mut self, candidate_hash: Digest) {
        let Some(candidate) = self.nodes.get(&candidate_hash) else {
            return;
        };

        let should_update = match self.best_tip.and_then(|hash| self.nodes.get(&hash)) {
            None => true,
            Some(best) => is_better_tip(candidate, best),
        };

        if should_update {
            self.best_tip = Some(candidate_hash);
        }
    }
}

fn is_better_tip(candidate: &ForkNode, best: &ForkNode) -> bool {
    candidate
        .work_score
        .cmp(&best.work_score)
        .then_with(|| candidate.next_height.cmp(&best.next_height))
        .then_with(|| candidate.hash.as_bytes().cmp(best.hash.as_bytes()))
        .is_gt()
}

fn configured_block_work_score(difficulty_zero_bits: u32) -> u128 {
    1_u128 << difficulty_zero_bits.min(120)
}

#[derive(Clone, Copy)]
struct DeterministicNoopSampler;

impl SamplingPort for DeterministicNoopSampler {
    fn sample_indices(
        &self,
        _trace_len: usize,
        _sample_count: usize,
        _trace_root: Digest,
        _entropy: u64,
    ) -> Vec<usize> {
        Vec::new()
    }
}

fn tamper_trace_step(entry: &mut TraceEntry) {
    let mut weights = *entry.step.state_after.weights();
    weights[0] = weights[0].saturating_add(1);
    entry.step.state_after = ModelState::new(weights);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{HashPort, SamplingPort};

    #[derive(Clone, Copy)]
    struct TestHash;

    impl HashPort for TestHash {
        fn digest(&self, bytes: &[u8]) -> Digest {
            let mut lanes = [0_u64; 4];
            for (index, byte) in bytes.iter().enumerate() {
                let lane = index % lanes.len();
                lanes[lane] = lanes[lane]
                    .wrapping_mul(1099511628211)
                    .wrapping_add(*byte as u64)
                    .wrapping_add(index as u64);
            }

            let mut out = [0_u8; 32];
            for (index, lane) in lanes.into_iter().enumerate() {
                out[index * 8..(index + 1) * 8].copy_from_slice(&lane.to_le_bytes());
            }
            Digest::from_bytes(out)
        }
    }

    #[derive(Clone, Copy)]
    struct TestSampler;

    impl SamplingPort for TestSampler {
        fn sample_indices(
            &self,
            trace_len: usize,
            sample_count: usize,
            _trace_root: Digest,
            _entropy: u64,
        ) -> Vec<usize> {
            (0..trace_len.min(sample_count)).collect()
        }
    }

    fn simulator() -> ProtocolSimulator<TestHash, TestSampler> {
        ProtocolSimulator::new(TestHash, TestSampler)
    }

    fn rebind_trace_commitments(block: &mut Block) {
        let simulator = simulator();
        let mut previous_commitment = Digest::zero();
        for entry in &mut block.trace.entries {
            entry.previous_commitment = previous_commitment;
            entry.commitment = simulator.commit_step(previous_commitment, &entry.step);
            previous_commitment = entry.commitment;
        }
        block.trace.root = previous_commitment;
        block.header.trace_root = previous_commitment;
    }

    #[test]
    fn simulation_is_deterministic_for_same_inputs() {
        let request = SimulationRequest {
            steps: 12,
            samples: 4,
            seed: 7,
            sampling_entropy: 99,
            tamper_step: None,
        };

        let first = simulator().run(request.clone());
        let second = simulator().run(request);

        assert_eq!(first.committed_trace.root, second.committed_trace.root);
        assert_eq!(first.sampled_indices, second.sampled_indices);
        assert_eq!(first.outcome, VerificationOutcome::Accepted);
        assert_eq!(second.outcome, VerificationOutcome::Accepted);
    }

    #[test]
    fn honest_trace_accepts() {
        let report = simulator().run(SimulationRequest::default());

        assert!(report.outcome.is_accepted());
        assert!(report.samples.iter().all(VerificationSample::accepted));
    }

    #[test]
    fn sampled_tamper_rejects() {
        let report = simulator().run(SimulationRequest {
            steps: 8,
            samples: 8,
            seed: 42,
            sampling_entropy: 2_026,
            tamper_step: Some(3),
        });

        assert_eq!(report.tamper_applied, Some(3));
        assert!(matches!(
            report.outcome,
            VerificationOutcome::Rejected {
                ref failed_indices
            } if failed_indices == &vec![3]
        ));
    }

    #[test]
    fn unsampled_tamper_can_escape_in_this_probabilistic_demo() {
        let report = simulator().run(SimulationRequest {
            steps: 8,
            samples: 0,
            seed: 42,
            sampling_entropy: 2_026,
            tamper_step: Some(3),
        });

        assert_eq!(report.tamper_applied, Some(3));
        assert!(report.outcome.is_accepted());
    }

    #[test]
    fn mined_block_applies_to_chain() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let mut chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });

        let block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        let hash = chain
            .apply_block(&hasher, &sampler, block)
            .expect("valid block");

        assert_eq!(chain.height(), 1);
        assert_eq!(chain.head_hash(&hasher), hash);
        assert_ne!(chain.current_model(), ModelState::genesis());
    }

    #[test]
    fn header_hash_matches_block_hash() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let block = chain.mine_next_block(&hasher, &sampler, 11, 22);

        assert_eq!(
            block_hash(&hasher, &block),
            block_header_hash(&hasher, &block.header)
        );
    }

    #[test]
    fn block_with_wrong_previous_hash_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        block.header.previous_block = Digest::from_bytes([9; 32]);

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongPreviousBlock)
        );
    }

    #[test]
    fn block_with_wrong_trace_length_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        block.trace.entries.pop();

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongTraceLength {
                expected: 4,
                actual: 3
            })
        );
    }

    #[test]
    fn block_with_wrong_sample_count_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        block.header.sample_count = 0;

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongSampleCount {
                expected: 4,
                actual: 0
            })
        );
    }

    #[test]
    fn block_with_tampered_commitment_chain_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        let mut weights = *block.trace.entries[1].step.state_after.weights();
        weights[0] += 1;
        block.trace.entries[1].step.state_after = ModelState::new(weights);

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongTraceRoot)
        );
    }

    #[test]
    fn block_with_disconnected_trace_state_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        block.trace.entries[2].step.state_before = ModelState::genesis();
        rebind_trace_commitments(&mut block);

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongTraceContinuity { index: 2 })
        );
    }

    #[test]
    fn block_with_candidate_model_not_matching_trace_tail_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        let mut weights = *block.header.model_after.weights();
        weights[0] = weights[0].saturating_add(99);
        let forged_model = ModelState::new(weights);
        block.trace.candidate_model = forged_model.clone();
        block.header.model_after = forged_model;

        assert_eq!(
            chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::WrongTraceContinuity { index: 4 })
        );
    }

    #[test]
    fn block_with_insufficient_work_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let easy_chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let strict_chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 255,
        });
        let block = easy_chain.mine_next_block(&hasher, &sampler, 11, 22);

        assert!(matches!(
            strict_chain.validate_next_block(&hasher, &sampler, &block),
            Err(BlockValidationError::InsufficientWork { .. })
        ));
    }

    #[test]
    fn self_consistent_opening_not_in_committed_trace_rejects() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let simulator = ProtocolSimulator::new(hasher, sampler);
        let committed_trace = simulator.build_trace(42, 4);
        let mut opened_trace = committed_trace.clone();
        let forged_step =
            TrainingStep::deterministic(99, 1, &committed_trace.entries[1].step.state_before);
        let forged_commitment =
            simulator.commit_step(committed_trace.entries[1].previous_commitment, &forged_step);
        opened_trace.entries[1].step = forged_step;
        opened_trace.entries[1].commitment = forged_commitment;

        let sample =
            simulator.verify_sample(42, 1, &committed_trace.entries[1], &opened_trace.entries[1]);

        assert!(!sample.accepted());
        assert!(!sample.commitment_ok);
    }

    #[test]
    fn fork_choice_rejects_unknown_parent() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let chain = ChainState::new(ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        });
        let mut block = chain.mine_next_block(&hasher, &sampler, 11, 22);
        block.header.previous_block = Digest::from_bytes([8; 32]);
        let mut fork_choice = ForkChoiceState::new(chain.config().clone());

        assert_eq!(
            fork_choice.insert_block(&hasher, &sampler, block),
            Err(ForkChoiceError::UnknownParent {
                previous_block: Digest::from_bytes([8; 32])
            })
        );
    }

    #[test]
    fn fork_choice_accepts_competing_branches_and_selects_extended_tip() {
        let hasher = TestHash;
        let sampler = TestSampler;
        let config = ChainConfig {
            steps_per_block: 4,
            samples_per_block: 4,
            difficulty_zero_bits: 0,
        };
        let mut base_chain = ChainState::new(config.clone());
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
        let branch_a2_hash = block_hash(&hasher, &branch_a2);

        let mut fork_choice = ForkChoiceState::new(config);
        fork_choice
            .insert_block(&hasher, &sampler, block0)
            .expect("block0 inserts");
        fork_choice
            .insert_block(&hasher, &sampler, branch_b1)
            .expect("branch b inserts");
        fork_choice
            .insert_block(&hasher, &sampler, branch_a1)
            .expect("branch a1 inserts");
        fork_choice
            .insert_block(&hasher, &sampler, branch_a2)
            .expect("branch a2 inserts");

        assert_eq!(fork_choice.best_height(), 3);
        assert_eq!(fork_choice.best_hash(), branch_a2_hash);
        assert_eq!(fork_choice.best_chain().len(), 3);
    }
}
