use crate::domain::Digest;

pub trait HashPort {
    fn digest(&self, bytes: &[u8]) -> Digest;
}

impl<T> HashPort for &T
where
    T: HashPort + ?Sized,
{
    fn digest(&self, bytes: &[u8]) -> Digest {
        (**self).digest(bytes)
    }
}

pub trait SamplingPort {
    fn sample_indices(
        &self,
        trace_len: usize,
        sample_count: usize,
        trace_root: Digest,
        entropy: u64,
    ) -> Vec<usize>;
}

impl<T> SamplingPort for &T
where
    T: SamplingPort + ?Sized,
{
    fn sample_indices(
        &self,
        trace_len: usize,
        sample_count: usize,
        trace_root: Digest,
        entropy: u64,
    ) -> Vec<usize> {
        (**self).sample_indices(trace_len, sample_count, trace_root, entropy)
    }
}
