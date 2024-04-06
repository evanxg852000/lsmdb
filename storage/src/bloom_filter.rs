use bincode::{Decode, Encode};
use bloomfilter::Bloom;

use crate::Key;

#[derive(Encode, Decode)]
pub struct BloomFilterState {
    bytes: Vec<u8>,
    bitmap_bits: u64,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
}

impl From<&Bloom<Key>> for BloomFilterState {
    fn from(bloom: &Bloom<Key>) -> Self {
        Self {
            bytes: bloom.bitmap(),
            bitmap_bits: bloom.number_of_bits(),
            k_num: bloom.number_of_hash_functions(),
            sip_keys: bloom.sip_keys(),
        }
    }
}

impl From<BloomFilterState> for Bloom<Key> {
    fn from(state: BloomFilterState) -> Self {
        Bloom::from_existing(&state.bytes, state.bitmap_bits, state.k_num, state.sip_keys)
    }
}
