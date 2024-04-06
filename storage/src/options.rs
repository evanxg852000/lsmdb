use crate::{compactor::CompactorPolicyConfig, controller::MemTableControllerPolicyConfig};

#[derive(Clone, Copy, Debug)]
pub struct LiteDbOptions {
    pub bloom_filter_size_bytes: usize,
    pub bloom_filter_item_count: usize,
    pub sparse_index_range_size: usize,
    pub mem_table_controller_policy: MemTableControllerPolicyConfig,
    pub compactor_policy: CompactorPolicyConfig,
}

impl Default for LiteDbOptions {
    fn default() -> Self {
        Self {
            bloom_filter_size_bytes: 3_000_000, // 3MB
            bloom_filter_item_count: 100_000_000,
            sparse_index_range_size: 1_000,
            mem_table_controller_policy: MemTableControllerPolicyConfig::SizeTiered {
                max_entries: 500_000,
                max_size_bytes: 3_000_000, // 3MB
            },
            compactor_policy: CompactorPolicyConfig::SizeTiered,
        }
    }
}

impl LiteDbOptions {
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            bloom_filter_size_bytes: 3_000_000, // 3MB
            bloom_filter_item_count: 100_000_000,
            sparse_index_range_size: 40,
            mem_table_controller_policy: MemTableControllerPolicyConfig::SizeTiered {
                max_entries: 200,
                max_size_bytes: 7000,
            },
            compactor_policy: CompactorPolicyConfig::SizeTiered,
        }
    }
}
