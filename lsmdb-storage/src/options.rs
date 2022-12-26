#[derive(Clone, Copy, Debug)]
pub struct LiteDbOptions {
    pub bloom_filter_size_bytes: usize,
    pub bloom_filter_item_count: usize,
    pub sparse_index_range_size: usize,
    pub mem_table_policy: &'static str,
    pub compaction_policy: &'static str,
}

impl Default for LiteDbOptions {
    fn default() -> Self {
        Self {
            bloom_filter_size_bytes: 3_000_000, // 3MB
            bloom_filter_item_count: 100_000_000,
            sparse_index_range_size: 1_000,
            mem_table_policy: "size_tiered: 500000 3000000", // 500k 3MB
            compaction_policy: "size_tiered",
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
            mem_table_policy: "size_tiered: 200 7000",
            compaction_policy: "size_tiered",
        }
    }
}
