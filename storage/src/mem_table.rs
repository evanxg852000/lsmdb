use std::{
    cmp::Ordering,
    fs::OpenOptions,
    io::{BufWriter, Write},
    ops::Bound,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
};

use bloomfilter::Bloom;
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_skiplist::{map::Range, SkipMap};
use memmap2::MmapOptions;
use ouroboros::self_referencing;

use crate::{
    batching::BatchOperations,
    bloom_filter::BloomFilterState,
    error::LiteDbResult,
    ss_table::{Offset, SSTable, SSTableMetadata, SSTableSparseIndex, SS_TABLE_FILE_EXTENSION},
    utils::encode_into_writer,
    wal::WriteAheadLogger,
    KVIterator, Key, RefKey, RefValue, Scannable, Value,
};

pub(crate) type SkipMapRangeIterator<'a, K, V> = Range<'a, K, (Bound<K>, Bound<K>), K, V>;

#[derive(Debug)]
pub(crate) struct MemTable {
    id: u64,
    entries: SkipMap<Key, Value>,
    size_bytes: AtomicUsize,
    wal: WriteAheadLogger,
    dir: PathBuf,
}

impl Ord for MemTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Eq for MemTable {}

impl PartialOrd for MemTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MemTable {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl MemTable {
    pub(crate) fn open(dir: PathBuf, id: u64) -> LiteDbResult<Self> {
        let wal = WriteAheadLogger::open(dir.clone(), id)?;
        let data = SkipMap::new();
        for item_result in wal.iter() {
            let item = item_result?;
            data.insert(item.key, item.value);
        }
        Ok(Self {
            id,
            entries: data,
            size_bytes: AtomicUsize::new(0),
            wal,
            dir,
        })
    }

    pub fn ss_table_file_path(&self) -> PathBuf {
        self.dir
            .join(format!("{:01$}.{SS_TABLE_FILE_EXTENSION}", self.id, 20))
    }

    pub fn set(&self, key: RefKey, value: RefValue) -> LiteDbResult<()> {
        self.wal.append(key, value)?;
        self.size_bytes
            .fetch_add(key.len() + value.len(), AtomicOrdering::SeqCst);
        self.entries.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    pub fn get(&self, key: RefKey) -> LiteDbResult<Option<Value>> {
        let value_opt = self.entries.get(key).map(|entry| entry.value().to_owned());
        Ok(value_opt)
    }

    pub fn apply_batch(&self, batch_ops: BatchOperations) -> LiteDbResult<()> {
        self.wal.apply_batch(batch_ops.operations())?;
        self.size_bytes
            .fetch_add(batch_ops.size_bytes(), AtomicOrdering::SeqCst);
        for operation in batch_ops.operations() {
            self.entries
                .insert(operation.0.to_owned(), operation.1.to_owned());
        }
        Ok(())
    }

    pub fn save(
        &self,
        bloom_filter_size_bytes: usize,
        bloom_filter_item_count: usize,
        sparse_index_range_size: usize,
    ) -> LiteDbResult<Arc<SSTable>> {
        // create & persist sparse.index
        let mut index_entries: Vec<(Key, Offset)> = Vec::new();

        // create & persist the bloom.filter
        let mut bloom_filter: Bloom<Key> =
            Bloom::new(bloom_filter_size_bytes, bloom_filter_item_count);

        let segment_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.ss_table_file_path())?;

        // Loop through data:
        // - append to segment_file,
        // - update bloom_filter & index_entries
        let mut writer = BufWriter::new(&segment_file);
        let mut size_of_serialized_data = 0usize;
        let mut last_num_bytes_written = 0usize;

        for entry in self.entries.iter() {
            let kv = (entry.key(), entry.value());
            last_num_bytes_written = encode_into_writer(&kv, &mut writer)?;

            bloom_filter.set(kv.0);
            if (size_of_serialized_data == 0)
                || (size_of_serialized_data % sparse_index_range_size == 0)
            {
                index_entries.push((kv.0.clone(), size_of_serialized_data));
            }
            size_of_serialized_data += last_num_bytes_written;
        }
        let last_key = self.entries.back().unwrap();
        index_entries.push((last_key.key().clone(), size_of_serialized_data));

        // create & persist meta.json
        let first_key: (Key, Offset) = (self.entries.front().unwrap().key().clone(), 0);
        let last_key: (Key, Offset) = (
            last_key.key().clone(),
            size_of_serialized_data - last_num_bytes_written,
        );
        let metadata = SSTableMetadata::new(
            self.id,
            first_key,
            last_key,
            self.size_bytes.load(AtomicOrdering::SeqCst),
            self.entries.len(),
        );

        // append meta, index, bloom
        encode_into_writer(&metadata, &mut writer)?;

        let index = SSTableSparseIndex::from(index_entries);
        encode_into_writer(&index, &mut writer)?;

        let bloom_filter_state = BloomFilterState::from(&bloom_filter);
        encode_into_writer(&bloom_filter_state, &mut writer)?;

        // append data size for offset calculation
        writer.write_u64::<LittleEndian>(size_of_serialized_data as u64)?;

        // flush segment_file
        writer.flush()?;
        segment_file.sync_all()?;

        let file = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(size_of_serialized_data)
                .map(&segment_file)
                .unwrap()
        };

        self.close()?;
        Ok(Arc::new(SSTable::new(metadata, file, index, bloom_filter)))
    }

    pub fn is_full(&self, max_entries: usize, max_size_bytes: usize) -> bool {
        let size_bytes = self.size_bytes.load(AtomicOrdering::SeqCst);
        self.entries.len() >= max_entries || size_bytes >= max_size_bytes
    }

    pub fn close(&self) -> LiteDbResult<()> {
        self.wal.remove()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn dir(&self) -> PathBuf {
        self.dir.clone()
    }
}

impl Scannable for Arc<MemTable> {
    fn scan(&self, from: &Option<Key>, to: &Option<Key>) -> KVIterator {
        KVIterator::MemTable(MemTableIterator::new(self.clone(), from, to))
    }
}

#[self_referencing]
struct MemTableIterInner {
    mem_table: Arc<MemTable>,
    #[borrows(mem_table)]
    #[not_covariant]
    range: SkipMapRangeIterator<'this, Key, Value>,
}

pub(crate) struct MemTableIterator {
    inner: MemTableIterInner,
}

impl MemTableIterator {
    pub fn new(mem_table: Arc<MemTable>, from: &Option<Key>, to: &Option<Key>) -> Self {
        let range = match (from.clone(), to.clone()) {
            (None, None) => (Bound::Unbounded, Bound::Unbounded),
            (None, Some(last_key)) => (Bound::Unbounded, Bound::Excluded(last_key)),
            (Some(first_key), None) => (Bound::Included(first_key), Bound::Unbounded),
            (Some(first_key), Some(last_key)) => {
                (Bound::Included(first_key), Bound::Excluded(last_key))
            }
        };

        let inner = MemTableIterInnerBuilder {
            mem_table,
            range_builder: |mem_table| mem_table.entries.range(range),
        }
        .build();

        Self { inner }
    }
}

impl Iterator for MemTableIterator {
    type Item = LiteDbResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.with_range_mut(|range| {
            range
                .next()
                .map(|entry| Ok((entry.key().clone(), entry.value().clone())))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{mem_table::MemTable, Scannable};

    use anyhow::Ok;
    use tempfile::tempdir;

    #[test]
    fn test_empty_mem_table() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();
        let mem_table = Arc::new(MemTable::open(dir, 1).unwrap());
        assert_eq!(mem_table.scan(&None, &None).count(), 0);
        Ok(())
    }

    #[test]
    fn test_mem_table() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();
        let mem_table = Arc::new(MemTable::open(dir, 1)?);

        for i in 0..=100 {
            let k = format!("k_{:01$}", i, 3);
            let v = format!("v_{:01$}", i, 3);
            mem_table.set(k.as_bytes(), v.as_bytes())?;
        }

        for i in 0..=100 {
            let k = format!("k_{:01$}", i, 3);
            let v = mem_table.get(k.as_bytes())?;
            let expected_v = format!("v_{:01$}", i, 3).as_bytes().to_vec();
            assert_eq!(v, Some(expected_v));
        }

        let unknown_v = mem_table.get(b"unknown")?;
        assert_eq!(unknown_v, None);

        Ok(())
    }
}
