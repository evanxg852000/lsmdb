use std::{
    cmp::Ordering,
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    mem,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use bloomfilter::Bloom;
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::{Mmap, MmapOptions};

use crate::{
    bloom_filter::BloomFilterState,
    error::LiteDbResult,
    utils::{decode, decode_from_reader},
    KVIterator, Key, LiteDbError, RefKey, Scannable, Value, TOMBSTONE,
};

pub(crate) const SS_TABLE_FILE_EXTENSION: &str = "sst";

pub(crate) fn is_ss_table_file(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .map(|ext| ext == SS_TABLE_FILE_EXTENSION)
            .unwrap_or(false)
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct SSTableMetadata {
    id: u64,                  // unique id
    first_key: (Key, Offset), // smallest key
    last_key: (Key, Offset),  // greatest key
    total_size: usize,        // total size in bytes
    num_entries: usize,       // number of entries
}

impl SSTableMetadata {
    pub(crate) fn new(
        id: u64,
        first_key: (Key, Offset),
        last_key: (Key, Offset),
        total_size: usize,
        num_entries: usize,
    ) -> Self {
        Self {
            id,
            first_key,
            last_key,
            total_size,
            num_entries,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SSTable {
    metadata: SSTableMetadata,
    file: Mmap,
    index: SSTableSparseIndex,
    bloom_filter: Bloom<Key>,
}

impl Ord for SSTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.metadata.id.cmp(&other.metadata.id)
    }
}

impl Eq for SSTable {}

impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SSTable {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl SSTable {
    pub fn new(
        metadata: SSTableMetadata,
        file: Mmap,
        index: SSTableSparseIndex,
        bloom_filter: Bloom<Key>,
    ) -> Self {
        Self {
            metadata,
            file,
            index,
            bloom_filter,
        }
    }

    pub fn open(path: PathBuf) -> LiteDbResult<Self> {
        let segment_file = File::open(path)?;
        let mut reader = BufReader::new(&segment_file);
        reader
            .seek(SeekFrom::End(-(mem::size_of::<u64>() as i64)))
            .unwrap();
        let size_of_serialized_data: u64 = reader.read_u64::<LittleEndian>()?;

        reader.seek(SeekFrom::Start(size_of_serialized_data))?;
        let metadata: SSTableMetadata = decode_from_reader(&mut reader)?;

        let index: SSTableSparseIndex = decode_from_reader(&mut reader)?;
        let bloom_filter_state: BloomFilterState = decode_from_reader(&mut reader)?;

        let file = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(size_of_serialized_data as usize)
                .map(&segment_file)
                .unwrap()
        };

        Ok(SSTable {
            metadata,
            file,
            index,
            bloom_filter: bloom_filter_state.into(),
        })
    }

    pub fn id(&self) -> u64 {
        self.metadata.id
    }

    pub fn potentially_contains_key(&self, key: &Key) -> bool {
        self.bloom_filter.check(key)
    }

    pub fn get(table: Arc<SSTable>, key: RefKey) -> LiteDbResult<Option<Value>> {
        let owned_key = key.to_vec();
        if !table.potentially_contains_key(&owned_key) {
            return Ok(None);
        }

        let iterator = SSTableIterator::new(table, &Some(owned_key), &None);
        for result in iterator {
            let (k, v) = result?;
            if k.as_slice() > key {
                return Ok(None);
            }

            if k == key {
                return if v == TOMBSTONE {
                    Ok(None)
                } else {
                    Ok(Some(v))
                };
            }
        }

        Ok(None)
    }
}

impl TryFrom<PathBuf> for SSTable {
    type Error = LiteDbError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Self::open(path)
    }
}

impl Scannable for Arc<SSTable> {
    fn scan(&self, from: &Option<Key>, to: &Option<Key>) -> KVIterator {
        KVIterator::SSTable(SSTableIterator::new(self.clone(), from, to))
    }
}

pub(crate) type Offset = usize;

// Sparse index for the SSTable
#[derive(Debug, Default, Encode, Decode)]
pub(crate) struct SSTableSparseIndex {
    items: Vec<(Key, Offset)>,
}

impl From<Vec<(Key, Offset)>> for SSTableSparseIndex {
    fn from(items: Vec<(Key, Offset)>) -> Self {
        Self { items }
    }
}

impl SSTableSparseIndex {
    fn get_offset(&self, key: RefKey) -> Option<Offset> {
        let idx = match self
            .items
            .partition_point(|item| item.0.as_slice() < key)
            .overflowing_sub(1)
        {
            (result, false) => result,
            (_, true) => 0,
        };
        if idx > self.items.len() {
            None
        } else {
            Some(self.items[idx].1)
        }
    }
}

pub(crate) struct SSTableIterator {
    ss_table: Arc<SSTable>,
    offset_opt: Option<usize>,
    start_key_opt: Option<Key>,
    stop_key_opt: Option<Key>,
}

impl SSTableIterator {
    pub fn new(ss_table: Arc<SSTable>, from: &Option<Key>, to: &Option<Key>) -> Self {
        let offset_opt = if let Some(first_key) = from {
            ss_table.index.get_offset(first_key)
        } else {
            Some(0)
        };
        Self {
            ss_table,
            offset_opt,
            start_key_opt: from.clone(),
            stop_key_opt: to.clone(),
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = LiteDbResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(offset) = self.offset_opt {
            if offset > self.ss_table.metadata.last_key.1 {
                return None;
            }

            let mut running_offset = offset;
            loop {
                let ((k, v), num_bytes): ((Key, Value), usize) =
                    match decode(&self.ss_table.file[running_offset..]) {
                        Ok(value) => value,
                        Err(err) => break Some(Err(err)),
                    };

                if let Some(start_key) = &self.start_key_opt {
                    if k < *start_key {
                        running_offset += num_bytes;
                        continue;
                    }
                }

                if let Some(stop_key) = &self.stop_key_opt {
                    if k >= *stop_key {
                        break None;
                    }
                }

                self.offset_opt = Some(running_offset + num_bytes);
                break Some(Ok((k, v)));
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Ok;
    use tempfile::tempdir;

    use crate::{mem_table::MemTable, ss_table::SSTable, Scannable};

    fn to_vec(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn check_ss_table(ss_table: Arc<SSTable>, size_bytes: usize) -> anyhow::Result<()> {
        // check metadata
        assert_eq!(ss_table.metadata.id, 1);
        assert_eq!(ss_table.metadata.num_entries, 1000);
        assert_eq!(ss_table.metadata.total_size, size_bytes);
        assert_eq!(ss_table.metadata.first_key, (to_vec("k_000"), 0));
        matches!(&ss_table.metadata.last_key, (v, _) if v == &to_vec("k_999"));

        // check bloom_filter
        assert!(ss_table.potentially_contains_key(&to_vec("k_000")));
        assert!(ss_table.potentially_contains_key(&to_vec("k_012")));
        assert!(!ss_table.potentially_contains_key(&to_vec("unknown")));

        // check sparse index
        let all_index_keys_exist = ss_table
            .index
            .items
            .iter()
            .map(|(key, _)| ss_table.potentially_contains_key(key))
            .all(|exists| exists);
        assert!(all_index_keys_exist);

        // check get
        assert_eq!(
            SSTable::get(ss_table.clone(), b"k_990")?,
            Some(to_vec("v_990"))
        );
        assert_eq!(
            SSTable::get(ss_table.clone(), b"k_020")?,
            Some(to_vec("v_020"))
        );
        assert_eq!(
            SSTable::get(ss_table.clone(), b"k_101")?,
            Some(to_vec("v_101"))
        );

        // check scan
        assert_eq!(ss_table.scan(&None, &None).count(), 1000);
        assert_eq!(ss_table.scan(&Some(to_vec("k_991")), &None).count(), 9);
        assert_eq!(
            ss_table
                .scan(&Some(to_vec("k_970")), &Some(to_vec("k_980")))
                .count(),
            10
        );

        Ok(())
    }

    #[test]
    fn test_ss_table_from_mem_table() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();

        let mem_table = MemTable::open(dir, 1).unwrap();
        let mut size_bytes = 0usize;
        for i in 0..1000 {
            let k = format!("k_{:01$}", i, 3);
            let v = format!("v_{:01$}", i, 3);
            size_bytes += k.len() + v.len();
            mem_table.set(k.as_bytes(), v.as_bytes())?;
        }

        let ss_table = mem_table.save(3_000_000, 1_000_000, 300)?;
        check_ss_table(ss_table, size_bytes)
    }

    #[test]
    fn test_ss_table_from_file() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();

        let mem_table = MemTable::open(dir, 1).unwrap();
        let mut size_bytes = 0usize;
        for i in 0..1000 {
            let k = format!("k_{:01$}", i, 3);
            let v = format!("v_{:01$}", i, 3);
            size_bytes += k.len() + v.len();
            mem_table.set(k.as_bytes(), v.as_bytes())?;
        }
        let file_path = mem_table.ss_table_file_path();
        mem_table.save(3_000_000, 1_000_000, 300)?;

        let ss_table = Arc::new(SSTable::open(file_path)?);
        check_ss_table(ss_table, size_bytes)
    }
}
