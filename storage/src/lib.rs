mod batching;
mod bloom_filter;
mod compactor;
mod controller;
mod error;
mod iterator;
mod mem_table;
mod options;
mod ss_table;
mod utils;
mod wal;

use batching::BatchOperations;
use compactor::Compactor;
pub use compactor::CompactorPolicyConfig;
use controller::MemTableController;
pub use controller::MemTableControllerPolicyConfig;
use crossbeam_skiplist::SkipSet;
use error::{LiteDbError, LiteDbResult};
use iterator::CombineIterator;
use mem_table::MemTable;
use mem_table::MemTableIterator;
pub use options::LiteDbOptions;
use ss_table::{SSTable, SSTableIterator};
use utils::AtomicOperationExecutor;
use wal::is_mem_table_file;

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::ss_table::is_ss_table_file;

pub(crate) const TOMBSTONE: [u8; 0] = [];

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type RefKey<'a> = &'a [u8];
pub type RefValue<'a> = &'a [u8];

pub(crate) enum KVIterator {
    MemTable(MemTableIterator),
    SSTable(SSTableIterator),
}

impl Iterator for KVIterator {
    type Item = LiteDbResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            KVIterator::MemTable(iter) => iter.next(),
            KVIterator::SSTable(iter) => iter.next(),
        }
    }
}

pub(crate) trait Scannable {
    fn scan(&self, from: &Option<Key>, to: &Option<Key>) -> KVIterator;
}

pub struct LiteDb {
    options: LiteDbOptions,
    /// An ordered list of MemTable.
    // All mem_tables are managed together.
    // They are ordered like as list [current, flushing_1, flushing_2, ...]
    // The current (more recent) mem_table the one used to handle writes
    // All subsequent mem_tables are those that are being flushed to ss_table.
    // A flushing mem_table gets removed as soon as it's flushed to disk as ss_table.
    mem_tables: Arc<SkipSet<Arc<MemTable>>>,
    /// An ordered list of SSTable.
    ss_tables: Arc<SkipSet<Arc<SSTable>>>,
    // atomic_operation_executor: Arc<AtomicOperationExecutor>,
    mem_controller: MemTableController,
    compactor: Compactor,
    path: PathBuf,
}

impl LiteDb {
    pub fn open<P: AsRef<Path>>(dir: P, options: LiteDbOptions) -> LiteDbResult<Self> {
        let path = PathBuf::from(dir.as_ref());
        let atomic_operation_executor = Arc::new(AtomicOperationExecutor::new());
        if !path.exists() {
            fs::create_dir_all(&path)?;
            let mem_tables = Arc::new(SkipSet::new());
            mem_tables.insert(Arc::new(MemTable::open(path.clone(), 0)?));

            let ss_tables = Arc::new(SkipSet::new());

            let mem_controller = MemTableController::start(
                mem_tables.clone(),
                ss_tables.clone(),
                atomic_operation_executor.clone(),
                options.bloom_filter_size_bytes,
                options.bloom_filter_item_count,
                options.sparse_index_range_size,
                &options.mem_table_controller_policy,
            )?;
            let compactor = Compactor::start(
                ss_tables.clone(),
                atomic_operation_executor.clone(),
                &options.compactor_policy,
            )?;

            return Ok(Self {
                options,
                mem_tables,
                ss_tables,
                mem_controller,
                compactor,
                path,
            });
        }

        // List all ss_tables & mem_tables
        let ss_tables = SkipSet::new();
        let mem_tables = Arc::new(SkipSet::new());
        let entries = fs::read_dir(dir.as_ref())?;
        for entry_result in entries {
            let entry_path = entry_result?.path();
            if is_ss_table_file(&entry_path) {
                let ss_table = SSTable::open(entry_path.clone())?;
                ss_tables.insert(Arc::new(ss_table));
            }

            if is_mem_table_file(&entry_path) {
                let id: u64 = entry_path
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .parse()
                    .expect("Expected a valid wal file id.");
                let mem_table = MemTable::open(path.clone(), id)?;
                mem_tables.insert(Arc::new(mem_table));
            }
        }

        // Create default mem_table if none is found
        if mem_tables.is_empty() {
            mem_tables.insert(Arc::new(MemTable::open(path.clone(), 0)?));
        }

        let ss_tables = Arc::new(ss_tables);
        let mem_controller = MemTableController::start(
            mem_tables.clone(),
            ss_tables.clone(),
            atomic_operation_executor.clone(),
            options.bloom_filter_size_bytes,
            options.bloom_filter_item_count,
            options.sparse_index_range_size,
            &options.mem_table_controller_policy,
        )?;
        let compactor = Compactor::start(
            ss_tables.clone(),
            atomic_operation_executor.clone(),
            &options.compactor_policy,
        )?;
        Ok(Self {
            options,
            mem_tables,
            ss_tables,
            // atomic_operation_executor,
            mem_controller,
            compactor,
            path,
        })
    }

    pub fn open_with_default<P: AsRef<Path>>(dir: P) -> LiteDbResult<Self> {
        Self::open(dir, LiteDbOptions::default())
    }

    pub fn set(&self, key: RefKey, value: RefValue) -> LiteDbResult<()> {
        self.mem_tables
            .front()
            .expect("Expected a valid mem_table")
            .set(key, value)
    }

    pub fn get(&self, key: RefKey) -> LiteDbResult<Option<Value>> {
        if let Ok(Some(value)) = self
            .mem_tables
            .front()
            .expect("Expected a valid mem_table")
            .get(key)
        {
            if value == TOMBSTONE {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        let ss_tables = {
            let owned_key = key.to_owned();
            self.ss_tables
                .iter()
                .map(|entry| entry.value().clone())
                .filter(|ss_table| ss_table.potentially_contains_key(&owned_key))
                .collect::<Vec<_>>()
        };
        for ss_table in ss_tables {
            if let Ok(Some(value)) = SSTable::get(ss_table, key) {
                if value == TOMBSTONE {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn delete(&self, key: RefKey) -> LiteDbResult<()> {
        self.set(key, &TOMBSTONE)
    }

    pub fn apply_batch(&self, operations: BatchOperations) -> LiteDbResult<()> {
        self.mem_tables
            .front()
            .expect("Expected a valid mem_table")
            .apply_batch(operations)
    }

    pub fn scan(
        &self,
        from: &Option<Key>,
        to: &Option<Key>,
    ) -> LiteDbResult<impl Iterator<Item = LiteDbResult<(Key, Value)>> + '_> {
        let mut iterators = Vec::with_capacity(self.mem_tables.len() + self.ss_tables.len());

        // add ss_table from oldest to newest
        for ss_table in self.ss_tables.iter() {
            iterators.push(ss_table.scan(from, to));
        }

        // add mem_table from oldest to newest
        for mem_table in self.mem_tables.iter() {
            iterators.push(mem_table.scan(from, to));
        }

        CombineIterator::try_new(iterators)
    }

    pub fn options(&self) -> &LiteDbOptions {
        &self.options
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn close(&mut self) {
        self.mem_controller.stop();
        self.compactor.stop();
    }
}

impl Drop for LiteDb {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{batching::BatchOperations, error::LiteDbResult, options::LiteDbOptions, LiteDb};

    #[test]
    fn test_lite_db() -> LiteDbResult<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("data");
        {
            let db = LiteDb::open(&db_path, LiteDbOptions::for_test()).unwrap();
            for i in 0..=1000 {
                let k = format!("k_{:01$}", i, 3);
                let v = format!("v_{:01$}", i, 3);
                db.set(k.as_bytes(), v.as_bytes())?;
            }

            // override first 500 entries
            for i in 0..=500 {
                let k = format!("k_{:01$}", i, 3);
                let v = format!("v2_{:01$}", i, 3);
                db.set(k.as_bytes(), v.as_bytes())?;
            }
        }

        {
            let db = LiteDb::open(&db_path, LiteDbOptions::for_test()).unwrap();
            for i in 0..=1000 {
                let k = format!("k_{:01$}", i, 3);
                let v = db.get(k.as_bytes())?;
                let expected_v = if i <= 500 {
                    format!("v2_{:01$}", i, 3).as_bytes().to_vec()
                } else {
                    format!("v_{:01$}", i, 3).as_bytes().to_vec()
                };
                assert_eq!(v, Some(expected_v));
            }
        }
        Ok(())
    }

    #[test]
    fn test_lite_db_batch() -> LiteDbResult<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("data");

        {
            let db = LiteDb::open(&db_path, LiteDbOptions::for_test()).unwrap();
            let mut batch = BatchOperations::default();
            for i in 1..=1000 {
                if i < 750 {
                    let k = format!("k_{:01$}", i, 3);
                    let v = format!("v_{:01$}", i, 3);
                    batch.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
                } else {
                    let k = format!("k_{:01$}", i - 750, 3);
                    batch.delete(k.as_bytes().to_vec());
                }
            }
            db.apply_batch(batch)?;
        }

        {
            let db = LiteDb::open(&db_path, LiteDbOptions::for_test()).unwrap();
            for i in 1..=300 {
                let k = format!("k_{:01$}", i, 3);
                let v = db.get(k.as_bytes())?;
                if i <= 250 {
                    assert_eq!(v, None);
                } else {
                    let expected_v = format!("v_{:01$}", i, 3).as_bytes().to_vec();
                    assert_eq!(v, Some(expected_v));
                }
            }
        }
        Ok(())
    }
}
