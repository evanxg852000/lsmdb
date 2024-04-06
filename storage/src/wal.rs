use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;

use crate::{
    error::LiteDbResult,
    utils::{crc32, decode_from_reader, encode_into_writer},
    Key, LiteDbError, RefKey, RefValue, Value,
};

use std::io::BufReader;

pub(crate) const WAL_FILE_EXTENSION: &str = "log";

pub(crate) fn is_mem_table_file(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .map(|ext| ext == WAL_FILE_EXTENSION)
            .unwrap_or(false)
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct LogItem {
    pub key: Key,
    pub value: Value,
    pub checksum: u32,
}

impl LogItem {
    fn new(key: Key, value: Value) -> Self {
        let checksum = crc32(&key, &value);
        Self {
            key,
            value,
            checksum,
        }
    }

    fn check(&self) -> bool {
        self.checksum == crc32(&self.key, &self.value)
    }

    fn is_empty(&self) -> bool {
        self.key.is_empty() && self.value.is_empty() && self.checksum == 0
    }
}

impl From<LogItem> for (Key, Value) {
    fn from(item: LogItem) -> Self {
        (item.key, item.value)
    }
}

#[derive(Debug)]
pub(crate) struct WriteAheadLogger {
    id: u64,
    file: RwLock<BufWriter<File>>,
    dir: PathBuf,
}

impl WriteAheadLogger {
    pub(crate) fn open(dir: PathBuf, id: u64) -> LiteDbResult<Self> {
        let log_file_path = wal_file_path(&dir, id);
        let file = if log_file_path.exists() {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(log_file_path)?
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(log_file_path)?;
            file
        };

        let file = RwLock::new(BufWriter::new(file));
        Ok(Self { id, file, dir })
    }

    pub(crate) fn append(&self, key: RefKey, value: RefValue) -> LiteDbResult<()> {
        let log_item = LogItem::new(key.to_owned(), value.to_owned());
        let mut file_lock_guard = self.file.write();
        encode_into_writer(&log_item, &mut file_lock_guard.by_ref())?;
        file_lock_guard.flush().map_err(LiteDbError::from)
    }

    pub(crate) fn apply_batch(&self, operations: &[(Key, Value)]) -> LiteDbResult<()> {
        let mut file_lock_guard = self.file.write();
        for operation in operations {
            let log_item = LogItem::new(operation.0.clone(), operation.1.clone());
            encode_into_writer(&log_item, &mut file_lock_guard.by_ref())?;
        }
        file_lock_guard.flush().map_err(LiteDbError::from)
    }

    pub(crate) fn iter(&self) -> WriteAheadLogIter {
        let file = self
            .file
            .write()
            .get_ref()
            .try_clone()
            .expect("Expected a valid file handle.");
        WriteAheadLogIter::new(file)
    }

    pub fn remove(&self) -> LiteDbResult<()> {
        fs::remove_file(self.file_path()).map_err(LiteDbError::from)
    }

    pub fn file_path(&self) -> PathBuf {
        wal_file_path(&self.dir, self.id)
    }
}

fn wal_file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:01$}.{WAL_FILE_EXTENSION}", id, 20))
}

pub(crate) struct WriteAheadLogIter {
    reader: BufReader<File>,
}

impl WriteAheadLogIter {
    pub(crate) fn new(file: File) -> Self {
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(0)).unwrap();
        Self { reader }
    }
}

impl Iterator for WriteAheadLogIter {
    type Item = Result<LogItem, LiteDbError>;

    fn next(&mut self) -> Option<Self::Item> {
        let log_item_result = decode_from_reader::<LogItem, _>(&mut self.reader);
        let log_item = match log_item_result {
            Ok(log_item) => log_item,
            Err(err) => {
                return match err {
                    LiteDbError::Decoding(_) => return None,
                    _ => Some(Err(err)),
                }
            }
        };

        if log_item.is_empty() {
            // end of stream
            return None;
        }

        if !log_item.check() {
            return Some(Err(LiteDbError::CorruptedData));
        }

        Some(Ok(log_item))
    }
}

#[cfg(test)]
mod tests {
    use super::WriteAheadLogger;
    use anyhow::Ok;
    use tempfile::tempdir;

    #[test]
    fn test_empty_wal() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();
        let wal = WriteAheadLogger::open(dir, 1).unwrap();
        assert_eq!(wal.iter().count(), 0);
        Ok(())
    }

    #[test]
    fn test_wal() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let dir = tempdir.path().to_path_buf();
        let wal = WriteAheadLogger::open(dir, 1).unwrap();
        for i in 0..1000 {
            let k = format!("k_{}", i);
            let v = format!("v_{}", i);
            wal.append(k.as_bytes(), v.as_bytes()).unwrap();
        }

        for (i, res) in wal.iter().enumerate() {
            let log_item = res.unwrap();
            let expected = (
                format!("k_{}", i).into_bytes(),
                format!("v_{}", i).into_bytes(),
            );
            assert_eq!(expected, log_item.into());
        }

        Ok(())
    }
}
