use bincode::{config::Configuration, Decode, Encode};
use crc::{Crc, CRC_32_ISCSI};
use parking_lot::Mutex;
use std::io::{Read, Write};

use crate::LiteDbError;

const CONFIG: Configuration = bincode::config::standard();

pub(crate) fn decode<T: Decode>(slice: &[u8]) -> Result<(T, usize), LiteDbError> {
    let decode_response: (T, usize) = bincode::decode_from_slice(slice, CONFIG)?;
    Ok(decode_response)
}

pub(crate) fn encode_into_writer<T: Encode, W: Write>(
    value: &T,
    writer: &mut W,
) -> Result<usize, LiteDbError> {
    let num_encoded_bytes = bincode::encode_into_std_write(value, writer, CONFIG)?;
    Ok(num_encoded_bytes)
}

pub(crate) fn decode_from_reader<T: Decode, R: Read>(reader: &mut R) -> Result<T, LiteDbError> {
    let decoded_value = bincode::decode_from_std_read(reader, CONFIG)?;
    Ok(decoded_value)
}

pub(crate) fn crc32(key: &[u8], value: &[u8]) -> u32 {
    let crc = Crc::<u32>::new(&CRC_32_ISCSI);
    let mut digest = crc.digest();
    digest.update(key);
    digest.update(value);
    digest.finalize()
}

pub(crate) struct AtomicOperationExecutor(Mutex<()>);

impl AtomicOperationExecutor {
    pub fn new() -> Self {
        Self(Mutex::new(()))
    }

    pub fn perform<F: Fn()>(&self, callback: F) {
        let _mutex_guard = self.0.lock();
        callback();
    }
}
