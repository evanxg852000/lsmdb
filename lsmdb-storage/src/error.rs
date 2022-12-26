use std::io;

use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

pub type LiteDbResult<T> = Result<T, LiteDbError>;

#[derive(Debug, Error)]
pub enum LiteDbError {
    #[error("Data encoding error: `{0}`.")]
    Encoding(EncodeError),
    #[error("Data decoding error: `{0}`.")]
    Decoding(DecodeError),
    #[error("Corrupted data error.")]
    CorruptedData,
    #[error("Io error: `{0}`.")]
    Io(io::Error),
    #[error("Policy error: `{0}`.")]
    PolicyError(String),
}

impl From<io::Error> for LiteDbError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<DecodeError> for LiteDbError {
    fn from(err: DecodeError) -> Self {
        Self::Decoding(err)
    }
}

impl From<EncodeError> for LiteDbError {
    fn from(err: EncodeError) -> Self {
        Self::Encoding(err)
    }
}
