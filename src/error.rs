//! # `KvsError`
//!
//! A error enum for `KvStore`.

extern crate failure;

use std::io;

/// Error for KvStore
#[derive(Fail, Debug)]
pub enum KvsError {
    ///
    #[fail(display = "IO error")]
    IOError(io::Error),
    ///
    #[fail(display = "Serde error")]
    SerdeError(serde_json::Error),
    ///
    #[fail(display = "The key is not found.")]
    KeyNotFoundError,
    ///
    #[fail(display = "An unknown error has occurred.")]
    UnknownError,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::IOError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::SerdeError(err)
    }
}

/// kvs Result type
pub type Result<T> = std::result::Result<T, KvsError>;
