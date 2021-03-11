//! # `KvsError`
//!
//! A error enum for `KvStore`.

extern crate failure;

use std::{io, string::FromUtf8Error};

/// Error for KvStore
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error
    #[fail(display = "IO error")]
    IOError(io::Error),
    /// Serde error
    #[fail(display = "Serde error")]
    SerdeError(serde_json::Error),
    /// The key is not found
    #[fail(display = "The key is not found.")]
    KeyNotFoundError,
    /// An unknown error
    #[fail(display = "An unknown error has occurred.")]
    UnknownError,
    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8Error(FromUtf8Error),
    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    SledError(sled::Error),
    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),
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

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8Error(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::SledError(err)
    }
}

/// kvs Result type
pub type Result<T> = std::result::Result<T, KvsError>;
