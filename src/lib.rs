#![deny(missing_docs)]
//! # Kvs
//!
//! A library for storing key/values.
//! The `kvs` library contains a type `KvStore`.

#[macro_use]
extern crate failure_derive;

pub mod error;
mod kv;

pub use error::{KvsError, Result};
pub use kv::KvStore;
