#![deny(missing_docs)]
//! # Kvs
//!
//! A library for storing key/values.
//! The `kvs` library contains a type `KvStore`.

#[macro_use]
extern crate failure_derive;

#[macro_use]
extern crate log;

mod client;
mod common;
mod engines;
pub mod error;
mod server;

pub use client::KvsClient;
pub use engines::KvStore;
pub use engines::KvsEngine;
pub use engines::SledKvsEngine;
pub use error::{KvsError, Result};
pub use server::KvsServer;
