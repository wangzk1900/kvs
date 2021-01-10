#![deny(missing_docs)]

//! # KvStore
//!
//! A library for storing key/values.

use std::collections::HashMap;

/// The `KvStore` stores key/values in memory.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Create a `KvStore`, it contains an empty `HashMap`.
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Set the value of a string key to a string.
    ///
    /// If the key exists, the value is updated.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Get the string value of the a string key.
    ///
    /// If the key does not exist, return `None`.
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
