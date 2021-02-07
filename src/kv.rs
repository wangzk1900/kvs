#![deny(missing_docs)]

use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::PathBuf;
use std::{collections::BTreeMap, fs, u64};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores key/values in log.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// # use tempfile::TempDir;
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = KvStore::open(temp_dir.path()).unwrap();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    path: PathBuf,
    writer: BufWriter<File>,
    reader: BufReader<File>,
    index: BTreeMap<String, CommandPos>,
    current_pointer: u64,
}

impl KvStore {
    /// Set the value of a string key to a string.
    ///
    /// If the key exists, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key.to_owned(), value.to_owned());

        // Append the serialized command to the log file
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        let new_offset = fs::metadata(self.path.join("log"))?.len();

        self.index.insert(
            key.to_owned(),
            CommandPos::new(self.current_pointer, new_offset - self.current_pointer),
        );

        self.current_pointer = new_offset;

        if self.current_pointer > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Get the string value of the a string key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(command_pos) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(command_pos.pos))?;
            let cmd_reader = self.reader.get_ref().take(command_pos.len);
            let command: Command = serde_json::from_reader(cmd_reader)?;
            if let Command::Set { value, .. } = command {
                return Ok(Some(value));
            }
            return Ok(None);
        }
        Ok(None)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(_) => {
                let rm_command = Command::remove(key);
                serde_json::to_writer(self.writer.get_ref(), &rm_command)?;
                return Ok(());
            }
            None => Err(KvsError::KeyNotFoundError),
        }
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        // Create a log directory
        fs::create_dir_all(&path)?;

        // Create a log file that record the commands.
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path.join("log"))?;
        let writer = BufWriter::new(log_file);

        // Open the log file for reading.
        let log_file = File::open(&path.join("log"))?;
        let mut reader = BufReader::new(log_file);

        // Store log pointers in the index.
        let mut index: BTreeMap<String, CommandPos> = BTreeMap::new();
        gen_index(&mut index, &mut reader)?;

        // Current log pointer.
        let current_pointer = fs::metadata(&path.join("log"))?.len();

        Ok(KvStore {
            path,
            writer,
            reader,
            index,
            current_pointer,
        })
    }

    /// Compact the log file according the index.
    pub fn compact(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;

        // Create a temp file.
        let tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(self.path.join("tmp"))
            .unwrap();

        // Copy the contents of the log file to the temp file.
        {
            let mut tmp_writer = BufWriter::new(tmp_file);
            io::copy(&mut self.reader, &mut tmp_writer)?;
        }

        // Create a reader of the temp file.
        let tmp_file = File::open(self.path.join("tmp"))?;
        let mut tmp_reader = BufReader::new(tmp_file);

        // Truncate the log file.
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.path.join("log"))?;

        // Copy distinct data from the temp file to the log file.
        self.writer.seek(SeekFrom::Start(0))?;
        for (_, CommandPos { pos, len }) in self.index.iter() {
            tmp_reader.get_mut().seek(SeekFrom::Start(*pos))?;
            let cmd_reader = tmp_reader.get_mut().take(*len);
            let command: Command = serde_json::from_reader(cmd_reader)?;
            serde_json::to_writer(&mut self.writer, &command)?;
        }
        self.writer.flush()?;

        // Update the current pointer
        self.current_pointer = fs::metadata(self.path.join("log"))?.len();

        // Remove the tmp file
        fs::remove_file(self.path.join("tmp"))?;

        // Rebuild the index
        self.index.clear();
        gen_index(&mut self.index, &mut self.reader)?;

        Ok(())
    }
}

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

/// A struct that represent the position and length in the log file.
#[derive(Debug)]
struct CommandPos {
    pos: u64,
    len: u64,
}

impl CommandPos {
    /// Create a instance of the `CommandPos` struct.
    fn new(pos: u64, len: u64) -> CommandPos {
        CommandPos { pos, len }
    }
}

// Read the entire log, record the key and log pointer to the index map.
fn gen_index(index: &mut BTreeMap<String, CommandPos>, reader: &mut BufReader<File>) -> Result<()> {
    reader.get_ref().seek(SeekFrom::Start(0))?;
    let deserializer = serde_json::Deserializer::from_reader(reader.get_ref());
    let mut commands = deserializer.into_iter::<Command>();
    loop {
        let offset = commands.byte_offset();
        let command = commands.next();
        match command {
            Some(cmd) => match cmd? {
                Command::Set { key, .. } => {
                    index.insert(
                        key,
                        CommandPos::new(offset as u64, (commands.byte_offset() - offset) as u64),
                    );
                }
                Command::Remove { key } => {
                    index.remove(&key);
                }
            },
            None => {
                break;
            }
        }
    }

    Ok(())
}
