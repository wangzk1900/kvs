#![deny(missing_docs)]

use std::ffi::OsStr;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::PathBuf;
use std::{collections::BTreeMap, fs, u64};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::{KvsError, Result};

use super::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores key/values in log.
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, KvsEngine};
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
    readers: HashMap<u64, BufReader<File>>,
    index: BTreeMap<String, CommandPos>,
    current_pointer: u64,
    compaction_size: u64,
    current_fid: u64,
}

impl KvStore {
    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        // Create a log directory
        fs::create_dir_all(&path)?;

        // Open the log files for reading.
        let mut readers: HashMap<u64, BufReader<File>> = HashMap::new();
        let mut current_fid = 0;
        let log_paths = get_log_paths(path.to_owned())?;
        for (fid, log_path) in log_paths.iter() {
            let reader = BufReader::new(File::open(log_path)?);
            readers.insert(*fid, reader);

            if *fid > current_fid {
                current_fid = *fid;
            }
        }

        // Open the active file for storing the commands.
        let active_log_file = new_log_file(current_fid, path.to_owned())?;
        let writer = BufWriter::new(active_log_file);

        if log_paths.len() == 0 {
            readers.insert(
                0,
                BufReader::new(open_log_file(current_fid, path.to_owned())?),
            );
        }

        // Store log pointers of the commands in the index.
        let mut index: BTreeMap<String, CommandPos> = BTreeMap::new();
        let mut compaction_size = 0;
        gen_index(&mut index, &mut readers, &mut compaction_size)?;

        // Current log pointer.
        let current_pointer = fs::metadata(get_log_path(current_fid, path.to_owned()))?.len();

        Ok(KvStore {
            path,
            writer,
            readers,
            index,
            current_pointer,
            compaction_size,
            current_fid,
        })
    }

    /// Compact the log file according the index.
    pub fn compact(&mut self) -> Result<()> {
        let old_max_fid = self.current_fid;

        // Create new log files.
        self.current_fid += 1;
        let mut log_size: u64 = 0;

        self.writer = BufWriter::new(new_log_file(self.current_fid, self.path.to_owned())?);
        self.readers.insert(
            self.current_fid,
            BufReader::new(File::open(get_log_path(
                self.current_fid,
                self.path.to_owned(),
            ))?),
        );

        // Copy distinct data from the old log files to the new log files.
        for (_, CommandPos { fid, pos, len }) in self.index.iter() {
            let reader = self.readers.get(&fid).unwrap();
            reader.get_ref().seek(SeekFrom::Start(*pos))?;
            let cmd_reader = reader.get_ref().take(*len);
            let command: Command = serde_json::from_reader(cmd_reader)?;

            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;

            log_size += len;

            if log_size > 1024 * 1024 {
                self.current_fid += 1;
                log_size = 0;
                self.writer = BufWriter::new(new_log_file(self.current_fid, self.path.to_owned())?);
                self.readers.insert(
                    self.current_fid,
                    BufReader::new(File::open(get_log_path(
                        self.current_fid,
                        self.path.to_owned(),
                    ))?),
                );
            }
        }
        self.compaction_size = 0;

        // Update the current pointer
        self.current_pointer = log_size;

        // Delete the old log file
        for (fid, log_path) in get_log_paths(self.path.to_owned())?.iter() {
            if *fid <= old_max_fid {
                fs::remove_file(log_path)?;
                self.readers.remove(&fid);
            }
        }

        // Rebuild the index
        self.index.clear();
        self.compaction_size = 0;
        gen_index(
            &mut self.index,
            &mut self.readers,
            &mut self.compaction_size,
        )?;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key.to_owned(), value.to_owned());

        // Append the serialized command to the active log file
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        let mut active_log = self.current_fid.to_string();
        active_log.push_str(".log");
        let new_offset = fs::metadata(self.path.join(active_log))?.len();

        // Store command position in the index
        let new_len = new_offset - self.current_pointer;
        let command_pos = self.index.insert(
            key.to_owned(),
            CommandPos::new(self.current_fid, self.current_pointer, new_len),
        );

        if let Some(CommandPos { len, .. }) = command_pos {
            self.compaction_size += len;
        }

        if self.compaction_size > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        self.current_pointer = new_offset;

        // If the current_pointer reaches the 1M then create a new log file.
        if self.current_pointer > 1024 * 1024 {
            self.current_fid += 1;

            let new_log_file = new_log_file(self.current_fid, self.path.to_owned())?;
            self.writer = BufWriter::new(new_log_file);

            self.readers.insert(
                self.current_fid,
                BufReader::new(File::open(get_log_path(
                    self.current_fid,
                    self.path.to_owned(),
                ))?),
            );

            self.current_pointer = 0;
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(CommandPos { fid, pos, len }) = self.index.get(&key) {
            let reader = self.readers.get(&fid).unwrap();
            reader.get_ref().seek(SeekFrom::Start(*pos))?;
            let cmd_reader = reader.get_ref().take(*len);
            let command: Command = serde_json::from_reader(cmd_reader)?;
            if let Command::Set { value, .. } = command {
                return Ok(Some(value));
            }
            return Ok(None);
        }
        Ok(None)
    }

    /// Removes a given key.
    ///
    /// Returns `KvsError::KeyNotFoundError` if the given key is not found.
    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(CommandPos { len, .. }) = self.index.remove(&key) {
            let rm_command = Command::remove(key.to_owned());
            serde_json::to_writer(self.writer.get_ref(), &rm_command)?;
            self.index.remove(&key);

            self.compaction_size += len;

            return Ok(());
        }

        Err(KvsError::KeyNotFoundError)
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
    fid: u64, // id of the log file that stores the command
    pos: u64, // the offset of the command in the log file
    len: u64, // the length of the command
}

impl CommandPos {
    /// Create a instance of the `CommandPos` struct.
    fn new(fid: u64, pos: u64, len: u64) -> CommandPos {
        CommandPos { fid, pos, len }
    }
}

// Read the entire log, record the key and log pointer to the index map.
fn gen_index(
    index: &mut BTreeMap<String, CommandPos>,
    readers: &mut HashMap<u64, BufReader<File>>,
    compaction_size: &mut u64,
) -> Result<()> {
    for (fid, reader) in readers.iter() {
        reader.get_ref().seek(SeekFrom::Start(0))?;
        let deserializer = serde_json::Deserializer::from_reader(reader.get_ref());
        let mut commands = deserializer.into_iter::<Command>();
        loop {
            let offset = commands.byte_offset();
            let command = commands.next();
            match command {
                Some(cmd) => match cmd? {
                    Command::Set { key, .. } => {
                        let command_pos = index.insert(
                            key,
                            CommandPos::new(
                                *fid,
                                offset as u64,
                                (commands.byte_offset() - offset) as u64,
                            ),
                        );

                        if let Some(CommandPos { len, .. }) = command_pos {
                            *compaction_size += len;
                        }
                    }
                    Command::Remove { key } => {
                        let command_pos = index.remove(&key);

                        if let Some(CommandPos { len, .. }) = command_pos {
                            *compaction_size += len;
                        }
                    }
                },
                None => {
                    break;
                }
            }
        }
    }

    Ok(())
}

// Create or open a log file for writing to it.
fn new_log_file(fid: u64, path: PathBuf) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(get_log_path(fid, path))?)
}

// Open the log file for reading.
fn open_log_file(fid: u64, path: PathBuf) -> Result<File> {
    Ok(File::open(get_log_path(fid, path))?)
}

// Return the log path according the fid.
fn get_log_path(fid: u64, path: PathBuf) -> PathBuf {
    let mut log_name = fid.to_string();
    log_name.push_str(".log");
    path.join(log_name.to_owned())
}

// Return all the log paths and their file id.
fn get_log_paths(path: PathBuf) -> Result<HashMap<u64, PathBuf>> {
    let mut paths: HashMap<u64, PathBuf> = HashMap::new();
    for entry in fs::read_dir(&path)? {
        let dir_entry = entry?;
        let entry_path = dir_entry.path();
        if entry_path.extension() == Some(OsStr::new("log")) {
            let fid = entry_path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();

            paths.insert(fid, entry_path);
        }
    }

    Ok(paths)
}
