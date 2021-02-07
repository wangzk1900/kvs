// extern crate structopt;

use serde::{Deserialize, Serialize};
use std::{env::current_dir, process::exit};
use structopt::StructOpt;

use kvs::{KvStore, KvsError, Result};

#[derive(Debug, StructOpt, Serialize, Deserialize)]
#[structopt(name = "kvs", about = "key-value store client")]
enum Opt {
    /// Set the value of a string key to a string
    Set {
        #[structopt(name = "KEY", required = true, help = "a string key")]
        key: String,
        #[structopt(name = "VALUE", required = true, help = "a string value")]
        value: String,
    },
    /// Get the string value of the a string key
    Get {
        #[structopt(name = "KEY", required = true, help = "a string key")]
        key: String,
    },
    /// Remove a given key
    Rm {
        #[structopt(name = "VALUE", required = true, help = "a string key")]
        key: String,
    },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let mut kvstore = KvStore::open(current_dir()?)?;

    match opt {
        Opt::Set { key, value } => {
            kvstore.set(key, value)?;
            exit(0);
        }
        Opt::Get { key } => {
            let value = kvstore.get(key)?.unwrap_or("Key not found".to_string());
            println!("{}", value);
            exit(0);
        }
        Opt::Rm { key } => {
            match kvstore.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFoundError) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
            exit(0);
        }
    }
}
