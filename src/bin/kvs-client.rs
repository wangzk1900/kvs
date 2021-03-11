// extern crate structopt;

use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

use kvs::{KvsClient, Result};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", about = "A key-value store client")]
enum Opt {
    /// Set the value of a string key to a string
    Set {
        #[structopt(name = "KEY", required = true, help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", required = true, help = "A string value")]
        value: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    /// Get the string value of the a string key
    Get {
        #[structopt(name = "KEY", required = true, help = "a string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    /// Remove a given key
    Rm {
        #[structopt(name = "VALUE", required = true, help = "a string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::from_args();

    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt {
        Opt::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }

        Opt::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }

        Opt::Rm { key, addr } => {
            let mut client = KvsClient::connect(addr)?;

            client.remove(key).expect("Key not found");
        }
    }

    Ok(())
}
