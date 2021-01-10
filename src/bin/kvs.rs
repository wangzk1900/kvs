extern crate structopt;

use std::process::exit;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
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

fn main() {
    let opt = Opt::from_args();

    match opt {
        Opt::Set {
            key: _key,
            value: _value,
        } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Opt::Get { key: _key } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Opt::Rm { key: _key } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
