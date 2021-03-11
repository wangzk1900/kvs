#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use log::{warn, LevelFilter};
use std::{env::current_dir, fs};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

use kvs::{KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = "A key-value store server")]
struct Opt {
    /// IP address and a port number, with the format IP:PORT.
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = ADDRESS_FORMAT,
        default_value = DEFAULT_LISTENING_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(long, value_name = "ENGINE-NAME", help = "Sets the storage engine")]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled,
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut opt = Opt::from_args();

    let res = current_engine().and_then(move |curr_engine| {
        if opt.engine.is_none() {
            opt.engine = curr_engine;
        }
        if curr_engine.is_some() && opt.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opt)
    });

    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("Kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, opt.addr),
        Engine::sled => run_with_engine(
            SledKvsEngine::new(sled::Db::start_default(current_dir()?)?),
            opt.addr,
        ),
    }
}

// Run with a given engine.
fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvsServer::new(engine);
    server.run(addr)
}

// Get the current engine.
fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");

    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
