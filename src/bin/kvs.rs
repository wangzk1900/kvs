extern crate clap;
use std::process::exit;

use clap::{load_yaml, App};

fn main() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", _sub_matches) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", _sub_matches) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", _sub_matches) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}
