#![allow(clippy::collapsible_else_if)]
#![allow(clippy::single_char_add_str)]
#![allow(clippy::manual_range_contains)]

use std::ffi::OsStr;

use dialects::postgresql::PostgreSQLParser;
use migration::generate_migration;
use model::syntax::Parser;

pub mod model;
pub mod dialects;
pub mod error;
pub mod format;
pub mod migration;
pub mod ordered_hash_map;

use model::words::*;
use postgres::{Client, NoTls};

use model::database::Database;
use dialects::postgresql::reflect::load_from_database;

fn load_database(url: &OsStr) -> Database {
    if let Some(url) = url.to_str() {
        if url.starts_with("postgresql:") {
            // TODO: TLS support
            match Client::connect(url, NoTls) {
                Ok(mut client) => {
                    match load_from_database(&mut client) {
                        Ok(db) => return db,
                        Err(err) => {
                            eprintln!("{url}: {err}");
                            std::process::exit(1);
                        }
                    }
                }
                Err(err) => {
                    eprintln!("{url}: {err}");
                    std::process::exit(1);
                }
            }
        }
    }

    match std::fs::read_to_string(&url) {
        Ok(source) => {
            match PostgreSQLParser::parse(&source) {
                Ok(db) => db,
                Err(err) => {
                    err.print(&url.to_string_lossy(), &source, &mut std::io::stderr()).unwrap();
                    std::process::exit(1);
                }
            }
        }
        Err(err) => {
            eprintln!("{}: {err}", &url.to_string_lossy());
            std::process::exit(1);
        }
    }
}

fn main() {
    let mut args = std::env::args_os();
    args.next().unwrap();

    let old = args.next().unwrap();
    let old_db = load_database(&old);

    if let Some(new) = args.next() {
        let new_db = load_database(&new);
        let migrations = generate_migration(&old_db, &new_db);

        let mut iter = migrations.iter();
        if let Some(mut prev) = iter.next() {
            println!("{BEGIN};");
            println!();
            println!("{prev}");
            for stmt in iter {
                if !prev.is_same_variant(stmt) {
                    println!();
                }
                println!("{stmt}");
                prev = stmt;
            }
            println!();
            println!("{COMMIT};");
        } else {
            println!("-- NO CHANGES");
        }
    } else {
        println!("{old_db}");
    }
}
