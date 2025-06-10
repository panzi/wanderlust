#![allow(clippy::collapsible_else_if)]
#![allow(clippy::single_char_add_str)]
#![allow(clippy::manual_range_contains)]

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

fn main() {
    let mut args = std::env::args_os();
    args.next().unwrap();

    let old = args.next().unwrap();
    let old_source = std::fs::read_to_string(&old).unwrap();

    match PostgreSQLParser::parse(&old_source) {
        Ok(old_schema) => {
            if let Some(new) = args.next() {
                let new_source = std::fs::read_to_string(&new).unwrap();

                match PostgreSQLParser::parse(&new_source) {
                    Ok(new_schema) => {
                        let migrations = generate_migration(&old_schema, &new_schema);

                        println!("{BEGIN};");
                        println!();
                        let mut iter = migrations.iter();
                        if let Some(mut prev) = iter.next() {
                            println!("{prev}");
                            for stmt in iter {
                                if !prev.is_same_variant(stmt) {
                                    println!();
                                }
                                println!("{stmt}");
                                prev = stmt;
                            }
                        }
                        println!();
                        println!("{COMMIT};");
                    }
                    Err(err) => {
                        err.print(&new.to_string_lossy(), &new_source, &mut std::io::stderr()).unwrap();
                        std::process::exit(1);
                    }
                }
            } else {
                println!("{old_schema}");
            }
        }
        Err(err) => {
            err.print(&old.to_string_lossy(), &old_source, &mut std::io::stderr()).unwrap();
            std::process::exit(1);
        }
    }
}
