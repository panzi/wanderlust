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
    let mut parser = PostgreSQLParser::new(&old_source);

    match parser.parse() {
        Ok(old_ddl) => {
            if let Some(new) = args.next() {
                let new_source = std::fs::read_to_string(&new).unwrap();
                let mut parser = PostgreSQLParser::new(&new_source);

                match parser.parse() {
                    Ok(new_ddl) => {
                        let migrations = generate_migration(&old_ddl, &new_ddl);

                        println!("{BEGIN};");
                        for stmt in &migrations {
                            println!("{stmt}");
                        }
                        println!("{COMMIT};");
                    }
                    Err(err) => {
                        err.print(&new.to_string_lossy(), &new_source, &mut std::io::stderr()).unwrap();
                        std::process::exit(1);
                    }
                }
            } else {
                println!("{old_ddl}");
            }
        }
        Err(err) => {
            err.print(&old.to_string_lossy(), &old_source, &mut std::io::stderr()).unwrap();
            std::process::exit(1);
        }
    }

    //let new = args.next().unwrap();
}
