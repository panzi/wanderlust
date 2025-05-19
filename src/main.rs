use dialects::postgresql::PostgreSQLParser;
use model::syntax::Parser;

pub mod model;
pub mod dialects;
pub mod error;
pub mod format;
pub mod migration;

fn main() {
    let mut args = std::env::args_os();
    args.next().unwrap();

    let old = args.next().unwrap();
    let old_source = std::fs::read_to_string(&old).unwrap();
    let mut parser = PostgreSQLParser::new(&old_source);
    match parser.parse() {
        Ok(ddl) => println!("{ddl:#?}\n\n{ddl}"),
        Err(err) => {
            err.print(&old.to_string_lossy(), &old_source, &mut std::io::stderr()).unwrap();
            std::process::exit(1);
        }
    }

    //let new = args.next().unwrap();
}
