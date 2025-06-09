use std::rc::Rc;

use wanderlust::{dialects::postgresql::PostgreSQLParser, model::{database::Database, syntax::Parser}};

pub struct TestData {
    pub before: Rc<Database>,
    pub after: Rc<Database>,
}

pub const BEFORE_MARKER: &str = "-- before\n";
pub const AFTER_MARKER: &str = "-- after\n";

pub fn load_file(filename: &str, source: &str) -> TestData {
    let before_index = source.find(BEFORE_MARKER).expect(&format!("{BEFORE_MARKER:?} marker not found"));
    let after_index = source[before_index..].find(AFTER_MARKER).expect(&format!("{AFTER_MARKER:?} marker not found")) + before_index;
    let before_source = &source[before_index..after_index];
    let after_source = &source[after_index..];

    let before = match PostgreSQLParser::new(before_source).parse() {
        Ok(db) => db,
        Err(mut err) => {
            let mut msg = String::new();
            if let Some(cursor) = err.cursor_mut() {
                cursor.advance_by(before_index);
            }
            err.write(filename, source, &mut msg).unwrap();
            panic!("{msg}");
        }
    };

    let after = match PostgreSQLParser::new(after_source).parse() {
        Ok(db) => db,
        Err(mut err) => {
            let mut msg = String::new();
            if let Some(cursor) = err.cursor_mut() {
                cursor.advance_by(after_index);
            }
            err.write(filename, source, &mut msg).unwrap();
            panic!("{msg}");
        }
    };

    TestData {
        before,
        after,
    }
}
