use std::{env::VarError, fmt::Write, str::FromStr};

use postgres::{Config, NoTls};
use pretty_assertions::{assert_str_eq};
use wanderlust::{dialects::postgresql::{reflect::load_from_database, PostgreSQLParser}, migration::generate_migration, model::{database::Database, name::Name, syntax::Parser}};

#[allow(unused)]
pub struct TestData<'a> {
    pub before: Database,
    pub after: Database,
    pub before_source: &'a str,
    pub after_source: &'a str,
}

pub const BEFORE_MARKER: &str = "-- before\n";
pub const AFTER_MARKER: &str = "-- after\n";

pub fn load_file<'a>(filename: &str, source: &'a str) -> TestData<'a> {
    let before_index = source.find(BEFORE_MARKER).expect(&format!("{BEFORE_MARKER:?} marker not found"));
    let after_index = source[before_index..].find(AFTER_MARKER).expect(&format!("{AFTER_MARKER:?} marker not found")) + before_index;
    let before_source = &source[before_index..after_index];
    let after_source = &source[after_index..];

    let before = match PostgreSQLParser::parse(before_source) {
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

    let after = match PostgreSQLParser::parse(after_source) {
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
        before_source,
        after_source,
    }
}

pub fn run_test(suite_name: &str, test_name: &str, filename: &str, source: &str) {
    let mut config = get_db_config();
    let test = load_file(filename, source);
    let stmts = generate_migration(&test.before, &test.after);
    let mut sql = format!("-- MIGRATE: {suite_name} {test_name}\n");
    for stmt in &stmts {
        writeln!(sql, "{stmt}").unwrap();
    }

    let dbname = Name::new(format!("test_{suite_name}_{test_name}"));

    let mut client = config.connect(NoTls).expect("connecting");
    client.batch_execute(&format!("DROP DATABASE IF EXISTS {dbname};")).expect("drop old database");
    client.batch_execute(&format!("CREATE DATABASE {dbname};")).expect("create new database");

    let mut client = config.dbname(dbname.name()).connect(NoTls).expect("connecting");
    if !test.before_source.trim().is_empty() {
        client.batch_execute(
            &format!("-- BEFORE: {suite_name} {test_name}\n {}", test.before_source)
        ).expect("create before");
    }

    let mut tx = client.build_transaction().start().expect("create migration database");
    tx.batch_execute(&sql).expect("execute migration");
    tx.commit().expect("commit migration");

    let actual = load_from_database(&mut client).expect("load new state from database");

    let stmts = generate_migration(&test.after, &actual);

    if !stmts.is_empty() {
        let actual_sql = actual.to_string();
        let expected_sql = test.after.to_string();

        assert_str_eq!(actual_sql, expected_sql);
    }
}

pub fn get_db_config() -> Config {
    match std::env::var("TEST_DATABASE_URL") {
        Ok(url) => match Config::from_str(&url) {
            Ok(config) => config,
            Err(err) => panic!("error parsing env var TEST_DATABASE_URL: {err}"),
        },
        Err(VarError::NotPresent) => {
            let mut config = Config::new();

            config
                .user("postgres")
                .password("wanderlust")
                .dbname("postgres")
                .host("127.0.0.1")
                .port(5432);

            config
        },
        Err(err) => panic!("error parsing env var TEST_DATABASE_URL: {err}")
    }
}
