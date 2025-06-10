use std::rc::Rc;

use postgres::{Client, NoTls};

use crate::{dialects::postgresql::PostgreSQLParser, error::Result, model::{database::Database, name::{Name, QName}, schema::Schema, syntax::Parser, table::Table}, ordered_hash_map::OrderedHashMap};

// mabe these functions help? https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-INFO-CATALOG

// TODO: implement loading from database
pub fn load_from_database(url: &str) -> Result<Database> {
    let mut client = Client::connect(url, NoTls)?;
    let mut database = PostgreSQLParser::new_database();

    let reflect_schemas = client.query("
        SELECT oid, nspname
        FROM pg_catalog.pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ", &[])?;

    for reflect_schema in &reflect_schemas {
        let schema_oid: i64 = reflect_schema.get("oid");
        let nspname: &str = reflect_schema.get("nspname");
        let schema_name = Name::new(nspname);

        let schema = database.schemas_mut()
            .entry(schema_name.clone())
            .or_insert_with(|| Schema::new(schema_name.clone()));

        let reflect_tables = client.query("
            SELECT oid, relname
            FROM pg_catalog.pg_class
            WHERE relnamespace = $1
        ", &[&schema_oid])?;

        for reflect_table in &reflect_tables {
            let relname: &str = reflect_table.get("relname");
            let table_name = Name::new(relname.to_string());
            let qual_table_name = QName::new(Some(schema_name.clone()), table_name.clone());

            // TODO...

            let mut logged = true;
            let mut columns = OrderedHashMap::new();
            let mut constraints = OrderedHashMap::new();
            let mut triggers = OrderedHashMap::new();
            let mut inherits = Vec::new();

            let table = Table::new(
                qual_table_name,
                logged,
                columns,
                constraints,
                triggers,
                inherits
            );

            schema.tables_mut().insert(table_name.clone(), Rc::new(table));
        }
    }

    Ok(database)
}
