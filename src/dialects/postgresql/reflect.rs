use std::{num::NonZeroU32, rc::Rc};

use postgres::{Client, NoTls};

use crate::{dialects::postgresql::PostgreSQLParser, error::{Error, ErrorKind, Result}, model::{column::{Column, ColumnConstraint, ColumnConstraintData, Storage}, database::Database, name::{Name, QName}, schema::Schema, syntax::Parser, table::Table, types::{BasicType, DataType, Value}}, ordered_hash_map::OrderedHashMap};

// mabe these functions help? https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-INFO-CATALOG
// https://www.postgresql.org/docs/17/catalogs.html

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
            SELECT oid, relname, (relpersistence = 'p') AS logged
            FROM pg_catalog.pg_class
            WHERE relnamespace = $1 AND relkind = 'r' AND relpersistence != 't'
        ", &[&schema_oid])?;

        for reflect_table in &reflect_tables {
            let table_oid: i64 = reflect_table.get("oid");
            let relname: &str = reflect_table.get("relname");
            let table_name = Name::new(relname.to_string());
            let qual_table_name = QName::new(Some(schema_name.clone()), table_name.clone());

            // TODO...

            let logged: bool = reflect_table.get("logged");
            let mut columns = OrderedHashMap::new();
            let mut constraints = OrderedHashMap::new(); // TODO
            let mut triggers = OrderedHashMap::new(); // TODO
            let mut inherits = Vec::new(); // TODO

            let reflect_columns = client.query("
                SELECT
                    a.oid,
                    attname,
                    attnum,
                    attnotnull,
                    attstorage,
                    attndims,
                    collname,
                    cn.nspname AS collschema,
                    attcompression,
                    typname,
                    atttypmod,
                    tyn.nspname AS typschema,
                    pg_get_expr(adbin, adrelid) as default_value
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
                LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = c.collnamespace
                LEFT JOIN pg_catalog.pg_type ty ON ty.oid = c.atttypid
                LEFT JOIN pg_catalog.pg_namespace tyn ON tyn.oid = ty.typnamespace
                WHERE a.attrelid = $1 AND a.atttypid IS NOT NULL
                ORDER BY a.attnum
            ", &[&table_oid])?;

            for reflect_column in reflect_columns {
                let column_oid: i64 = reflect_column.get("oid");
                let column_name: &str = reflect_column.get("attname");
                let column_name = Name::new(column_name);
                let is_not_null: bool = reflect_column.get("attnotnull");
                let default_value: &str = reflect_column.get("default_value"); // TODO

                let mut column_constraints = Vec::new();

                if is_not_null {
                    column_constraints.push(ColumnConstraint::new(
                        None,
                        ColumnConstraintData::NotNull,
                        None,
                        None
                    ).into());
                }

                let collation_name: Option<&str> = reflect_column.get("collname");
                let collation_schema: Option<&str> = reflect_column.get("collschema");
                let collation = if let Some(collation_name) = collation_name {
                    Some(QName::new(collation_schema.map(Into::into), collation_name.into()))
                } else {
                    None
                };

                let storage: &str = reflect_column.get("attstorage");
                let storage = match storage {
                    "p" => Storage::Plain,
                    "e" => Storage::External,
                    "m" => Storage::Main,
                    "x" => Storage::Extended,
                    "" => Storage::Default,
                    _ => {
                        return Err(Error::new(
                            ErrorKind::NotSupported,
                            None,
                            Some(format!("{schema_name}.{table_name}.{column_name}: unsupported storage value: {storage:?}")),
                            None
                        ));
                    }
                };

                let compression = if matches!(storage, Storage::Main | Storage::Extended) {
                    let compression_value: &str = reflect_column.get("attcompression");
                    match compression_value {
                        "" | "\x00" => None,
                        "p" => Some(Name::new("pglz")),
                        "l" => Some(Name::new("LZ4")),
                        _ => {
                            return Err(Error::new(
                                ErrorKind::NotSupported,
                                None,
                                Some(format!("{schema_name}.{table_name}.{column_name}: unsupported compression value: {compression_value:?}")),
                                None
                            ));
                        }
                    }
                } else {
                    None
                };

                let atttypmod: i32 = reflect_column.get("atttypmod"); // e.g. varchar parameter
                let attndims: i16 = reflect_column.get("attndims");

                let array_dimensions = if attndims > 0 {
                    Some(vec![None; attndims as usize].into())
                } else {
                    None
                };

                let type_name: &str = reflect_column.get("typname");
                let type_schema: &str = reflect_column.get("typschema");

                let basic_type = if type_schema.eq_ignore_ascii_case("pg_catalog") {
                    match type_name {
                        "internal" => BasicType::Internal,
                        "int8" => BasicType::Bigint,
                        "bit" => BasicType::Bit(if atttypmod > 0 {
                            NonZeroU32::new(atttypmod as u32)
                        } else {
                            None
                        }),
                        "varbit" => BasicType::BitVarying(if atttypmod > 0 {
                            NonZeroU32::new(atttypmod as u32)
                        } else {
                            None
                        }),
                        "bool" => BasicType::Boolean,
                        "box" => BasicType::Box,
                        "bytea" => BasicType::ByteA,
                        "char" => BasicType::Character(if atttypmod > 0 {
                            NonZeroU32::new(atttypmod as u32)
                        } else {
                            None
                        }),
                        "varchar" => BasicType::CharacterVarying(if atttypmod > 0 {
                            NonZeroU32::new(atttypmod as u32)
                        } else {
                            None
                        }),
                        "cidr" => BasicType::CIDR,
                        "circle" => BasicType::Circle,
                        "date" => BasicType::Date,
                        "float8" => BasicType::DoublePrecision,
                        "inet" => BasicType::INet,
                        "int4" => BasicType::Integer,
                        // TODO: see intervaltypmodin() in https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c
                        "interval" => unimplemented!(), // TODO
                        "json" => BasicType::JSON,
                        "jsonb" => BasicType::JSONB,
                        "line" => BasicType::Line,
                        "lseg" => BasicType::LSeg,
                        "macaddr" => BasicType::MacAddr,
                        "macaddr8" => BasicType::MacAddr8,
                        "money" => BasicType::Money,
                        "numeric" => {
                            let atttypmod = (atttypmod - 4) as u32;
                            if let Some(p) = NonZeroU32::new(atttypmod >> 16) {
                                BasicType::Numeric(Some((p, (atttypmod & 0xFFFF) as i32)))
                            } else {
                                BasicType::Numeric(None)
                            }
                        },
                        "path" => BasicType::Path,
                        "pg_lsn" => BasicType::PgLSN,
                        "pg_snapshot" => BasicType::PgSnapshot,
                        "point" => BasicType::Point,
                        "polygon" => BasicType::Polygon,
                        "float4" => BasicType::Real,
                        "int2" => BasicType::SmallInt,
                        "text" => BasicType::Text,
                        "time" => BasicType::Time {
                            precision: if atttypmod > 0 {
                                NonZeroU32::new(atttypmod as u32)
                            } else {
                                None
                            },
                            with_time_zone: false
                        },
                        "timetz" => BasicType::Time {
                            precision: if atttypmod > 0 {
                                NonZeroU32::new(atttypmod as u32)
                            } else {
                                None
                            },
                            with_time_zone: true
                        },
                        "timestamp" => BasicType::Timestamp {
                            precision: if atttypmod > 0 {
                                NonZeroU32::new(atttypmod as u32)
                            } else {
                                None
                            },
                            with_time_zone: false
                        },
                        "timestamptz" => BasicType::Timestamp {
                            precision: if atttypmod > 0 {
                                NonZeroU32::new(atttypmod as u32)
                            } else {
                                None
                            },
                            with_time_zone: true
                        },
                        "tsquery" => BasicType::TsQuery,
                        "tsvector" => BasicType::TsVector,
                        "txid_snapshot" => BasicType::TxIdSnapshot,
                        "uuid" => BasicType::UUID,
                        "xml" => BasicType::XML,

                        _ => BasicType::UserDefined {
                            name: QName::new(Some(type_schema.into()), type_name.into()),
                            parameters: if atttypmod != -1 {
                                Some(vec![Value::Integer(atttypmod.into())].into())
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    // TODO: how to detect column type? Is that actually copied on creation?
                    BasicType::UserDefined {
                        name: QName::new(Some(type_schema.into()), type_name.into()),
                        parameters: if atttypmod != -1 {
                            Some(vec![Value::Integer(atttypmod.into())].into())
                        } else {
                            None
                        }
                    }
                };

                let data_type = DataType::new(
                    basic_type,
                    array_dimensions
                );

                let column = Column::new(
                    column_name.clone(),
                    data_type,
                    storage,
                    compression,
                    collation,
                    column_constraints
                );

                columns.insert(column_name, column.into());
            }

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
