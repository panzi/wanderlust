use std::{num::NonZeroU32, rc::Rc};

use postgres::Client;

use crate::{dialects::postgresql::{PostgreSQLParser, PostgreSQLTokenizer}, error::{Error, ErrorKind, Result}, format::IsoString, model::{column::{Column, ColumnConstraint, ColumnConstraintData, Storage}, database::Database, extension::Extension, name::{Name, QName}, schema::Schema, syntax::Parser, table::Table, types::{BasicType, DataType, IntervalFields, Value}}, ordered_hash_map::OrderedHashMap};

// mabe these functions help? https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-INFO-CATALOG
// https://www.postgresql.org/docs/17/catalogs.html

#[allow(unused)]
mod consts {
    pub const MONTH:       u16 =  1;
    pub const YEAR:        u16 =  2;
    pub const DAY:         u16 =  3;
    pub const JULIAN:      u16 =  4;
    pub const TZ:          u16 =  5;
    pub const DTZ:         u16 =  6;
    pub const DYNTZ:       u16 =  7;
    pub const IGNORE_DTF:  u16 =  8;
    pub const AMPM:        u16 =  9;
    pub const HOUR:        u16 = 10;
    pub const MINUTE:      u16 = 11;
    pub const SECOND:      u16 = 12;
    pub const MILLISECOND: u16 = 13;
    pub const MICROSECOND: u16 = 14;
    pub const INTERVAL_FULL_RANGE: u16 = 0x7FFF;
}

macro_rules! interval_mask {
    ($val:expr) => {
        1 << $val
    };
}

// TODO: implement loading from database
pub fn load_from_database(client: &mut Client) -> Result<Database> {
    let mut database = PostgreSQLParser::new_database();

    // TODO: types
    // TODO: functions
    // TODO: table contraints
    // TODO: triggers
    // TODO: indices
    // TODO: comments

    let reflect_schemas = client.query("
        SELECT oid, nspname
        FROM pg_catalog.pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ", &[])?;

    for reflect_schema in &reflect_schemas {
        let schema_oid: u32 = reflect_schema.get("oid");
        let nspname: &str = reflect_schema.get("nspname");
        let schema_name = Name::new(nspname);

        let schema = database.schemas_mut()
            .entry(schema_name.clone())
            .or_insert_with(|| Schema::new(schema_name.clone()));

        let reflect_extensions = client.query("
            SELECT
                extname,
                extversion,
                pg_catalog.obj_description(oid, 'pg_extension'::name) AS comment
            FROM pg_catalog.pg_extension
            WHERE extnamespace = $1
            ORDER BY extname
        ", &[&schema_oid])?;

        for reflect_extension in &reflect_extensions {
            let extname: &str = reflect_extension.get("extname");
            let extversion: Option<&str> = reflect_extension.get("extversion");
            let comment: Option<&str> = reflect_extension.get("comment");
            let extname: Name = extname.into();
            let mut extension = Extension::new(
                QName::new(Some(schema_name.clone()), extname.clone()),
                extversion
            );

            extension.set_comment(comment.map(Into::into));

            schema.extensions_mut().insert(
                extname,
                Rc::new(extension)
            );
        }

        let reflect_tables = client.query("
            SELECT
                oid,
                relname,
                (relpersistence = 'p') AS logged,
                pg_catalog.obj_description(oid, 'pg_class'::name) AS comment
            FROM pg_catalog.pg_class c
            WHERE relnamespace = $1 AND relkind = 'r' AND relpersistence != 't'
        ", &[&schema_oid])?;

        for reflect_table in &reflect_tables {
            let table_oid: u32 = reflect_table.get("oid");
            let relname: &str = reflect_table.get("relname");
            let table_name = Name::new(relname.to_string());
            let comment: Option<&str> = reflect_table.get("comment");
            let qual_table_name = QName::new(Some(schema_name.clone()), table_name.clone());

            // TODO...

            let logged: bool = reflect_table.get("logged");
            let mut columns = OrderedHashMap::new();
            let mut constraints = OrderedHashMap::new(); // TODO
            let mut triggers = OrderedHashMap::new(); // TODO
            let mut inherits = Vec::new();

            let reflect_inherits = client.query("
                SELECT nspname, relname
                FROM pg_catalog.pg_inherits i
                LEFT JOIN pg_catalog.pg_class c ON c.oid = i.inhparent
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE i.inhrelid = $1
                ORDER BY i.inhseqno
            ", &[&table_oid])?;

            for reflect_inherit in &reflect_inherits {
                let nspname: Option<&str> = reflect_inherit.get("nspname");
                let relname: &str = reflect_inherit.get("relname");

                inherits.push(QName::new(
                    nspname.map(Into::into), relname.into()
                ));
            }

            let reflect_columns = client.query("
                SELECT
                    attname,
                    attnum,
                    attnotnull,
                    (case when attstorage = typstorage then '' else attstorage end) AS storage,
                    attndims,
                    collname,
                    cn.nspname AS collschema,
                    attcompression,
                    typname,
                    atttypmod,
                    tyn.nspname AS typschema,
                    pg_get_expr(adbin, adrelid) AS default_value,
                    col_description($1, attnum) AS comment
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
                LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = c.collnamespace
                LEFT JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
                LEFT JOIN pg_catalog.pg_namespace tyn ON tyn.oid = ty.typnamespace
                WHERE a.attrelid = $1 AND a.atttypid IS NOT NULL AND a.attnum > 0
                ORDER BY a.attnum
            ", &[&table_oid])?;

            for reflect_column in reflect_columns {
                let column_name: &str = reflect_column.get("attname");
                let column_name = Name::new(column_name);
                let is_not_null: bool = reflect_column.get("attnotnull");
                let comment: Option<&str> = reflect_column.get("comment");
                let default_value: Option<&str> = reflect_column.get("default_value"); // TODO

                let mut column_constraints = Vec::new();

                if is_not_null {
                    column_constraints.push(ColumnConstraint::new(
                        None,
                        ColumnConstraintData::NotNull,
                        None,
                        None
                    ).into());
                }

                if let Some(default_value) = default_value {
                    if !default_value.is_empty() {
                        let tokens = PostgreSQLTokenizer::parse_all(default_value)?;
                        column_constraints.push(ColumnConstraint::new(
                            None,
                            ColumnConstraintData::Default { value: tokens.into() },
                            None,
                            None
                        ).into());
                    }
                }

                let collation_schema: Option<&str> = reflect_column.get("collschema");
                let collation_name: Option<&str> = reflect_column.get("collname");
                let collation = match (collation_schema, collation_name) {
                    (Some(collation_schema), Some(collation_name)) => {
                        if collation_schema.eq_ignore_ascii_case("pg_catalog") && collation_name.eq_ignore_ascii_case("default") {
                            None
                        } else {
                            Some(QName::new(Some(collation_schema.into()), collation_name.into()))
                        }
                    }
                    (None, Some(collation_name)) => {
                        if collation_name.eq_ignore_ascii_case("default") {
                            None
                        } else {
                            Some(QName::new(None, collation_name.into()))
                        }
                    }
                    (_, None) => {
                        None
                    }
                };

                let storage: i8 = reflect_column.get("storage");
                let storage = match storage {
                    0x70 => Storage::Plain,    // 'p'
                    0x64 => Storage::External, // 'e'
                    0x6D => Storage::Main,     // 'm'
                    0x78 => Storage::Extended, // 'x'
                    0 => Storage::Default,
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
                    let compression_value: i8 = reflect_column.get("attcompression");
                    match compression_value {
                        0 => None, // documentation says '\0', reality is ''
                        0x70 => Some(Name::new("pglz")), // 'p'
                        0x6C => Some(Name::new("LZ4")), // 'l'
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

                let type_name: &str = reflect_column.get("typname");
                let type_schema: &str = reflect_column.get("typschema");

                let data_type = load_data_type(type_schema, type_name, atttypmod, attndims)?;

                let mut column = Column::new(
                    column_name.clone(),
                    data_type,
                    storage,
                    compression,
                    collation,
                    column_constraints
                );
                column.set_comment(comment.map(Into::into));

                columns.insert(column_name, column.into());
            }

            let mut table = Table::new(
                qual_table_name,
                logged,
                columns,
                constraints,
                triggers,
                inherits
            );
            table.set_comment(comment.map(Into::into));

            schema.tables_mut().insert(table_name.clone(), Rc::new(table));
        }
    }

    Ok(database)
}

pub fn load_basic_type(type_schema: &str, type_name: &str, atttypmod: i32) -> Result<BasicType> {
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
            "boolean" => BasicType::Boolean,
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
            "interval" => {
                // See intervaltypmodout() in https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c
                let fields = (atttypmod as u32 >> 16) as u16 & 0x7FFF;
                let precision = (atttypmod as u32 & 0xFFFF) as u16;

                use consts::*;

                let fields = if fields == interval_mask!(YEAR) {
                    Some(IntervalFields::Year)
                } else if fields == interval_mask!(MONTH) {
                    Some(IntervalFields::Month)
                } else if fields == interval_mask!(DAY) {
                    Some(IntervalFields::Day)
                } else if fields == interval_mask!(HOUR) {
                    Some(IntervalFields::Hour)
                } else if fields == interval_mask!(MINUTE) {
                    Some(IntervalFields::Minute)
                } else if fields == interval_mask!(SECOND) {
                    Some(IntervalFields::Second)
                } else if fields == interval_mask!(YEAR) | interval_mask!(MONTH) {
                    Some(IntervalFields::YearToMonth)
                } else if fields == interval_mask!(DAY) | interval_mask!(HOUR) {
                    Some(IntervalFields::DayToHour)
                } else if fields == interval_mask!(DAY) | interval_mask!(HOUR) | interval_mask!(MINUTE) {
                    Some(IntervalFields::DayToMinute)
                } else if fields == interval_mask!(DAY) | interval_mask!(HOUR) | interval_mask!(MINUTE) | interval_mask!(SECOND) {
                    Some(IntervalFields::DayToSecond)
                } else if fields == interval_mask!(HOUR) | interval_mask!(MINUTE) {
                    Some(IntervalFields::HourToMinute)
                } else if fields == interval_mask!(HOUR) | interval_mask!(MINUTE) | interval_mask!(SECOND) {
                    Some(IntervalFields::HourToSecond)
                } else if fields == interval_mask!(MINUTE) | interval_mask!(SECOND) {
                    Some(IntervalFields::MinuteToSecond)
                } else if fields == INTERVAL_FULL_RANGE {
                    None
                } else {
                    return Err(Error::new(
                        ErrorKind::NotSupported,
                        None,
                        Some(format!("unsupported atttypmod value for interval type: {atttypmod}")),
                        None
                    ));
                };

                let precision = if precision != 0xFFFF {
                    NonZeroU32::new(precision.into())
                } else {
                    None
                };

                BasicType::Interval { fields, precision }
            },
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

    Ok(basic_type)
}

pub fn load_data_type(type_schema: &str, type_name: &str, atttypmod: i32, attndims: i16) -> Result<DataType> {
    let array_dimensions = if attndims > 0 {
        Some(vec![None; attndims as usize].into())
    } else {
        None
    };

    let basic_type = load_basic_type(type_schema, type_name, atttypmod)?;

    let data_type = DataType::new(
        basic_type,
        array_dimensions
    );

    Ok(data_type)
}
