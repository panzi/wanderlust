use std::{num::NonZeroU32, ops::Not, rc::Rc};

use postgres::Client;

use crate::{
    dialects::postgresql::{PostgreSQLParser, PostgreSQLTokenizer},
    error::{Error, ErrorKind, Result},
    model::{
        column::{Column, ColumnConstraint, ColumnConstraintData, ColumnMatch, ReferentialAction, Storage}, database::Database, extension::Extension, index::IndexParameters, name::{Name, QName}, schema::Schema, syntax::Parser, table::{Table, TableConstraint, TableConstraintData}, types::{
            BasicType, CompositeAttribute, DataType, IntervalFields, TypeData, TypeDef, Value,
        }
    },
    ordered_hash_map::OrderedHashMap,
};

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

    // TODO: functions
    // TODO: table contraints
    // TODO: triggers
    // TODO: indices
    // TODO: comments of the above

    let reflect_schemas = client.query("
        SELECT oid, nspname
        FROM pg_catalog.pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ", &[])?;

    for reflect_schema in &reflect_schemas {
        let schema_oid: u32 = reflect_schema.get("oid");
        let nspname: &str = reflect_schema.get("nspname");
        let schema_name = Name::from_normed(nspname);

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
            let extname = Name::from_normed(extname);
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

        let reflect_types = client.query("
            SELECT
                t.oid,
                typname,
                typtype,
                typcategory,
                typrelid,
                pg_catalog.obj_description(t.oid, 'pg_type'::name) AS comment
            FROM pg_catalog.pg_type t
            LEFT JOIN pg_catalog.pg_class c ON c.oid = typrelid
            WHERE typnamespace = $1 AND typcategory != 'A' and (relkind IS NULL OR relkind = 'c');
        ", &[&schema_oid])?;

        for reflect_type in &reflect_types {
            let type_oid: u32 = reflect_type.get("oid");
            let typname: &str = reflect_type.get("typname");
            let typtype: i8 = reflect_type.get("typtype");
            let type_name = Name::from_normed(typname);
            let comment: Option<&str> = reflect_type.get("comment");

            let mut type_def = match typtype as u8 {
                b'e' => {
                    let reflect_enum_values = client.query("
                        SELECT
                            enumlabel
                        FROM pg_catalog.pg_enum
                        WHERE enumtypid = $1
                        ORDER BY enumsortorder
                    ", &[&type_oid])?;

                    let mut enum_values = Vec::new();
                    for reflect_enum_value in &reflect_enum_values {
                        let enumlabel: &str = reflect_enum_value.get("enumlabel");
                        enum_values.push(enumlabel.into());
                    }

                    TypeDef::new(
                        QName::new(Some(schema_name.clone()), type_name.clone()),
                        TypeData::Enum { values: enum_values.into() }
                    )
                }
                b'c' => {
                    let typrelid: u32 = reflect_type.get("typrelid");
                    let mut attributes = OrderedHashMap::new();

                    let reflect_attributes = client.query("
                        SELECT
                            attname,
                            attnum,
                            attndims,
                            collname,
                            cn.nspname AS collschema,
                            typname,
                            atttypmod,
                            tyn.nspname AS typschema
                        FROM pg_catalog.pg_attribute a
                        LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
                        LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = c.collnamespace
                        LEFT JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
                        LEFT JOIN pg_catalog.pg_namespace tyn ON tyn.oid = ty.typnamespace
                        WHERE a.attrelid = $1 AND a.atttypid IS NOT NULL AND a.attnum > 0 AND NOT attisdropped
                        ORDER BY a.attnum
                    ", &[&typrelid])?;

                    for reflect_attribute in reflect_attributes {
                        let attribute_name: &str = reflect_attribute.get("attname");
                        let attribute_name = Name::from_normed(attribute_name);

                        let collation_schema: Option<&str> = reflect_attribute.get("collschema");
                        let collation_name: Option<&str> = reflect_attribute.get("collname");
                        let collation = load_collation(collation_schema, collation_name);

                        let atttypmod: i32 = reflect_attribute.get("atttypmod"); // e.g. varchar parameter
                        let attndims: i16 = reflect_attribute.get("attndims");

                        let type_name: &str = reflect_attribute.get("typname");
                        let type_schema: &str = reflect_attribute.get("typschema");

                        let data_type = load_data_type(type_schema, type_name, atttypmod, attndims)?;

                        let attribute = CompositeAttribute::new(
                            attribute_name.clone(),
                            data_type,
                            collation
                        );

                        attributes.insert(attribute_name, attribute.into());
                    }

                    TypeDef::new(
                        QName::new(Some(schema_name.clone()), type_name.clone()),
                        TypeData::Composite { attributes }
                    )
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::NotSupported,
                        None,
                        Some(format!("{schema_name}.{type_name}: unsupported typtype: {typtype:?}")),
                        None
                    ));
                }
            };
            type_def.set_comment(comment.map(Into::into));

            schema.types_mut().insert(type_name, Rc::new(type_def));
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
            let table_name = Name::from_normed(relname.to_string());
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
                    nspname.map(Name::from_normed), Name::from_normed(relname)
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
                WHERE a.attrelid = $1 AND a.atttypid IS NOT NULL AND a.attnum > 0 AND NOT attisdropped
                ORDER BY a.attnum
            ", &[&table_oid])?;

            for reflect_column in reflect_columns {
                let column_name: &str = reflect_column.get("attname");
                let column_name = Name::from_normed(column_name);
                let is_not_null: bool = reflect_column.get("attnotnull");
                let comment: Option<&str> = reflect_column.get("comment");
                let default_value: Option<&str> = reflect_column.get("default_value");

                let mut column_constraints = Vec::new();

                if is_not_null {
                    column_constraints.push(ColumnConstraint::new(
                        None,
                        ColumnConstraintData::NotNull,
                        false,
                        false
                    ).into());
                }

                if let Some(default_value) = default_value {
                    if !default_value.is_empty() {
                        let tokens = PostgreSQLTokenizer::parse_all(default_value)?;
                        column_constraints.push(ColumnConstraint::new(
                            None,
                            ColumnConstraintData::Default { value: tokens.into() },
                            false,
                            false
                        ).into());
                    }
                }

                let collation_schema: Option<&str> = reflect_column.get("collschema");
                let collation_name: Option<&str> = reflect_column.get("collname");
                let collation = load_collation(collation_schema, collation_name);

                let storage: i8 = reflect_column.get("storage");
                let storage = match storage as u8 {
                    b'p' => Storage::Plain,
                    b'e' => Storage::External,
                    b'm' => Storage::Main,
                    b'x' => Storage::Extended,
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
                    match compression_value as u8 {
                        b'p' => Some(Name::new("pglz")),
                        b'l' => Some(Name::new("LZ4")),
                        0 => None,
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

            let reflect_constraints = client.query("
                SELECT
                    conname,
                    contype,
                    condeferrable,
                    condeferred,
                    confdeltype,
                    confmatchtype,
                    connoinherit,
                    confupdtype,
                    confdeltype,
                    fkrel.relname AS confrelname,
                    fkn.nspname AS confrelschema,
                    (
                        SELECT coalesce(array_agg(attname), ARRAY[]::pg_catalog.name[])
                        FROM unnest(conkey) AS k
                        LEFT JOIN pg_catalog.pg_attribute a ON a.attnum = k
                        WHERE attrelid = conrelid
                    ) AS keycols,
                    (
                        SELECT coalesce(array_agg(attname), ARRAY[]::pg_catalog.name[])
                        FROM unnest(confkey) AS k
                        LEFT JOIN pg_catalog.pg_attribute a ON a.attnum = k
                        WHERE attrelid = conrelid
                    ) AS fkeycols,
                    (
                        SELECT array_agg(attname)
                        FROM unnest(confdelsetcols) AS k
                        LEFT JOIN pg_catalog.pg_attribute a ON a.attnum = k
                        WHERE attrelid = conrelid
                    ) AS delsetcols,
                    indnullsnotdistinct,
                    pg_get_expr(conbin, conrelid) AS check_expr
                FROM pg_catalog.pg_constraint c
                LEFT JOIN pg_catalog.pg_class fkrel ON fkrel.oid = confrelid
                LEFT JOIN pg_catalog.pg_namespace fkn ON fkn.oid = fkrel.relnamespace
                LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = conindid
                WHERE conrelid = $1
            ", &[&table_oid])?;

            for reflect_constraint in &reflect_constraints {
                let conname: &str = reflect_constraint.get("conname");
                let contype: i8 = reflect_constraint.get("contype");
                let condeferrable: bool = reflect_constraint.get("condeferrable");
                let condeferred: bool = reflect_constraint.get("condeferred");
                let confupdtype: i8 = reflect_constraint.get("confupdtype");
                let confdeltype: i8 = reflect_constraint.get("confdeltype");
                let confmatchtype: i8 = reflect_constraint.get("confmatchtype");
                let connoinherit: bool = reflect_constraint.get("connoinherit");
                let confrelname: Option<&str> = reflect_constraint.get("confrelname");
                let confrelschema: Option<&str> = reflect_constraint.get("confrelschema");
                let check_expr: Option<&str> = reflect_constraint.get("check_expr");
                let keycols: Vec<&str> = reflect_constraint.get("keycols");
                let fkeycols: Vec<&str> = reflect_constraint.get("fkeycols");
                let delsetcols: Option<Vec<&str>> = reflect_constraint.get("delsetcols");
                let delsetcols = if let Some(delsetcols) = delsetcols {
                    if delsetcols.is_empty() {
                        None
                    } else {
                        Some(delsetcols)
                    }
                } else {
                    None
                };

                let constraint_name = Name::new(conname);
                let indnullsnotdistinct: Option<bool> = reflect_constraint.get("indnullsnotdistinct");

                let constraint_data = match contype as u8 {
                    b'c' => TableConstraintData::Check {
                        expr: if let Some(check_expr) = check_expr {
                            PostgreSQLTokenizer::parse_all(check_expr)?.into()
                        } else {
                            [].into()
                        },
                        inherit: !connoinherit
                    },
                    b'f' => TableConstraintData::ForeignKey {
                        columns: keycols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into(),
                        ref_table: if let Some(confrelname) = confrelname {
                            QName::new(
                                confrelschema.map(Name::from_normed),
                                Name::from_normed(confrelname)
                            )
                        } else {
                            return Err(Error::new(
                                ErrorKind::NotSupported,
                                None,
                                Some(format!("{schema_name}.{table_name}: In constraint {constraint_name}: missing foreing table")),
                                None
                            ));
                        },
                        ref_columns: Some(fkeycols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into()),
                        column_match: Some(match confmatchtype as u8 {
                            b'f' => ColumnMatch::Full,
                            b'p' => ColumnMatch::Partial,
                            b's' => ColumnMatch::Simple,
                            confmatchtype => {
                                return Err(Error::new(
                                    ErrorKind::NotSupported,
                                    None,
                                    Some(format!("{schema_name}.{table_name}: In constraint {constraint_name}: unsupported match type: {:?} (0x{confmatchtype:x})", confmatchtype as char)),
                                    None
                                ));
                            }
                        }),
                        on_delete: Some(
                            match confdeltype as u8 {
                                b'a' => ReferentialAction::NoAction,
                                b'r' => ReferentialAction::Restrict,
                                b'c' => ReferentialAction::Cascade,
                                b'n' => ReferentialAction::SetNull {
                                    columns: delsetcols.map(|delsetcols| delsetcols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into())
                                },
                                b'd' => ReferentialAction::SetDefault {
                                    columns: delsetcols.map(|delsetcols| delsetcols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into())
                                },
                                confdeltype => {
                                    return Err(Error::new(
                                        ErrorKind::NotSupported,
                                        None,
                                        Some(format!("{schema_name}.{table_name}: In constraint {constraint_name}: unsupported on delete action: {:?} (0x{confdeltype:x})", confdeltype as char)),
                                        None
                                    ));
                                }
                            }
                        ),
                        on_update: Some(
                            match confupdtype as u8 {
                                b'a' => ReferentialAction::NoAction,
                                b'r' => ReferentialAction::Restrict,
                                b'c' => ReferentialAction::Cascade,
                                b'n' => ReferentialAction::SetNull {
                                    columns: None // doesn't actually exist
                                },
                                b'd' => ReferentialAction::SetDefault {
                                    columns: None // doesn't actually exist
                                },
                                confupdtype => {
                                    return Err(Error::new(
                                        ErrorKind::NotSupported,
                                        None,
                                        Some(format!("{schema_name}.{table_name}: In constraint {constraint_name}: unsupported on update action: {:?} (0x{confupdtype:x})", confupdtype as char)),
                                        None
                                    ));
                                }
                            }
                        )
                    },
                    b'p' => TableConstraintData::PrimaryKey {
                        columns: keycols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into(),
                        index_parameters: IndexParameters::new(None, None, None), // TODO: IndexParameters
                    },
                    b'u' => TableConstraintData::Unique {
                        nulls_distinct: indnullsnotdistinct.map(Not::not),
                        columns: keycols.into_iter().map(Name::from_normed).collect::<Vec<_>>().into(),
                        index_parameters: IndexParameters::new(None, None, None), // TODO: IndexParameters
                    },
                    // b'n' => // TODO: not null? only for "domains"?
                    // b't' => // TODO: constraint trigger. what is this?
                    // b'x' => // TODO: exclusion constraint
                    contype => {
                        return Err(Error::new(
                            ErrorKind::NotSupported,
                            None,
                            Some(format!("{schema_name}.{table_name}: In constraint {constraint_name}: unsupported constraint type: {:?} (0x{contype:x})", contype as char)),
                            None
                        ));
                    }
                };

                let constraint = TableConstraint::new(
                    Some(constraint_name.clone()),
                    constraint_data,
                    condeferrable,
                    condeferred
                );

                constraints.insert(constraint_name, Rc::new(constraint));
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

            table.convert_serial();

            schema.tables_mut().insert(table_name.clone(), Rc::new(table));
        }

        let reflect_functions = client.query("
            WITH
                argtype AS (
                    SELECT
                        t.oid,
                        (array[nspname, typname]::pg_catalog.name[]) AS typqualname
                    FROM pg_catalog.pg_type t
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = typnamespace
                ),
                argdefault AS (
                    SELECT
                        f.oid AS procid,
                        argnum,
                        pg_get_function_arg_default(f.oid, argnum::integer) AS default_value
                    FROM pg_catalog.pg_proc f,
                    unnest(f.proallargtypes) WITH ORDINALITY xx(argtype, argnum)
                )
            SELECT
                p.oid,
                proname,
                l.lanname AS lang,
                procost,
                prorows,
                prokind,
                prosupport,
                prosecdef,
                proleakproof,
                proisstrict,
                proparallel,
                provolatile,
                proretset,
                pronargdefaults,
                proargnames,
                proargmodes,
                vt.typname AS varname,
                vn.nspname AS varschema,
                (
                    SELECT coalesce(array_agg(typqualname), array[]::pg_catalog.name[][2])
                    FROM unnest(proargtypes) x
                    left join argtype on argtype.oid = x
                ) AS args,
                (
                    SELECT array_agg(typqualname)
                    FROM unnest(proallargtypes) x
                    left join argtype on argtype.oid = x
                ) AS allargs,
                (
                    SELECT coalesce(array_agg(d.default_value), array[]::text[])
                    FROM argdefault d
                    WHERE d.procid = p.oid
                ) AS default_values,
                rt.typname AS retname,
                rn.nspname AS retschema,
                prosrc,
                probin,
                pg_get_expr(prosqlbody, 0) AS sqlbody
            FROM pg_catalog.pg_proc p

            LEFT JOIN pg_catalog.pg_language l ON l.oid = prolang

            LEFT JOIN pg_catalog.pg_type rt ON rt.oid = prorettype
            LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rt.typnamespace

            LEFT JOIN pg_catalog.pg_type vt ON vt.oid = provariadic
            LEFT JOIN pg_catalog.pg_namespace vn ON vn.oid = vt.typnamespace

            WHERE pronamespace = $1
              AND NOT EXISTS(
                SELECT *
                FROM pg_catalog.pg_depend d
                WHERE d.refclassid = 'pg_extension'::regclass
                  AND d.classid = 'pg_proc'::regclass
                  AND d.objid = p.oid
                  AND d.deptype IN ('i', 'P', 'S', 'e')
              )
        ", &[&schema_oid])?;

        for reflect_function in &reflect_functions {
            let proname: &str = reflect_function.get("proname");
            let lang: &str = reflect_function.get("lang");
            let procost: f32 = reflect_function.get("procost");
            let prorows: f32 = reflect_function.get("prorows");

            // if true then out paramerters are actually record attributes
            let proretset: bool = reflect_function.get("proretset");

            // TODO
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
                name: QName::new(Some(Name::from_normed(type_schema)), Name::from_normed(type_name)),
                parameters: if atttypmod != -1 {
                    Some(vec![Value::Integer(atttypmod.into())].into())
                } else {
                    None
                }
            }
        }
    } else {
        BasicType::UserDefined {
            name: QName::new(Some(Name::from_normed(type_schema)), Name::from_normed(type_name)),
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

pub fn load_collation(schema: Option<&str>, name: Option<&str>) -> Option<QName> {
    match (schema, name) {
        (Some(schema), Some(name)) => {
            if schema == "pg_catalog" && name == "default" {
                None
            } else {
                Some(QName::new(Some(Name::from_normed(schema)), Name::from_normed(name)))
            }
        }
        (None, Some(name)) => {
            if name == "default" {
                None
            } else {
                Some(QName::new(None, Name::from_normed(name)))
            }
        }
        (_, None) => {
            None
        }
    }
}
