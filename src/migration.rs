use std::{collections::HashMap, rc::Rc};

use crate::model::{alter::{AlterColumn, AlterTable}, column::{Column, ColumnConstraintData, ColumnMatch, ReferentialAction}, ddl::DDL, name::Name, statement::Statement, table::{Table, TableConstraint, TableConstraintData}, token::ParsedToken};

pub fn generate_migration(old: &DDL, new: &DDL) -> Vec<Statement> {
    let mut stmts = Vec::new();

    let mut old_tables = HashMap::new();
    let mut new_tables = HashMap::new();

    let mut old_types = HashMap::new();
    let mut new_types = HashMap::new();

    let mut old_indices = HashMap::new();
    let mut new_indices = HashMap::new();

    for type_def in old.types() {
        old_types.insert(type_def.name(), type_def);
    }

    for type_def in new.types() {
        new_types.insert(type_def.name(), type_def);
    }

    // TODO: correlate unnamed indices somehow!
    for index in old.indices().iter() {
        if let Some(name) = index.name() {
            old_indices.insert(name, index);
        }
    }

    for index in new.indices() {
        if let Some(name) = index.name() {
            new_indices.insert(name, index);
        }
    }

    for table in old.tables() {
        old_tables.insert(table.name(), table);
    }

    for table in new.tables() {
        new_tables.insert(table.name(), table);
    }

    for type_def in new.types() {
        if let Some(&old_type_def) = old_types.get(type_def.name()) {
            // TODO: migrate type, needs changes in all tables that use it
        } else {
            stmts.push(Statement::CreateType(type_def.clone()));
        } 
    }

    for table in old.tables() {
        if !new_tables.contains_key(table.name()) {
            stmts.push(Statement::drop_table(table.name().clone()));
        }
    }

    let mut create_indices = Vec::new();

    for index in old.indices() {
        if let Some(name) = index.name() {
            if let Some(&new_index) = new_indices.get(name) {
                if new_index != index {
                    stmts.push(Statement::drop_index(name.clone()));
                    create_indices.push(Statement::CreateIndex(new_index.clone()));
                }
            } else {
                // XXX: DBMSs generate indices for things like foreign keys and such.
                //      Those should of course not be dropped!
                stmts.push(Statement::drop_index(name.clone()));
            }
        }
    }

    for table in new.tables() {
        if let Some(&old_table) = old_tables.get(table.name()) {
            migrate_table(old_table, table, &mut stmts);
        } else {
            stmts.push(Statement::CreateTable(table.clone()));
        }
    }

    for index in new.indices() {
        if let Some(name) = index.name() {
            if !new_indices.contains_key(name) {
                stmts.push(Statement::CreateIndex(index.clone()));
            }
        }
    }

    stmts.extend(create_indices);

    for type_def in old.types() {
        if !new_types.contains_key(type_def.name()) {
            stmts.push(Statement::drop_type(type_def.name().clone()));
        }
    }

    stmts
}

fn migrate_table(old_table: &Table, new_table: &Table, stmts: &mut Vec<Statement>) {
    let mut old_columns = HashMap::new();
    let mut new_columns = HashMap::new();
    // TODO: constraints

    for column in old_table.columns() {
        old_columns.insert(column.name(), column);
    }

    for column in new_table.columns() {
        new_columns.insert(column.name(), column);
    }

    for column in old_table.columns() {
        if let Some(&new_column) = new_columns.get(column.name()) {
            if column != new_column {
                migrate_column(new_table.name(), column, new_column, stmts);
            }
        } else {
            stmts.push(Statement::AlterTable(
                AlterTable::drop_column(old_table.name().clone(), column.name().clone())
            ));
        }
    }

    for column in new_table.columns() {
        if !old_columns.contains_key(column.name()) {
            stmts.push(Statement::AlterTable(
                AlterTable::add_column(new_table.name().clone(), column.clone())
            ));
        }
    }

    // TODO
}

fn migrate_column(table_name: &Name, old_column: &Column, new_column: &Column, stmts: &mut Vec<Statement>) {
    if old_column.name() != new_column.name() {
        stmts.push(Statement::AlterTable(
            AlterTable::rename_column(table_name.clone(), old_column.name().clone(), new_column.name().clone())
        ));
    }

    if old_column.data_type() != new_column.data_type() || old_column.collation() != new_column.collation() {
        stmts.push(Statement::AlterTable(
            AlterTable::alter_column(table_name.clone(), AlterColumn::change_type(
                new_column.name().clone(),
                new_column.data_type().clone(),
                new_column.collation().map(Clone::clone),
                Some(new_column.data_type().cast(new_column.name()).into())
            ))
        ));
    }

    #[derive(PartialEq)]
    struct Check<'a> {
        expr: &'a Rc<[ParsedToken]>,
        inherit: bool,
    }

    #[derive(PartialEq)]
    struct References<'a> {
        ref_table: &'a Name,
        ref_column: &'a Option<Name>,
        column_match: &'a Option<ColumnMatch>,
        on_delete: &'a Option<ReferentialAction>,
        on_update: &'a Option<ReferentialAction>,
    }

    let mut old_constraints = HashMap::new();
    let mut new_constraints = HashMap::new();

    let mut old_primary_key = false;
    let mut new_primary_key = false;

    let mut old_null = true;
    let mut new_null = true;

    let mut old_foreign_key = None;
    let mut new_foreign_key = None;

    let mut old_default = None;
    let mut new_default = None;

    let mut old_check = None;
    let mut new_check = None;

    let mut old_unique = None;
    let mut new_unique = None;

    for constraint in old_column.constraints() {
        if let Some(name) = constraint.name() {
            old_constraints.insert(name, constraint);
        }

        match constraint.data() {
            ColumnConstraintData::Null => {
                old_null = true;
            }
            ColumnConstraintData::NotNull => {
                old_null = false;
            }
            ColumnConstraintData::PrimaryKey => {
                old_primary_key = true;
            }
            ColumnConstraintData::Check { expr, inherit } => {
                old_check = Some(Check { expr, inherit: *inherit });
            }
            ColumnConstraintData::Default { value } => {
                old_default = Some(value);
            }
            ColumnConstraintData::Unique { nulls_distinct } => {
                old_unique = Some(nulls_distinct.unwrap_or(true));
            }
            ColumnConstraintData::References { ref_table, ref_column, column_match, on_delete, on_update } => {
                old_foreign_key = Some(References { ref_table, ref_column, column_match, on_delete, on_update });
            }
        }
    }

    for constraint in new_column.constraints() {
        if let Some(name) = constraint.name() {
            new_constraints.insert(name, constraint);
        }

        if constraint.is_primary_key() {
            new_primary_key = true;
        }

        match constraint.data() {
            ColumnConstraintData::Null => {
                new_null = true;
            }
            ColumnConstraintData::NotNull => {
                new_null = false;
            }
            ColumnConstraintData::PrimaryKey => {
                new_primary_key = true;
            }
            ColumnConstraintData::Check { expr, inherit } => {
                new_check = Some(Check { expr, inherit: *inherit });
            }
            ColumnConstraintData::Default { value } => {
                new_default = Some(value);
            }
            ColumnConstraintData::Unique { nulls_distinct } => {
                new_unique = Some(nulls_distinct.unwrap_or(true));
            }
            ColumnConstraintData::References { ref_table, ref_column, column_match, on_delete, on_update } => {
                new_foreign_key = Some(References { ref_table, ref_column, column_match, on_delete, on_update });
            }
        }
    }

    if old_null != new_null {
        if new_null {
            stmts.push(Statement::AlterTable(
                AlterTable::alter_column(table_name.clone(), AlterColumn::drop_not_null(new_column.name().clone()))
            ));
        } else {
            stmts.push(Statement::AlterTable(
                AlterTable::alter_column(table_name.clone(), AlterColumn::set_not_null(new_column.name().clone()))
            ));
        }
    }

    if old_primary_key != new_primary_key {
        if new_primary_key {
            stmts.push(Statement::AlterTable(
                AlterTable::add_constraint(
                    table_name.clone(),
                    TableConstraint::new(
                        None,
                        TableConstraintData::PrimaryKey {
                            columns: [new_column.name().clone()].into(),
                        },
                        None,
                        None
                    )
                )
            ));
        } else {
            // TODO: how? I think all constraints get automatic names and need to be dropped with that?
        }
    }

    if old_default != new_default {
        if let Some(new_default) = new_default {
            stmts.push(Statement::AlterTable(
                AlterTable::alter_column(table_name.clone(), AlterColumn::set_default(new_column.name().clone(), new_default.clone()))
            ));
        } else {
            stmts.push(Statement::AlterTable(
                AlterTable::alter_column(table_name.clone(), AlterColumn::drop_default(new_column.name().clone()))
            ));
        }
    }

    if old_unique != new_unique {
        if let Some(nulls_distinct) = new_unique {
            stmts.push(Statement::AlterTable(
                AlterTable::add_constraint(
                    table_name.clone(),
                    TableConstraint::new(
                        None,
                        TableConstraintData::Unique {
                            nulls_distinct: Some(nulls_distinct),
                            columns: [new_column.name().clone()].into(),
                        },
                        None,
                        None
                    )
                )
            ));
        } else {
            // TODO: how? I think all constraints get automatic names and need to be dropped with that?
        }
    }

    if old_foreign_key != new_foreign_key {
        if let Some(new_foreign_key) = new_foreign_key {
            let ref_columns = if let Some(ref_column) = new_foreign_key.ref_column {
                Some([ref_column.clone()].into())
            } else {
                None
            };
            stmts.push(Statement::AlterTable(
                AlterTable::add_constraint(
                    table_name.clone(),
                    TableConstraint::new(
                        None,
                        TableConstraintData::ForeignKey {
                            columns: [new_column.name().clone()].into(),
                            ref_table: new_foreign_key.ref_table.clone(),
                            ref_columns,
                            column_match: *new_foreign_key.column_match,
                            on_delete: new_foreign_key.on_delete.clone(),
                            on_update: new_foreign_key.on_update.clone(),
                        },
                        None,
                        None
                    )
                )
            ));

        } else {
            // TODO: how? I think all constraints get automatic names and need to be dropped with that?
        }
    }

    if old_check != new_check {

    }

    // TODO: constraints etc.
}
