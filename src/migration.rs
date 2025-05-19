use std::collections::HashMap;

use crate::model::{alter::{AlterColumn, AlterTable}, column::Column, ddl::DDL, name::Name, statement::Statement, table::Table};

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

    if old_column.data_type() != new_column.data_type() || old_column.collate() != new_column.collate() {
        stmts.push(Statement::AlterTable(
            AlterTable::alter_column(table_name.clone(), AlterColumn::Type {
                data_type: new_column.data_type().clone(),
                collate: new_column.collate().map(Into::into),
                using: None, // TODO: maybe cast?
            })
        ));
    }

    // TODO: constraints etc.
}
