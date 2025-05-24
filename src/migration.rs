use std::{collections::HashMap, rc::Rc};

use crate::model::{alter::{extension::{AlterExtension, AlterExtensionData}, table::{AlterColumn, AlterTable}, types::AlterType}, column::{Column, ColumnConstraintData}, extension::{CreateExtension}, name::{Name, QName}, schema::Schema, statement::Statement, table::Table};

pub fn generate_migration(old: &Schema, new: &Schema) -> Vec<Statement> {
    let mut stmts = Vec::new();
    let mut create_indices = Vec::new();

    let old_tables = old.tables();
    let new_tables = new.tables();

    let old_types = old.types();
    let new_types = new.types();

    let old_indices = old.indices();
    let new_indices = new.indices();

    // TODO: correlate unnamed indices somehow!

    let mut tmp_id = 0u64;

    for type_def in new.types().values() {
        if let Some(old_type_def) = old_types.get(type_def.name()) {
            if type_def != old_type_def {
                if let Some(missing_values) = old_type_def.missing_enum_values(type_def) {
                    // strictly only new values
                    for (new_value, position) in missing_values {
                        stmts.push(Statement::AlterType(
                            AlterType::add_value(type_def.name().clone(), new_value, position)
                        ));
                    }
                } else {
                    let schema = type_def.name().schema();
                    let mut tmp_name = QName::new(
                        schema.cloned(),
                        Name::new(format!("_wanderlust_tmp_type_{tmp_id}"))
                    );
                    while old_types.contains_key(&tmp_name) {
                        tmp_id += 1;
                        tmp_name.set_name(Name::new(format!("_wanderlust_tmp_type_{tmp_id}")));
                    }

                    let tmp_type_def = type_def.with_name(tmp_name.clone());
                    stmts.push(Statement::CreateType(tmp_type_def.into()));

                    for (table_name, column) in old.find_columns_with_type(type_def.name()) {
                        let new_type = column.data_type().with_user_type(tmp_name.clone(), None);
                        let using = new_type.cast(column.name());

                        stmts.push(Statement::AlterTable(
                            AlterTable::alter_column(
                                table_name.clone(),
                                AlterColumn::change_type(
                                    column.name().clone(),
                                    new_type.into(),
                                    column.collation().cloned(),
                                    Some(using.into())
                                )
                            )
                        ));
                    }

                    stmts.push(Statement::drop_type(type_def.name().clone()));
                    stmts.push(Statement::AlterType(
                        AlterType::rename(tmp_name, type_def.name().clone())
                    ));
                }
            }
        } else {
            stmts.push(Statement::CreateType(type_def.clone()));
        }
    }

    for extension in old.extensions().values() {
        if !new.extensions().contains_key(extension.name()) {
            stmts.push(Statement::drop_extension(extension.name().clone()));
        }
    }

    for table in old_tables.values() {
        if !new_tables.contains_key(table.name()) {
            stmts.push(Statement::drop_table(table.name().clone()));
        }
    }

    for index in old.indices().values() {
        if let Some(name) = index.name() {
            if let Some(new_index) = new_indices.get(name) {
                if new_index != index {
                    stmts.push(Statement::drop_index(name.clone()));
                    create_indices.push(Statement::create_index(new_index.clone()));
                }
            } else {
                // XXX: DBMSs generate indices for things like foreign keys and such.
                //      Those should of course not be dropped!
                stmts.push(Statement::drop_index(name.clone()));
            }
        }
    }

    for extension in new.extensions().values() {
        if let Some(old_extension) = old.extensions().get(extension.name()) {
            if let (Some(version), Some(old_version)) = (extension.version(), old_extension.version()) {
                if version != old_version {
                    stmts.push(Statement::AlterExtension(
                        Rc::new(AlterExtension::new(
                            extension.name().clone(),
                            AlterExtensionData::Update(Some(version.clone()))
                        ))
                    ));
                }
            }
        } else {
            stmts.push(Statement::CreateExtension(
                Rc::new(CreateExtension::new(false, extension.clone(), false))
            ));
        }
    }

    for table in new.tables().values() {
        if let Some(old_table) = old_tables.get(table.name()) {
            migrate_table(old_table, table, &mut stmts);
        } else {
            stmts.push(Statement::create_table(table.clone()));
        }
    }

    for index in new.indices().values() {
        if let Some(name) = index.name() {
            if !old_indices.contains_key(name) {
                // TODO: find matching unnamed index?
                stmts.push(Statement::create_index(index.clone()));
            }
        }
    }

    for index in old.indices().values() {
        if let Some(name) = index.name() {
            if !new_indices.contains_key(name) {
                // TODO: find matching unnamed index?
                stmts.push(Statement::drop_index(name.clone()));
            }
        }
    }

    stmts.extend(create_indices);

    for type_def in old.types().values() {
        if !new_types.contains_key(type_def.name()) {
            stmts.push(Statement::drop_type(type_def.name().clone()));
        }
    }

    stmts
}

fn migrate_table(old_table: &Table, new_table: &Table, stmts: &mut Vec<Statement>) {
    let old_columns = old_table.columns();
    let new_columns = new_table.columns();

    for column in old_table.columns().values() {
        if let Some(new_column) = new_columns.get(column.name()) {
            if column != new_column {
                migrate_column(new_table.name(), column, new_column, stmts);
            }
        } else {
            stmts.push(Statement::AlterTable(
                AlterTable::drop_column(old_table.name().clone(), column.name().clone())
            ));
        }
    }

    let mut new_columns = HashMap::new();

    for column in new_table.columns().values() {
        if !old_columns.contains_key(column.name()) {
            stmts.push(Statement::AlterTable(
                AlterTable::add_column(new_table.name().clone(), column.without_table_constraints())
            ));
            new_columns.insert(column.name(), column);
        }
    }

    // constraints
    let old_merged_constraints = old_table.merged_constraints();
    let new_merged_constraints = new_table.merged_constraints();

    for constraint in &old_merged_constraints {
        if let Some(old_name) = constraint.name() {
            if let Some(new_constraint) = new_merged_constraints.iter().find(|other| other.matches(constraint)) {
                if let Some(new_name) = new_constraint.name() {
                    if old_name != new_name {
                        stmts.push(Statement::AlterTable(
                            AlterTable::rename_constraint(new_table.name().clone(), old_name.clone(), new_name.clone())
                        ));
                    }
                }
            } else if let Some(new_constraint) = new_merged_constraints.iter().find(|other| other.data() == constraint.data()) {
                stmts.push(Statement::AlterTable(
                    AlterTable::alter_constraint(
                        new_table.name().clone(),
                        old_name.clone(),
                        Some(new_constraint.default_deferrable()),
                        Some(new_constraint.default_initially_deferred())
                    )
                ));

                if let Some(new_name) = new_constraint.name() {
                    if old_name != new_name {
                        stmts.push(Statement::AlterTable(
                            AlterTable::rename_constraint(new_table.name().clone(), old_name.clone(), new_name.clone())
                        ));
                    }
                }
            } else {
                stmts.push(Statement::AlterTable(
                    AlterTable::drop_constraint(new_table.name().clone(), old_name.clone(), None)
                ));
            }
        }
    }

    for constraint in &new_merged_constraints {
        if !old_merged_constraints.iter().any(|other| other.data() == constraint.data()) {
            stmts.push(Statement::AlterTable(
                AlterTable::add_constraint(new_table.name().clone(), constraint.clone())
            ));
        }
    }
}

fn migrate_column(table_name: &QName, old_column: &Column, new_column: &Column, stmts: &mut Vec<Statement>) {
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
                new_column.collation().cloned(),
                Some(new_column.data_type().cast(new_column.name()).into())
            ))
        ));
    }

    let mut old_constraints = HashMap::new();
    let mut new_constraints = HashMap::new();

    let mut old_null = true;
    let mut new_null = true;

    let mut old_default = None;
    let mut new_default = None;

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
            ColumnConstraintData::Default { value } => {
                old_default = Some(value);
            }
            // these are converted into table constraints and thus done in migrate_table()
            ColumnConstraintData::Check { .. } => {}
            ColumnConstraintData::PrimaryKey => {}
            ColumnConstraintData::Unique { .. } => {}
            ColumnConstraintData::References { .. } => {}
        }
    }

    for constraint in new_column.constraints() {
        if let Some(name) = constraint.name() {
            new_constraints.insert(name, constraint);
        }

        match constraint.data() {
            ColumnConstraintData::Null => {
                new_null = true;
            }
            ColumnConstraintData::NotNull => {
                new_null = false;
            }
            ColumnConstraintData::Default { value } => {
                new_default = Some(value);
            }
            ColumnConstraintData::Check { .. } => {}
            ColumnConstraintData::PrimaryKey => {}
            ColumnConstraintData::Unique { .. } => {}
            ColumnConstraintData::References { .. } => {}
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
}
