use std::{collections::{HashMap, HashSet}, ops::Deref, rc::Rc};

use crate::{format::{write_token_list, FmtWriter}, make_tokens, model::{
    alter::{
        extension::{AlterExtension, AlterExtensionData},
        table::{AlterColumn, AlterTable},
        types::AlterType,
    },
    column::{Column, ColumnConstraintData},
    database::Database,
    extension::Extension,
    function::{CreateFunction, Function, QFunctionRef},
    index::Index,
    name::{Name, QName},
    object_ref::ObjectRef,
    schema::Schema,
    statement::Statement,
    table::Table,
    token::ParsedToken,
    trigger::{CreateTrigger, Trigger},
    types::{DataType, TypeData, TypeDef},
}};

pub fn create_extension(extension: &Rc<Extension>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::create_extension(
        extension.name().clone(),
        extension.version().cloned()
    ));

    if let Some(comment) = extension.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::extension(extension.name().clone()),
            Some(comment.clone())
        ));
    }
}

pub fn create_type(type_def: &Rc<TypeDef>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::CreateType(type_def.clone()));

    if let Some(comment) = type_def.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::type_def(type_def.name().clone()),
            Some(comment.clone())
        ));
    }
}

pub fn create_function(or_replace: bool, function: &Rc<Function>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::CreateFunction(
        Rc::new(CreateFunction::new(or_replace, function.clone()))
    ));

    if let Some(comment) = function.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::function(function.to_signature()),
            Some(comment.clone())
        ));
    }
}

pub fn create_table(table: &Rc<Table>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::create_table(table.clone()));

    if let Some(comment) = table.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::table(table.name().clone()),
            Some(comment.clone())
        ));
    }

    for constraint in table.constraints().values() {
        if let Some(comment) = constraint.comment() {
            if let Some(name) = constraint.name() {
                stmts.push(Statement::comment_on(
                    ObjectRef::constraint(table.name().clone(), name.clone()),
                    Some(comment.clone())
                ));
            } else {
                // XXX: can't put comment on unnamed constraint!
            }
        }
    }

    for trigger in table.triggers().values() {
        create_trigger(table.name(), trigger, stmts);
    }
}

pub fn create_trigger(table_name: &QName, trigger: &Rc<Trigger>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::CreateTrigger(
        Rc::new(CreateTrigger::new(false, table_name.clone(), trigger.clone()))
    ));

    if let Some(comment) = trigger.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::trigger(table_name.clone(), trigger.name().clone()),
            Some(comment.clone())
        ));
    }
}

pub fn create_index(index: &Rc<Index>, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::create_index(index.clone()));

    if let Some(comment) = index.comment() {
        if let Some(name) = index.name() {
            stmts.push(Statement::comment_on(
                ObjectRef::index(name.clone()),
                Some(comment.clone())
            ));
        } else {
            // XXX: can't put comment on unnamed index!
        }
    }
}

pub fn create_schema(schema: &Schema, stmts: &mut Vec<Statement>) {
    stmts.push(Statement::create_schema(schema.name().clone()));

    if let Some(comment) = schema.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::schema(schema.name().clone()),
            Some(comment.clone())
        ));
    }

    for extension in schema.extensions().values() {
        create_extension(extension, stmts);
    }

    for type_def in schema.types().values() {
        create_type(type_def, stmts);
    }

    for function in schema.functions().values() {
        create_function(false, function, stmts);
    }

    for table in schema.tables().values() {
        create_table(table, stmts);
    }

    for index in schema.indices().values() {
        create_index(index, stmts);
    }
}

pub fn generate_migration(old: &Database, new: &Database) -> Vec<Statement> {
    let mut stmts = Vec::new();
    let mut epilog = Vec::new();
    let mut replace_functions = HashSet::new();

    for schema in old.schemas().values() {
        if let Some(new_schema) = new.schemas().get(schema.name()) {
            migrate_types(old, schema, new_schema, &mut replace_functions, &mut stmts, &mut epilog);
        }
    }

    for schema in old.schemas().values() {
        if let Some(new_schema) = new.schemas().get(schema.name()) {
            migrate_schema(schema, new_schema, &replace_functions, &mut stmts);
        } else {
            stmts.push(Statement::drop_schema(schema.name().clone()));
        }
    }

    for schema in new.schemas().values() {
        if !old.schemas().contains_key(schema.name()) {
            create_schema(schema, &mut stmts);
        }
    }

    stmts.extend(epilog);

    stmts
}

pub fn migrate_types(old_database: &Database, old: &Schema, new: &Schema, replace_functions: &mut HashSet<QFunctionRef>, stmts: &mut Vec<Statement>, epilog: &mut Vec<Statement>) {
    let old_types = old.types();

    let mut tmp_id = 0u64;

    for type_def in new.types().values() {
        if let Some(old_type_def) = old_types.get(type_def.name().name()) {
            if type_def.data() != old_type_def.data() {
                if let Some(missing_values) = old_type_def.missing_enum_values(type_def) {
                    // strictly only new values
                    for (new_value, position) in missing_values {
                        stmts.push(Statement::AlterType(
                            AlterType::add_value(type_def.name().clone(), new_value, position)
                        ));
                    }
                } else {
                    match (type_def.data(), old_type_def.data()) {
                        (TypeData::Composite { attributes: new_attributes }, TypeData::Composite { attributes: old_attributes }) => {
                            for old_attribute in old_attributes.values() {
                                if let Some(new_attribute) = new_attributes.get(old_attribute.name()) {
                                    if new_attribute.data_type() != old_attribute.data_type() || new_attribute.collation() != old_attribute.collation() {
                                        stmts.push(Statement::AlterType(
                                            AlterType::alter_attribute(
                                                type_def.name().clone(),
                                                new_attribute.name().clone(),
                                                new_attribute.data_type().clone(),
                                                new_attribute.collation().cloned()
                                            )
                                        ));
                                    }
                                } else {
                                    stmts.push(Statement::AlterType(
                                        AlterType::drop_attribute(
                                            type_def.name().clone(),
                                            old_attribute.name().clone()
                                        )
                                    ));
                                }
                            }

                            for new_attribute in new_attributes.values() {
                                if !old_attributes.contains_key(new_attribute.name()) {
                                    stmts.push(Statement::AlterType(
                                        AlterType::add_attribute(
                                            type_def.name().clone(),
                                            new_attribute.name().clone(),
                                            new_attribute.data_type().clone(),
                                            new_attribute.collation().cloned()
                                        )
                                    ));
                                }
                            }
                        }
                        _ => {
                            let schema = type_def.name().schema();
                            let mut tmp_name = Name::new(format!("wanderlust_tmp_type_{tmp_id}"));
                            while old_types.contains_key(&tmp_name) {
                                tmp_id += 1;
                                tmp_name = Name::new(format!("wanderlust_tmp_type_{tmp_id}"));
                            }
                            let tmp_name = QName::new(schema.cloned(), tmp_name.clone());

                            let tmp_type_def = type_def.with_name(tmp_name.clone());
                            stmts.push(Statement::CreateType(Rc::new(tmp_type_def)));

                            for (table_name, column) in old_database.find_columns_with_type(type_def.name()) {
                                let new_type = column.data_type().with_user_type(tmp_name.clone(), None);
                                let mut using = Vec::new();
                                make_tokens!(&mut using, {column.name()}::TEXT::{new_type});
                                //let using = new_type.cast(
                                //    DataType::basic(BasicType::Text).cast(
                                //        column.name()
                                //    )
                                //);

                                let default_value = column.default();
                                let set_default;

                                if let Some(default_value) = default_value {
                                    stmts.push(Statement::AlterTable(
                                        AlterTable::alter_column(
                                            table_name.clone(),
                                            AlterColumn::drop_default(column.name().clone())
                                        )
                                    ));

                                    let mut default_value: Vec<ParsedToken> = default_value.deref().into();
                                    if default_value.len() >= 3 {
                                        let mut index = default_value.len() - 1;

                                        if matches!(default_value[index], ParsedToken::Name(..)) && index > 0 {
                                            index -= 1;

                                            if index >= 2 &&
                                               matches!(default_value[index], ParsedToken::Period) &&
                                               matches!(default_value[index - 1], ParsedToken::Name(..)) {
                                                index -= 2;
                                            }

                                            if index >= 2 &&
                                               matches!(default_value[index], ParsedToken::Period) &&
                                               matches!(default_value[index - 1], ParsedToken::Name(..)) {
                                                index -= 2;
                                            }

                                            if matches!(default_value[index], ParsedToken::DoubleColon) {
                                                default_value.truncate(index);
                                            }
                                        }
                                    }

                                    make_tokens!(&mut default_value, ::{new_type});

                                    set_default = Some(Statement::AlterTable(
                                        AlterTable::alter_column(
                                            table_name.clone(),
                                            AlterColumn::set_default(
                                                column.name().clone(),
                                                default_value.into()
                                            )
                                        )
                                    ));
                                } else {
                                    set_default = None;
                                }

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

                                if let Some(set_default) = set_default {
                                    stmts.push(set_default);
                                }
                            }

                            replace_functions.extend(
                                old_database.find_functions_with_type(type_def.name()).into_iter().map(|(k, _)| k)
                            );

                            epilog.push(Statement::drop_type(type_def.name().clone()));
                            epilog.push(Statement::AlterType(
                                AlterType::rename(tmp_name, type_def.name().name().clone())
                            ));
                        }
                    }
                }
            }

            if type_def.comment() != old_type_def.comment() {
                stmts.push(Statement::comment_on(
                    ObjectRef::type_def(type_def.name().clone()),
                    type_def.comment().cloned()
                ));
            }
        } else {
            create_type(type_def, stmts);
        }
    }
}

pub fn migrate_schema(old: &Schema, new: &Schema, replace_functions: &HashSet<QFunctionRef>, stmts: &mut Vec<Statement>) {
    let mut create_indices = Vec::new();

    let old_tables = old.tables();
    let new_tables = new.tables();

    let new_types = new.types();

    let old_indices = old.indices();
    let new_indices = new.indices();

    for table in old_tables.values() {
        if !new_tables.contains_key(table.name().name()) {
            stmts.push(Statement::drop_table(table.name().clone()));
        }
    }

    for extension in old.extensions().values() {
        if !new.extensions().contains_key(extension.name().name()) {
            stmts.push(Statement::drop_extension(extension.name().clone()));
        }
    }

    for function in old.functions().values() {
        if !new.functions().contains_key(&function.to_ref()) {
            stmts.push(Statement::drop_function(function.to_signature()));
        }
    }

    for index in old.indices().values() {
        if let Some(name) = index.name() {
            if let Some(new_index) = new_indices.get(name.name()) {
                if !new_index.eq_content(index) {
                    stmts.push(Statement::drop_index(name.clone()));
                    create_indices.push(Statement::create_index(new_index.clone()));
                }

                if index.comment() != new_index.comment() {
                    create_indices.push(Statement::comment_on(
                        ObjectRef::index(name.clone()),
                        new_index.comment().cloned()
                    ));
                }
            } else {
                stmts.push(Statement::drop_index(name.clone()));
            }
        }
    }

    for extension in new.extensions().values() {
        if let Some(old_extension) = old.extensions().get(extension.name().name()) {
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

            if extension.comment() != old_extension.comment() {
                // NOTE: Comments on extensions can be permission problems.
                // TODO: Maybe make it optional?
                stmts.push(Statement::comment_on(
                    ObjectRef::extension(extension.name().clone()),
                    extension.comment().cloned()
                ));
            }
        } else {
            create_extension(extension, stmts);
        }
    }

    for function in new.functions().values() {
        if let Some(old_function) = old.functions().get(&function.to_ref()) {
            if !function.eq_content(old_function) || replace_functions.contains(&old_function.to_qref()) {
                stmts.push(Statement::CreateFunction(
                    Rc::new(CreateFunction::new(true, function.clone()))
                ));
            }

            if function.comment() != old_function.comment() {
                stmts.push(Statement::comment_on(
                    ObjectRef::function(function.to_signature()),
                    function.comment().cloned()
                ));
            }
        } else {
            create_function(false, function, stmts);
        }
    }

    for table in new.tables().values() {
        if let Some(old_table) = old_tables.get(table.name().name()) {
            migrate_table(old_table, table, stmts);
        } else {
            create_table(table, stmts);
        }
    }

    for index in new.indices().values() {
        if let Some(name) = index.name() {
            if !old_indices.contains_key(name.name()) {
                // TODO: find matching unnamed index?
                create_index(index, stmts);
            }
        } else {
            // TODO: find matching unnamed index?
            create_index(index, stmts);
        }
    }

    stmts.extend(create_indices);

    for type_def in old.types().values() {
        if !new_types.contains_key(type_def.name().name()) {
            stmts.push(Statement::drop_type(type_def.name().clone()));
        }
    }
}

fn migrate_table(old_table: &Table, new_table: &Table, stmts: &mut Vec<Statement>) {
    let old_columns = old_table.columns();
    let new_columns = new_table.columns();

    if old_table.logged() != new_table.logged() {
        stmts.push(Statement::set_logged(
            new_table.name().clone(),
            new_table.logged()
        ));
    }

    if old_table.comment() != new_table.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::table(new_table.name().clone()),
            new_table.comment().cloned()
        ));
    }

    let old_super_tables = old_table.inherits();
    let new_super_tables = new_table.inherits();

    for super_table in old_super_tables {
        if !new_super_tables.contains(super_table) {
            stmts.push(Statement::AlterTable(
                AlterTable::no_inherit(
                    new_table.name().clone(),
                    super_table.clone()
                )
            ));
        }
    }

    for super_table in new_super_tables {
        if !old_super_tables.contains(super_table) {
            stmts.push(Statement::AlterTable(
                AlterTable::inherit(
                    new_table.name().clone(),
                    super_table.clone()
                )
            ));
        }
    }

    // drop triggers
    for trigger in old_table.triggers().values() {
        if !new_table.triggers().contains_key(trigger.name()) {
            stmts.push(Statement::drop_trigger(
                trigger.name().clone(),
                new_table.name().clone()
            ));
        }
    }

    // columns
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
            if let Some(new_constraint) = new_merged_constraints.iter().find(|other| other.eq_content(constraint)) {
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
                        Some(new_constraint.deferrable()),
                        Some(new_constraint.initially_deferred())
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

    // create triggers
    for trigger in new_table.triggers().values() {
        if let Some(old_trigger) = old_table.triggers().get(trigger.name()) {
            if !trigger.eq_content(old_trigger) {
                stmts.push(Statement::CreateTrigger(
                    Rc::new(CreateTrigger::new(true, new_table.name().clone(), trigger.clone()))
                ));
            }

            if trigger.comment() != old_trigger.comment() {
                stmts.push(Statement::comment_on(
                    ObjectRef::trigger(new_table.name().clone(), trigger.name().clone()),
                    trigger.comment().cloned()
                ));
            }
        } else {
            create_trigger(new_table.name(), trigger, stmts);
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

    if old_column.storage() != new_column.storage() {
        stmts.push(Statement::AlterTable(
            AlterTable::alter_column(table_name.clone(), AlterColumn::set_storage(
                new_column.name().clone(),
                new_column.storage()
            ))
        ));
    }

    if old_column.compression() != new_column.compression() {
        stmts.push(Statement::AlterTable(
            AlterTable::alter_column(table_name.clone(), AlterColumn::set_compression(
                new_column.name().clone(),
                new_column.compression().cloned()
            ))
        ));
    }

    if old_column.comment() != new_column.comment() {
        stmts.push(Statement::comment_on(
            ObjectRef::column(table_name.clone(), new_column.name().clone()),
            new_column.comment().cloned()
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
            ColumnConstraintData::PrimaryKey { .. } => {}
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
            // these are converted into table constraints and thus done in migrate_table()
            ColumnConstraintData::Check { .. } => {}
            ColumnConstraintData::PrimaryKey { .. } => {}
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
            if let Some(old_default) = old_default {
                if !is_same_default_value(new_column.data_type(), old_default, new_default) {
                    stmts.push(Statement::AlterTable(
                        AlterTable::alter_column(table_name.clone(), AlterColumn::set_default(new_column.name().clone(), new_default.clone()))
                    ));
                }
            } else {
                stmts.push(Statement::AlterTable(
                    AlterTable::alter_column(table_name.clone(), AlterColumn::set_default(new_column.name().clone(), new_default.clone()))
                ));
            }
        } else {
            stmts.push(Statement::AlterTable(
                AlterTable::alter_column(table_name.clone(), AlterColumn::drop_default(new_column.name().clone()))
            ));
        }
    }
}

fn is_same_default_value(data_type: &DataType, old_default: &[ParsedToken], new_default: &[ParsedToken]) -> bool {
    let shorter;
    let longer;
    if old_default.len() < new_default.len() {
        shorter = old_default;
        longer = new_default;
    } else if old_default.len() > new_default.len() {
        shorter = new_default;
        longer = old_default;
    } else {
        return old_default == new_default;
    }

    let mut tokens = Vec::with_capacity(longer.len());
    make_tokens!(&mut tokens, {shorter}::{data_type});

    if tokens == longer {
        return true;
    }

    tokens.clear();
    make_tokens!(&mut tokens, ({shorter}));
    if tokens == longer {
        return true;
    }

    tokens.clear();
    make_tokens!(&mut tokens, CAST({shorter} AS {data_type}));

    tokens == longer
}
