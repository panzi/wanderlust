use std::ops::Deref;
use std::rc::Rc;

use crate::error::{Error, ErrorKind, Result};
use crate::format::IsoString;
use crate::model::alter::table::{AlterColumnData, AlterTable, AlterTableAction, AlterTableData};
use crate::model::alter::types::{AlterType, AlterTypeData, ValuePosition};
use crate::model::types::TypeData;
use crate::ordered_hash_map::OrderedHashMap;

use super::alter::extension::{AlterExtension, AlterExtensionData};
use super::column::Column;
use super::extension::{CreateExtension, Extension};
use super::function::{CreateFunction, Function, FunctionSignature};
use super::index::CreateIndex;
use super::name::{Name, QName};
use super::table::CreateTable;
use super::trigger::CreateTrigger;
use super::types::BasicType;
use super::{index::Index, table::Table, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    types: OrderedHashMap<QName, Rc<TypeDef>>,
    tables: OrderedHashMap<QName, Rc<Table>>,
    indices: OrderedHashMap<QName, Rc<Index>>,
    extensions: OrderedHashMap<QName, Rc<Extension>>,
    functions: OrderedHashMap<FunctionSignature, Rc<Function>>,
    default_schema: Name,
    search_path: Vec<Name>,
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{BEGIN};")?;
        for (_, type_def) in &self.types {
            writeln!(f, "{type_def}")?;
        }
        for (_, table) in &self.tables {
            writeln!(f, "{table}")?;
        }
        for (_, index) in &self.indices {
            writeln!(f, "{index}")?;
        }
        writeln!(f, "{COMMIT};")
    }
}

impl Schema {
    #[inline]
    pub fn new(default_schema: Name) -> Self {
        Self {
            types: OrderedHashMap::new(),
            tables: OrderedHashMap::new(),
            indices: OrderedHashMap::new(),
            extensions: OrderedHashMap::new(),
            functions: OrderedHashMap::new(),
            default_schema: default_schema.clone(),
            search_path: vec![default_schema],
        }
    }

    pub fn clear(&mut self) {
        self.types.clear();
        self.tables.clear();
        self.indices.clear();
        self.search_path.clear();
        self.search_path.push(self.default_schema.clone());
    }

    /// Resolve table name using the current search_path
    pub fn resolve_table_name(&self, name: &Name) -> QName {
        for schema in &self.search_path {
            let some_schema = Some(schema);
            for qname in self.tables.keys_unordered() {
                if qname.schema() == some_schema && qname.name() == name {
                    return qname.clone();
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve type name using the current search_path
    pub fn resolve_type_name(&self, name: &Name) -> QName {
        for schema in &self.search_path {
            let some_schema = Some(schema);
            for qname in self.types.keys_unordered() {
                if qname.schema() == some_schema && qname.name() == name {
                    return qname.clone();
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve index name using the current search_path
    pub fn resolve_index_name(&self, name: &Name) -> QName {
        for schema in &self.search_path {
            let some_schema = Some(schema);
            for qname in self.indices.keys_unordered() {
                if qname.schema() == some_schema && qname.name() == name {
                    return qname.clone();
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve function name using the current search_path
    pub fn resolve_function_name(&self, name: &Name) -> QName {
        for schema in &self.search_path {
            let some_schema = Some(schema);
            for sig in self.functions.keys_unordered() {
                if sig.name().schema() == some_schema && sig.name().name() == name {
                    return sig.name().clone();
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    #[inline]
    pub fn default_schema(&self) -> &Name {
        &self.default_schema
    }

    #[inline]
    pub fn types(&self) -> &OrderedHashMap<QName, Rc<TypeDef>> {
        &self.types
    }

    #[inline]
    pub fn tables(&self) -> &OrderedHashMap<QName, Rc<Table>> {
        &self.tables
    }

    #[inline]
    pub fn indices(&self) -> &OrderedHashMap<QName, Rc<Index>> {
        &self.indices
    }

    #[inline]
    pub fn extensions(&self) -> &OrderedHashMap<QName, Rc<Extension>> {
        &self.extensions
    }

    #[inline]
    pub fn functions(&self) -> &OrderedHashMap<FunctionSignature, Rc<Function>> {
        &self.functions
    }

    #[inline]
    pub fn search_path(&self) -> &[Name] {
        &self.search_path
    }

    #[inline]
    pub fn search_path_mut(&mut self) -> &mut Vec<Name> {
        &mut self.search_path
    }

    pub fn set_default_search_path(&mut self) {
        self.search_path.clear();
        self.search_path.push(self.default_schema.clone());
    }

    pub fn create_table(&mut self, create_table: CreateTable) -> bool {
        // TODO: use some kind of ordered hashtable?
        if self.tables.contains_key(create_table.table().name()) {
            return create_table.if_not_exists();
        }
        let name = create_table.table().name().clone();
        self.tables.insert(name, create_table.into());
        true
    }

    pub fn create_index(&mut self, mut create_index: CreateIndex) -> bool {
        let index_name = create_index.index_mut().ensure_name();

        if self.indices.contains_key(index_name) {
            return create_index.if_not_exists();
        }
        self.indices.insert(index_name.clone(), create_index.into());
        true
    }

    pub fn create_type(&mut self, type_def: impl Into<Rc<TypeDef>>) -> bool {
        let type_def = type_def.into();
        if self.types.contains_key(type_def.name()) {
            return false;
        }
        self.types.insert(type_def.name().clone(), type_def);
        true
    }

    pub fn create_extension(&mut self, create_extension: CreateExtension) -> bool {
        if self.extensions.contains_key(create_extension.extension().name()) {
            return create_extension.if_not_exists();
        }
        let name = create_extension.extension().name().clone();
        self.extensions.insert(name, create_extension.into());
        true
    }

    pub fn create_function(&mut self, create_function: CreateFunction) -> bool {
        let signature = create_function.function().signature();

        if create_function.or_replace() {
            self.functions.insert(
                signature,
                create_function.into_function()
            );
            return true;
        }

        if self.functions.contains_key(&signature) {
            return false;
        }

        self.functions.insert(
            signature,
            create_function.into_function()
        );
        true
    }

    pub fn create_trigger(&mut self, create_trigger: CreateTrigger) -> Result<()> {
        let Some(table) = self.tables.get_mut(create_trigger.trigger().table_name()) else {
            return Err(Error::new(
                ErrorKind::TableNotExists,
                None,
                Some(format!("table {} not found: {create_trigger}", create_trigger.trigger().table_name())),
                None
            ));
        };

        if !create_trigger.or_replace() && table.triggers().contains_key(create_trigger.trigger().name()) {
            return Err(Error::new(
                ErrorKind::TriggerExists,
                None,
                Some(format!("trigger {} already exists in table {} not found: {create_trigger}",
                    create_trigger.trigger().name(),
                    create_trigger.trigger().table_name())),
                None
            ));
        }

        Rc::make_mut(table).triggers_mut().insert(
            create_trigger.trigger().name().clone(),
            create_trigger.into_trigger()
        );

        Ok(())
    }

    pub fn find_columns_with_type(&self, type_name: &QName) -> Vec<(&QName, &Rc<Column>)> {
        let mut found_columns = Vec::new();

        for table in self.tables.values_unordered() {
            for column in table.columns().values_unordered() {
                if let BasicType::UserDefined { name, .. } = column.data_type().basic_type() {
                    if type_name == name {
                        found_columns.push((table.name(), column));
                    }
                }
            }
        }

        found_columns
    }

    pub fn alter_type(&mut self, alter_type: &AlterType) -> Result<()> {
        match alter_type.data() {
            AlterTypeData::Rename { new_name } => {
                if self.types.contains_key(new_name) {
                    return Err(Error::new(
                        ErrorKind::TypeExists,
                        None,
                        Some(format!(
                            "type {} already exists: {alter_type}",
                            alter_type.type_name()
                        )),
                        None
                    ));
                }

                if let Some(mut type_def) = self.types.remove(alter_type.type_name()) {
                    Rc::make_mut(&mut type_def).set_name(new_name.clone());
                    self.types.insert(new_name.clone(), type_def);

                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::TypeNotExists,
                        None,
                        Some(format!(
                            "type {} not found: {alter_type}",
                            alter_type.type_name()
                        )),
                        None
                    ))
                }
            }
            AlterTypeData::SetSchema { new_schema } => {
                let new_name = QName::new(
                    Some(new_schema.clone()),
                    alter_type.type_name().name().clone()
                );
                if self.types.contains_key(&new_name) {
                    return Err(Error::new(
                        ErrorKind::TypeExists,
                        None,
                        Some(format!(
                            "type {} already exists: {alter_type}",
                            alter_type.type_name()
                        )),
                        None
                    ));
                }

                if let Some(mut type_def) = self.types.remove(alter_type.type_name()) {
                    Rc::make_mut(&mut type_def).set_name(new_name.clone());
                    self.types.insert(new_name, type_def);
                } else {
                    return Err(Error::new(
                        ErrorKind::TypeNotExists,
                        None,
                        Some(format!(
                            "type {} not found: {alter_type}",
                            alter_type.type_name()
                        )),
                        None
                    ));
                }
                Ok(())
            }
            _ => {
                if let Some(type_def) = self.types.get_mut(alter_type.type_name()) {
                    match alter_type.data() {
                        AlterTypeData::OwnerTo { .. } => {}, // TODO: should I care about ownership?
                        AlterTypeData::AddValue { if_not_exists, value, position } => {
                            let type_def = Rc::make_mut(type_def);
                            match type_def.data_mut() {
                                TypeData::Enum { values } => {
                                    if values.contains(value) {
                                        if *if_not_exists {
                                            return Ok(());
                                        } else {
                                            return Err(Error::new(
                                                ErrorKind::ValueExists,
                                                None,
                                                Some(format!(
                                                    "value {} already exists in {}: {alter_type}",
                                                    IsoString(value), alter_type.type_name()
                                                )),
                                                None
                                            ));
                                        }
                                    }

                                    match position {
                                        None => {
                                            Rc::make_mut(values).push(value.clone());
                                        }
                                        Some(ValuePosition::After(other_value)) => {
                                            let Some(index) = values.iter().position(|v| v == other_value) else {
                                                return Err(Error::new(
                                                    ErrorKind::ValueNotExists,
                                                    None,
                                                    Some(format!(
                                                        "value {} does not exist in {}: {alter_type}",
                                                        IsoString(other_value), alter_type.type_name()
                                                    )),
                                                    None
                                                ));
                                            };
                                            Rc::make_mut(values).insert(index + 1, value.clone());
                                        }
                                        Some(ValuePosition::Before(other_value)) => {
                                            let Some(index) = values.iter().position(|v| v == other_value) else {
                                                return Err(Error::new(
                                                    ErrorKind::ValueNotExists,
                                                    None,
                                                    Some(format!(
                                                        "value {} does not exist in {}: {alter_type}",
                                                        IsoString(other_value), alter_type.type_name()
                                                    )),
                                                    None
                                                ));
                                            };
                                            Rc::make_mut(values).insert(index, value.clone());
                                        }
                                    }
                                }
                            }
                        }
                        AlterTypeData::RenameValue { existing_value, new_value } => {
                            let type_def = Rc::make_mut(type_def);
                            match type_def.data_mut() {
                                TypeData::Enum { values } => {
                                    let Some(index) = values.iter().position(|v| v == existing_value) else {
                                        return Err(Error::new(
                                            ErrorKind::ValueNotExists,
                                            None,
                                            Some(format!(
                                                "value {} does not exist in {}: {alter_type}",
                                                IsoString(existing_value), alter_type.type_name()
                                            )),
                                            None
                                        ));
                                    };

                                    if values.contains(new_value) {
                                        return Err(Error::new(
                                            ErrorKind::ValueExists,
                                            None,
                                            Some(format!(
                                                "value {} already exists in {}: {alter_type}",
                                                IsoString(new_value), alter_type.type_name()
                                            )),
                                            None
                                        ));
                                    }

                                    Rc::make_mut(values)[index] = new_value.clone();
                                }
                            }
                        }
                        AlterTypeData::Rename { .. } => {}
                        AlterTypeData::SetSchema { .. } => {}
                    }
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::TypeNotExists,
                        None,
                        Some(format!(
                            "type {} not found: {alter_type}",
                            alter_type.type_name()
                        )),
                        None
                    ))
                }
            }
        }
    }

    pub fn drop_table(&mut self, table_name: &QName) -> bool {
        self.indices.retain(|_, v| v.table_name() != table_name);
        self.tables.remove(table_name).is_some()
    }

    pub fn alter_table(&mut self, alter_table: &Rc<AlterTable>) -> Result<()> {
        match alter_table.data() {
            AlterTableData::RenameTable { if_exists, new_name } => {
                if self.tables.contains_key(new_name) {
                    return Err(Error::new(
                        ErrorKind::TableExists,
                        None,
                        Some(format!(
                            "table {} already exists: {alter_table}",
                            alter_table.name()
                        )),
                        None
                    ));
                }

                if let Some(mut table) = self.tables.remove(alter_table.name()) {
                    Rc::make_mut(&mut table).set_name(new_name.clone());
                    self.tables.insert(new_name.clone(), table);

                    Ok(())
                } else if *if_exists {
                    Err(Error::new(
                        ErrorKind::TypeNotExists,
                        None,
                        Some(format!(
                            "table {} not found: {alter_table}",
                            alter_table.name()
                        )),
                        None
                    ))
                } else {
                    Ok(())
                }
            }
            AlterTableData::SetSchema { if_exists, new_schema } => {
                let new_name = QName::new(
                    Some(new_schema.clone()),
                    alter_table.name().name().clone()
                );
                if self.tables.contains_key(&new_name) {
                    return Err(Error::new(
                        ErrorKind::TableExists,
                        None,
                        Some(format!(
                            "table {} already exists: {alter_table}",
                            alter_table.name()
                        )),
                        None
                    ));
                }

                if let Some(mut table) = self.tables.remove(alter_table.name()) {
                    Rc::make_mut(&mut table).set_name(new_name.clone());
                    self.tables.insert(new_name, table);

                    Ok(())
                } else if *if_exists {
                    Err(Error::new(
                        ErrorKind::TypeNotExists,
                        None,
                        Some(format!(
                            "table {} not found: {alter_table}",
                            alter_table.name()
                        )),
                        None
                    ))
                } else {
                    Ok(())
                }
            }
            _ => {
                let Some(table) = self.tables.get_mut(alter_table.name()) else {
                    if alter_table.data().if_exists() {
                        return Ok(());
                    }
                    return Err(Error::new(
                        ErrorKind::TableNotExists,
                        None,
                        Some(format!(
                            "table {} not found: {alter_table}",
                            alter_table.name()
                        )),
                        None
                    ));
                };
                match alter_table.data() {
                    AlterTableData::RenameColumn { if_exists, only: _, column_name, new_column_name } => {
                        if table.columns().contains_key(new_column_name) {
                            if *if_exists && !table.columns().contains_key(column_name) {
                                return Ok(());
                            }
                            return Err(Error::new(
                                ErrorKind::ColumnExists,
                                None,
                                Some(format!(
                                    "column {new_column_name} of table {} already exists: {alter_table}",
                                    alter_table.name()
                                )),
                                None
                            ));
                        }

                        let table = Rc::make_mut(table);
                        if let Some(mut column) = table.columns_mut().remove(column_name) {
                            Rc::make_mut(&mut column).set_name(new_column_name.clone());
                            table.columns_mut().insert(new_column_name.clone(), column);
                        } else {
                            if *if_exists {
                                return Ok(());
                            }
                            return Err(Error::new(
                                ErrorKind::ColumnNotExists,
                                None,
                                Some(format!(
                                    "column {column_name} of table {} not found: {alter_table}",
                                    alter_table.name()
                                )),
                                None
                            ));
                        }

                        Ok(())
                    }
                    AlterTableData::RenameConstraint { if_exists, only: _, constraint_name, new_constraint_name } => {
                        if table.constraints().contains_key(new_constraint_name) {
                            return Err(Error::new(
                                ErrorKind::ConstraintExists,
                                None,
                                Some(format!(
                                    "constraint {} of table {} already exists: {alter_table}",
                                    new_constraint_name, alter_table.name()
                                )),
                                None
                            ));
                        }

                        let table = Rc::make_mut(table);
                        if let Some(mut constraint) = table.constraints_mut().remove(constraint_name) {
                            Rc::make_mut(&mut constraint).set_name(Some(new_constraint_name.clone()));
                            table.constraints_mut().insert(constraint_name.clone(), constraint);
                        } else {
                            if *if_exists {
                                return Ok(());
                            }
                            return Err(Error::new(
                                ErrorKind::ConstraintExists,
                                None,
                                Some(format!(
                                    "constraint {} of table {} not found: {alter_table}",
                                    constraint_name, alter_table.name()
                                )),
                                None
                            ));
                        }

                        Ok(())
                    }
                    AlterTableData::Actions { if_exists, only: _, actions } => {
                        for action in actions.deref() {
                            match action {
                                AlterTableAction::OwnerTo { .. } => {}
                                AlterTableAction::AddColumn { if_not_exists, column } => {
                                    if table.columns().contains_key(column.name()) {
                                        if *if_not_exists {
                                            return Ok(());
                                        }
                                        return Err(Error::new(
                                            ErrorKind::ColumnExists,
                                            None,
                                            Some(format!(
                                                "column {} of table {} already exists: {alter_table}",
                                                column.name(), alter_table.name()
                                            )),
                                            None
                                        ));
                                    }

                                    Rc::make_mut(table).columns_mut().insert(column.name().clone(), column.clone());
                                }
                                AlterTableAction::AddConstraint { constraint } => {
                                    let mut constraint = constraint.clone();
                                    let constraint_name = Rc::make_mut(&mut constraint).ensure_name();

                                    if table.constraints().contains_key(constraint_name) {
                                        return Err(Error::new(
                                            ErrorKind::ConstraintExists,
                                            None,
                                            Some(format!(
                                                "constraint {} of table {} already exists: {alter_table}",
                                                constraint_name, alter_table.name()
                                            )),
                                            None
                                        ));
                                    }

                                    Rc::make_mut(table).constraints_mut().insert(constraint_name.clone(), constraint);
                                }
                                AlterTableAction::AlterColumn { alter_column } => {
                                    let Some(column) = Rc::make_mut(table).columns_mut().get_mut(alter_column.column_name()) else {
                                        if *if_exists {
                                            return Ok(());
                                        }
                                        return Err(Error::new(
                                            ErrorKind::ColumnNotExists,
                                            None,
                                            Some(format!(
                                                "column {} of table {} not found: {alter_table}",
                                                alter_column.column_name(), alter_table.name()
                                            )),
                                            None
                                        ));
                                    };
                                    let column = Rc::make_mut(column);

                                    match alter_column.data() {
                                        AlterColumnData::DropDefault => {
                                            column.drop_default();
                                        }
                                        AlterColumnData::SetDefault { expr } => {
                                            column.set_default(expr.clone());
                                        }
                                        AlterColumnData::DropNotNull => {
                                            column.drop_not_null();
                                        }
                                        AlterColumnData::SetNotNull => {
                                            column.set_not_null();
                                        }
                                        AlterColumnData::Type { data_type, collation, using: _ } => {
                                            column.set_data_type(data_type.clone());
                                            column.set_collation(collation.clone());
                                        }
                                    }
                                }
                                AlterTableAction::AlterConstraint { constraint_name, deferrable, initially_deferred } => {
                                    let Some(constraint) = Rc::make_mut(table).constraints_mut().get_mut(constraint_name) else {
                                        return Err(Error::new(
                                            ErrorKind::ConstraintNotExists,
                                            None,
                                            Some(format!(
                                                "constraint {} of table {} not found: {alter_table}",
                                                constraint_name, alter_table.name()
                                            )),
                                            None
                                        ));
                                    };

                                    let constraint = Rc::make_mut(constraint);

                                    if deferrable.is_some() {
                                        constraint.set_deferrable(*deferrable);
                                    }

                                    if initially_deferred.is_some() {
                                        constraint.set_initially_deferred(*initially_deferred);
                                    }
                                }
                                AlterTableAction::DropColumn { if_exists, column_name, behavior } => {
                                    // TODO: CASCADE and RESTRICT will be some work!
                                    if Rc::make_mut(table).columns_mut().remove(column_name).is_none() {
                                        if *if_exists {
                                            return Ok(());
                                        }
                                        return Err(Error::new(
                                            ErrorKind::ColumnNotExists,
                                            None,
                                            Some(format!(
                                                "column {} of table {} not found: {alter_table}",
                                                column_name, alter_table.name()
                                            )),
                                            None
                                        ));
                                    }
                                }
                                AlterTableAction::DropConstraint { if_exists, constraint_name, behavior } => {
                                    // TODO: CASCADE and RESTRICT will be some work!
                                    if Rc::make_mut(table).constraints_mut().remove(constraint_name).is_none() {
                                        if *if_exists {
                                            return Ok(());
                                        }
                                        return Err(Error::new(
                                            ErrorKind::ConstraintNotExists,
                                            None,
                                            Some(format!(
                                                "constraint {} of table {} not found: {alter_table}",
                                                constraint_name, alter_table.name()
                                            )),
                                            None
                                        ));
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                    AlterTableData::RenameTable { .. } => Ok(()),
                    AlterTableData::SetSchema { .. } => Ok(())
                }
            }
        }
    }

    pub fn alter_extension(&mut self, alter_extension: &Rc<AlterExtension>) -> bool {
        match alter_extension.data() {
            AlterExtensionData::Update(version) => {
                let Some(extension) = self.extensions.get_mut(alter_extension.name()) else {
                    return false;
                };

                Rc::make_mut(extension).set_version(version.clone());

                true
            }
            AlterExtensionData::SetSchema(new_schema) => {
                let new_name = QName::new(
                    Some(new_schema.clone()),
                    alter_extension.name().name().clone()
                );

                if self.extensions.contains_key(&new_name) {
                    return false;
                }

                if let Some(mut extension) = self.extensions.remove(alter_extension.name()) {
                    Rc::make_mut(&mut extension).set_schema(Some(new_schema.clone()));
                    self.extensions.insert(new_name, extension);
                    true
                } else {
                    false
                }
            }
        }
    }
}
