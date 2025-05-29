use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use crate::error::{Error, ErrorKind, Result};
use crate::format::IsoString;
use crate::model::alter::table::{AlterColumnData, AlterTable, AlterTableAction, AlterTableData};
use crate::model::alter::types::{AlterType, AlterTypeData, ValuePosition};
use crate::model::types::TypeData;

use super::alter::extension::{AlterExtension, AlterExtensionData};
use super::column::Column;
use super::extension::{CreateExtension, Extension};
use super::function::{CreateFunction, Function, FunctionSignature};
use super::index::{CreateIndex, Index};
use super::name::{Name, QName};
use super::schema::Schema;
use super::table::{CreateTable, Table};
use super::trigger::CreateTrigger;
use super::types::BasicType;
use super::types::TypeDef;

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Database {
    schemas: HashMap<Name, Schema>,
    default_schema: Name,
    search_path: Vec<Name>,
}

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{BEGIN};")?;
        for schema in self.schemas.values() {
            if schema.name() != &self.default_schema {
                writeln!(f, "{CREATE} {SCHEMA} {IF} {NOT} {EXISTS} {};", schema.name())?;
            }
            for extension in schema.extensions().values() {
                writeln!(f, "{extension}")?;
            }
        }
        for schema in self.schemas.values() {
            for type_def in schema.types().values() {
                writeln!(f, "{type_def}")?;
            }
            for function in schema.functions().values() {
                writeln!(f, "{function}")?;
            }
        }
        for schema in self.schemas.values() {
            for table in schema.tables().values() {
                writeln!(f, "{table}")?;
            }
            for table in schema.tables().values() {
                for trigger in table.triggers().values() {
                    writeln!(f, "{trigger}")?;
                }
            }
            for index in schema.indices().values() {
                writeln!(f, "{index}")?;
            }
        }
        writeln!(f, "{COMMIT};")
    }
}

impl Database {
    #[inline]
    pub fn new(default_schema: Name) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(default_schema.clone(), Schema::new(default_schema.clone()));

        Self {
            schemas,
            default_schema: default_schema.clone(),
            search_path: vec![default_schema],
        }
    }

    pub fn clear(&mut self) {
        self.schemas.clear();
        self.schemas.insert(self.default_schema.clone(), Schema::new(self.default_schema.clone()));
        self.search_path.clear();
        self.search_path.push(self.default_schema.clone());
    }

    #[inline]
    pub fn schemas(&self) -> &HashMap<Name, Schema> {
        &self.schemas
    }

    // TODO: fix search_path based name resolution
    pub fn has_table(&self, name: &QName) -> bool {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.tables().contains_key(name.name())
        } else {
            false
        }
    }

    pub fn get_table(&self, name: &QName) -> Option<&Rc<Table>> {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.tables().get(name.name())
        } else {
            None
        }
    }

    pub fn get_table_mut(&mut self, name: &QName) -> Option<&mut Rc<Table>> {
        if let Some(schema) = self.schemas.get_mut(name.schema().unwrap_or(&self.default_schema)) {
            schema.tables_mut().get_mut(name.name())
        } else {
            None
        }
    }

    pub fn has_index(&self, name: &QName) -> bool {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.indices().contains_key(name.name())
        } else {
            false
        }
    }

    pub fn get_index(&self, name: &QName) -> Option<&Rc<Index>> {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.indices().get(name.name())
        } else {
            None
        }
    }

    pub fn get_index_mut(&mut self, name: &QName) -> Option<&mut Rc<Index>> {
        if let Some(schema) = self.schemas.get_mut(name.schema().unwrap_or(&self.default_schema)) {
            schema.indices_mut().get_mut(name.name())
        } else {
            None
        }
    }

    pub fn has_type(&self, name: &QName) -> bool {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.types().contains_key(name.name())
        } else {
            false
        }
    }

    pub fn get_type(&self, name: &QName) -> Option<&Rc<TypeDef>> {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.types().get(name.name())
        } else {
            None
        }
    }

    pub fn get_type_mut(&mut self, name: &QName) -> Option<&mut Rc<TypeDef>> {
        if let Some(schema) = self.schemas.get_mut(name.schema().unwrap_or(&self.default_schema)) {
            schema.types_mut().get_mut(name.name())
        } else {
            None
        }
    }

    pub fn has_extension(&self, name: &QName) -> bool {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.extensions().contains_key(name.name())
        } else {
            false
        }
    }

    pub fn get_extension(&self, name: &QName) -> Option<&Rc<Extension>> {
        if let Some(schema) = self.schemas.get(name.schema().unwrap_or(&self.default_schema)) {
            schema.extensions().get(name.name())
        } else {
            None
        }
    }

    pub fn get_extension_mut(&mut self, name: &QName) -> Option<&mut Rc<Extension>> {
        if let Some(schema) = self.schemas.get_mut(name.schema().unwrap_or(&self.default_schema)) {
            schema.extensions_mut().get_mut(name.name())
        } else {
            None
        }
    }

    pub fn has_function(&self, signature: &FunctionSignature) -> bool {
        if let Some(schema) = self.schemas.get(signature.name().schema().unwrap_or(&self.default_schema)) {
            schema.functions().contains_key(signature)
        } else {
            false
        }
    }

    pub fn get_function(&self, signature: &FunctionSignature) -> Option<&Rc<Function>> {
        if let Some(schema) = self.schemas.get(signature.name().schema().unwrap_or(&self.default_schema)) {
            schema.functions().get(signature)
        } else {
            None
        }
    }

    pub fn get_function_mut(&mut self, signature: &FunctionSignature) -> Option<&mut Rc<Function>> {
        if let Some(schema) = self.schemas.get_mut(signature.name().schema().unwrap_or(&self.default_schema)) {
            schema.functions_mut().get_mut(signature)
        } else {
            None
        }
    }

    /// Resolve table name using the current search_path
    pub fn resolve_table_name(&self, name: &Name) -> QName {
        for schema_name in &self.search_path {
            if let Some(schema) = self.schemas.get(schema_name) {
                if schema.tables().contains_key(name) {
                    return QName::new(
                        Some(schema_name.clone()),
                        name.clone()
                    );
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve type name using the current search_path
    pub fn resolve_type_name(&self, name: &Name) -> QName {
        for schema_name in &self.search_path {
            if let Some(schema) = self.schemas.get(schema_name) {
                if schema.types().contains_key(name) {
                    return QName::new(
                        Some(schema_name.clone()),
                        name.clone()
                    );
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve index name using the current search_path
    pub fn resolve_index_name(&self, name: &Name) -> QName {
        for schema_name in &self.search_path {
            if let Some(schema) = self.schemas.get(schema_name) {
                if schema.indices().contains_key(name) {
                    return QName::new(
                        Some(schema_name.clone()),
                        name.clone()
                    );
                }
            }
        }

        QName::new(Some(self.default_schema.clone()), name.clone())
    }

    /// Resolve function name using the current search_path
    pub fn resolve_function_name(&self, name: &Name) -> QName {
        for schema_name in &self.search_path {
            if let Some(schema) = self.schemas.get(schema_name) {
                if schema.functions().keys_unordered().any(|sig| sig.name().name() == name) {
                    return QName::new(
                        Some(schema_name.clone()),
                        name.clone()
                    );
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
    pub fn set_default_schema(&mut self, mut default_schema: Name) -> Name {
        std::mem::swap(&mut self.default_schema, &mut default_schema);
        default_schema
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

    fn get_schema_mut(&mut self, name: &QName) -> Result<&mut Schema> {
        let schema_name = name.schema().unwrap_or(&self.default_schema);
        if let Some(schema) = self.schemas.get_mut(schema_name) {
            Ok(schema)
        } else {
            Err(Error::new(
                ErrorKind::SchemaNotExists,
                None,
                Some(format!("schema {} not found", schema_name)),
                None
            ))
        }
    }

    pub fn create_table(&mut self, create_table: CreateTable) -> Result<()> {
        let schema = self.get_schema_mut(create_table.table().name())?;
        if schema.tables().contains_key(create_table.table().name().name()) {
            if create_table.if_not_exists() {
                return Ok(());
            } else {
                return Err(Error::new(
                    ErrorKind::TableExists,
                    None,
                    Some(format!("table {} already exists", create_table.table().name())),
                    None
                ));
            }
        }
        let name = create_table.table().name().name().clone();
        schema.tables_mut().insert(name, create_table.into());
        Ok(())
    }

    pub fn create_index(&mut self, mut create_index: CreateIndex) -> Result<()> {
        let index_name = create_index.index_mut().ensure_name().clone();
        let schema = self.get_schema_mut(&index_name)?;

        if schema.indices().contains_key(index_name.name()) {
            if create_index.if_not_exists() {
                return Ok(());
            } else {
                return Err(Error::new(
                    ErrorKind::IndexExists,
                    None,
                    Some(format!("index {} already exists", index_name)),
                    None
                ));
            }
        }
        schema.indices_mut().insert(index_name.into_name(), create_index.into());
        Ok(())
    }

    pub fn create_type(&mut self, type_def: impl Into<Rc<TypeDef>>) -> Result<()> {
        let type_def = type_def.into();
        let schema = self.get_schema_mut(type_def.name())?;

        if schema.types().contains_key(type_def.name().name()) {
            return Err(Error::new(
                ErrorKind::TypeExists,
                None,
                Some(format!("type {} already exists", type_def.name())),
                None
            ));
        }
        schema.types_mut().insert(type_def.name().name().clone(), type_def);
        Ok(())
    }

    pub fn create_extension(&mut self, create_extension: CreateExtension) -> Result<()> {
        let schema = self.get_schema_mut(create_extension.extension().name())?;

        let name = create_extension.extension().name().name();
        if schema.extensions().contains_key(name) {
            if create_extension.if_not_exists() {
                return Ok(());
            } else {
                return Err(Error::new(
                    ErrorKind::ExtensionExists,
                    None,
                    Some(format!("extension {} already exists", name)),
                    None
                ));
            }
        }
        schema.extensions_mut().insert(name.clone(), create_extension.into());
        Ok(())
    }

    pub fn create_function(&mut self, create_function: CreateFunction) -> Result<()> {
        let signature = create_function.function().signature();
        let schema = self.get_schema_mut(signature.name())?;

        if create_function.or_replace() {
            schema.functions_mut().insert(
                signature,
                create_function.into_function()
            );
            return Ok(());
        }

        if schema.functions().contains_key(&signature) {
            return Err(Error::new(
                ErrorKind::FunctionExists,
                None,
                Some(format!("function {} already exists", signature)),
                None
            ));
        }

        schema.functions_mut().insert(
            signature,
            create_function.into_function()
        );
        Ok(())
    }

    pub fn create_trigger(&mut self, create_trigger: CreateTrigger) -> Result<()> {
        let schema = self.get_schema_mut(create_trigger.trigger().table_name())?;

        let Some(table) = schema.tables_mut().get_mut(create_trigger.trigger().table_name().name()) else {
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

        for schema in self.schemas.values() {
            for table in schema.tables().values_unordered() {
                for column in table.columns().values_unordered() {
                    if let BasicType::UserDefined { name, .. } = column.data_type().basic_type() {
                        if type_name == name {
                            found_columns.push((table.name(), column));
                        }
                    }
                }
            }
        }

        found_columns
    }

    pub fn alter_type(&mut self, alter_type: &AlterType) -> Result<()> {
        match alter_type.data() {
            AlterTypeData::Rename { new_name } => {
                let schema = self.get_schema_mut(alter_type.type_name())?;

                if schema.types().contains_key(new_name) {
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

                if let Some(mut type_def) = schema.types_mut().remove(alter_type.type_name().name()) {
                    Rc::make_mut(&mut type_def).name_mut().set_name(new_name.clone());
                    schema.types_mut().insert(new_name.clone(), type_def);

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
                if self.has_type(&new_name) {
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

                if let Some(mut type_def) = self.get_schema_mut(alter_type.type_name())?.types_mut().remove(alter_type.type_name().name()) {
                    Rc::make_mut(&mut type_def).set_name(new_name.clone());
                    self.get_schema_mut(&new_name)?.types_mut().insert(new_name.into_name(), type_def);
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
                if let Some(type_def) = self.get_type_mut(alter_type.type_name()) {
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

    pub fn drop_table(&mut self, table_name: &QName) -> Result<()> {
        let schema = self.get_schema_mut(table_name)?;
        
        schema.indices_mut().retain(|_, v| v.table_name() != table_name);
        if schema.tables_mut().remove(table_name.name()).is_none() {
            return Err(Error::new(
                ErrorKind::TableNotExists,
                None,
                Some(format!("table {table_name} not found")),
                None
            ));
        }

        Ok(())
    }

    pub fn alter_table(&mut self, alter_table: &Rc<AlterTable>) -> Result<()> {
        match alter_table.data() {
            AlterTableData::RenameTable { if_exists, new_name } => {
                let schema = self.get_schema_mut(alter_table.name())?;

                if schema.tables().contains_key(new_name) {
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

                if let Some(mut table) = schema.tables_mut().remove(alter_table.name().name()) {
                    Rc::make_mut(&mut table).name_mut().set_name(new_name.clone());
                    schema.tables_mut().insert(new_name.clone(), table);

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
                if self.has_table(&new_name) {
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

                if let Some(mut table) = self.get_schema_mut(alter_table.name())?.tables_mut().remove(alter_table.name().name()) {
                    Rc::make_mut(&mut table).name_mut().set_schema(Some(new_schema.clone()));
                    self.get_schema_mut(&new_name)?.tables_mut().insert(new_name.into_name(), table);

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
                let Some(table) = self.get_table_mut(alter_table.name()) else {
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
                                        AlterColumnData::SetStorage { storage } => {
                                            column.set_storage(*storage);
                                        }
                                        AlterColumnData::SetCompression { compression } => {
                                            column.set_compression(compression.clone());
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

    pub fn alter_extension(&mut self, alter_extension: &Rc<AlterExtension>) -> Result<()> {
        match alter_extension.data() {
            AlterExtensionData::Update(version) => {
                let schema = self.get_schema_mut(alter_extension.name())?;
                let Some(extension) = schema.extensions_mut().get_mut(alter_extension.name().name()) else {
                    return Err(Error::new(
                        ErrorKind::ExtensionNotExists,
                        None,
                        Some(format!("extension {} not found", alter_extension.name())),
                        None
                    ));
                };

                Rc::make_mut(extension).set_version(version.clone());

                Ok(())
            }
            AlterExtensionData::SetSchema(new_schema) => { // XXX: WRONG
                let new_name = QName::new(
                    Some(new_schema.clone()),
                    alter_extension.name().name().clone()
                );

                if self.has_extension(&new_name) {
                    return Err(Error::new(
                        ErrorKind::ExtensionExists,
                        None,
                        Some(format!("extension {} already exists", alter_extension.name())),
                        None
                    ));
                }

                if let Some(mut extension) = self.get_schema_mut(alter_extension.name())?.extensions_mut().remove(alter_extension.name().name()) {
                    Rc::make_mut(&mut extension).set_schema(Some(new_schema.clone()));
                    self.get_schema_mut(&new_name)?.extensions_mut().insert(new_name.into_name(), extension);
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::ExtensionNotExists,
                        None,
                        Some(format!("extension {} not found", alter_extension.name())),
                        None
                    ))
                }
            }
        }
    }

    pub fn create_schema(&mut self, if_not_exists: bool, name: Name) -> Result<()> {
        if self.schemas.contains_key(&name) {
            if !if_not_exists {
                return Err(Error::new(
                    ErrorKind::SchemaExists,
                    None,
                    Some(format!("schema {name} already exists")),
                    None
                ));
            }
        } else {
            self.schemas.insert(name.clone(), Schema::new(name.clone()));
        }

        Ok(())
    }
}
