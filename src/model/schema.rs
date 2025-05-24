use std::ops::Deref;
use std::rc::Rc;

use crate::error::{Error, ErrorKind, Result};
use crate::format::IsoString;
use crate::model::alter::{AlterTableAction, AlterTableData, AlterTypeData, ValuePosition};
use crate::model::types::TypeData;
use crate::ordered_hash_map::OrderedHashMap;

use super::alter::{AlterColumnData, AlterTable, AlterType};
use super::column::Column;
use super::index::CreateIndex;
use super::name::{Name, QName};
use super::table::CreateTable;
use super::types::DataType;
use super::{index::Index, table::Table, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    types: OrderedHashMap<QName, Rc<TypeDef>>,
    tables: OrderedHashMap<QName, Rc<Table>>,
    indices: OrderedHashMap<QName, Rc<Index>>,
    search_path: Vec<Name>,
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{BEGIN};\n")?;
        for (_, type_def) in &self.types {
            write!(f, "{type_def}\n")?;
        }
        for (_, table) in &self.tables {
            write!(f, "{table}\n")?;
        }
        for (_, index) in &self.indices {
            write!(f, "{index}\n")?;
        }
        write!(f, "{COMMIT};\n")
    }
}

impl Schema {
    #[inline]
    pub fn new(search_path: impl Into<Vec<Name>>) -> Self {
        Self {
            types: OrderedHashMap::new(),
            tables: OrderedHashMap::new(),
            indices: OrderedHashMap::new(),
            search_path: search_path.into(),
        }
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
    pub fn search_path(&self) -> &[Name] {
        &self.search_path
    }

    #[inline]
    pub fn search_path_mut(&mut self) -> &mut Vec<Name> {
        &mut self.search_path
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

        if self.indices.contains_key(&index_name) {
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

    pub fn find_columns_with_type(&self, type_name: &QName) -> Vec<(&QName, &Rc<Column>)> {
        let mut found_columns = Vec::new();

        for table in self.tables.values_unordered() {
            for column in table.columns().values_unordered() {
                if let DataType::UserDefined { name, .. } = column.data_type().data_type() {
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
                let Some(mut table) = self.tables.get_mut(alter_table.name()) else {
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

                        let table = Rc::make_mut(&mut table);
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

                        let table = Rc::make_mut(&mut table);
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

                                    Rc::make_mut(&mut table).columns_mut().insert(column.name().clone(), column.clone());
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

                                    Rc::make_mut(&mut table).constraints_mut().insert(constraint_name.clone(), constraint);
                                }
                                AlterTableAction::AlterColumn { alter_column } => {
                                    let Some(mut column) = Rc::make_mut(&mut table).columns_mut().get_mut(alter_column.column_name()) else {
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
                                    let column = Rc::make_mut(&mut column);

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
                                    let Some(mut constraint) = Rc::make_mut(&mut table).constraints_mut().get_mut(constraint_name) else {
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

                                    let constraint = Rc::make_mut(&mut constraint);

                                    if deferrable.is_some() {
                                        constraint.set_deferrable(*deferrable);
                                    }

                                    if initially_deferred.is_some() {
                                        constraint.set_initially_deferred(*initially_deferred);
                                    }
                                }
                                AlterTableAction::DropColumn { if_exists, column_name, drop_option } => {
                                    // TODO: CASCADE and RESTRICT will be some work!
                                    if Rc::make_mut(&mut table).columns_mut().remove(column_name).is_none() {
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
                                AlterTableAction::DropConstraint { if_exists, constraint_name, drop_option } => {
                                    // TODO: CASCADE and RESTRICT will be some work!
                                    if Rc::make_mut(&mut table).constraints_mut().remove(constraint_name).is_none() {
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
}
