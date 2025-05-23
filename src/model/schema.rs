use std::rc::Rc;

use crate::error::{Error, ErrorKind, Result};
use crate::format::IsoString;
use crate::model::alter::{AlterTypeData, ValuePosition};
use crate::model::types::TypeData;
use crate::ordered_hash_map::OrderedHashMap;

use super::alter::AlterType;
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

    pub fn create_index(&mut self, create_index: CreateIndex) -> bool {
        let schema_name = create_index.index().table_name().schema().cloned();
        let index_name = if let Some(name) = create_index.index().name() {
            name.clone()
        } else {
            QName::new(
                schema_name,
                create_index.index().make_name()
            )
        };

        if self.indices.contains_key(&index_name) {
            return create_index.if_not_exists();
        }
        self.indices.insert(index_name, create_index.into());
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
            for column in table.columns() {
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

                if let Some(type_def) = self.types.remove(alter_type.type_name()) {
                    let type_def = type_def.with_name(new_name.clone());
                    self.types.insert(new_name.clone(), Rc::new(type_def));

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

                            if let Some(type_def) = self.types.remove(alter_type.type_name()) {
                                let type_def = type_def.with_name(new_name.clone());
                                self.types.insert(new_name, Rc::new(type_def));
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
                        }
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
}
