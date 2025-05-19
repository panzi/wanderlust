use super::index::CreateIndex;
use super::table::CreateTable;
use super::{index::Index, table::Table, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct DDL {
    types: Vec<TypeDef>,
    tables: Vec<Table>,
    indices: Vec<Index>,
}

impl std::fmt::Display for DDL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{BEGIN};\n")?;
        for type_def in &self.types {
            write!(f, "{type_def}\n")?;
        }
        for table in &self.tables {
            write!(f, "{table}\n")?;
        }
        for index in &self.indices {
            write!(f, "{index}\n")?;
        }
        write!(f, "{COMMIT};\n")
    }
}

impl DDL {
    #[inline]
    pub fn new() -> Self {
        Self { types: Vec::new(), tables: Vec::new(), indices: Vec::new() }
    }

    #[inline]
    pub fn types(&self) -> &[TypeDef] {
        &self.types
    }

    #[inline]
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    #[inline]
    pub fn indices(&self) -> &[Index] {
        &self.indices
    }

    pub fn create_table(&mut self, create_table: CreateTable) -> bool {
        // TODO: use some kind of ordered hashtable?
        let exists = self.tables.iter().any(|t| t.name() == create_table.table().name());
        if exists {
            return create_table.if_not_exists();
        }
        self.tables.push(create_table.into());
        true
    }

    pub fn create_index(&mut self, create_index: CreateIndex) -> bool {
        let exists = if let Some(name) = create_index.index().name() {
            self.indices.iter().any(|other|
                if let Some(other_name) = other.name() {
                    other_name == name
                } else {
                    false
                }
            )
        } else {
            false
        };
        if exists {
            return create_index.if_not_exists();
        }
        self.indices.push(create_index.into());
        true
    }

    pub fn create_type(&mut self, type_def: TypeDef) -> bool {
        if self.types.iter().any(|t| t.name() == type_def.name()) {
            return false;
        }
        self.types.push(type_def);
        true
    }
}
