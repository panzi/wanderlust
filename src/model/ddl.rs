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

    #[inline]
    pub fn types_mut(&mut self) -> &mut Vec<TypeDef> {
        &mut self.types
    }

    #[inline]
    pub fn tables_mut(&mut self) -> &mut Vec<Table> {
        &mut self.tables
    }

    #[inline]
    pub fn indices_mut(&mut self) -> &mut Vec<Index> {
        &mut self.indices
    }
}
