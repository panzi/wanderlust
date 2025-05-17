use super::{index::Index, table::Table, types::TypeDef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DDL {
    types: Vec<TypeDef>,
    tables: Vec<Table>,
    indices: Vec<Index>,
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
