use super::{name::Name, syntax::{Cursor, Locatable}};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Type {
    cursor: Cursor,
    name: Name,
    data: TypeData
}

impl Type {
    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn data(&self) -> &TypeData {
        &self.data
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeData {
    Enum { values: Vec<String> },
    // TODO: more types
}

impl Locatable for Type {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}
