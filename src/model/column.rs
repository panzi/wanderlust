use super::{name::Name, syntax::{Cursor, Locatable}, token::Token};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    cursor: Cursor,
    name: Name,
    type_name: Name,
    collation: Option<String>,
    constraints: Vec<ColumnConstraint>,
}

impl Column {
    #[inline]
    pub fn new(cursor: Cursor, name: Name, type_name: Name, collation: Option<impl Into<String>>) -> Self {
        Self { cursor, name, type_name, collation: collation.map(|c| c.into()), constraints: Vec::new() }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn type_name(&self) -> &Name {
        &self.type_name
    }

    #[inline]
    pub fn collation(&self) -> Option<&str> {
        self.collation.as_deref()
    }

    #[inline]
    pub fn constraints(&self) -> &[ColumnConstraint] {
        &self.constraints
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut Vec<ColumnConstraint> {
        &mut self.constraints
    }

}

impl Locatable for Column {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMatch {
    Full,
    Partial,
    Simple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull { columns: Option<Vec<Name>> },
    SetDefault { columns: Option<Vec<Name>> },
}

impl Default for ReferentialAction {
    #[inline]
    fn default() -> Self {
        Self::NoAction
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnConstraintData {
    Null,
    NotNull,
    Check {
        expr: Vec<Token>,
        inherit: bool,
    },
    Default { value: Vec<Token> },
    Unique { nulls_distinct: bool },
    PrimaryKey,
    References {
        table_name: Name,
        column: Option<Name>,
        column_match: ColumnMatch,
        on_update: ReferentialAction,
        on_delete: ReferentialAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnConstraint {
    cursor: Cursor,
    name: Option<String>,
    data: ColumnConstraintData,
    deferrable: bool,
    initially_deferred: bool,
}

impl Locatable for ColumnConstraint {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}
