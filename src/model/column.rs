use super::{name::Name, syntax::{Cursor, Locatable}, token::Token, types::ColumnDataType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    cursor: Cursor,
    name: Name,
    data_type: ColumnDataType,
    collation: Option<String>,
    constraints: Vec<ColumnConstraint>,
}

impl Column {
    #[inline]
    pub fn new(cursor: Cursor, name: Name, data_type: ColumnDataType, collation: Option<impl Into<String>>, constraints: Vec<ColumnConstraint>) -> Self {
        Self { cursor, name, data_type, collation: collation.map(|c| c.into()), constraints }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn data_type(&self) -> &ColumnDataType {
        &self.data_type
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
    name: Option<Name>,
    data: ColumnConstraintData,
    deferrable: bool,
    initially_deferred: bool,
}

impl ColumnConstraint {
    #[inline]
    pub fn new(
        cursor: Cursor,
        name: Option<Name>,
        data: ColumnConstraintData,
        deferrable: bool,
        initially_deferred: bool
    ) -> Self {
        Self { cursor, name, data, deferrable, initially_deferred }
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn data(&self) -> &ColumnConstraintData {
        &self.data
    }

    #[inline]
    pub fn deferrable(&self) -> bool {
        self.deferrable
    }

    #[inline]
    pub fn initially_deferred(&self) -> bool {
        self.initially_deferred
    }
}

impl Locatable for ColumnConstraint {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}
