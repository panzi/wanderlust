use super::{column::{Column, ColumnMatch, ReferentialAction}, name::Name, syntax::{Cursor, Locatable}, token::ParsedToken};

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    cursor: Cursor,
    name: Name,
    columns: Vec<Column>,
    constraints: Vec<TableConstraint>,
}

impl Table {
    #[inline]
    pub fn new(cursor: Cursor, name: Name) -> Self {
        Self { cursor, name, columns: Vec::new(), constraints: Vec::new() }
    }

    #[inline]
    pub fn with_all(cursor: Cursor, name: Name, columns: Vec<Column>, constraints: Vec<TableConstraint>) -> Self {
        Self { cursor, name, columns, constraints }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    #[inline]
    pub fn constraints(&self) -> &[TableConstraint] {
        &self.constraints
    }

    #[inline]
    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut Vec<TableConstraint> {
        &mut self.constraints
    }
}

impl Locatable for Table {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintData {
    Check { expr: Vec<ParsedToken>, inherit: bool },
    Unique {
        nulls_distinct: bool,
        columns: Vec<Name>,
    },
    PrimaryKey {
        columns: Vec<Name>,
    },
    ForeignKey {
        columns: Vec<Name>,
        ref_table: Name,
        ref_columns: Option<Vec<Name>>,
        column_match: Option<ColumnMatch>,
        on_update: ReferentialAction,
        on_delete: ReferentialAction,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    cursor: Cursor,
    name: Option<Name>,
    data: TableConstraintData,
    deferrable: bool,
    initially_deferred: bool,
}

impl TableConstraint {
    #[inline]
    pub fn new(
        cursor: Cursor,
        name: Option<Name>,
        data: TableConstraintData,
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
    pub fn data(&self) -> &TableConstraintData {
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

impl Locatable for TableConstraint {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}
