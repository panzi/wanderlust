use super::{column::{Column, ColumnMatch, ReferentialAction}, name::Name, token::ParsedToken};

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    name: Name,
    columns: Vec<Column>,
    constraints: Vec<TableConstraint>,
}

impl Table {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { name, columns: Vec::new(), constraints: Vec::new() }
    }

    #[inline]
    pub fn with_all(name: Name, columns: Vec<Column>, constraints: Vec<TableConstraint>) -> Self {
        Self { name, columns, constraints }
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
    name: Option<Name>,
    data: TableConstraintData,
    deferrable: bool,
    initially_deferred: bool,
}

impl TableConstraint {
    #[inline]
    pub fn new(
        name: Option<Name>,
        data: TableConstraintData,
        deferrable: bool,
        initially_deferred: bool
    ) -> Self {
        Self { name, data, deferrable, initially_deferred }
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
