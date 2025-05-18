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

#[derive(Debug, Clone)]
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
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

impl PartialEq for TableConstraintData {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Check { expr, inherit } => {
                match other {
                    Self::Check { expr: other_expr, inherit: other_inherit } => {
                        inherit == other_inherit && expr == other_expr
                    }
                    _ => false
                }
            },
            Self::Unique { nulls_distinct, columns } => {
                match other {
                    Self::Unique { nulls_distinct: other_nulls_distinct, columns: other_columns } => {
                        nulls_distinct == other_nulls_distinct && columns == other_columns
                    }
                    _ => false
                }
            },
            Self::PrimaryKey { columns } => {
                match other {
                    Self::PrimaryKey { columns: other_columns } => {
                        columns == other_columns
                    }
                    _ => false
                }
            },
            Self::ForeignKey { columns, ref_table, ref_columns, column_match, on_delete, on_update } => {
                match other {
                    Self::ForeignKey {
                        columns: other_columns,
                        ref_table: other_ref_table,
                        ref_columns: other_ref_columns,
                        column_match: other_column_match,
                        on_delete: other_on_delete,
                        on_update: other_on_update,
                    } => {
                        ref_table == other_ref_table &&
                        columns == other_columns &&
                        ref_columns == other_ref_columns && // XXX: false negatives if only one side uses the default
                        column_match.unwrap_or_default() == other_column_match.unwrap_or_default() &&
                        on_delete.as_ref().unwrap_or(&ReferentialAction::NoAction) == other_on_delete.as_ref().unwrap_or(&ReferentialAction::NoAction) &&
                        on_update.as_ref().unwrap_or(&ReferentialAction::NoAction) == other_on_update.as_ref().unwrap_or(&ReferentialAction::NoAction)
                    }
                    _ => false
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableConstraint {
    name: Option<Name>,
    data: TableConstraintData,
    deferrable: Option<bool>,
    initially_deferred: Option<bool>,
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        self.data == other.data &&
        self.default_deferrable() == other.default_deferrable() &&
        self.default_initially_deferred() == other.default_initially_deferred()
    }
}

impl TableConstraint {
    #[inline]
    pub fn new(
        name: Option<Name>,
        data: TableConstraintData,
        deferrable: Option<bool>,
        initially_deferred: Option<bool>,
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
    pub fn deferrable(&self) -> Option<bool> {
        self.deferrable
    }

    #[inline]
    pub fn initially_deferred(&self) -> Option<bool> {
        self.initially_deferred
    }

    #[inline]
    pub fn default_deferrable(&self) -> bool {
        self.deferrable.unwrap_or(false)
    }

    #[inline]
    pub fn default_initially_deferred(&self) -> bool {
        self.initially_deferred.unwrap_or(false)
    }
}
