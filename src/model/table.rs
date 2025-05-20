use std::rc::Rc;

use crate::format::{write_paren_names, write_token_list};

use super::{column::{Column, ColumnMatch, ReferentialAction}, name::Name, token::ParsedToken};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    table: Rc<Table>,
    if_not_exists: bool,
}

impl CreateTable {
    #[inline]
    pub fn new(table: impl Into<Rc<Table>>, if_not_exists: bool) -> Self {
        Self { table: table.into(), if_not_exists }
    }

    #[inline]
    pub fn table(&self) -> &Rc<Table> {
        &self.table
    }

    #[inline]
    pub fn into_table(self) -> Rc<Table> {
        self.table
    }

    #[inline]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl From<CreateTable> for Rc<Table> {
    #[inline]
    fn from(value: CreateTable) -> Self {
        value.table
    }
}

impl std::fmt::Display for CreateTable {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.if_not_exists {
            write!(f, "{CREATE} {TABLE} {IF} {NOT} {EXISTS} ")?;
        } else {
            write!(f, "{CREATE} {TABLE} ")?;
        }
        self.table.write(f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    name: Name,
    columns: Vec<Rc<Column>>,
    constraints: Vec<Rc<TableConstraint>>,
}

impl std::fmt::Display for Table {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} {TABLE} ")?;
        self.write(f)
    }
}

impl Table {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { name, columns: Vec::new(), constraints: Vec::new() }
    }

    fn write(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (\n", self.name)?;

        let mut iter = self.columns.iter();
        if let Some(first) = iter.next() {
            write!(f, "    {first}")?;

            for column in iter {
                write!(f, ",\n    {column}")?;
            }

            for constraint in &self.constraints {
                write!(f, ",\n    {constraint}")?;
            }
        } else {
            let mut iter = self.constraints.iter();

            if let Some(first) = iter.next() {
                write!(f, "    {first}")?;

                for constraint in iter {
                    write!(f, ",\n    {constraint}")?;
                }
            }
        }

        write!(f, "\n);")
    }

    #[inline]
    pub fn with_all(name: Name, columns: Vec<Rc<Column>>, constraints: Vec<Rc<TableConstraint>>) -> Self {
        Self { name, columns, constraints }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn columns(&self) -> &[Rc<Column>] {
        &self.columns
    }

    #[inline]
    pub fn constraints(&self) -> &[Rc<TableConstraint>] {
        &self.constraints
    }

    #[inline]
    pub fn columns_mut(&mut self) -> &mut Vec<Rc<Column>> {
        &mut self.columns
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut Vec<Rc<TableConstraint>> {
        &mut self.constraints
    }
}

#[derive(Debug, Clone)]
pub enum TableConstraintData {
    Check {
        expr: Rc<[ParsedToken]>,
        inherit: bool,
    },
    Unique {
        nulls_distinct: Option<bool>,
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

impl std::fmt::Display for TableConstraintData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Check { expr, inherit } => {
                write!(f, "{CHECK} (")?;
                write_token_list(expr, f)?;
                if *inherit {
                    f.write_str(")")
                } else {
                    write!(f, ") {NO} {INHERIT}")
                }
            },
            Self::Unique { nulls_distinct, columns } => {
                f.write_str(UNIQUE)?;

                if let Some(nulls_distinct) = nulls_distinct {
                    if *nulls_distinct {
                        write!(f, " {NULLS} {DISTINCT}")?;
                    } else {
                        write!(f, " {NULLS} {NOT} {DISTINCT}")?;
                    }
                }

                write_paren_names(columns, f)
            },
            Self::PrimaryKey { columns } => {
                write!(f, "{PRIMARY} {KEY} ")?;

                write_paren_names(columns, f)
            },
            Self::ForeignKey { columns, ref_table, ref_columns, column_match, on_delete, on_update } => {
                write!(f, "{FOREIGN} {KEY} ")?;

                write_paren_names(&columns, f)?;

                write!(f, " {REFERENCES} {ref_table}")?;

                if let Some(ref_columns) = ref_columns {
                    f.write_str(" ")?;
                    write_paren_names(&ref_columns, f)?;
                }

                if let Some(column_match) = column_match {
                    write!(f, " {column_match}")?;
                }

                if let Some(on_delete) = on_delete {
                    write!(f, " {ON} {DELETE} {on_delete}")?;
                }

                if let Some(on_update) = on_update {
                    write!(f, " {ON} {UPDATE} {on_update}")?;
                }

                Ok(())
            }
        }
    }
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
                        nulls_distinct.unwrap_or(true) == other_nulls_distinct.unwrap_or(true) &&
                        columns == other_columns
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

impl std::fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{CONSTRAINT} {name} ")?;
        }

        self.data.fmt(f)?;

        if let Some(deferrable) = self.deferrable {
            if deferrable {
                write!(f, " {DEFERRABLE}")?;
            } else {
                write!(f, " {NOT} {DEFERRABLE}")?;
            }
        }

        if let Some(initially_deferred) = self.initially_deferred {
            if initially_deferred {
                write!(f, " {INITIALLY} {DEFERRED}")?;
            } else {
                write!(f, " {INITIALLY} {IMMEDIATE}")?;
            }
        }

        Ok(())
    }
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
