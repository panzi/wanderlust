use std::{ops::Deref, rc::Rc};

use crate::{format::{write_paren_names, write_token_list}, ordered_hash_map::OrderedHashMap};

use super::{column::{Column, ColumnMatch, ReferentialAction}, name::{Name, QName}, token::ParsedToken, trigger::Trigger};

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
    name: QName,
    columns: OrderedHashMap<Name, Rc<Column>>,
    constraints: OrderedHashMap<Name, Rc<TableConstraint>>,
    triggers: OrderedHashMap<Name, Rc<Trigger>>,
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
    pub fn new(
        name: QName,
        columns: OrderedHashMap<Name, Rc<Column>>,
        constraints: OrderedHashMap<Name, Rc<TableConstraint>>,
        triggers: OrderedHashMap<Name, Rc<Trigger>>
    ) -> Self {
        Self { name, columns, constraints, triggers }
    }

    fn write(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{} (", self.name)?;

        let mut iter = self.columns.values();
        if let Some(first) = iter.next() {
            write!(f, "    {first}")?;

            for column in iter {
                write!(f, ",\n    {column}")?;
            }

            for constraint in self.constraints.values() {
                write!(f, ",\n    {constraint}")?;
            }
        } else {
            let mut iter = self.constraints.values();

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
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn set_name(&mut self, name: QName) {
        self.name = name;
    }

    #[inline]
    pub fn name_mut(&mut self) -> &mut QName {
        &mut self.name
    }

    #[inline]
    pub fn columns(&self) -> &OrderedHashMap<Name, Rc<Column>> {
        &self.columns
    }

    #[inline]
    pub fn constraints(&self) -> &OrderedHashMap<Name, Rc<TableConstraint>> {
        &self.constraints
    }

    #[inline]
    pub fn triggers(&self) -> &OrderedHashMap<Name, Rc<Trigger>> {
        &self.triggers
    }

    #[inline]
    pub fn columns_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<Column>> {
        &mut self.columns
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<TableConstraint>> {
        &mut self.constraints
    }

    #[inline]
    pub fn triggers_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<Trigger>> {
        &mut self.triggers
    }

    pub fn merged_constraints(&self) -> Vec<Rc<TableConstraint>> {
        // TODO: Maybe its more correct to merge column contraints into table constraints directly when parsing?
        let mut merged: Vec<_> = self.constraints.values().cloned().collect();

        for column in self.columns.values_unordered() {
            for constraint in column.constraints() {
                if let Some(table_constraint) = constraint.to_table_constraint(column.name()) {
                    merged.push(Rc::new(table_constraint));
                }
            }
        }

        merged
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
        columns: Rc<[Name]>,
    },
    PrimaryKey {
        columns: Rc<[Name]>,
    },
    ForeignKey {
        columns: Rc<[Name]>,
        ref_table: QName,
        ref_columns: Option<Rc<[Name]>>,
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

fn eq_expr(lhs: &[ParsedToken], rhs: &[ParsedToken]) -> bool {
    if lhs.len() < rhs.len() {
        rhs.starts_with(&[ParsedToken::LParen]) &&
        rhs.ends_with(&[ParsedToken::RParen]) &&
        lhs == &rhs[1..rhs.len() - 1]
    } else if lhs.len() > rhs.len() {
        lhs.starts_with(&[ParsedToken::LParen]) &&
        lhs.ends_with(&[ParsedToken::RParen]) &&
        &lhs[1..rhs.len() - 1] == rhs
    } else {
        lhs == rhs
    }
}

impl PartialEq for TableConstraintData {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Check { expr, inherit } => {
                match other {
                    Self::Check { expr: other_expr, inherit: other_inherit } => {
                        *inherit == *other_inherit &&
                        eq_expr(expr, other_expr)
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
                        ref_columns.as_ref().unwrap_or(columns) == other_ref_columns.as_ref().unwrap_or(columns) &&
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

pub fn make_constraint_name(data: &TableConstraintData) -> Name {
    let mut constraint_name = String::new();
    match data {
        TableConstraintData::Check { expr, .. } => {
            for token in expr.deref() {
                if let ParsedToken::Name(name) = token {
                    constraint_name.push_str(name.name());
                    constraint_name.push_str("_");
                }
            }
            constraint_name.push_str("check");
        }
        TableConstraintData::ForeignKey { columns, .. } => {
            for column in columns.deref() {
                constraint_name.push_str(column.name());
                constraint_name.push_str("_");
            }
            constraint_name.push_str("fkey");
        }
        TableConstraintData::PrimaryKey { columns } => {
            for column in columns.deref() {
                constraint_name.push_str(column.name());
                constraint_name.push_str("_");
            }
            constraint_name.push_str("pkey");
        }
        TableConstraintData::Unique { columns, .. } => {
            for column in columns.deref() {
                constraint_name.push_str(column.name());
                constraint_name.push_str("_");
            }
            constraint_name.push_str("unique");
        }
    }

    Name::new(constraint_name)
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
    pub fn set_name(&mut self, name: Option<Name>) {
        self.name = name;
    }

    pub fn ensure_name(&mut self) -> &Name {
        if self.name.is_none() {
            self.name = Some(make_constraint_name(&self.data));
        }
        self.name.as_ref().unwrap()
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
    pub fn set_deferrable(&mut self, value: Option<bool>) {
        self.deferrable = value;
    }

    #[inline]
    pub fn initially_deferred(&self) -> Option<bool> {
        self.initially_deferred
    }

    #[inline]
    pub fn set_initially_deferred(&mut self, value: Option<bool>) {
        self.initially_deferred = value;
    }

    #[inline]
    pub fn default_deferrable(&self) -> bool {
        self.deferrable.unwrap_or(false)
    }

    #[inline]
    pub fn default_initially_deferred(&self) -> bool {
        self.initially_deferred.unwrap_or(false)
    }

    pub fn matches(&self, other: &TableConstraint) -> bool {
        self.data == other.data &&
        self.default_deferrable() == other.default_deferrable() &&
        self.default_initially_deferred() == other.default_initially_deferred()
    }

    pub fn columns(&self) -> Option<&Rc<[Name]>> {
        match self.data() {
            TableConstraintData::Check { .. } => None,
            TableConstraintData::Unique { columns, .. } => Some(columns),
            TableConstraintData::PrimaryKey { columns, .. } => Some(columns),
            TableConstraintData::ForeignKey { columns, .. } => Some(columns),
        }
    }
}
