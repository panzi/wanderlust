use std::{collections::HashSet, rc::Rc};

use crate::{
    format::{write_paren_names, write_token_list},
    make_tokens,
    model::index::IndexParameters,
    ordered_hash_map::OrderedHashMap,
};

use super::{
    column::{Column, ColumnMatch, ReferentialAction},
    name::{Name, QName},
    token::ParsedToken,
    trigger::Trigger,
};

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
        write!(f, "{CREATE} {TABLE} ")?;

        if !self.table.logged() {
            write!(f, "{UNLOGGED} ")?;
        }

        if self.if_not_exists {
            write!(f, "{IF} {NOT} {EXISTS} ")?;
        }

        self.table.write(f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    name: QName,
    logged: bool,
    columns: OrderedHashMap<Name, Rc<Column>>,
    constraints: OrderedHashMap<Name, Rc<TableConstraint>>,
    triggers: OrderedHashMap<Name, Rc<Trigger>>,
    inherits: Vec<QName>,
    comment: Option<Rc<str>>,
}

impl std::fmt::Display for Table {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.logged {
            write!(f, "{CREATE} {TABLE} ")?;
        } else {
            write!(f, "{CREATE} {UNLOGGED} {TABLE} ")?;
        }
        self.write(f)
    }
}

impl Table {
    #[inline]
    pub fn new(
        name: QName,
        logged: bool,
        columns: OrderedHashMap<Name, Rc<Column>>,
        constraints: OrderedHashMap<Name, Rc<TableConstraint>>,
        triggers: OrderedHashMap<Name, Rc<Trigger>>,
        inherits: Vec<QName>
    ) -> Self {
        Self { name, logged, columns, constraints, triggers, inherits, comment: None }
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

        f.write_str("\n)")?;

        if !self.inherits.is_empty() {
            write!(f, " {INHERITS} (")?;
            let mut iter = self.inherits.iter();
            if let Some(name) = iter.next() {
                std::fmt::Display::fmt(name, f)?;

                for name in iter {
                    write!(f, ", {name}")?;
                }
            }

            write!(f, ")")?;
        }

        f.write_str(";\n")
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
    pub fn logged(&self) -> bool {
        self.logged
    }

    #[inline]
    pub fn set_logged(&mut self, logged: bool) {
        self.logged = logged;
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
    pub fn inherits(&self) -> &[QName] {
        &self.inherits
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

    #[inline]
    pub fn inherits_mut(&mut self) -> &mut Vec<QName> {
        &mut self.inherits
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment;
    }

    pub fn merged_constraints(&self) -> Vec<Rc<TableConstraint>> {
        // TODO: Maybe its more correct to merge column contraints into table constraints directly when parsing?
        let mut merged: Vec<_> = self.constraints.values().cloned().collect();

        for column in self.columns.values_unordered() {
            for constraint in column.constraints() {
                if let Some(table_constraint) = constraint.to_table_constraint(self.name.name(), column.name()) {
                    merged.push(Rc::new(table_constraint));
                }
            }
        }

        merged
    }

    pub fn convert_serial(&mut self) {
        for column in self.columns.values_unordered_mut() {
            if let Some(integer_type) = column.data_type().serial_to_integer() {
                let column = Rc::make_mut(column);
                column.set_data_type(integer_type);
                column.set_not_null();
                let seq_name = format!(
                    "{}_{}_seq",
                    self.name, column.name()
                );

                let mut tokens = Vec::new();
                make_tokens!(&mut tokens, nextval({seq_name}::regclass));
                column.set_default(tokens.into());
            }
        }
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
        index_parameters: IndexParameters,
    },
    PrimaryKey {
        columns: Rc<[Name]>,
        index_parameters: IndexParameters,
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
            Self::Unique { nulls_distinct, columns, index_parameters } => {
                f.write_str(UNIQUE)?;

                if let Some(nulls_distinct) = nulls_distinct {
                    if *nulls_distinct {
                        write!(f, " {NULLS} {DISTINCT}")?;
                    } else {
                        write!(f, " {NULLS} {NOT} {DISTINCT}")?;
                    }
                }

                write_paren_names(columns, f)?;

                index_parameters.fmt(f)
            },
            Self::PrimaryKey { columns, index_parameters } => {
                write!(f, "{PRIMARY} {KEY}")?;

                write_paren_names(columns, f)?;
                index_parameters.fmt(f)
            },
            Self::ForeignKey { columns, ref_table, ref_columns, column_match, on_delete, on_update } => {
                write!(f, "{FOREIGN} {KEY}")?;

                write_paren_names(columns, f)?;

                write!(f, " {REFERENCES} {ref_table}")?;

                if let Some(ref_columns) = ref_columns {
                    write_paren_names(ref_columns, f)?;
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
        &lhs[1..lhs.len() - 1] == rhs
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
            Self::Unique { nulls_distinct, columns, index_parameters } => {
                match other {
                    Self::Unique { nulls_distinct: other_nulls_distinct, columns: other_columns, index_parameters: other_index_parameters } => {
                        nulls_distinct.unwrap_or(true) == other_nulls_distinct.unwrap_or(true) &&
                        columns == other_columns &&
                        index_parameters == other_index_parameters
                    }
                    _ => false
                }
            },
            Self::PrimaryKey { columns, index_parameters } => {
                match other {
                    Self::PrimaryKey { columns: other_columns, index_parameters: other_index_parameters } => {
                        columns == other_columns &&
                        index_parameters == other_index_parameters
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
    comment: Option<Rc<str>>,
}

fn append_constraint_name_columns(constraint_name: &mut String, columns: &[Name]) {
    let mut visited = HashSet::new();
    for column in columns {
        if visited.insert(column) {
            constraint_name.push_str(column.name());
            constraint_name.push_str("_");
        }
    }
}

pub fn make_constraint_name(table_name: &Name, data: &TableConstraintData) -> String {
    let mut constraint_name = table_name.name().to_string();
    constraint_name.push_str("_");
    match data {
        TableConstraintData::Check { .. } => {
            constraint_name.push_str("check");
        }
        TableConstraintData::ForeignKey { columns, .. } => {
            append_constraint_name_columns(&mut constraint_name, columns);
            constraint_name.push_str("fkey");
        }
        TableConstraintData::PrimaryKey { .. } => {
            constraint_name.push_str("pkey");
        }
        TableConstraintData::Unique { columns, .. } => {
            append_constraint_name_columns(&mut constraint_name, columns);
            constraint_name.push_str("key");
        }
    }

    constraint_name
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
        Self { name, data, deferrable, initially_deferred, comment: None }
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn set_name(&mut self, name: Option<Name>) {
        self.name = name;
    }

    pub fn ensure_name(&mut self, table_name: &Name, other_constraints: &OrderedHashMap<Name, Rc<TableConstraint>>) -> &Name {
        if self.name.is_none() {
            let prefix = make_constraint_name(table_name, &self.data);
            let mut name = Name::new(prefix.clone());
            let mut counter = 0u32;

            while other_constraints.contains_key(&name) {
                counter += 1;
                name = Name::new(format!("{prefix}{counter}"));
            }

            self.name = Some(name);
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

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment;
    }

    pub fn columns(&self) -> Option<&Rc<[Name]>> {
        match self.data() {
            TableConstraintData::Check { .. } => None,
            TableConstraintData::Unique { columns, .. } => Some(columns),
            TableConstraintData::PrimaryKey { columns, .. } => Some(columns),
            TableConstraintData::ForeignKey { columns, .. } => Some(columns),
        }
    }

    pub fn eq_content(&self, other: &TableConstraint) -> bool {
        self.data == other.data &&
        self.deferrable == other.deferrable &&
        self.initially_deferred == other.initially_deferred
    }
}
