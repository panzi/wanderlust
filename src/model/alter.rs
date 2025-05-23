use std::fmt::Debug;
use std::rc::Rc;

use crate::format::{format_iso_string, write_token_list};

use super::name::QName;
use super::types::ColumnDataType;
use super::{column::Column, name::Name, table::TableConstraint, token::ParsedToken};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    table_name: QName,
    data: AlterTableData,
}

impl std::fmt::Display for AlterTable {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ALTER} {TABLE}")?;

        if self.data().if_exists() {
            write!(f, " {IF} {EXISTS}")?
        }

        if self.data.only() {
            write!(f, " {ONLY}")?
        }

        write!(f, " {} {};", self.table_name, self.data)
    }
}

impl AlterTable {
    #[inline]
    pub fn new(table_name: QName, data: AlterTableData) -> Self {
        Self { table_name, data }
    }

    #[inline]
    pub fn add_column(table_name: QName, column: Rc<Column>) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::add_column(column) })
    }

    #[inline]
    pub fn drop_column(table_name: QName, column_name: Name) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::drop_column(column_name) })
    }

    #[inline]
    pub fn alter_column(table_name: QName, alter_column: AlterColumn) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::alter_column(alter_column) })
    }

    #[inline]
    pub fn rename_table(table_name: QName, new_name: Name) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::rename_table(new_name) })
    }

    #[inline]
    pub fn rename_column(table_name: QName, column_name: Name, new_column_name: Name) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::rename_column(column_name, new_column_name) })
    }

    #[inline]
    pub fn rename_constraint(table_name: QName, constraint_name: Name, new_constraint_name: Name) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::rename_constraint(constraint_name, new_constraint_name) })
    }

    #[inline]
    pub fn add_constraint(table_name: QName, constraint: impl Into<Rc<TableConstraint>>) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::add_constraint(constraint) })
    }

    #[inline]
    pub fn alter_constraint(table_name: QName, constraint_name: Name, deferrable: Option<bool>, initially_deferred: Option<bool>) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::alter_constraint(constraint_name, deferrable, initially_deferred) })
    }

    #[inline]
    pub fn drop_constraint(table_name: QName, constraint_name: Name, drop_option: Option<DropOption>) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::drop_constraint(constraint_name, drop_option) })
    }

    #[inline]
    pub fn set_schema(table_name: QName, new_schema: Name) -> Rc<Self> {
        Rc::new(Self { table_name, data: AlterTableData::set_schema(new_schema) })
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.table_name
    }

    #[inline]
    pub fn data(&self) -> &AlterTableData {
        &self.data
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Owner {
    User(Name),
    CurrentRole,
    CurrentUser,
    SessionUser,
}

impl std::fmt::Display for Owner {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User(name) => std::fmt::Display::fmt(&name, f),
            Self::CurrentRole => f.write_str(CURRENT_ROLE),
            Self::CurrentUser => f.write_str(CURRENT_USER),
            Self::SessionUser => f.write_str(SESSION_USER),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableData {
    Actions { if_exists: bool, only: bool, actions: Rc<[AlterTableAction]> },
    RenameTable { if_exists: bool, new_name: Name },
    RenameColumn { if_exists: bool, only: bool, column_name: Name, new_column_name: Name },
    RenameConstraint { if_exists: bool, only: bool, constraint_name: Name, new_constraint_name: Name },
    SetSchema { if_exists: bool, new_schema: Name },
}

impl AlterTableData {
    #[inline]
    pub fn add_column(column: Rc<Column>) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::AddColumn { if_not_exists: false, column }].into() }
    }

    #[inline]
    pub fn drop_column(column_name: Name) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::DropColumn { if_exists: false, column_name, drop_option: None }].into() }
    }

    #[inline]
    pub fn alter_column(alter_column: AlterColumn) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::AlterColumn { alter_column }].into() }
    }

    #[inline]
    pub fn rename_table(new_name: Name) -> Self {
        Self::RenameTable { if_exists: false, new_name }
    }

    #[inline]
    pub fn rename_column(column_name: Name, new_column_name: Name) -> Self {
        Self::RenameColumn { if_exists: false, only: false, column_name, new_column_name }
    }

    #[inline]
    pub fn rename_constraint(constraint_name: Name, new_constraint_name: Name) -> Self {
        Self::RenameConstraint { if_exists: false, only: false, constraint_name, new_constraint_name }
    }

    #[inline]
    pub fn add_constraint(constraint: impl Into<Rc<TableConstraint>>) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::AddConstraint { constraint: constraint.into() }].into() }
    }

    #[inline]
    pub fn alter_constraint(constraint_name: Name, deferrable: Option<bool>, initially_deferred: Option<bool>) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::AlterConstraint { constraint_name, deferrable, initially_deferred }].into() }
    }

    #[inline]
    pub fn drop_constraint(constraint_name: Name, drop_option: Option<DropOption>) -> Self {
        Self::Actions { if_exists: false, only: false, actions: [AlterTableAction::DropConstraint { if_exists: false, constraint_name, drop_option }].into() }
    }

    #[inline]
    pub fn set_schema(new_schema: Name) -> Self {
        Self::SetSchema { if_exists: false, new_schema }
    }

    #[inline]
    pub fn only(&self) -> bool {
        matches!(self,
            Self::Actions { only: true, .. } |
            Self::RenameColumn { only: true, .. } |
            Self::RenameConstraint { only: true, .. }
        )
    }

    #[inline]
    pub fn if_exists(&self) -> bool {
        matches!(self,
            Self::Actions { if_exists: true, .. } |
            Self::RenameTable { if_exists: true, .. } |
            Self::RenameColumn { if_exists: true, .. } |
            Self::RenameConstraint { if_exists: true, .. } |
            Self::SetSchema { if_exists: true, .. }
        )
    }
}

impl std::fmt::Display for AlterTableData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actions { if_exists: _, only: _, actions } => {
                if actions.len() > 1 {
                    let mut iter = actions.iter();
                    if let Some(first) = iter.next() {
                        write!(f, "\n{first}")?;
                        for action in iter {
                            write!(f, ",\n{action}")?;
                        }
                    }
                } else if let Some(first) = actions.first() {
                    std::fmt::Display::fmt(first, f)?;
                }

                Ok(())
            }
            Self::RenameTable { if_exists: _, new_name } => {
                write!(f, "{RENAME} {TO} {new_name}")
            }
            Self::RenameColumn { if_exists: _, only: _, column_name, new_column_name } => {
                write!(f, "{RENAME} {COLUMN} {column_name} {TO} {new_column_name}")
            }
            Self::RenameConstraint { if_exists: _, only: _, constraint_name, new_constraint_name } => {
                write!(f, "{RENAME} {CONSTRAINT} {constraint_name} {TO} {new_constraint_name}")
            }
            Self::SetSchema { if_exists: _, new_schema } => {
                write!(f, "{SET} {SCHEMA} {new_schema}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropOption {
    Restrict,
    Cascade,
}

impl std::fmt::Display for DropOption {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Restrict => f.write_str(RESTRICT),
            Self::Cascade => f.write_str(CASCADE),
        }
    }
}

impl Default for DropOption {
    #[inline]
    fn default() -> Self {
        Self::Restrict
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn { if_not_exists: bool, column: Rc<Column> },
    DropColumn { if_exists: bool, column_name: Name, drop_option: Option<DropOption> },
    AlterColumn { alter_column: AlterColumn },
    OwnerTo { new_owner: Owner },
    AddConstraint { constraint: Rc<TableConstraint> },
    AlterConstraint { constraint_name: Name, deferrable: Option<bool>, initially_deferred: Option<bool> },
    DropConstraint { if_exists: bool, constraint_name: Name, drop_option: Option<DropOption> },
    // TODO: more
}

impl std::fmt::Display for AlterTableAction {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddColumn { if_not_exists, column } => {
                write!(f, "{ADD} {COLUMN}")?;
                if *if_not_exists {
                    write!(f, " {IF} {NOT} {EXISTS}")?;
                }
                write!(f, " {column}")
            }
            Self::DropColumn { if_exists, column_name, drop_option } => {
                write!(f, "{DROP} {COLUMN}")?;
                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }
                write!(f, " {column_name}")?;

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                Ok(())
            }
            Self::AlterColumn { alter_column } => {
                write!(f, "{ALTER} {COLUMN} {alter_column}")
            }
            Self::OwnerTo { new_owner } => {
                write!(f, "{OWNER} {TO} {new_owner}")
            }
            Self::AddConstraint { constraint } => {
                write!(f, "{ADD} {constraint}")
            }
            Self::AlterConstraint { constraint_name, deferrable, initially_deferred } => {
                write!(f, "{ALTER} {CONSTRAINT} {constraint_name}")?;

                if let Some(deferrable) = deferrable {
                    if *deferrable {
                        write!(f, " {DEFERRABLE}")?;
                    } else {
                        write!(f, " {NOT} {DEFERRABLE}")?;
                    }
                }

                if let Some(initially_deferred) = initially_deferred {
                    if *initially_deferred {
                        write!(f, " {INITIALLY} {DEFERRED}")?;
                    } else {
                        write!(f, " {INITIALLY} {IMMEDIATE}")?;
                    }
                }

                Ok(())
            }
            Self::DropConstraint { if_exists, constraint_name, drop_option } => {
                write!(f, "{DROP} {CONSTRAINT}")?;
                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }
                write!(f, " {constraint_name}")?;

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterColumn {
    column_name: Name,
    data: AlterColumnData,
}

impl AlterColumn {
    #[inline]
    pub fn new(column_name: Name, data: AlterColumnData) -> Self {
        Self { column_name, data }
    }

    #[inline]
    pub fn change_type(column_name: Name, data_type: Rc<ColumnDataType>, collation: Option<Name>, using: Option<Rc<[ParsedToken]>>) -> Self {
        Self { column_name, data: AlterColumnData::Type { data_type, collation, using } }
    }

    #[inline]
    pub fn set_default(column_name: Name, expr: Rc<[ParsedToken]>) -> Self {
        Self { column_name, data: AlterColumnData::SetDefault { expr } }
    }

    #[inline]
    pub fn drop_default(column_name: Name) -> Self {
        Self { column_name, data: AlterColumnData::DropDefault }
    }

    #[inline]
    pub fn set_not_null(column_name: Name) -> Self {
        Self { column_name, data: AlterColumnData::SetNotNull }
    }

    #[inline]
    pub fn drop_not_null(column_name: Name) -> Self {
        Self { column_name, data: AlterColumnData::DropNotNull }
    }

    #[inline]
    pub fn column_name(&self) -> &Name {
        &self.column_name
    }

    #[inline]
    pub fn data(&self) -> &AlterColumnData {
        &self.data
    }
}

impl std::fmt::Display for AlterColumn {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.column_name, self.data)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnData {
    Type { data_type: Rc<ColumnDataType>, collation: Option<Name>, using: Option<Rc<[ParsedToken]>> },
    SetDefault { expr: Rc<[ParsedToken]> },
    DropDefault,
    SetNotNull,
    DropNotNull,
    // TODO: more
}

impl std::fmt::Display for AlterColumnData {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Type { data_type, collation, using } => {
                write!(f, "{TYPE} {data_type}")?;

                if let Some(collation) = collation {
                    write!(f, " {COLLATE} {collation}")?;
                }

                if let Some(using) = using {
                    if !using.is_empty() {
                        write!(f, " {USING} ")?;
                        write_token_list(using, f)?;
                    }
                }

                Ok(())
            },
            Self::SetDefault { expr } => {
                write!(f, "{SET} {DEFAULT} ")?;
                write_token_list(expr, f)
            },
            Self::DropDefault => {
                write!(f, "{DROP} {DEFAULT}")
            },
            Self::SetNotNull => {
                write!(f, "{SET} {NOT} {NULL}")
            },
            Self::DropNotNull => {
                write!(f, "{DROP} {NOT} {NULL}")
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterType {
    type_name: QName,
    data: AlterTypeData,
}

impl AlterType {
    #[inline]
    pub fn new(type_name: QName, data: AlterTypeData) -> Self {
        Self { type_name, data }
    }

    #[inline]
    pub fn rename(type_name: QName, new_name: QName) -> Rc<Self> {
        Rc::new(Self { type_name, data: AlterTypeData::Rename { new_name } })
    }

    #[inline]
    pub fn add_value(type_name: QName, value: impl Into<Rc<str>>, position: Option<ValuePosition>) -> Rc<Self> {
        Rc::new(Self { type_name, data: AlterTypeData::AddValue { if_not_exists: true, value: value.into(), position } })
    }

    #[inline]
    pub fn rename_value(type_name: QName, existing_value: impl Into<Rc<str>>, new_value: impl Into<Rc<str>>) -> Rc<Self> {
        Rc::new(Self { type_name, data: AlterTypeData::RenameValue { existing_value: existing_value.into(), new_value: new_value.into() } })
    }

    #[inline]
    pub fn owner_to(type_name: QName, new_owner: Owner) -> Rc<Self> {
        Rc::new(Self { type_name, data: AlterTypeData::OwnerTo { new_owner } })
    }

    #[inline]
    pub fn set_schema(type_name: QName, new_schema: Name) -> Rc<Self> {
        Rc::new(Self { type_name, data: AlterTypeData::SetSchema { new_schema }})
    }

    #[inline]
    pub fn type_name(&self) -> &QName {
        &self.type_name
    }

    #[inline]
    pub fn data(&self) -> &AlterTypeData {
        &self.data
    }
}

impl std::fmt::Display for AlterType {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ALTER} {TYPE} {} {};", self.type_name, self.data)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTypeData {
    Rename { new_name: QName },
    AddValue { if_not_exists: bool, value: Rc<str>, position: Option<ValuePosition> },
    RenameValue { existing_value: Rc<str>, new_value: Rc<str> },
    OwnerTo { new_owner: Owner },
    SetSchema { new_schema: Name },
}

impl std::fmt::Display for AlterTypeData {
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rename { new_name } => {
                write!(f, "{RENAME} {TO} {new_name}")
            }
            Self::AddValue { if_not_exists, value, position } => {
                write!(f, "{ADD} {VALUE} ")?;

                if *if_not_exists {
                    write!(f, "{IF} {NOT} {EXISTS} ")?;
                }

                format_iso_string(&mut f, value)?;

                if let Some(position) = position {
                    write!(f, " {position}")?;
                }

                Ok(())
            }
            Self::RenameValue { existing_value, new_value } => {
                write!(f, "{RENAME} {VALUE} ")?;
                format_iso_string(&mut f, existing_value)?;
                write!(f, " {TO} ")?;
                format_iso_string(&mut f, new_value)
            }
            Self::OwnerTo { new_owner } => {
                write!(f, "{OWNER} {TO} {new_owner}")
            }
            Self::SetSchema { new_schema } => {
                write!(f, "{SET} {SCHEMA} {new_schema}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValuePosition {
    Before(Rc<str>),
    After(Rc<str>),
}

impl ValuePosition {
    #[inline]
    pub fn before(value: impl Into<Rc<str>>) -> Self {
        Self::Before(value.into())
    }

    #[inline]
    pub fn after(value: impl Into<Rc<str>>) -> Self {
        Self::After(value.into())
    }
}

impl std::fmt::Display for ValuePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Before(value) => {
                write!(f, "{BEFORE} ")?;
                format_iso_string(f, value)
            }
            Self::After(value) => {
                write!(f, "{AFTER} ")?;
                format_iso_string(f, value)
            }
        }
    }
}
