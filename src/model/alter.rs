use std::fmt::Debug;

use crate::format::{format_iso_string, write_token_list};

use super::types::ColumnDataType;
use super::{column::Column, name::Name, table::TableConstraint, token::ParsedToken, types::DataType};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    table_name: Name,
    data: AlterTableData,
}

impl std::fmt::Display for AlterTable {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ALTER} {TABLE} {} {};", self.table_name, self.data)
    }
}

impl AlterTable {
    #[inline]
    pub fn new(table_name: Name, data: AlterTableData) -> Self {
        Self { table_name, data }
    }

    #[inline]
    pub fn add_column(table_name: Name, column: Column) -> Self {
        Self { table_name, data: AlterTableData::add_column(column) }
    }

    #[inline]
    pub fn drop_column(table_name: Name, column_name: Name) -> Self {
        Self { table_name, data: AlterTableData::drop_column(column_name) }
    }

    #[inline]
    pub fn alter_column(table_name: Name, alter_column: AlterColumn) -> Self {
        Self { table_name, data: AlterTableData::alter_column(alter_column) }
    }

    #[inline]
    pub fn rename_table(table_name: Name, new_name: Name) -> Self {
        Self { table_name, data: AlterTableData::rename_table(new_name) }
    }

    #[inline]
    pub fn rename_column(table_name: Name, column_name: Name, new_column_name: Name) -> Self {
        Self { table_name, data: AlterTableData::rename_column(column_name, new_column_name) }
    }

    #[inline]
    pub fn rename_constraint(table_name: Name, constraint_name: Name, new_constraint_name: Name) -> Self {
        Self { table_name, data: AlterTableData::rename_constraint(constraint_name, new_constraint_name) }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.table_name
    }

    #[inline]
    pub fn data(&self) -> &AlterTableData {
        &self.data
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableData {
    Actions { actions: Vec<AlterTableAction> },
    RenameTable { new_name: Name },
    RenameColumn { column_name: Name, new_column_name: Name },
    RenameConstraint { constraint_name: Name, new_constraint_name: Name },
}

impl AlterTableData {
    #[inline]
    pub fn add_column(column: Column) -> Self {
        Self::Actions { actions: vec![AlterTableAction::AddColumn { column }] }
    }

    #[inline]
    pub fn drop_column(column_name: Name) -> Self {
        Self::Actions { actions: vec![AlterTableAction::DropColumn { column_name, drop_option: None }] }
    }

    #[inline]
    pub fn alter_column(alter_column: AlterColumn) -> Self {
        Self::Actions { actions: vec![AlterTableAction::AlterColumn { alter_column }] }
    }

    #[inline]
    pub fn rename_table(new_name: Name) -> Self {
        Self::RenameTable { new_name }
    }

    #[inline]
    pub fn rename_column(column_name: Name, new_column_name: Name) -> Self {
        Self::RenameColumn { column_name, new_column_name }
    }

    #[inline]
    pub fn rename_constraint(constraint_name: Name, new_constraint_name: Name) -> Self {
        Self::RenameConstraint { constraint_name, new_constraint_name }
    }
}

impl std::fmt::Display for AlterTableData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actions { actions } => {
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
            },
            Self::RenameTable { new_name } => {
                write!(f, "{RENAME} {TO} {new_name}")
            },
            Self::RenameColumn { column_name, new_column_name } => {
                write!(f, "{RENAME} {COLUMN} {column_name} {TO} {new_column_name}")
            },
            Self::RenameConstraint { constraint_name, new_constraint_name } => {
                write!(f, "{RENAME} {CONSTRAINT} {constraint_name} {TO} {new_constraint_name}")
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
    AddColumn { column: Column },
    DropColumn { column_name: Name, drop_option: Option<DropOption> },
    AlterColumn { alter_column: AlterColumn },
    // TODO: more
}

impl std::fmt::Display for AlterTableAction {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddColumn { column } => {
                write!(f, "{ADD} {COLUMN} {column}")
            },
            Self::DropColumn { column_name, drop_option } => {
                write!(f, "{DROP} {COLUMN} {column_name}")?;

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                Ok(())
            },
            AlterTableAction::AlterColumn { alter_column } => {
                write!(f, "{ALTER} {COLUMN} {alter_column}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumn {
    Type { data_type: ColumnDataType, collate: Option<String>, using: Option<Vec<ParsedToken>> },
    SetDefault { expr: Vec<ParsedToken> },
    DropDefault,
    SetNotNull,
    DropNotNull,
    AddConstraint { constraint: TableConstraint },
    AlterConstraint { constraint_name: Name, deferrable: Option<bool>, initially_deferred: Option<bool> },
    DropConstraint { constraint_name: Name, drop_option: Option<DropOption> },
    // TODO: more
}

impl std::fmt::Display for AlterColumn {
    #[inline]
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Type { data_type, collate, using } => {
                write!(f, "{TYPE} {data_type}")?;

                if let Some(collate) = collate {
                    write!(f, " {COLLATE} ")?;
                    format_iso_string(&mut f, collate)?;
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
            Self::AddConstraint { constraint } => {
                write!(f, "{ADD} {constraint}")
            },
            Self::AlterConstraint { constraint_name, deferrable, initially_deferred } => {
                write!(f, "{ALTER} {constraint_name}")?;

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
            },
            Self::DropConstraint { constraint_name, drop_option } => {
                write!(f, "{DROP} {CONSTRAINT} {constraint_name}")?;

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                Ok(())
            }
        }
    }
}
