use std::rc::Rc;

use super::alter::extension::AlterExtension;
use super::alter::table::AlterTable;
use super::alter::types::AlterType;
use super::extension::{CreateExtension, Extension, Version};
use super::index::{CreateIndex, Index};
use super::table::{CreateTable, Table};
use super::{alter::DropBehavior, name::QName, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(Rc<CreateTable>),
    CreateType(Rc<TypeDef>),
    CreateIndex(Rc<CreateIndex>),
    CreateExtension(Rc<CreateExtension>),
    AlterTable(Rc<AlterTable>),
    AlterType(Rc<AlterType>),
    AlterExtension(Rc<AlterExtension>),
    DropTable { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropIndex { concurrently: bool, if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropType { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropExtension { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
}

impl Statement {
    #[inline]
    pub fn create_index(index: impl Into<Rc<Index>>) -> Self {
        Self::CreateIndex(Rc::new(CreateIndex::new(index, false, false)))
    }

    #[inline]
    pub fn create_table(table: impl Into<Rc<Table>>) -> Self {
        Self::CreateTable(Rc::new(CreateTable::new(table, false)))
    }

    #[inline]
    pub fn create_extension(name: QName, version: Option<Version>) -> Self {
        Self::CreateExtension(Rc::new(CreateExtension::new(false, Extension::new(name, version), false)))
    }

    #[inline]
    pub fn drop_table(name: QName) -> Self {
        Self::DropTable { if_exists: false, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_index(name: QName) -> Self {
        Self::DropIndex { concurrently: false, if_exists: false, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_type(name: QName) -> Self {
        Self::DropType { if_exists: false, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_extension(name: QName) -> Self {
        Self::DropExtension { if_exists: false, names: vec![name], behavior: None }
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(table)    => table.fmt(f),
            Self::CreateType(type_def)  => type_def.fmt(f),
            Self::CreateIndex(index)    => index.fmt(f),
            Self::CreateExtension(ext)  => ext.fmt(f),
            Self::AlterTable(alter)     => alter.fmt(f),
            Self::AlterType(alter)      => alter.fmt(f),
            Self::AlterExtension(alter) => alter.fmt(f),
            Self::DropTable { if_exists, names, behavior } => {
                write!(f, "{DROP} {TABLE}")?;
                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }

                let mut iter = names.iter();
                if let Some(first) = iter.next() {
                    write!(f, " {first}")?;
                    for name in iter {
                        write!(f, ", {name}")?;
                    }
                }

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::DropIndex { concurrently, if_exists, names, behavior } => {
                write!(f, "{DROP} {INDEX}")?;
                if *concurrently {
                    write!(f, " {CONCURRENTLY}")?;
                }
                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }

                let mut iter = names.iter();
                if let Some(first) = iter.next() {
                    write!(f, " {first}")?;
                    for name in iter {
                        write!(f, ", {name}")?;
                    }
                }

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::DropType { if_exists, names, behavior } => {
                write!(f, "{DROP} {TYPE}")?;
                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }

                let mut iter = names.iter();
                if let Some(first) = iter.next() {
                    write!(f, " {first}")?;
                    for name in iter {
                        write!(f, ", {name}")?;
                    }
                }

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::DropExtension { if_exists, names, behavior } => {
                write!(f, "{DROP} {EXTENSION} ")?;

                if *if_exists {
                    write!(f, "{IF} {EXISTS} ")?;
                }

                let mut iter = names.iter();
                if let Some(first) = iter.next() {
                    first.fmt(f)?;
                    for name in iter {
                        write!(f, ", {name}")?;
                    }
                }

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
        }
    }
}
