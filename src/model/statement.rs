use std::rc::Rc;

use crate::format::IsoString;

use super::alter::extension::AlterExtension;
use super::alter::table::AlterTable;
use super::alter::types::AlterType;
use super::extension::{CreateExtension, Extension, Version};
use super::function::{CreateFunction, FunctionSignature};
use super::index::{CreateIndex, Index};
use super::name::Name;
use super::object_ref::ObjectRef;
use super::table::{CreateTable, Table};
use super::trigger::CreateTrigger;
use super::{alter::DropBehavior, name::QName, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(Rc<CreateTable>),
    CreateType(Rc<TypeDef>),
    CreateIndex(Rc<CreateIndex>),
    CreateExtension(Rc<CreateExtension>),
    CreateFunction(Rc<CreateFunction>),
    CreateTrigger(Rc<CreateTrigger>),
    CreateSchema { if_not_exists: bool, name: Name },
    AlterTable(Rc<AlterTable>),
    AlterType(Rc<AlterType>),
    AlterExtension(Rc<AlterExtension>),
    DropTable { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropIndex { concurrently: bool, if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropType { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropExtension { if_exists: bool, names: Vec<QName>, behavior: Option<DropBehavior> },
    DropFunction { if_exists: bool, signatures: Vec<FunctionSignature>, behavior: Option<DropBehavior> },
    DropTrigger { if_exists: bool, name: Name, table_name: QName, behavior: Option<DropBehavior> },
    DropSchema { if_exists: bool, name: Name, behavior: Option<DropBehavior> },
    Comment { object_ref: ObjectRef, comment: Option<Rc<str>> },
}

impl Statement {
    #[inline]
    pub fn create_schema(name: Name) -> Self {
        Self::CreateSchema { if_not_exists: false, name }
    }

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
        Self::DropTable { if_exists: true, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_index(name: QName) -> Self {
        Self::DropIndex { concurrently: false, if_exists: true, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_type(name: QName) -> Self {
        Self::DropType { if_exists: true, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_extension(name: QName) -> Self {
        Self::DropExtension { if_exists: true, names: vec![name], behavior: None }
    }

    #[inline]
    pub fn drop_function(signature: FunctionSignature) -> Self {
        Self::DropFunction { if_exists: true, signatures: vec![signature], behavior: None }
    }

    #[inline]
    pub fn drop_trigger(name: Name, table_name: QName) -> Self {
        Self::DropTrigger { if_exists: true, name, table_name, behavior: None }
    }

    #[inline]
    pub fn drop_schema(name: Name) -> Self {
        Self::DropSchema { if_exists: true, name, behavior: None }
    }

    #[inline]
    pub fn set_logged(table_name: QName, logged: bool) -> Self {
        Self::AlterTable(
            AlterTable::set_logged(table_name, logged)
        )
    }

    #[inline]
    pub fn comment_on(object_ref: ObjectRef, comment: Option<Rc<str>>) -> Self {
        Self::Comment { object_ref, comment }
    }

    #[inline]
    pub fn is_same_variant(&self, other: &Statement) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(table)     => table.fmt(f),
            Self::CreateType(type_def)   => type_def.fmt(f),
            Self::CreateIndex(index)     => index.fmt(f),
            Self::CreateExtension(ext)   => ext.fmt(f),
            Self::CreateFunction(func)   => func.fmt(f),
            Self::CreateTrigger(trigger) => trigger.fmt(f),
            Self::AlterTable(alter)      => alter.fmt(f),
            Self::AlterType(alter)       => alter.fmt(f),
            Self::AlterExtension(alter)  => alter.fmt(f),
            Self::CreateSchema { if_not_exists, name } => {
                write!(f, "{CREATE} {SCHEMA}")?;
                if *if_not_exists {
                    write!(f, " {IF} {NOT} {EXISTS}")?;
                }
                write!(f, " {name};")
            }
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
            Self::DropFunction { if_exists, signatures, behavior } => {
                write!(f, "{DROP} {FUNCTION} ")?;

                if *if_exists {
                    write!(f, "{IF} {EXISTS} ")?;
                }

                let mut iter = signatures.iter();
                if let Some(first) = iter.next() {
                    first.fmt(f)?;
                    for signature in iter {
                        write!(f, ", {signature}")?;
                    }
                }

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::DropTrigger { if_exists, name, table_name, behavior } => {
                write!(f, "{DROP} {TRIGGER} ")?;

                if *if_exists {
                    write!(f, "{IF} {EXISTS} ")?;
                }

                write!(f, " {name} {ON} {table_name}")?;

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::DropSchema { if_exists, name, behavior } => {
                write!(f, "{DROP} {SCHEMA}")?;

                if *if_exists {
                    write!(f, " {IF} {EXISTS}")?;
                }

                write!(f, " {name}")?;

                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }

                f.write_str(";")
            }
            Self::Comment { object_ref, comment } => {
                if let Some(comment) = comment {
                    write!(f, "{COMMENT} {ON} {object_ref} {IS} {};", IsoString(comment))
                } else {
                    write!(f, "{COMMENT} {ON} {object_ref} {IS} {NULL};")
                }
            }
        }
    }
}
