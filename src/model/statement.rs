use std::rc::Rc;

use super::{alter::{AlterTable, AlterType, DropOption}, index::Index, name::Name, table::Table, types::TypeDef};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(Rc<Table>),
    CreateType(Rc<TypeDef>),
    CreateIndex(Rc<Index>),
    AlterTable(Rc<AlterTable>),
    DropTable { if_exists: bool, names: Vec<Name>, drop_option: Option<DropOption> },
    DropIndex { concurrently: bool, if_exists: bool, names: Vec<Name>, drop_option: Option<DropOption> },
    DropType { if_exists: bool, names: Vec<Name>, drop_option: Option<DropOption> },
    AlterType(Rc<AlterType>),
}

impl Statement {
    #[inline]
    pub fn drop_table(name: Name) -> Self {
        Self::DropTable { if_exists: false, names: vec![name], drop_option: None }
    }

    #[inline]
    pub fn drop_index(name: Name) -> Self {
        Self::DropIndex { concurrently: false, if_exists: false, names: vec![name], drop_option: None }
    }

    #[inline]
    pub fn drop_type(name: Name) -> Self {
        Self::DropType { if_exists: false, names: vec![name], drop_option: None }
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(table)   => table.fmt(f),
            Self::CreateType(type_def) => type_def.fmt(f),
            Self::CreateIndex(index)   => index.fmt(f),
            Self::AlterTable(alter)    => alter.fmt(f),
            Self::AlterType(alter)     => alter.fmt(f),
            Self::DropTable { if_exists, names, drop_option } => {
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

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                f.write_str(";")
            },
            Self::DropIndex { concurrently, if_exists, names, drop_option } => {
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

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                f.write_str(";")
            },
            Self::DropType { if_exists, names, drop_option } => {
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

                if let Some(drop_option) = drop_option {
                    write!(f, " {drop_option}")?;
                }

                f.write_str(";")
            },
        }
    }
}
