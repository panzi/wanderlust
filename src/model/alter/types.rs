use std::rc::Rc;

use crate::format::format_iso_string;

use crate::model::name::{Name, QName};
use crate::model::words::*;

use super::Owner;

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
