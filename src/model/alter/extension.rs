use std::rc::Rc;

use crate::model::name::{Name, QName};

use crate::model::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct AlterExtension {
    name: QName,
    data: AlterExtensionData,
}

impl AlterExtension {
    #[inline]
    pub fn new(name: QName, data: AlterExtensionData) -> Self {
        Self { name, data }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn data(&self) -> &AlterExtensionData {
        &self.data
    }
}

impl std::fmt::Display for AlterExtension {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ALTER} {EXTENSION} {} {}", self.name, self.data)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterExtensionData {
    Update(Option<Rc<str>>),
    SetSchema(Name),
}

impl std::fmt::Display for AlterExtensionData {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Update(version) => {
                write!(f, "{UPDATE}")?;

                if let Some(version) = version {
                    write!(f, " {TO} {version}")?;
                }
            }
            Self::SetSchema(schema) => {
                write!(f, "{SET} {SCHEMA} {schema}")?;
            }
        }
        f.write_str(";")
    }
}
