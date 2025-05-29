use super::{function::FunctionSignature, name::{Name, QName}};

use crate::model::words::*;

#[derive(Debug, Clone, PartialEq)]
pub enum ObjectRef {
    Column { table_name: QName, column_name: Name },
    Extension { name: QName },
    Index { name: QName },
    Schema { name: Name },
    Table { name: QName },
    Type { name: QName },
    Constraint { table_name: QName, constraint_name: Name },
    Trigger { table_name: QName, trigger_name: Name },
    Function { signature: FunctionSignature },
    // TODO: more
}

impl ObjectRef {
    #[inline]
    pub fn column(table_name: QName, column_name: Name) -> Self {
        Self::Column { table_name, column_name }
    }

    #[inline]
    pub fn extension(name: QName) -> Self {
        Self::Extension { name }
    }

    #[inline]
    pub fn index(name: QName) -> Self {
        Self::Index { name }
    }

    #[inline]
    pub fn schema(name: Name) -> Self {
        Self::Schema { name }
    }

    #[inline]
    pub fn table(name: QName) -> Self {
        Self::Table { name }
    }

    #[inline]
    pub fn type_def(name: QName) -> Self {
        Self::Type { name }
    }

    #[inline]
    pub fn constraint(table_name: QName, constraint_name: Name) -> Self {
        Self::Constraint { table_name, constraint_name }
    }

    #[inline]
    pub fn trigger(table_name: QName, trigger_name: Name) -> Self {
        Self::Trigger { table_name, trigger_name }
    }

    #[inline]
    pub fn function(signature: FunctionSignature) -> Self {
        Self::Function { signature }
    }
}

impl std::fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column { table_name, column_name } => {
                write!(f, "{COLUMN} {table_name}.{column_name}")
            }
            Self::Extension { name } => {
                write!(f, "{EXTENSION} {name}")
            }
            Self::Index { name } => {
                write!(f, "{INDEX} {name}")
            }
            Self::Schema { name } => {
                write!(f, "{SCHEMA} {name}")
            }
            Self::Table { name } => {
                write!(f, "{TABLE} {name}")
            }
            Self::Type { name } => {
                write!(f, "{TYPE} {name}")
            }
            Self::Constraint { table_name, constraint_name } => {
                write!(f, "{CONSTRAINT} {constraint_name} {ON} {table_name}")
            }
            Self::Trigger { table_name, trigger_name } => {
                write!(f, "{TRIGGER} {trigger_name} {ON} {table_name}")
            }
            Self::Function { signature } => {
                write!(f, "{FUNCTION} {signature}")
            }
        }
    }
}
