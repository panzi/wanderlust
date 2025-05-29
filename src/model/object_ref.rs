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
    Trigger { table_name: QName, trigger_name: Name },
    Function { signature: FunctionSignature },
    // TODO: more
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
            Self::Trigger { table_name, trigger_name } => {
                write!(f, "{TRIGGER} {trigger_name} {ON} {table_name}")
            }
            Self::Function { signature } => {
                write!(f, "{FUNCTION} {signature}")
            }
        }
    }
}
