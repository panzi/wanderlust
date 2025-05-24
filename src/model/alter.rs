pub mod extension;
pub mod table;
pub mod types;
pub mod index;

use super::{name::Name};

use super::words::*;

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

