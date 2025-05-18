use crate::format::{format_iso_string, write_token_list};

use super::{name::Name, token::ParsedToken};

use super::words::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

impl Default for Direction {
    #[inline]
    fn default() -> Self {
        Self::Asc
    }
}

impl std::fmt::Display for Direction {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Asc  => f.write_str(ASC),
            Direction::Desc => f.write_str(DESC),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsPosition {
    First,
    Last,
}

impl std::fmt::Display for NullsPosition {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::First => write!(f, "{NULLS} {FIRST}"),
            Self::Last  => write!(f, "{NULLS} {LAST}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexItemData {
    Column(Name),
    Expr(Vec<ParsedToken>),
}

impl std::fmt::Display for IndexItemData {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => name.fmt(f),
            Self::Expr(expr) => {
                f.write_str("(")?;
                write_token_list(expr, f)?;
                f.write_str(")")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexItem {
    data: IndexItemData,
    collation: Option<String>,
    direction: Option<Direction>,
    nulls_position: Option<NullsPosition>,
}

impl std::fmt::Display for IndexItem {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.fmt(f)?;

        if let Some(collation) = &self.collation {
            write!(f, " {COLLATE} {collation}")?;
        }

        if let Some(direction) = self.direction {
            write!(f, " {direction}")?;
        }

        if let Some(nulls_position) = self.nulls_position {
            write!(f, " {nulls_position}")?;
        }

        Ok(())
    }
}

impl IndexItem {
    #[inline]
    pub fn new(
        data: IndexItemData,
        collation: Option<String>,
        direction: Option<Direction>,
        nulls_position: Option<NullsPosition>,
    ) -> Self {
        Self { data, collation, direction, nulls_position }
    }

    #[inline]
    pub fn data(&self) -> &IndexItemData {
        &self.data
    }

    #[inline]
    pub fn collation(&self) -> Option<&str> {
        self.collation.as_deref()
    }

    #[inline]
    pub fn direction(&self) -> Option<Direction> {
        self.direction
    }

    #[inline]
    pub fn nulls_position(&self) -> Option<NullsPosition> {
        self.nulls_position
    }
}

#[derive(Debug, Clone)]
pub struct Index {
    unique: bool,
    name: Option<Name>,
    table_name: Name,
    method: Option<String>,
    items: Vec<IndexItem>,
    nulls_distinct: Option<bool>,
    predicate: Option<Vec<ParsedToken>>,
}

impl std::fmt::Display for Index {
    #[inline]
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CREATE)?;

        if self.unique {
            write!(f, " {UNIQUE}")?;
        }

        write!(f, " {INDEX}")?;

        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }

        write!(f, " {ON} {}", self.table_name)?;

        if let Some(method) = &self.method {
            write!(f, " {USING} ")?;
            format_iso_string(&mut f, method)?;
        }

        f.write_str(" (")?;
        let mut iter = self.items.iter();
        if let Some(first) = iter.next() {
            first.fmt(f)?;
            for item in iter {
                write!(f, ", {item}")?;
            }
        }
        f.write_str(")")?;

        if let Some(nulls_distinct) = self.nulls_distinct {
            if nulls_distinct {
                write!(f, " {NULLS} {DISTINCT}")?;
            } else {
                write!(f, " {NULLS} {NOT} {DISTINCT}")?;
            }
        }

        if let Some(predicate) = &self.predicate {
            write!(f, " {WHERE} ")?;
            write_token_list(predicate, f)?;
        }

        f.write_str(";")
    }
}

impl Index {
    #[inline]
    pub fn new(
        unique: bool,
        name: Option<Name>,
        table_name: Name,
        method: Option<String>,
        items: Vec<IndexItem>,
        nulls_distinct: Option<bool>,
        predicate: Option<Vec<ParsedToken>>,
    ) -> Self {
        Self { unique, name, table_name, method, items, nulls_distinct, predicate }
    }

    #[inline]
    pub fn unique(&self) -> bool {
        self.unique
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn table_name(&self) -> &Name {
        &self.table_name
    }

    #[inline]
    pub fn items(&self) -> &[IndexItem] {
        &self.items
    }

    #[inline]
    pub fn method(&self) -> Option<&str> {
        self.method.as_deref()
    }

    #[inline]
    pub fn nulls_distinct(&self) -> Option<bool> {
        self.nulls_distinct
    }

    #[inline]
    pub fn predicate(&self) -> Option<&[ParsedToken]> {
        self.predicate.as_deref()
    }
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.unique == other.unique &&
        self.name == other.name &&
        self.table_name == other.table_name &&
        self.items == other.items &&
        self.method == other.method &&
        self.nulls_distinct.unwrap_or(true) == other.nulls_distinct.unwrap_or(true) &&
        self.predicate == other.predicate
    }
}
