use std::ops::Deref;
use std::rc::Rc;

use crate::format::write_token_list;

use super::name::QName;
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
    Expr(Rc<[ParsedToken]>),
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
    collation: Option<Name>,
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
        collation: Option<Name>,
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
    pub fn collation(&self) -> Option<&Name> {
        self.collation.as_ref()
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
    name: Option<QName>,
    table_name: QName,
    method: Option<Name>,
    items: Rc<[IndexItem]>,
    nulls_distinct: Option<bool>,
    predicate: Option<Rc<[ParsedToken]>>,
}

impl std::fmt::Display for Index {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CREATE)?;

        if self.unique {
            write!(f, " {UNIQUE}")?;
        }

        write!(f, " {INDEX}")?;
        self.write(f)
    }
}

impl Index {
    #[inline]
    pub fn new(
        unique: bool,
        name: Option<Name>,
        table_name: QName,
        method: Option<Name>,
        items: impl Into<Rc<[IndexItem]>>,
        nulls_distinct: Option<bool>,
        predicate: Option<impl Into<Rc<[ParsedToken]>>>,
    ) -> Self {
        Self {
            unique,
            name: name.map(|name| QName::new(table_name.schema().cloned(), name)),
            table_name,
            method,
            items: items.into(),
            nulls_distinct,
            predicate: predicate.map(Into::into),
        }
    }

    pub fn ensure_name(&mut self) -> &QName {
        if self.name.is_none() {
            self.name = Some(QName::new(
                self.table_name.schema().cloned(),
                self.make_name()
            ));
        }

        self.name.as_ref().unwrap()
    }

    pub fn make_name(&self) -> Name {
        let table_name = self.table_name.name().name();
        let mut index_name = if self.unique {
            format!("unique_idx_{table_name}")
        } else {
            format!("idx_{table_name}")
        };

        for item in self.items.deref() {
            match item.data() {
                IndexItemData::Column(column) => {
                    index_name.push_str("_");
                    index_name.push_str(&column.name());
                }
                IndexItemData::Expr(expr) => {
                    for token in expr.deref() {
                        if let ParsedToken::Name(name) = token {
                            index_name.push_str("_");
                            index_name.push_str(&name.name());
                        }
                    }
                }
            }
        }

        Name::new(index_name)
    }

    #[inline]
    pub fn unique(&self) -> bool {
        self.unique
    }

    #[inline]
    pub fn name(&self) -> Option<&QName> {
        self.name.as_ref()
    }

    #[inline]
    pub fn set_name(&mut self, name: Option<QName>) {
        self.name = name;
    }

    #[inline]
    pub fn table_name(&self) -> &QName {
        &self.table_name
    }

    #[inline]
    pub fn items(&self) -> &[IndexItem] {
        &self.items
    }

    #[inline]
    pub fn method(&self) -> Option<&Name> {
        self.method.as_ref()
    }

    #[inline]
    pub fn nulls_distinct(&self) -> Option<bool> {
        self.nulls_distinct
    }

    #[inline]
    pub fn predicate(&self) -> Option<&[ParsedToken]> {
        self.predicate.as_deref()
    }

    fn write(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }

        write!(f, " {ON} {}", self.table_name)?;

        if let Some(method) = &self.method {
            write!(f, " {USING} {method}")?;
        }

        f.write_str(" (")?;
        let mut iter = self.items.iter();
        if let Some(first) = iter.next() {
            std::fmt::Display::fmt(first, f)?;
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

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    index: Rc<Index>,
    concurrently: bool,
    if_not_exists: bool,
}

impl CreateIndex {
    #[inline]
    pub fn new(index: impl Into<Rc<Index>>, concurrently: bool, if_not_exists: bool) -> Self {
        Self { index: index.into(), concurrently, if_not_exists }
    }

    #[inline]
    pub fn index(&self) -> &Rc<Index> {
        &self.index
    }

    #[inline]
    pub fn index_mut(&mut self) -> &mut Index {
        Rc::make_mut(&mut self.index)
    }

    #[inline]
    pub fn into_index(self) -> Rc<Index> {
        self.index
    }

    #[inline]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    #[inline]
    pub fn concurrently(&self) -> bool {
        self.concurrently
    }
}

impl From<CreateIndex> for Rc<Index> {
    #[inline]
    fn from(value: CreateIndex) -> Self {
        value.index
    }
}

impl std::fmt::Display for CreateIndex {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE}")?;
        if self.index.unique() {
            write!(f, " {UNIQUE}")?;
        }
        write!(f, " {INDEX}")?;
        if self.concurrently {
            write!(f, " {CONCURRENTLY}")?;
        }
        if self.if_not_exists {
            write!(f, " {IF} {NOT} {EXISTS}")?;
        }
        self.index.write(f)
    }
}
