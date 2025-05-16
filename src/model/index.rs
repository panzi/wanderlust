use super::{name::Name, syntax::{Cursor, Locatable}, token::Token};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsPosition {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexItemData {
    Column(Name),
    Expr(Vec<Token>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexItem {
    collation: Option<String>,
    direction: Option<Direction>,
    nulls_position: Option<NullsPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    cursor: Cursor,
    unique: bool,
    name: Option<Name>,
    table_name: Name,
    items: Vec<IndexItem>,
    concurrently: bool,
    method: String,
    nulls_distinct: bool,
    predicate: Option<Vec<Token>>,
}

impl Index {
    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }
}

impl Locatable for Index {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}
