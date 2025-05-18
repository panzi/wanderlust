use super::{name::Name, token::ParsedToken};

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

#[derive(Debug, Clone, PartialEq)]
pub enum IndexItemData {
    Column(Name),
    Expr(Vec<ParsedToken>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexItem {
    data: IndexItemData,
    collation: Option<String>,
    direction: Option<Direction>,
    nulls_position: Option<NullsPosition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Index {
    unique: bool,
    name: Option<Name>,
    table_name: Name,
    items: Vec<IndexItem>,
    concurrently: bool,
    method: String,
    nulls_distinct: bool,
    predicate: Option<Vec<ParsedToken>>,
}

impl Index {
    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }
}
