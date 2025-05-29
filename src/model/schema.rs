use std::rc::Rc;

use crate::ordered_hash_map::OrderedHashMap;

use super::extension::Extension;
use super::function::{Function, FunctionRef};
use super::name::Name;
use super::{index::Index, table::Table, types::TypeDef};

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    name: Name,
    types: OrderedHashMap<Name, Rc<TypeDef>>,
    tables: OrderedHashMap<Name, Rc<Table>>,
    indices: OrderedHashMap<Name, Rc<Index>>,
    extensions: OrderedHashMap<Name, Rc<Extension>>,
    functions: OrderedHashMap<FunctionRef, Rc<Function>>,
    comment: Option<Rc<str>>,
}

impl Schema {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self {
            name,
            types: OrderedHashMap::new(),
            tables: OrderedHashMap::new(),
            indices: OrderedHashMap::new(),
            extensions: OrderedHashMap::new(),
            functions: OrderedHashMap::new(),
            comment: None,
        }
    }

    pub fn clear(&mut self) {
        self.types.clear();
        self.tables.clear();
        self.indices.clear();
        self.extensions.clear();
        self.functions.clear();
        self.comment = None;
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn types(&self) -> &OrderedHashMap<Name, Rc<TypeDef>> {
        &self.types
    }

    #[inline]
    pub fn tables(&self) -> &OrderedHashMap<Name, Rc<Table>> {
        &self.tables
    }

    #[inline]
    pub fn indices(&self) -> &OrderedHashMap<Name, Rc<Index>> {
        &self.indices
    }

    #[inline]
    pub fn extensions(&self) -> &OrderedHashMap<Name, Rc<Extension>> {
        &self.extensions
    }

    #[inline]
    pub fn functions(&self) -> &OrderedHashMap<FunctionRef, Rc<Function>> {
        &self.functions
    }

    #[inline]
    pub fn types_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<TypeDef>> {
        &mut self.types
    }

    #[inline]
    pub fn tables_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<Table>> {
        &mut self.tables
    }

    #[inline]
    pub fn indices_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<Index>> {
        &mut self.indices
    }

    #[inline]
    pub fn extensions_mut(&mut self) -> &mut OrderedHashMap<Name, Rc<Extension>> {
        &mut self.extensions
    }

    #[inline]
    pub fn functions_mut(&mut self) -> &mut OrderedHashMap<FunctionRef, Rc<Function>> {
        &mut self.functions
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment.into();
    }
}
