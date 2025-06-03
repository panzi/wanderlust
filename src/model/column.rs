use std::rc::Rc;

use crate::format::{write_paren_names, write_token_list};
use crate::model::index::IndexParameters;

use super::name::QName;
use super::table::TableConstraint;
use super::{name::Name, token::ParsedToken, types::DataType};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    name: Name,
    data_type: Rc<DataType>,
    storage: Storage,
    compression: Option<Name>,
    collation: Option<QName>,
    constraints: Vec<Rc<ColumnConstraint>>,
    comment: Option<Rc<str>>,
}

impl std::fmt::Display for Column {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        if self.storage != Storage::Default {
            write!(f, " {STORAGE} {}", self.storage)?
        }

        if let Some(compression) = &self.compression {
            write!(f, " {COMPRESSION} {compression}")?;
        }

        if let Some(collation) = &self.collation {
            write!(f, " {COLLATE} {collation}")?;
        }

        for constraint in &self.constraints {
            write!(f, " {constraint}")?;
        }

        Ok(())
    }
}

impl Column {
    #[inline]
    pub fn new(
        name: Name,
        data_type: impl Into<Rc<DataType>>,
        storage: Storage,
        compression: Option<Name>,
        collation: Option<QName>,
        constraints: Vec<Rc<ColumnConstraint>>
    ) -> Self {
        Self {
            name,
            data_type: data_type.into(),
            storage,
            compression,
            collation,
            constraints,
            comment: None
        }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn set_name(&mut self, name: Name) {
        self.name = name;
    }

    #[inline]
    pub fn data_type(&self) -> &Rc<DataType> {
        &self.data_type
    }

    #[inline]
    pub fn set_data_type(&mut self, data_type: impl Into<Rc<DataType>>) {
        self.data_type = data_type.into();
    }

    #[inline]
    pub fn collation(&self) -> Option<&QName> {
        self.collation.as_ref()
    }

    #[inline]
    pub fn set_collation(&mut self, collation: Option<QName>) {
        self.collation = collation;
    }

    #[inline]
    pub fn storage(&self) -> Storage {
        self.storage
    }

    #[inline]
    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = storage;
    }

    #[inline]
    pub fn compression(&self) -> Option<&Name> {
        self.compression.as_ref()
    }

    #[inline]
    pub fn set_compression(&mut self, compression: Option<Name>) {
        self.compression = compression;
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment;
    }

    pub fn drop_default(&mut self) {
        self.constraints.retain(|c| !c.is_default());
    }

    pub fn set_default(&mut self, value: Rc<[ParsedToken]>) {
        self.constraints.retain(|c| !c.is_default());
        self.constraints.push(Rc::new(ColumnConstraint::new(
            None,
            ColumnConstraintData::Default { value },
            None,
            None
        )));
    }

    pub fn drop_not_null(&mut self) {
        self.constraints.retain(|c| !c.is_not_null());
    }

    pub fn set_not_null(&mut self) {
        self.constraints.retain(|c| !matches!(c.data(),
            ColumnConstraintData::Null |
            ColumnConstraintData::NotNull
        ));
        self.constraints.push(Rc::new(ColumnConstraint::new(
            None,
            ColumnConstraintData::NotNull,
            None,
            None
        )));
    }

    #[inline]
    pub fn constraints(&self) -> &[Rc<ColumnConstraint>] {
        &self.constraints
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut Vec<Rc<ColumnConstraint>> {
        &mut self.constraints
    }

    pub fn get_constraint(&self, name: &Name) -> Option<&Rc<ColumnConstraint>> {
        let some_name = Some(name);
        self.constraints.iter().find(|c| c.name() == some_name)
    }

    pub fn get_constraint_mut(&mut self, name: &Name) -> Option<&mut Rc<ColumnConstraint>> {
        let some_name = Some(name);
        self.constraints.iter_mut().find(|c| c.name() == some_name)
    }

    pub fn drop_constraint(&mut self, name: &Name) -> Option<Rc<ColumnConstraint>> {
        let some_name = Some(name);
        let index = self.constraints.iter().position(|c| c.name() == some_name)?;

        Some(self.constraints.remove(index))
    }

    pub fn without_table_constraints(self: &Rc<Self>) -> Rc<Self> {
        if self.constraints.iter().all(|constraint| !constraint.is_table_constraint()) {
            return self.clone();
        }

        Rc::new(Self {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            storage: self.storage,
            compression: self.compression.clone(),
            collation: self.collation.clone(),
            constraints: self.constraints.iter()
                .filter(|&constraint| !constraint.is_table_constraint())
                .cloned()
                .collect(),
            comment: self.comment.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    Plain,
    External,
    Extended,
    Main,
    Default,
}

impl std::fmt::Display for Storage {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain    => f.write_str(PLAIN),
            Self::External => f.write_str(EXTERNAL),
            Self::Extended => f.write_str(EXTENDED),
            Self::Main     => f.write_str(MAIN),
            Self::Default  => f.write_str(DEFAULT),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMatch {
    Full,
    Partial,
    Simple,
}

impl Default for ColumnMatch {
    #[inline]
    fn default() -> Self {
        Self::Simple
    }
}

impl std::fmt::Display for ColumnMatch {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full    => write!(f, "{MATCH} {FULL}"),
            Self::Partial => write!(f, "{MATCH} {PARTIAL}"),
            Self::Simple  => write!(f, "{MATCH} {SIMPLE}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull    { columns: Option<Vec<Name>> },
    SetDefault { columns: Option<Vec<Name>> },
}

impl Default for ReferentialAction {
    #[inline]
    fn default() -> Self {
        Self::NoAction
    }
}

impl std::fmt::Display for ReferentialAction {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAction => write!(f, "{NO} {ACTION}"),
            Self::Restrict => f.write_str(RESTRICT),
            Self::Cascade  => f.write_str(CASCADE),
            Self::SetNull { columns } => {
                write!(f, "{SET} {NULL}")?;

                if let Some(columns) = columns {
                    write_paren_names(columns, f)?;
                }

                Ok(())
            },
            Self::SetDefault { columns } => {
                write!(f, "{SET} {DEFAULT}")?;

                if let Some(columns) = columns {
                    write_paren_names(columns, f)?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnConstraintData {
    Null,
    NotNull,
    Check {
        expr: Rc<[ParsedToken]>,
        inherit: bool,
    },
    Default { value: Rc<[ParsedToken]> },
    Unique { nulls_distinct: Option<bool>, index_parameters: IndexParameters },
    PrimaryKey { index_parameters: IndexParameters },
    References {
        ref_table: QName,
        ref_column: Option<Name>,
        column_match: Option<ColumnMatch>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

impl PartialEq for ColumnConstraintData {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Null => matches!(other, Self::Null),
            Self::NotNull => matches!(other, Self::NotNull),
            Self::Check { expr, inherit } => {
                match other {
                    Self::Check { expr: other_expr, inherit: other_inherit } => {
                        inherit == other_inherit &&
                        expr == other_expr
                    }
                    _ => false
                }
            },
            Self::Default { value } => {
                match other {
                    Self::Default { value: other_value } => {
                        value == other_value
                    }
                    _ => false
                }
            },
            Self::Unique { nulls_distinct, index_parameters } => {
                match other {
                    Self::Unique { nulls_distinct: other_nulls_distinct, index_parameters: other_index_parameters } => {
                        nulls_distinct == other_nulls_distinct &&
                        index_parameters == other_index_parameters
                    }
                    _ => false
                }
            },
            Self::PrimaryKey { index_parameters } => {
                match other {
                    Self::PrimaryKey { index_parameters: other_index_parameters } => {
                        index_parameters == other_index_parameters
                    }
                    _ => false
                }
            },
            Self::References { ref_table, ref_column, column_match, on_delete, on_update } => {
                match other {
                    Self::References {
                        ref_table: other_ref_table,
                        ref_column: other_ref_column,
                        column_match: other_column_match,
                        on_delete: other_on_delete,
                        on_update: other_on_update,
                    } => {
                        ref_table == other_ref_table &&
                        ref_column == other_ref_column && // XXX: false negatives if only one side uses the default
                        column_match.unwrap_or_default() == other_column_match.unwrap_or_default() &&
                        on_delete.as_ref().unwrap_or(&ReferentialAction::NoAction) == other_on_delete.as_ref().unwrap_or(&ReferentialAction::NoAction) &&
                        on_update.as_ref().unwrap_or(&ReferentialAction::NoAction) == other_on_update.as_ref().unwrap_or(&ReferentialAction::NoAction)
                    }
                    _ => false
                }
            }
        }
    }
}

impl std::fmt::Display for ColumnConstraintData {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str(NULL),
            Self::NotNull => write!(f, "{NOT} {NULL}"),
            Self::Check { expr, inherit } => {
                write!(f, "{CHECK} (")?;
                write_token_list(expr, f)?;
                if *inherit {
                    f.write_str(")")
                } else {
                    write!(f, ") {NO} {INHERIT}")
                }
            }
            Self::Default { value } => {
                write!(f, "{DEFAULT} ")?;
                write_token_list(value, f)
            },
            Self::Unique { nulls_distinct, index_parameters } => {
                f.write_str(UNIQUE)?;

                if let Some(nulls_distinct) = nulls_distinct {
                    if *nulls_distinct {
                        write!(f, " {NULLS} {DISTINCT}")?;
                    } else {
                        write!(f, " {NULLS} {NOT} {DISTINCT}")?;
                    }
                }

                index_parameters.fmt(f)
            },
            Self::PrimaryKey { index_parameters } => {
                write!(f, "{PRIMARY} {KEY}{index_parameters}")
            },
            Self::References { ref_table, ref_column, column_match, on_delete, on_update } => {
                write!(f, "{REFERENCES} {ref_table}")?;

                if let Some(ref_column) = ref_column {
                    write!(f, " ({ref_column})")?;
                }

                if let Some(column_match) = column_match {
                    write!(f, " {column_match}")?;
                }

                if let Some(on_delete) = on_delete {
                    write!(f, " {ON} {DELETE} {on_delete}")?;
                }

                if let Some(on_update) = on_update {
                    write!(f, " {ON} {UPDATE} {on_update}")?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnConstraint {
    name: Option<Name>,
    data: ColumnConstraintData,
    deferrable: Option<bool>,
    initially_deferred: Option<bool>,
}

pub fn make_constraint_name(table_name: &Name, column_name: &Name, data: &ColumnConstraintData) -> Name {
    let mut constraint_name = String::new();
    constraint_name.push_str(table_name.name());

    match data {
        ColumnConstraintData::Check { .. } => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_check");
        }
        ColumnConstraintData::References { .. } => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_fkey");
        }
        ColumnConstraintData::PrimaryKey { .. } => {
            constraint_name.push_str("_pkey");
        }
        ColumnConstraintData::Unique { .. } => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_key");
        }
        // shouldn't be called with any of the below
        ColumnConstraintData::Null => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_null");
        }
        ColumnConstraintData::NotNull => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_not_null");
        }
        ColumnConstraintData::Default { .. } => {
            constraint_name.push_str("_");
            constraint_name.push_str(column_name.name());
            constraint_name.push_str("_default");
        }
    }

    Name::new(constraint_name)
}

impl ColumnConstraint {
    pub fn ensure_name(&mut self, table_name: &Name, column_name: &Name) -> &Name {
        if self.name.is_none() {
            self.name = Some(make_constraint_name(table_name, column_name, &self.data));
        }
        self.name.as_ref().unwrap()
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self.data, ColumnConstraintData::Null)
    }

    #[inline]
    pub fn is_not_null(&self) -> bool {
        matches!(self.data, ColumnConstraintData::NotNull)
    }

    #[inline]
    pub fn is_check(&self) -> bool {
        matches!(self.data, ColumnConstraintData::Check { .. })
    }

    #[inline]
    pub fn is_default(&self) -> bool {
        matches!(self.data, ColumnConstraintData::Default { .. })
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        matches!(self.data, ColumnConstraintData::Unique { .. })
    }

    #[inline]
    pub fn is_primary_key(&self) -> bool {
        matches!(self.data, ColumnConstraintData::PrimaryKey { .. })
    }

    #[inline]
    pub fn is_foreign_key(&self) -> bool {
        matches!(self.data, ColumnConstraintData::References { .. })
    }

    #[inline]
    pub fn is_table_constraint(&self) -> bool {
        matches!(self.data,
            ColumnConstraintData::Check { .. } |
            ColumnConstraintData::Unique { .. } |
            ColumnConstraintData::PrimaryKey { .. } |
            ColumnConstraintData::References { .. }
        )
    }

    fn some_name(&self, table_name: &Name, column_name: &Name) -> Name {
        if let Some(name) = &self.name {
            name.clone()
        } else {
            make_constraint_name(table_name, column_name, &self.data)
        }
    }

    pub fn to_table_constraint(&self, table_name: &Name, column_name: &Name) -> Option<TableConstraint> {
        match self.data() {
            ColumnConstraintData::Default { .. } => None,
            ColumnConstraintData::NotNull => None,
            ColumnConstraintData::Null => None,
            ColumnConstraintData::Check { expr, inherit } => {
                Some(TableConstraint::new(
                    Some(self.some_name(table_name, column_name)),
                    super::table::TableConstraintData::Check {
                        expr: expr.clone(),
                        inherit: *inherit,
                    },
                    self.deferrable,
                    self.initially_deferred
                ))
            },
            ColumnConstraintData::PrimaryKey { index_parameters } => {
                Some(TableConstraint::new(
                    Some(self.some_name(table_name, column_name)),
                    super::table::TableConstraintData::PrimaryKey {
                        columns: [column_name.clone()].into(),
                        index_parameters: index_parameters.clone(),
                    },
                    self.deferrable,
                    self.initially_deferred
                ))
            },
            ColumnConstraintData::Unique { nulls_distinct, index_parameters } => {
                Some(TableConstraint::new(
                    Some(self.some_name(table_name, column_name)),
                    super::table::TableConstraintData::Unique {
                        columns: [column_name.clone()].into(),
                        nulls_distinct: *nulls_distinct,
                        index_parameters: index_parameters.clone(),
                    },
                    self.deferrable,
                    self.initially_deferred
                ))
            },
            ColumnConstraintData::References { ref_table, ref_column, column_match, on_delete, on_update } => {
                Some(TableConstraint::new(
                    Some(self.some_name(table_name, column_name)),
                    super::table::TableConstraintData::ForeignKey {
                        columns: [column_name.clone()].into(),
                        ref_table: ref_table.clone(),
                        ref_columns: ref_column.as_ref().map(|ref_column| [ref_column.clone()].into()),
                        column_match: *column_match,
                        on_delete: on_delete.clone(),
                        on_update: on_update.clone(),
                    },
                    self.deferrable,
                    self.initially_deferred
                ))
            }
        }
    }
}

impl PartialEq for ColumnConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        self.data == other.data &&
        self.default_deferrable() == other.default_deferrable() &&
        self.default_initially_deferred() == other.default_initially_deferred()
    }
}

impl std::fmt::Display for ColumnConstraint {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{CONSTRAINT} {name} ")?;
        }

        self.data.fmt(f)?;

        if let Some(deferrable) = self.deferrable {
            if deferrable {
                write!(f, " {DEFERRABLE}")?;
            } else {
                write!(f, " {NOT} {DEFERRABLE}")?;
            }
        }

        if let Some(initially_deferred) = self.initially_deferred {
            if initially_deferred {
                write!(f, " {INITIALLY} {DEFERRED}")?;
            } else {
                write!(f, " {INITIALLY} {IMMEDIATE}")?;
            }
        }

        Ok(())
    }
}

impl ColumnConstraint {
    #[inline]
    pub fn new(
        name: Option<Name>,
        data: ColumnConstraintData,
        deferrable: Option<bool>,
        initially_deferred: Option<bool>,
    ) -> Self {
        Self { name, data, deferrable, initially_deferred }
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn data(&self) -> &ColumnConstraintData {
        &self.data
    }

    #[inline]
    pub fn deferrable(&self) -> Option<bool> {
        self.deferrable
    }

    #[inline]
    pub fn initially_deferred(&self) -> Option<bool> {
        self.initially_deferred
    }

    #[inline]
    pub fn default_deferrable(&self) -> bool {
        self.deferrable.unwrap_or(false)
    }

    #[inline]
    pub fn default_initially_deferred(&self) -> bool {
        self.initially_deferred.unwrap_or(false)
    }
}
