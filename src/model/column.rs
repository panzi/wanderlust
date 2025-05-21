use std::rc::Rc;

use crate::format::{format_iso_string, write_paren_names, write_token_list};

use super::name::QName;
use super::{name::Name, token::ParsedToken, types::ColumnDataType};

use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    name: Name,
    data_type: Rc<ColumnDataType>,
    collation: Option<Rc<str>>,
    constraints: Vec<Rc<ColumnConstraint>>,
}

impl std::fmt::Display for Column {
    #[inline]
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        if let Some(collation) = &self.collation {
            write!(f, " {COLLATE} ")?;
            format_iso_string(&mut f, collation)?;
        }

        for constraint in &self.constraints {
            write!(f, " {constraint}")?;
        }

        Ok(())
    }
}

impl Column {
    #[inline]
    pub fn new(name: Name, data_type: impl Into<Rc<ColumnDataType>>, collation: Option<impl Into<Rc<str>>>, constraints: Vec<Rc<ColumnConstraint>>) -> Self {
        Self { name, data_type: data_type.into(), collation: collation.map(|c| c.into()), constraints }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn data_type(&self) -> &Rc<ColumnDataType> {
        &self.data_type
    }

    #[inline]
    pub fn collation(&self) -> Option<&Rc<str>> {
        self.collation.as_ref()
    }

    #[inline]
    pub fn constraints(&self) -> &[Rc<ColumnConstraint>] {
        &self.constraints
    }

    #[inline]
    pub fn constraints_mut(&mut self) -> &mut Vec<Rc<ColumnConstraint>> {
        &mut self.constraints
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
                    write_paren_names(&columns, f)?;
                }

                Ok(())
            },
            Self::SetDefault { columns } => {
                write!(f, "{SET} {DEFAULT}")?;

                if let Some(columns) = columns {
                    write_paren_names(&columns, f)?;
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
    Unique { nulls_distinct: Option<bool> },
    PrimaryKey,
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
            Self::Unique { nulls_distinct } => {
                match other {
                    Self::Unique { nulls_distinct: other_nulls_distinct } => {
                        nulls_distinct == other_nulls_distinct
                    }
                    _ => false
                }
            },
            Self::PrimaryKey => matches!(other, Self::PrimaryKey),
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
            Self::Unique { nulls_distinct } => {
                f.write_str(UNIQUE)?;

                if let Some(nulls_distinct) = nulls_distinct {
                    if *nulls_distinct {
                        write!(f, " {NULLS} {DISTINCT}")?;
                    } else {
                        write!(f, " {NULLS} {NOT} {DISTINCT}")?;
                    }
                }

                Ok(())
            },
            Self::PrimaryKey => {
                write!(f, "{PRIMARY} {KEY}")
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

impl ColumnConstraint {
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
        matches!(self.data, ColumnConstraintData::PrimaryKey)
    }

    #[inline]
    pub fn is_foreign_key(&self) -> bool {
        matches!(self.data, ColumnConstraintData::References { .. })
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
