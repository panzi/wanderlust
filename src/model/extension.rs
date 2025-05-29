use std::rc::Rc;

use crate::format::format_iso_string;

use super::name::{Name, QName};

use crate::model::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtension {
    if_not_exists: bool,
    extension: Rc<Extension>,
    cascade: bool,
}

impl CreateExtension {
    #[inline]
    pub fn new(if_not_exists: bool, extension: impl Into<Rc<Extension>>, cascade: bool) -> Self {
        Self { if_not_exists, extension: extension.into(), cascade }
    }

    #[inline]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    #[inline]
    pub fn extension(&self) -> &Rc<Extension> {
        &self.extension
    }

    #[inline]
    pub fn cascade(&self) -> bool {
        self.cascade
    }

    #[inline]
    pub fn into_extension(self) -> Rc<Extension> {
        self.extension
    }
}

impl From<CreateExtension> for Rc<Extension> {
    #[inline]
    fn from(value: CreateExtension) -> Self {
        value.into_extension()
    }
}

impl std::fmt::Display for CreateExtension {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} {EXTENSION} ")?;

        if self.if_not_exists {
            write!(f, "{IF} {NOT} {EXISTS} ")?;
        }

        self.extension.write(self.cascade, f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Version {
    Name(Name),
    String(Rc<str>),
}

impl std::fmt::Display for Version {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(name) => name.fmt(f),
            Self::String(value) => format_iso_string(f, value),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Extension {
    name: QName,
    version: Option<Version>,
    comment: Option<Rc<str>>,
}

impl Extension {
    #[inline]
    pub fn new(name: QName, version: Option<Version>) -> Self {
        Self { name, version, comment: None }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub(crate) fn set_schema(&mut self, schema: Option<Name>) {
        self.name.set_schema(schema);
    }

    #[inline]
    pub fn version(&self) -> Option<&Version> {
        self.version.as_ref()
    }

    #[inline]
    pub fn set_version(&mut self, version: Option<Version>) {
        self.version = version;
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment.into();
    }

    pub fn write(&self, cascade: bool, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.name.name(), f)?;

        if self.name.schema().is_some() || self.version.is_some() || cascade {
            write!(f, " {WITH}")?;

            if let Some(schema) = self.name.schema() {
                write!(f, " {SCHEMA} {schema}")?;
            }

            if let Some(version) = &self.version {
                write!(f, " {VERSION} {version}")?;
            }

            if cascade {
                write!(f, " {CASCADE}")?;
            }
        }

        f.write_str(";")
    }
}

impl std::fmt::Display for Extension {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} {EXTENSION} ")?;
        self.write(false, f)
    }
}
