use std::{hash::Hash, ops::Deref, rc::Rc};

#[derive(Debug, Clone, Eq)]
pub struct Name {
    name: Rc<str>,
    quoted: bool,
}

impl Name {
    #[inline]
    pub fn needs_quoting(name: &str) -> bool {
        let Some(c) = name.chars().next() else {
            return false;
        };

        if !c.is_alphabetic() && c != '_' {
            return false;
        }

        name[c.len_utf8()..].contains(|c: char| !c.is_alphanumeric() && c != '_' && c != '$')
    }

    #[inline]
    pub fn new(name: impl Into<Rc<str>>) -> Self {
        let name = name.into();
        let quoted = Self::needs_quoting(&name);
        Self { name, quoted }
    }

    #[inline]
    pub fn new_quoted(name: impl Into<Rc<str>>) -> Self {
        Self { name: name.into(), quoted: true }
    }

    #[inline]
    pub(crate) fn new_unquoted(name: impl Into<Rc<str>>) -> Self {
        Self { name: name.into(), quoted: false }
    }

    #[inline]
    pub fn name(&self) -> &Rc<str> {
        &self.name
    }

    #[inline]
    pub fn quoted(&self) -> bool {
        self.quoted
    }

    #[inline]
    pub fn equals(&self, name: impl AsRef<str>) -> bool {
        if self.quoted {
            self.name.deref() == name.as_ref()
        } else {
            self.name.eq_ignore_ascii_case(name.as_ref())
        }
    }

    #[inline]
    pub fn into_name(self) -> Rc<str> {
        self.name
    }
}

impl From<Name> for Rc<str> {
    #[inline]
    fn from(value: Name) -> Self {
        value.into_name()
    }
}

impl std::fmt::Display for Name {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.quoted {
            if self.name.contains(|c: char| c < ' ' || c > '~') {
                f.write_str("U&\"")?;

                let mut tail = &self.name[..];
                while let Some(index) = tail.find(|c: char| c < ' ' || c > '~' || c == '"') {
                    f.write_str(&tail[..index])?;
                    let ch = tail[index..].chars().next().unwrap();
                    if ch == '"' {
                        f.write_str("\"\"")?;
                    } else {
                        write!(f, "\\+{:06x}", ch as u32)?;
                    }
                    tail = &tail[index + ch.len_utf8()..];
                }
                f.write_str(tail)?;

                return f.write_str("\"");
            }
            "\"".fmt(f)?;
            let mut tail = &self.name[..];
            while let Some(index) = tail.find('"') {
                tail[..index + 1].fmt(f)?;
                "\"".fmt(f)?;
                tail = &tail[index + 1..];
            }
            tail.fmt(f)?;
            "\"".fmt(f)
        } else {
            self.name.fmt(f)
        }
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        if self.quoted {
            if other.quoted {
                self.name == other.name
            } else {
                if self.name.len() != other.name.len() {
                    return false;
                }

                self.name.chars().zip(other.name.chars().map(|c| c.to_ascii_lowercase())).all(|(a, b)| a == b)
            }
        } else {
            if other.quoted {
                if self.name.len() != other.name.len() {
                    return false;
                }

                self.name.chars().map(|c| c.to_ascii_lowercase()).zip(other.name.chars()).all(|(a, b)| a == b)
            } else {
                self.name.eq_ignore_ascii_case(&other.name)
            }
        }
    }
}

impl Hash for Name {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_usize(self.name.len());
        if self.quoted {
            for c in self.name.chars() {
                c.hash(state);
            }
        } else {
            for c in self.name.chars() {
                c.to_ascii_lowercase().hash(state);
            }
        }
    }
}

impl From<QName> for Name {
    #[inline]
    fn from(value: QName) -> Self {
        value.into_name()
    }
}

/// Qualified name
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct QName {
    // actually there would also be (at least for PostgreSQL): database: Option<Name>

    /// Option because some DMBSs don't have that and thus there can't be statements generated with qualified names.
    schema: Option<Name>,
    name: Name,
}

impl QName {
    #[inline]
    pub fn new(schema: Option<Name>, name: Name) -> Self {
        Self { schema, name }
    }

    #[inline]
    pub fn unqual(name: impl Into<Rc<str>>) -> Self {
        Self { schema: None, name: Name::new(name) }
    }

    #[inline]
    pub fn schema(&self) -> Option<&Name> {
        self.schema.as_ref()
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn set_schema(&mut self, schema: Option<Name>) {
        self.schema = schema;
    }

    #[inline]
    pub fn set_name(&mut self, name: Name) {
        self.name = name;
    }

    #[inline]
    pub fn with_default_schema(&self, default_schema: &Name) -> Self {
        Self {
            schema: if let Some(schema) = &self.schema {
                Some(schema.clone())
            } else {
                Some(default_schema.clone())
            },
            name: self.name.clone(),
        }
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.name() == other.name() &&
        (if let Some(schema) = self.schema() {
            if let Some(other_schema) = other.schema() {
                schema == other_schema
            } else { true }
        } else { true })
    }

    #[inline]
    pub fn into_name(self) -> Name {
        self.name
    }
}

impl std::fmt::Display for QName {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = &self.schema {
            write!(f, "{schema}.")?;
        }
        self.name.fmt(f)
    }
}

impl From<Name> for QName {
    #[inline]
    fn from(value: Name) -> Self {
        QName::new(None, value)
    }
}

impl From<&Name> for QName {
    #[inline]
    fn from(value: &Name) -> Self {
        QName::new(None, value.clone())
    }
}
