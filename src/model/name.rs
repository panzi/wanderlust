use std::{hash::Hash, ops::Deref, rc::Rc};

#[derive(Debug, Clone, Eq)]
pub struct Name {
    name: Rc<str>,
    quoted: bool,
}

impl Name {
    #[inline]
    pub fn needs_quoting(name: &str) -> bool {
        if !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
            return false;
        }
        name[1..].contains(|c: char| !c.is_ascii_alphanumeric() && c != '_')
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
    pub fn name(&self) -> &str {
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
        if self.quoted {
            self.name.hash(state);
        } else {
            for c in self.name.chars() {
                c.to_ascii_lowercase().hash(state);
            }
        }
    }
}
