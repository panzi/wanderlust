use std::hash::Hash;

#[derive(Debug, Clone, Eq)]
pub struct Name {
    name: String,
    quoted: bool,
}

impl Name {
    #[inline]
    pub fn new(name: impl Into<String>, quoted: bool) -> Self {
        Self { name: name.into(), quoted }
    }

    #[inline]
    pub fn new_quoted(name: impl Into<String>) -> Self {
        Self { name: name.into(), quoted: true }
    }

    #[inline]
    pub fn new_unquoted(name: impl Into<String>) -> Self {
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
    pub fn equals(&self, name: &impl AsRef<str>) -> bool {
        if self.quoted {
            self.name == name.as_ref()
        } else {
            self.name.eq_ignore_ascii_case(name.as_ref())
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
