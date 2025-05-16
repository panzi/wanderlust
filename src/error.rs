use crate::model::syntax::Cursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    IllegalToken,
    UnexpectedToken,

    /// not sure about that one
    UnknownType,
    UnexpectedEOF,
}

impl std::fmt::Display for ErrorKind {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IllegalToken    => "Illegal Token".fmt(f),
            Self::UnexpectedToken => "Unexpected Token".fmt(f),
            Self::UnknownType     => "Unknown Type".fmt(f),
            Self::UnexpectedEOF   => "Unexpected EOF".fmt(f),
        }
    }
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cursor: Option<Cursor>,
    message: Option<String>,
    source: Option<Box<dyn std::error::Error>>,
}

impl Error {
    #[inline]
    pub fn new(kind: ErrorKind, cursor: Option<Cursor>, message: Option<impl Into<String>>, source: Option<Box<dyn std::error::Error>>) -> Self {
        Self { kind, cursor, message: message.map(|s| s.into()), source }
    }

    #[inline]
    pub fn with_cursor(kind: ErrorKind, cursor: Cursor) -> Self {
        Self { kind, cursor: Some(cursor), message: None, source: None }
    }

    #[inline]
    pub fn with_message(kind: ErrorKind, cursor: Cursor, message: impl Into<String>) -> Self {
        Self { kind, cursor: Some(cursor), message: Some(message.into()), source: None }
    }

    #[inline]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[inline]
    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }

    #[inline]
    pub fn message(&self) -> Option<&str> {
        self.message.as_ref().map(|s| s.as_str())
    }

    #[inline]
    pub fn write(&self, filename: &str, source: &str, write: &mut impl std::fmt::Write) -> std::fmt::Result {
        if let Some(cursor) = &self.cursor {
            let start = cursor.locate_start(source);
            write!(write, "{filename}:{}:{}: {self}\n", start.lineno(), start.column())?;
            cursor.write(source, write)?;
        } else {
            write!(write, "{filename}: {self}\n")?;
        }

        Ok(())
    }

    #[inline]
    pub fn format(&self, filename: &str, source: &str) -> String {
        let mut result = String::new();
        let _ = self.write(source, filename, &mut result);
        result
    }
}

impl std::fmt::Display for Error {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;

        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }

        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
