use crate::{format::FmtWriter, model::syntax::Cursor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    IllegalToken,
    UnexpectedToken,
    UnexpectedEOF,

    TableExists,
    ColumnExists,
    ConstraintExists,
    IndexExists,
    TypeExists,
    ValueExists,
    ExtensionExists,
    FunctionExists,
    TriggerExists,

    TableNotExists,
    ColumnNotExists,
    ConstraintNotExists,
    IndexNotExists,
    TypeNotExists,
    ValueNotExists,

    NotSupported,
}

impl std::fmt::Display for ErrorKind {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IllegalToken        => f.write_str("Illegal Token"),
            Self::UnexpectedToken     => f.write_str("Unexpected Token"),
            Self::UnexpectedEOF       => f.write_str("Unexpected EOF"),

            Self::TableExists         => f.write_str("Table Exists"),
            Self::ColumnExists        => f.write_str("Column Exists"),
            Self::ConstraintExists    => f.write_str("Constraint Exists"),
            Self::IndexExists         => f.write_str("Index Exists"),
            Self::TypeExists          => f.write_str("Type Exists"),
            Self::ValueExists         => f.write_str("Enum Value Exists"),
            Self::ExtensionExists     => f.write_str("Extension Exists"),
            Self::FunctionExists      => f.write_str("Function Exists"),
            Self::TriggerExists       => f.write_str("Trigger Exists"),

            Self::TableNotExists      => f.write_str("Table Not Exists"),
            Self::IndexNotExists      => f.write_str("Index Not Exists"),
            Self::TypeNotExists       => f.write_str("Type Not Exists"),
            Self::ValueNotExists      => f.write_str("Enum Value Not Exists"),
            Self::ColumnNotExists     => f.write_str("Column Not Exists"),
            Self::ConstraintNotExists => f.write_str("Constraint Not Exists"),

            Self::NotSupported        => f.write_str("Not Supported"),
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
        self.message.as_deref()
    }

    #[inline]
    pub fn cursor_mut(&mut self) -> &mut Option<Cursor> {
        &mut self.cursor
    }

    #[inline]
    pub fn message_mut(&mut self) -> &mut Option<String> {
        &mut self.message
    }

    #[inline]
    pub fn source_mut(&mut self) -> &mut Option<Box<dyn std::error::Error>> {
        &mut self.source
    }

    #[inline]
    pub fn print(&self, filename: &str, source: &str, write: &mut impl std::io::Write) -> std::fmt::Result {
        self.write(filename, source, &mut FmtWriter::new(write))
    }

    #[inline]
    pub fn write(&self, filename: &str, source: &str, write: &mut impl std::fmt::Write) -> std::fmt::Result {
        if let Some(cursor) = &self.cursor {
            let start = cursor.locate_start(source);
            writeln!(write, "{filename}:{}:{}: {self}", start.lineno(), start.column())?;
            cursor.write(source, write)?;
        } else {
            writeln!(write, "{filename}: {self}")?;
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
