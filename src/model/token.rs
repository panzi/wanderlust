use super::{name::Name, syntax::{Cursor, Locatable}};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TokenKind {
    BinInt,
    OctInt,
    DecInt,
    HexInt,
    Float,
    String,
    UString,
    EString,
    DollarString,
    Word,
    QIdent,
    UIdent,
    Operator,
    LParen,
    RParen,
    LBracket,
    RBracket,
    //Dollar,
    Comma,
    DoubleColon,
    Colon,
    SemiColon,
    Period,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Token {
    kind: TokenKind,
    cursor: Cursor,
}

impl Token {
    #[inline]
    pub fn new(kind: TokenKind, cursor: Cursor) -> Self {
        Self { kind, cursor }
    }

    #[inline]
    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    #[inline]
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

impl Locatable for Token {
    #[inline]
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ParsedToken {
    Integer(i64),
    Float(f64),
    String(String),
    Name(Name),
    Operator(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Colon,
    DoubleColon,
    SemiColon,
    Period,
}

pub fn format_iso_string(mut write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    if value.contains(|c: char| c < ' ' || c > '~') {
        write.write_str("U&'")?;

        let mut tail = value;
        while let Some(index) = tail.find(|c: char| c < ' ' || c > '~' || c == '\'') {
            write.write_str(&tail[..index])?;
            let ch = tail[index..].chars().next().unwrap();
            if ch == '\'' {
                write.write_str("''")?;
            } else {
                write!(write, "\\+{:06x}", ch as u32)?;
            }
            tail = &tail[index + ch.len_utf8()..];
        }
        write.write_str(tail)?;

        return write.write_str("'");
    }

    write.write_str("'")?;
    let mut tail = value;
    while let Some(index) = tail.find("'") {
        write.write_str(&tail[..index + 1])?;
        write.write_str("'")?;
        tail = &tail[index + 1..];
    }
    write.write_str(tail)?;

    write.write_str("'")
}

impl std::fmt::Display for ParsedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(val) => val.fmt(f),
            Self::Float(val) => val.fmt(f),
            Self::String(val) => format_iso_string(f, val),
            Self::Name(val) => val.fmt(f),
            Self::Operator(val) => val.fmt(f),
            Self::LParen => "(".fmt(f),
            Self::RParen => ")".fmt(f),
            Self::LBracket => "[".fmt(f),
            Self::RBracket => "]".fmt(f),
            Self::Comma => ",".fmt(f),
            Self::Colon => ":".fmt(f),
            Self::DoubleColon => "::".fmt(f),
            Self::SemiColon => ";".fmt(f),
            Self::Period => ".".fmt(f),
        }
    }
}
