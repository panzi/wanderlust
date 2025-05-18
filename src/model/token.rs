use crate::format::format_iso_string;

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

impl std::fmt::Display for ParsedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(val)  => val.fmt(f),
            Self::Float(val)    => std::fmt::Debug::fmt(&val, f),
            Self::String(val)   => format_iso_string(f, val),
            Self::Name(val)     => val.fmt(f),
            Self::Operator(val) => f.write_str(val),
            Self::LParen        => f.write_str("("),
            Self::RParen        => f.write_str(")"),
            Self::LBracket      => f.write_str("["),
            Self::RBracket      => f.write_str("]"),
            Self::Comma         => f.write_str(","),
            Self::Colon         => f.write_str(":"),
            Self::DoubleColon   => f.write_str("::"),
            Self::SemiColon     => f.write_str(";"),
            Self::Period        => f.write_str("."),
        }
    }
}
