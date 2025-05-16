use super::syntax::{Cursor, Locatable};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TokenKind {
    Int,
    Float,
    String,
    UString,
    EString,
    DollarString,
    Word,
    QIdent,

    /// Sequence of + - * / < > = ~ ! @ # % ^ & | ` ? but not containing -- or /*
    Operator,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dollar,
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
