use std::{num::NonZeroU32, rc::Rc};

use crate::format::format_iso_string;

use super::{name::{Name, QName}, syntax::{Cursor, Locatable}};

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
    String(Rc<str>),
    Name(Name),
    Operator(Rc<str>),
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

impl ParsedToken {
    #[inline]
    pub fn from_string(value: impl Into<Rc<str>>) -> Self {
        Self::String(value.into())
    }

    #[inline]
    pub fn from_name(name: impl Into<Rc<str>>) -> Self {
        Self::Name(Name::new(name))
    }

    #[inline]
    pub fn from_operator(value: impl Into<Rc<str>>) -> Self {
        Self::Operator(value.into())
    }


    #[inline]
    pub(crate) fn new_unquoted(name: impl Into<Rc<str>>) -> Self {
        Self::Name(Name::new_unquoted(name))
    }
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

pub trait ToTokens {
    #[inline]
    fn to_tokens(&self) -> Vec<ParsedToken> {
        let mut tokens = Vec::new();
        self.to_tokens_into(&mut tokens);
        tokens
    }

    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>);
}

impl<T> From<&T> for ParsedToken where T: From<T> {
    fn from(value: &T) -> Self {
        value.into()
    }
}

impl From<Rc<str>> for ParsedToken {
    #[inline]
    fn from(value: Rc<str>) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ParsedToken {
    #[inline]
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<String> for ParsedToken {
    #[inline]
    fn from(value: String) -> Self {
        Self::String(value.into())
    }
}

impl From<u8> for ParsedToken {
    #[inline]
    fn from(value: u8) -> Self {
        Self::Integer(value.into())
    }
}

impl From<u16> for ParsedToken {
    #[inline]
    fn from(value: u16) -> Self {
        Self::Integer(value.into())
    }
}

impl From<u32> for ParsedToken {
    #[inline]
    fn from(value: u32) -> Self {
        Self::Integer(value.into())
    }
}

impl From<i8> for ParsedToken {
    #[inline]
    fn from(value: i8) -> Self {
        Self::Integer(value.into())
    }
}

impl From<i16> for ParsedToken {
    #[inline]
    fn from(value: i16) -> Self {
        Self::Integer(value.into())
    }
}

impl From<i32> for ParsedToken {
    #[inline]
    fn from(value: i32) -> Self {
        Self::Integer(value.into())
    }
}

impl From<i64> for ParsedToken {
    #[inline]
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f32> for ParsedToken {
    #[inline]
    fn from(value: f32) -> Self {
        Self::Float(value.into())
    }
}

impl From<f64> for ParsedToken {
    #[inline]
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Name> for ParsedToken {
    #[inline]
    fn from(value: Name) -> Self {
        Self::Name(value)
    }
}

impl From<NonZeroU32> for ParsedToken {
    #[inline]
    fn from(value: NonZeroU32) -> Self {
        Self::Integer(value.get().into())
    }
}

impl ToTokens for u8 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for u16 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for u32 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for i8 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for i16 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for i32 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for i64 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for f32 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for f64 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for Name {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for QName {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        if let Some(schema) = self.schema() {
            tokens.push(schema.into());
            tokens.push(ParsedToken::Period);
        }
        tokens.push(self.name().into());
    }
}

impl ToTokens for NonZeroU32 {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for Rc<str> {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for &str {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl ToTokens for String {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.into());
    }
}

impl<T> ToTokens for [T] where T: ToTokens {
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        for item in self {
            item.to_tokens_into(tokens);
        }
    }
}

impl<T> ToTokens for Vec<T> where T: ToTokens {
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        for item in self {
            item.to_tokens_into(tokens);
        }
    }
}

impl<T> ToTokens for &T where T: ToTokens {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        (*self).to_tokens_into(tokens);
    }
}

#[macro_export]
macro_rules! make_tokens {
    ($out:expr, $($tokens:tt)*) => {
        { make_tokens!(@make ($out) $($tokens)*); }
    };
    (@make ($out:expr)) => {};
    (@make ($out:expr) $word:ident $($tail:tt)*) => {
        $out.push(ParsedToken::new_unquoted(super::words::$word));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ($($expr:tt)*) $($tail:tt)*) => {
        $out.push(ParsedToken::LParen);
        make_tokens!(@make ($out) $($expr)*);
        $out.push(ParsedToken::RParen);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) [$($expr:tt)*] $($tail:tt)*) => {
        $out.push(ParsedToken::LBracket);
        make_tokens!(@make ($out) $($expr)*);
        $out.push(ParsedToken::RBracket);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ?{$($expr:tt)+} $($tail:tt)*) => {
        if let Some(_value) = ($($expr)+) {
            _value.to_tokens_into($out);
        }
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ?$ident:ident $($tail:tt)*) => {
        if let Some(_value) = $ident {
            _value.to_tokens_into($out);
        }
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) {$($expr:tt)+} $($tail:tt)*) => {
        {
            ($($expr)+).to_tokens_into($out);
        }
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) {} $($tail:tt)*) => {
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) $val:literal $($tail:tt)*) => {
        {
            $val.to_tokens_into($out);
        }
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) , $($tail:tt)*) => {
        $out.push(ParsedToken::Comma);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) :: $($tail:tt)*) => {
        $out.push(ParsedToken::DoubleColon);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) : $($tail:tt)*) => {
        $out.push(ParsedToken::Colon);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ; $($tail:tt)*) => {
        $out.push(ParsedToken::SemiColon);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) . $($tail:tt)*) => {
        $out.push(ParsedToken::Period);
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) + $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("+".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) -> $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("->".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ->> $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("->>".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) - $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("-".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) * $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("*".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) / $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("/".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) % $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("%".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) || $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("||".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) | $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("|".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) && $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("&&".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) & $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("&".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ! $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("!".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ~ $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("~".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
    (@make ($out:expr) ^ $($tail:tt)*) => {
        $out.push(ParsedToken::Operator("^".into()));
        make_tokens!(@make ($out) $($tail)*);
    };
}
