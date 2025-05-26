use std::{hash::{DefaultHasher, Hasher}};

use crate::model::{name::Name, token::ParsedToken};

#[derive(Debug)]
pub struct FmtWriter<W: std::io::Write>(W);

impl<W: std::io::Write> FmtWriter<W> {
    #[inline]
    pub fn new(write: W) -> Self {
        Self(write)
    }

    #[inline]
    pub fn into(self) -> W {
        self.0
    }
}

impl<W: std::io::Write> std::fmt::Write for FmtWriter<W> {
    #[inline]
    fn write_str(&mut self, s: &str) -> Result<(), std::fmt::Error> {
        self.0.write_all(s.as_bytes()).map_err(|_| std::fmt::Error)
    }

    #[inline]
    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> Result<(), std::fmt::Error> {
        self.0.write_fmt(args).map_err(|_| std::fmt::Error)
    }
}

pub fn write_paren_names(names: &[Name], f: &mut impl std::fmt::Write) -> std::fmt::Result {
    let mut iter = names.iter();
    if let Some(name) = iter.next() {
        write!(f, " ({name}")?;

        for name in iter {
            write!(f, ", {name}")?;
        }

        f.write_str(")")?;
    }

    Ok(())
}

#[inline]
pub fn write_token_list(tokens: &[ParsedToken], f: &mut impl std::fmt::Write) -> std::fmt::Result {
    write_token_list_with_options(tokens, f, false)
}

pub fn write_token_list_with_options(tokens: &[ParsedToken], mut f: &mut impl std::fmt::Write, for_functions: bool) -> std::fmt::Result {
    let mut iter = tokens.iter();
    if let Some(first) = iter.next() {
        write!(f, "{first}")?;
        let mut prev = first;
        for token in iter {
            if matches!(token,
                    ParsedToken::Comma |
                    ParsedToken::SemiColon |
                    ParsedToken::LParen |
                    ParsedToken::RParen |
                    ParsedToken::LBracket |
                    ParsedToken::RBracket)
                ||
                (matches!(token,
                    ParsedToken::DoubleColon) &&
                 matches!(prev,
                    ParsedToken::Name(..) |
                    ParsedToken::String(..) |
                    ParsedToken::Integer(..) |
                    ParsedToken::Float(..) |
                    ParsedToken::RParen |
                    ParsedToken::RBracket))
                ||
                (matches!(token,
                    ParsedToken::Period) &&
                 matches!(prev,
                    ParsedToken::Name(..) |
                    ParsedToken::RParen |
                    ParsedToken::RBracket))
                ||
                (matches!(token, ParsedToken::Name(..)) &&
                 matches!(prev,
                    ParsedToken::Period |
                    ParsedToken::DoubleColon |
                    ParsedToken::LParen |
                    ParsedToken::LBracket))
                ||
                (matches!(token,
                    ParsedToken::String(..) |
                    ParsedToken::Integer(..) |
                    ParsedToken::Float(..)) &&
                 matches!(prev,
                    ParsedToken::LParen |
                    ParsedToken::LBracket))
            {
                // pass
                //f.write_str("/* pass */")?;
            } else {
                f.write_str(" ")?;
            }
            if for_functions {
                if let ParsedToken::String(value) = token {
                    format_string_for_functions(&mut f, value)?;
                } else {
                    write!(f, "{token}")?;
                }
            } else {
                write!(f, "{token}")?;
            }
            prev = token;
        }
    }
    Ok(())
}

pub fn format_dollar_string(mut write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    if value.contains("$$") {
        use std::fmt::Write;
        use std::hash::Hash;

        // get some random-ish number
        let mut state = DefaultHasher::new();
        value.hash(&mut state);
        let mut number = state.finish();

        let mut quote = String::new();

        loop {
            quote.clear();
            write!(quote, "$WANDERLUST{number}$")?;

            if !value.contains(&quote) {
                break;
            }

            number = number.wrapping_add(1);
        }

        return write!(write, "{quote}{value}{quote}");
    }
    write!(write, "$${value}$$")
}

pub fn format_ustring(mut write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    write.write_str("U&'")?;

    let mut tail = value;
    while let Some(index) = tail.find(|c: char| c < ' ' || c > '~' || c == '\'' || c == '\\') {
        write.write_str(&tail[..index])?;
        let ch = tail[index..].chars().next().unwrap();
        if ch == '\'' {
            write.write_str("''")?;
        } else if ch == '\\' {
            write.write_str("\\\\")?;
        } else {
            let num = ch as u32;
            if num > 0xFFFF {
                write!(write, "\\+{:06x}", num)?;
            } else {
                write!(write, "\\{:04x}", num)?;
            }
        }
        tail = &tail[index + ch.len_utf8()..];
    }
    write.write_str(tail)?;

    write.write_str("'")
}

fn fomrat_simple_string(mut write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
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

pub fn format_iso_string(write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    if value.contains(|c: char| c < ' ' || c > '~') {
        return format_ustring(write, value);
    }
    fomrat_simple_string(write, value)
}

pub fn format_string_for_functions(write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    let mut dollar = false;
    for ch in value.chars() {
        if ch == '\n' || ch == '\'' {
            dollar = true;
        } else if ch < ' ' || ch > '~' {
            return format_ustring(write, value);
        }
    }

    if dollar {
        format_dollar_string(write, value)
    } else {
        fomrat_simple_string(write, value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IsoString<'a>(pub &'a str);

impl<'a> std::fmt::Display for IsoString<'a> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_iso_string(f, self.0)
    }
}

pub fn join_into<T: std::fmt::Display>(delim: &str, items: &[T], f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut iter = items.iter();
    if let Some(first) = iter.next() {
        std::fmt::Display::fmt(first, f)?;

        for item in iter {
            f.write_str(delim)?;
            std::fmt::Display::fmt(item, f)?;
        }
    }

    Ok(())
}
