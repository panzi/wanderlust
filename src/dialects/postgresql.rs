// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// https://www.postgresql.org/docs/current/sql-createtable.html
// https://www.postgresql.org/docs/current/sql-createindex.html
// https://www.postgresql.org/docs/current/sql-createtype.html

pub mod tokenizer;
pub mod parser;
pub mod reflect;

pub use tokenizer::PostgreSQLTokenizer;
pub use parser::PostgreSQLParser;

use crate::{
    error::{Error, ErrorKind, Result},
    model::{
        integers::{Integer, SignedInteger, UnsignedInteger},
        token::{Token, TokenKind},
    },
};

pub fn uunescape(string: &str, escape: char) -> Option<String> {
    let mut buf = String::with_capacity(string.len());
    let mut tail = string;

    while let Some(index) = tail.find(escape) {
        buf.push_str(&tail[..index]);

        tail = &tail[index + 1..];

        if tail.starts_with('+') {
            let hex = &tail[1..7];
            let Ok(ch) = u32::from_str_radix(hex, 16) else {
                return None;
            };
            let Ok(ch) = ch.try_into() else {
                return None;
            };

            buf.push(ch);

            tail = &tail[7..];
        } else {
            let hex = &tail[..4];
            let Ok(ch) = u32::from_str_radix(hex, 16) else {
                return None;
            };
            let Ok(ch) = ch.try_into() else {
                return None;
            };

            buf.push(ch);

            tail = &tail[4..];
        }
    }

    buf.push_str(tail);

    Some(buf)
}

pub fn parse_uint<I: UnsignedInteger>(token: &Token, mut source: &str) -> Result<I> {
    if source.starts_with('-') {
        return Err(Error::with_message(
            ErrorKind::UnexpectedToken,
            *token.cursor(),
            format!("expected: <unsigned integer>, actual: {source}")
        ));
    }

    if source.starts_with('+') {
        source = &source[1..];
    }

    let value = parse_int_intern(token, source)?;

    Ok(value)
}

pub fn parse_int<I: SignedInteger>(token: &Token, mut source: &str) -> Result<I> {
    if source.starts_with('-') {
        source = &source[1..];
        return parse_neg_int_intern(token, source);
    } else if source.starts_with('+') {
        source = &source[1..];
    }

    parse_int_intern(token, source)
}

fn parse_int_intern<I: Integer>(token: &Token, mut source: &str) -> Result<I> {
    let mut value = I::ZERO;

    // TODO: handle overflow?
    match token.kind() {
        TokenKind::BinInt => {
            source = &source[2..];
            for ch in source.chars() {
                match ch {
                    '0' => {
                        value <<= I::ONE;
                    }
                    '1' => {
                        value <<= I::ONE;
                        value |= I::ONE;
                    }
                    _ => {}
                }
            }
        }
        TokenKind::OctInt => {
            source = &source[2..];
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::EIGHT;
                    value += I::value_of(ch);
                }
            }
        }
        TokenKind::DecInt => {
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::TEN;
                    value += I::value_of(ch);
                }
            }
        }
        TokenKind::HexInt => {
            source = &source[2..];
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::SIXTEEN;
                    value += I::hex_value_of(ch);
                }
            }
        }
        _ => {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: <unsigned integer>, actual: {source}")
            ));
        }
    }

    Ok(value)
}

fn parse_neg_int_intern<I: SignedInteger>(token: &Token, mut source: &str) -> Result<I> {
    let mut value = I::ZERO;

    // TODO: handle overflow?
    match token.kind() {
        TokenKind::BinInt => {
            source = &source[2..];
            for ch in source.chars() {
                match ch {
                    '0' => {
                        value *= I::TWO;
                    }
                    '1' => {
                        value *= I::TWO;
                        value -= I::ONE;
                    }
                    _ => {}
                }
            }
        }
        TokenKind::OctInt => {
            source = &source[2..];
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::EIGHT;
                    value -= I::value_of(ch);
                }
            }
        }
        TokenKind::DecInt => {
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::TEN;
                    value -= I::value_of(ch);
                }
            }
        }
        TokenKind::HexInt => {
            source = &source[2..];
            for ch in source.chars() {
                if ch != '_' {
                    value *= I::SIXTEEN;
                    value -= I::hex_value_of(ch);
                }
            }
        }
        _ => {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: <unsigned integer>, actual: {source}")
            ));
        }
    }

    Ok(value)
}

pub fn parse_string(source: &str) -> Option<String> {
    if !source.starts_with("'") || !source.ends_with("'") || source.len() < 2 {
        return None;
    }
    let mut source = &source[1..source.len() - 1];
    let mut value = String::with_capacity(source.len());

    while let Some(index) = source.find('\'') {
        value.push_str(&source[..index + 1]);
        source = &source[index + 2..];
    }

    value.push_str(source);
    value.shrink_to_fit();

    Some(value)
}

pub fn parse_quot_name(source: &str) -> Option<String> {
    if !source.starts_with("\"") || !source.ends_with("\"") || source.len() < 2 {
        return None;
    }

    let mut source = &source[1..source.len() - 1];
    let mut value = String::with_capacity(source.len());

    while let Some(index) = source.find('"') {
        value.push_str(&source[..index]);
        source = &source[index..];
        if source.starts_with("\"\"") {
            value.push_str("\"");
            source = &source[2..];
        } else {
            return None;
        }
    }

    value.push_str(source);
    value.shrink_to_fit();

    Some(value)
}

pub fn parse_uname(source: &str) -> Option<String> {
    if !source.starts_with("U&\"") || !source.ends_with("\"") || source.len() < 4 {
        return None;
    }

    let mut source = &source[3..source.len() - 1];
    let mut value = String::with_capacity(source.len());

    while let Some(index) = source.find('"') {
        value.push_str(&source[..index]);
        source = &source[index..];
        if source.starts_with("\"\"") {
            value.push_str("\"");
            source = &source[2..];
        } else {
            return None;
        }
    }

    value.push_str(source);
    value.shrink_to_fit();

    Some(value)
}

pub fn parse_ustring(source: &str) -> Option<String> {
    if !source.starts_with("U&'") || !source.ends_with("'") || source.len() < 4 {
        return None;
    }

    let mut source = &source[3..source.len() - 1];
    let mut value = String::with_capacity(source.len());

    while let Some(index) = source.find('\'') {
        value.push_str(&source[..index]);
        source = &source[index..];
        if source.starts_with("''") {
            value.push_str("'");
            source = &source[2..];
        } else {
            return None;
        }
    }

    value.push_str(source);
    value.shrink_to_fit();

    Some(value)
}

pub fn parse_estring(source: &str) -> Option<String> {
    if !source.starts_with("E'") || !source.ends_with("'") || source.len() < 3 {
        return None;
    }

    let mut source = &source[2..source.len() - 1];
    let mut value = String::with_capacity(source.len());

    while let Some(index) = source.find(|c: char| c == '\'' || c == '\\') {
        value.push_str(&source[..index]);
        source = &source[index..];
        if source.starts_with("''") {
            value.push_str("'");
            source = &source[2..];
        } else if source.starts_with("\\") {
            source = &source[1..];
            let ch = source.chars().next()?;
            source = &source[ch.len_utf8()..];
            match ch {
                'b' => value.push_str("\x08"),
                'f' => value.push_str("\x0C"),
                'n' => value.push_str("\n"),
                'r' => value.push_str("\r"),
                't' => value.push_str("\t"),
                'u' => {
                    let slice = source.get(..4)?;
                    let Ok(ch) = u32::from_str_radix(slice, 16) else {
                        return None;
                    };
                    value.push(char::from_u32(ch)?);
                    source = &source[4..];
                }
                'U' => {
                    let slice = source.get(..8)?;
                    let Ok(ch) = u32::from_str_radix(slice, 16) else {
                        return None;
                    };
                    value.push(char::from_u32(ch)?);
                    source = &source[6..];
                }
                'x' => {
                    #[inline]
                    fn from_hex(ch: char) -> Option<u8> {
                        if '0' <= ch && ch <= '9' {
                            Some((ch as u32 - '0' as u32) as u8)
                        } else if 'a' <= ch && ch <= 'f' {
                            Some((ch as u32 - 'a' as u32) as u8 + 10)
                        } else if 'A' <= ch && ch <= 'F' {
                            Some((ch as u32 - 'A' as u32) as u8 + 10)
                        } else {
                            None
                        }
                    }

                    let ch = source.chars().next()?;
                    let mut num = from_hex(ch)?;
                    source = &source[1..];

                    if let Some(ch) = source.chars().next() {
                        if let Some(digit) = from_hex(ch) {
                            num *= 16;
                            num += digit;
                            source = &source[1..];
                        }
                    }

                    value.push(num.into());
                }
                '0'..='7' => {
                    let mut num = (ch as u32 - '0' as u32) as u8;

                    if let Some(ch) = source.chars().next() {
                        if '0' <= ch && ch <= '7' {
                            let digit = (ch as u32 - '0' as u32) as u8;

                            num *= 8;
                            num += digit;

                            source = &source[1..];
                            if let Some(ch) = source.chars().next() {
                                if '0' <= ch && ch <= '7' {
                                    let digit = (ch as u32 - '0' as u32) as u8;

                                    num *= 8;
                                    num += digit;

                                    source = &source[1..];
                                }
                            }
                        }
                    }
                    value.push(num.into());
                }
                _ => value.push(ch),
            }
        } else {
            return None;
        }
    }

    value.push_str(source);
    value.shrink_to_fit();

    Some(value)
}

/// **NOTE:** requires value to be a valid dollar string, else might panic
fn strip_dollar_string(value: &str) -> &str {
    let value = &value[1..value.len() - 1];
    let index = value.find('$').unwrap();
    let value = &value[index + 1..];
    let index = value.rfind('$').unwrap();
    &value[..index]
}
