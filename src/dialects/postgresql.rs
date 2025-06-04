// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// https://www.postgresql.org/docs/current/sql-createtable.html
// https://www.postgresql.org/docs/current/sql-createindex.html
// https://www.postgresql.org/docs/current/sql-createtype.html

use std::{num::NonZeroU32, rc::Rc};

use crate::{
    error::{Error, ErrorKind, Result},
    model::{
        alter::{
            table::{AlterColumn, AlterColumnData, AlterTable, AlterTableAction, AlterTableData},
            types::{AlterType, AlterTypeAction, AlterTypeData, ValuePosition},
            DropBehavior, Owner
        },
        column::{Column, ColumnConstraint, ColumnConstraintData, ColumnMatch, ReferentialAction, Storage},
        database::Database,
        extension::{CreateExtension, Extension, Version},
        floats::Float,
        function::{self, Argmode, Argument, ConfigurationValue, CreateFunction, Function, FunctionBody, FunctionRef, FunctionSignature, ReturnType, SignatureArgument},
        index::{CreateIndex, Direction, Index, IndexItem, IndexItemData, IndexParameters, NullsPosition},
        integers::{Integer, SignedInteger, UnsignedInteger},
        name::{Name, QName},
        syntax::{Cursor, Parser, SourceLocation, Tokenizer},
        table::{CreateTable, Table, TableConstraint, TableConstraintData},
        token::{ParsedToken, ToTokens, Token, TokenKind},
        trigger::{CreateTrigger, Event, LifeCycle, ReferencedTable, Trigger, When},
        types::{BasicType, CompositeAttribute, DataType, IntervalFields, TypeDef, Value}
    },
    ordered_hash_map::OrderedHashMap,
    peek_token
};

use crate::model::words::*;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgreSQLTokenizer<'a> {
    source: &'a str,
    offset: usize,
    peeked: Option<Token>,
}

impl<'a> PostgreSQLTokenizer<'a> {
    #[inline]
    pub fn new(source: &'a str) -> Self {
        Self { source, offset: 0, peeked: None }
    }

    #[inline]
    pub fn source(&self) -> &'a str {
        self.source
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub fn peeked(&self) -> Option<&Token> {
        self.peeked.as_ref()
    }

    #[inline]
    pub fn location(&self) -> SourceLocation {
        SourceLocation::from_offset(self.offset, self.source)
    }

    #[inline]
    fn move_to(&mut self, offset: usize) {
        self.offset = offset;
        self.peeked = None;
    }

    fn skip_ws(&mut self) -> Result<()> {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.offset += ch.len_utf8();
            } else if ch == '-' && self.peek_char_at(1) == Some('-') {
                self.offset += 2;
                if let Some(index) = self.source[self.offset..].find('\n') {
                    self.offset += index + 1;
                } else {
                    self.offset = self.source.len();
                    break;
                }
            } else if ch == '/' && self.peek_char_at(1) == Some('*') {
                let start_offset = self.offset;
                self.offset += 2;
                let mut nesting = 1;
                while let Some(ch) = self.peek_char() {
                    if ch == '*' && self.peek_char_at(1) == Some('/') {
                        nesting -= 1;
                        if nesting == 0 {
                            break;
                        }
                    } else if ch == '/' && self.peek_char_at(1) == Some('*') {
                        nesting += 1;
                    } else {
                        self.offset += 1;
                    }
                }

                if nesting > 0 {
                    let end_offset = self.offset;
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedEOF,
                        Cursor::new(start_offset, end_offset),
                        "actual: <EOF>, expected: */".to_string()
                    ));
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    #[inline]
    fn peek_char(&self) -> Option<char> {
        self.source[self.offset..].chars().next()
    }

    #[inline]
    fn peek_char_at(&self, offset: usize) -> Option<char> {
        self.source[self.offset + offset..].chars().next()
    }

    fn find_string_end(&mut self) -> Result<usize> {
        let slice = &self.source[self.offset..];
        if !slice.starts_with('\'') {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                Cursor::new(self.offset, self.offset + 1),
                "expected: <string>".to_string()
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find('\'') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    "expected: <string>, actual: <EOF>".to_string()
                ));
            };

            len += index + 1;
            slice = &slice[index + 1..];

            if slice.starts_with('\'') {
                len += 1;
                slice = &slice[1..];
            } else {
                break;
            }
        }

        Ok(self.offset + len)
    }

    fn find_quot_name_end(&mut self) -> Result<usize> {
        let slice = &self.source[self.offset..];
        if !slice.starts_with('"') {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                Cursor::new(self.offset, self.offset + 1),
                "expected: <quoted name>".to_string()
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find('"') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    "expected: <quoted name>, actual: <EOF>".to_string()
                ));
            };

            len += index + 1;
            slice = &slice[index + 1..];

            if slice.starts_with('"') {
                len += 1;
                slice = &slice[1..];
            } else {
                break;
            }
        }

        Ok(self.offset + len)
    }

    fn find_estring_end(&mut self) -> Result<usize> {
        let slice = &self.source[self.offset..];
        if !slice.starts_with('\'') {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                Cursor::new(self.offset, self.offset + 1),
                "expected: <estring>".to_string()
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find(|c: char| c == '\'' || c == '\\') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    "expected: <estring>, actual: <EOF>".to_string()
                ));
            };

            len += index;
            slice = &slice[index..];

            if slice.starts_with("''") {
                slice = &slice[2..];
                len += 2;
            } else if slice.starts_with("\\") {
                len += 2;
                if len >= self.source.len() {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(self.offset, self.offset + len),
                        "expected: <estring>, actual: <EOF>".to_string()
                    ));
                }
                slice = &slice[2..];
            } else { // '\''
                len += 1;
                break;
            }

            if slice.starts_with('\'') {
                len += 1;
                slice = &slice[1..];
            } else {
                break;
            }
        }

        Ok(self.offset + len)
    }

    fn parse_uescape(&mut self) -> Result<char> {
        let mut escape = '\\';
        if let Some(next) = self.peek()? {
            if next.kind() == TokenKind::Word && self.get(next.cursor()).eq_ignore_ascii_case(UESCAPE) {
                self.next()?;
                let Some(next) = self.next()? else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *next.cursor(),
                        "expected: <single char string>, actual: <EOF>".to_string()
                    ));
                };

                let source = self.get(next.cursor());
                if next.kind() != TokenKind::String {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *next.cursor(),
                        format!("expected: <single char string>, actual: {source}")
                    ));
                }

                let Some(value) = parse_string(source) else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *next.cursor(),
                        format!("expected: <single char string>, actual: {source}")
                    ));
                };

                if value.chars().count() != 1 {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *next.cursor(),
                        format!("expected: <single char string>, actual: {source}")
                    ));
                }

                escape = value.chars().next().unwrap();
            }
        }

        Ok(escape)
    }
}

macro_rules! operators {
    () => {
        '+' | '-' | '*' | '/' | '<' | '>' | '=' | '~' | '!' | '@' | '#' | '%' | '^' | '&' | '|' | '`' | '?'
    };
}

macro_rules! is_operator {
    ($ch:expr) => {
        matches!($ch, operators!())
    };
}

impl<'a> Tokenizer for PostgreSQLTokenizer<'a> {
    #[inline]
    fn get_offset(&self, start_offset: usize, end_offset: usize) -> &str {
        &self.source[start_offset..end_offset]
    }

    fn parse(&mut self) -> Result<ParsedToken> {
        let Some(token) = self.next()? else {
            return Err(Error::with_cursor(
                ErrorKind::UnexpectedEOF,
                Cursor::new(self.offset, self.offset)
            ));
        };
        let source = token.cursor().get(self.source);
        match token.kind() {
            TokenKind::BinInt | TokenKind::OctInt | TokenKind::DecInt | TokenKind::HexInt => {
                let value = parse_int(&token, source)?;
                Ok(ParsedToken::Integer(value))
            },
            TokenKind::Float => {
                let stripped = source.replace("_", "");
                let Ok(value) = stripped.parse() else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *token.cursor(),
                        format!("expected: <floating point number>, actual: {source}")
                    ));
                };
                Ok(ParsedToken::Float(value))
            },
            TokenKind::String => {
                let Some(value) = parse_string(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <string>, actual: {}", token.kind())
                    ));
                };
                Ok(ParsedToken::String(value.into()))
            },
            TokenKind::UString => {
                let start_offset = token.cursor().start_offset();
                let Some(value) = parse_ustring(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <unicode string>, actual: {}", token.kind())
                    ));
                };

                let escape = self.parse_uescape()?;
                let end_offset = self.offset;
                let Some(value) = uunescape(&value, escape) else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, end_offset),
                        format!("illegal escape sequence in: {source}")
                    ));
                };

                Ok(ParsedToken::String(value.into()))
            },
            TokenKind::EString => {
                let Some(value) = parse_estring(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <estring>, actual: {}", token.kind())
                    ));
                };
                Ok(ParsedToken::String(value.into()))
            },
            TokenKind::DollarString => {
                Ok(ParsedToken::String(strip_dollar_string(source).into()))
            },
            TokenKind::Word => Ok(ParsedToken::Name(Name::new_unquoted(source))),
            TokenKind::QuotName => {
                let Some(value) = parse_quot_name(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <quoted name>, actual: {}", token.kind())
                    ));
                };
                Ok(ParsedToken::Name(Name::new_quoted(value)))
            },
            TokenKind::UName => {
                let start_offset = token.cursor().start_offset();
                let Some(value) = parse_uname(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <unicode name>, actual: {}", token.kind())
                    ));
                };

                let escape = self.parse_uescape()?;
                let end_offset = self.offset;
                let Some(value) = uunescape(&value, escape) else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, end_offset),
                        format!("illegal escape sequence in: {source}")
                    ));
                };

                Ok(ParsedToken::Name(Name::new_quoted(value)))
            },
            TokenKind::Operator => Ok(ParsedToken::Operator(source.into())),
            TokenKind::LParen => Ok(ParsedToken::LParen),
            TokenKind::RParen => Ok(ParsedToken::RParen),
            TokenKind::LBracket => Ok(ParsedToken::LBracket),
            TokenKind::RBracket => Ok(ParsedToken::RBracket),
            TokenKind::Comma => Ok(ParsedToken::Comma),
            TokenKind::DoubleColon => Ok(ParsedToken::DoubleColon),
            TokenKind::Colon => Ok(ParsedToken::Colon),
            TokenKind::SemiColon => Ok(ParsedToken::SemiColon),
            TokenKind::Period => Ok(ParsedToken::Period),
            TokenKind::Equal => Ok(ParsedToken::Equal),
        }
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        if self.peeked.is_none() {
            self.peeked = self.next()?;
        }

        Ok(self.peeked)
    }

    fn next(&mut self) -> Result<Option<Token>> {
        if let Some(token) = self.peeked {
            self.peeked = None;
            return Ok(Some(token));
        }

        self.skip_ws()?;

        let Some(ch) = self.peek_char() else {
            return Ok(None);
        };

        match ch {
            _ if ch.is_ascii_digit() || ((ch == '+' || ch == '-') && self.peek_char_at(1).unwrap_or('\0').is_ascii_digit()) => {
                // number
                let start_offset = self.offset;
                if ch == '+' || ch == '-' {
                    self.offset += 1;
                }
                let tail = &self.source[self.offset..];

                if tail.starts_with("0x") || tail.starts_with("0X") {
                    // hexadecimal
                    self.offset += 2;
                    let end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| !c.is_ascii_hexdigit() && c != '_') {
                        self.offset + index
                    } else {
                        self.source.len()
                    };
                    let len = end_offset - start_offset;
                    if len == 0 {
                        return Err(Error::with_cursor(
                            ErrorKind::IllegalToken,
                            Cursor::new(start_offset, end_offset)
                        ));
                    }
                    self.offset = end_offset;
                    Ok(Some(Token::new(
                        TokenKind::HexInt,
                        Cursor::new(start_offset, end_offset)
                    )))
                } else if tail.starts_with("0o") || tail.starts_with("0O") {
                    // octal
                    self.offset += 2;
                    let end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| !(c >= '0' && c <= '7') && c != '_') {
                        self.offset + index
                    } else {
                        self.source.len()
                    };
                    let len = end_offset - start_offset;
                    if len == 0 {
                        return Err(Error::with_cursor(
                            ErrorKind::IllegalToken,
                            Cursor::new(start_offset, end_offset)
                        ));
                    }
                    self.offset = end_offset;
                    Ok(Some(Token::new(
                        TokenKind::OctInt,
                        Cursor::new(start_offset, end_offset)
                    )))
                } else if tail.starts_with("0b") || tail.starts_with("0B") {
                    // binary
                    self.offset += 2;
                    let end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| c != '0' && c != '1' && c != '_') {
                        self.offset + index
                    } else {
                        self.source.len()
                    };
                    let len = end_offset - start_offset;
                    if len == 0 {
                        return Err(Error::with_cursor(
                            ErrorKind::IllegalToken,
                            Cursor::new(start_offset, end_offset)
                        ));
                    }
                    self.offset = end_offset;
                    Ok(Some(Token::new(
                        TokenKind::BinInt,
                        Cursor::new(start_offset, end_offset)
                    )))
                } else {
                    let mut end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| !c.is_ascii_digit() && c != '_') {
                        self.offset + index
                    } else {
                        self.source.len()
                    };
                    let mut tail = &self.source[end_offset..];
                    let mut float = false;

                    if tail.starts_with(".") {
                        // decimals
                        float = true;
                        end_offset += 1;
                        end_offset = if let Some(index) = self.source[end_offset..].find(|c: char| !c.is_ascii_digit() && c != '_') {
                            end_offset + index
                        } else {
                            self.source.len()
                        };
                        tail = &self.source[end_offset..];
                    }

                    if tail.starts_with("e") || tail.starts_with("E") {
                        // exponent
                        float = true;
                        end_offset += 1;
                        tail = &self.source[end_offset..];
                        if tail.starts_with('+') || tail.starts_with('-') {
                            end_offset += 1;
                        }
                        let exp_start = end_offset;
                        end_offset = if let Some(index) = self.source[end_offset..].find(|c: char| !c.is_ascii_digit() && c != '_') {
                            end_offset + index
                        } else {
                            self.source.len()
                        };

                        if end_offset == exp_start {
                            return Err(Error::with_cursor(
                                ErrorKind::IllegalToken,
                                Cursor::new(start_offset, end_offset)
                            ));
                        }
                    }

                    self.offset = end_offset;
                    Ok(Some(Token::new(
                        if float { TokenKind::Float } else { TokenKind::DecInt },
                        Cursor::new(start_offset, end_offset)
                    )))
                }
            }
            'E' if self.source[self.offset + 1..].starts_with('\'') => {
                // E string
                let start_offset = self.offset;
                self.offset += 1;
                let end_offset = self.find_estring_end()?;
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::EString,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            'U' if self.source[self.offset + 1..].starts_with("&'") => {
                // U& string
                let start_offset = self.offset;
                self.offset += 2;
                let end_offset = self.find_string_end()?;
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::UString,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            'U' if self.source[self.offset + 1..].starts_with("&\"") => {
                // U& identifier
                let start_offset = self.offset;
                self.offset += 2;
                let end_offset = self.find_quot_name_end()?;
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::UName,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            _ if ch.is_alphabetic() || ch == '_' => {
                // word/identifier
                let start_offset = self.offset;
                let end_offset = if let Some(index) = self.source[start_offset..].find(|c: char| !c.is_alphanumeric() && c != '_' && c != '$') {
                    start_offset + index
                } else {
                    self.source.len()
                };
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::Word,
                    Cursor::new(start_offset, end_offset))))
            }
            '\'' => {
                // string
                let start_offset = self.offset;
                let end_offset = self.find_string_end()?;
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::String,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            '"' => {
                // quoted identifier
                let start_offset = self.offset;
                let end_offset = self.find_quot_name_end()?;
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::QuotName,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            '$' => {
                // dollar string or single dollar?
                let start_offset = self.offset;
                self.offset += 1;
                if !self.source[start_offset..].starts_with(|c: char| c.is_alphabetic() || c == '_' || c == '$') {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, start_offset + 2),
                        "expected: <dollar string>".to_string()
                    ));
                }

                let end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| !c.is_alphanumeric() && c != '_') {
                    self.offset + index
                } else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, self.source.len()),
                        "expected: <dollar string>".to_string()
                    ));
                };

                if !self.source[end_offset..].starts_with("$") {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, end_offset),
                        "expected: <dollar string>".to_string()
                    ));
                }

                let end_offset = end_offset + 1;
                let tag = &self.source[start_offset..end_offset];

                let end_offset = if let Some(index) = self.source[end_offset..].find(tag) {
                    end_offset + index
                } else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, self.source.len()),
                        "expected: <dollar string>".to_string()
                    ));
                };
                let end_offset = end_offset + tag.len();
                self.offset = end_offset;

                Ok(Some(Token::new(
                    TokenKind::DollarString,
                    Cursor::new(start_offset, end_offset)
                )))
            }
            ':' => {
                let start_offset = self.offset;
                if self.source[self.offset + 1..].starts_with(":") {
                    self.offset += 2;
                    return Ok(Some(Token::new(
                        TokenKind::DoubleColon,
                        Cursor::new(start_offset, self.offset),
                    )));
                }
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::Colon,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            ',' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::Comma,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            ';' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::SemiColon,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            '.' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::Period,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            '(' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::LParen,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            ')' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::RParen,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            '[' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::LBracket,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            ']' => {
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::RBracket,
                    Cursor::new(start_offset, self.offset),
                )))
            }
            '=' if !self.source[self.offset + 1..].starts_with(|c: char| is_operator!(c)) => {
                // equal
                let start_offset = self.offset;
                self.offset += 1;

                Ok(Some(Token::new(
                    TokenKind::Equal,
                    Cursor::new(start_offset, self.offset))
                ))
            }
            operators!() => {
                // operator
                let start_offset = self.offset;
                self.offset += 1;
                self.offset = if let Some(index) = self.source[self.offset..].find(|c| !is_operator!(c)) {
                    self.offset + index
                } else {
                    self.source.len()
                };

                Ok(Some(Token::new(
                    TokenKind::Operator,
                    Cursor::new(start_offset, self.offset))
                ))
            }
            _ => {
                Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + 1),
                    format!("unexpected: {ch}")
                ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostgreSQLParser<'a> {
    tokenizer: PostgreSQLTokenizer<'a>,
    database: Rc<Database>,
}

impl<'a> PostgreSQLParser<'a> {
    #[inline]
    pub fn new(source: &'a str) -> Self {
        let default_schema = Name::new("public");
        Self {
            tokenizer: PostgreSQLTokenizer::new(source),
            database: Rc::new(Database::new(default_schema)),
        }
    }

    fn parse_quot_name(&self, cursor: &Cursor) -> Result<Name> {
        // assumes that the quoted name is syntactically correct
        let source = cursor.get(self.tokenizer.source());
        let Some(value) = parse_quot_name(source) else {
            return Err(Error::with_message(
                ErrorKind::IllegalToken,
                *cursor,
                format!("expected: <quoted name>, actual: {source}")
            ));
        };

        Ok(Name::new_quoted(value))
    }

    fn parse_uname(&mut self, cursor: &Cursor) -> Result<Name> {
        // parse U&"..." and U&"..." UESCAPE '.'
        // assumes that the quoted name is syntactically correct
        let source = cursor.get(self.tokenizer.source());
        let Some(name) = parse_uname(source) else {
            return Err(Error::with_message(
                ErrorKind::IllegalToken,
                *cursor,
                format!("expected: <unicode name>, actual: {source}")
            ));
        };

        let escape = self.tokenizer.parse_uescape()?;

        let Some(name) = uunescape(&name, escape) else {
            return Err(Error::with_message(
                ErrorKind::IllegalToken,
                Cursor::new(cursor.start_offset(), self.tokenizer.offset()),
                format!("illegal escape sequence in: {source}")
            ));
        };

        Ok(Name::new_quoted(name))
    }

    #[inline]
    fn parse_name(&self, cursor: &Cursor) -> Name {
        let source = cursor.get(self.tokenizer.source());
        Name::new_unquoted(source)
    }

    fn parse_expr(&mut self) -> Result<Vec<ParsedToken>> {
        let Some(mut token) = self.tokenizer.peek()? else {
            return Err(Error::with_message(
                ErrorKind::UnexpectedEOF,
                Cursor::new(self.tokenizer.offset(), self.tokenizer.offset()),
                "expected: <expression>, actual: <EOF>".to_string()
            ));
        };

        match token.kind() {
            TokenKind::LParen => {
                return self.parse_token_list(true, true);
            }
            TokenKind::Comma | TokenKind::SemiColon | TokenKind::Colon | TokenKind::DoubleColon | TokenKind::Period | TokenKind::RParen | TokenKind::RBracket => {
                let actual = self.tokenizer.get(token.cursor());
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <expression>, actual: {actual}")
                ));
            }
            _ => {}
        }

        let mut expr = vec![self.tokenizer.parse()?];

        while let Some(next) = self.tokenizer.peek()? {
            match next.kind() {
                TokenKind::Comma | TokenKind::SemiColon | TokenKind::RParen | TokenKind::RBracket => {
                    break;
                }
                TokenKind::DoubleColon => {
                    expr.push(self.tokenizer.parse()?);

                    let data_type = self.parse_data_type()?;
                    data_type.to_tokens_into(&mut expr);
                    continue;
                }
                TokenKind::Word if !matches!(token.kind(), TokenKind::Period | TokenKind::Operator | TokenKind::DoubleColon) => {
                    break;
                }
                TokenKind::LParen | TokenKind::LBracket => {
                    expr.push(self.tokenizer.parse()?);
                    self.parse_token_list_into(&mut expr, true, true)?;

                    token = if let Some(token) = self.tokenizer.peek()? {
                        token
                    } else {
                        return Err(Error::with_message(
                            ErrorKind::UnexpectedEOF,
                            Cursor::new(self.tokenizer.offset(), self.tokenizer.offset()),
                            "expected: expression, actual: <EOF>".to_string()
                        ));
                    };

                    if next.kind() == TokenKind::LParen {
                        if token.kind() != TokenKind::RParen {
                            let actual = self.tokenizer.get(token.cursor());
                            return Err(Error::with_message(
                                ErrorKind::UnexpectedEOF,
                                *token.cursor(),
                                format!("expected: ), actual: {actual}")));
                        }
                    } else {
                        if token.kind() != TokenKind::RBracket {
                            let actual = self.tokenizer.get(token.cursor());
                            return Err(Error::with_message(
                                ErrorKind::UnexpectedEOF,
                                *token.cursor(),
                                format!("expected: ], actual: {actual}")));
                        }
                    }

                    expr.push(self.tokenizer.parse()?);
                }
                _ => {
                    expr.push(self.tokenizer.parse()?);
                    token = next;
                }
            }
        }

        Ok(expr)
    }

    fn parse_token_list(&mut self, stop_on_comma: bool, stop_on_semicolon: bool) -> Result<Vec<ParsedToken>> {
        let mut tokens = Vec::new();

        self.parse_token_list_into(&mut tokens, stop_on_comma, stop_on_semicolon)?;

        Ok(tokens)
    }

    fn parse_token_list_into(&mut self, tokens: &mut Vec<ParsedToken>, stop_on_comma: bool, stop_on_semicolon: bool) -> Result<()> {
        let mut stack = Vec::new();

        while let Some(token) = self.tokenizer.peek()? {
            match token.kind() {
                TokenKind::LParen => {
                    stack.push(BracketType::Round);
                }
                TokenKind::LBracket => {
                    stack.push(BracketType::Square);
                }
                TokenKind::RParen => {
                    if let Some(br) = stack.pop() {
                        if br != BracketType::Round {
                            return Err(Error::with_message(
                                ErrorKind::UnexpectedToken,
                                *token.cursor(),
                                format!("actual: ), expected: {}", br.closing())
                            ));
                        }
                    } else {
                        return Ok(());
                    }
                }
                TokenKind::RBracket => {
                    if let Some(br) = stack.pop() {
                        if br != BracketType::Square {
                            return Err(Error::with_message(
                                ErrorKind::UnexpectedToken,
                                *token.cursor(),
                                format!("actual: ], expected: {}", br.closing())
                            ));
                        }
                    } else {
                        return Ok(());
                    }
                }
                TokenKind::SemiColon if stop_on_semicolon && stack.is_empty() => {
                    return Ok(());
                }
                TokenKind::Comma if stop_on_comma && stack.is_empty() => {
                    return Ok(());
                }
                _ => {}
            }

            tokens.push(self.tokenizer.parse()?);
        }

        if let Some(br) = stack.pop() {
            return Err(Error::with_message(
                ErrorKind::UnexpectedEOF,
                Cursor::new(self.tokenizer.offset(), self.tokenizer.offset()),
                format!("expected: {}, actual: <EOF>", br.closing())
            ))
        }

        Ok(())
    }

    fn parse_uint<I: UnsignedInteger>(&mut self) -> Result<(Token, I)> {
        let token = self.expect_some()?;
        let source = self.get_source(token.cursor());
        let value = parse_uint(&token, source)?;

        Ok((token, value))
    }

    #[inline]
    fn expect_uint<I: UnsignedInteger>(&mut self) -> Result<I> {
        Ok(self.parse_uint()?.1)
    }

    fn parse_int<I: SignedInteger>(&mut self) -> Result<(Token, I)> {
        let token = self.expect_some()?;
        let source = self.get_source(token.cursor());
        let value = parse_int(&token, source)?;

        Ok((token, value))
    }

    #[inline]
    fn expect_int<I: SignedInteger>(&mut self) -> Result<I> {
        Ok(self.parse_int()?.1)
    }

    fn parse_float<F: Float>(&mut self) -> Result<(Token, F)> {
        let token = self.expect_some()?;
        let source = self.get_source(token.cursor());

        if !matches!(token.kind(), TokenKind::Float | TokenKind::DecInt) {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: <floating-point number>, actual: {source}")
            ));
        }

        let Ok(value) = source.replace("_", "").parse() else {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: <floating-point number>, actual: {source}")
            ));
        };

        Ok((token, value))
    }

    #[inline]
    fn expect_float<F: Float>(&mut self) -> Result<F> {
        Ok(self.parse_float()?.1)
    }

    fn parse_precision(&mut self) -> Result<NonZeroU32> {
        let (token, value) = self.parse_uint()?;
        let Some(value) = NonZeroU32::new(value) else {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                "precision may not be zero".to_string()
            ));
        };
        Ok(value)
    }

    fn parse_drop_behavior(&mut self) -> Result<Option<DropBehavior>> {
        if self.parse_word(RESTRICT)? {
            Ok(Some(DropBehavior::Restrict))
        } else if self.parse_word(CASCADE)? {
            Ok(Some(DropBehavior::Cascade))
        } else {
            Ok(None)
        }
    }

    fn parse_ref_action(&mut self) -> Result<ReferentialAction> {
        if self.parse_word(NO)? {
            self.expect_word(ACTION)?;
            Ok(ReferentialAction::NoAction)
        } else if self.parse_word(RESTRICT)? {
            Ok(ReferentialAction::Restrict)
        } else if self.parse_word(CASCADE)? {
            Ok(ReferentialAction::Cascade)
        } else if self.parse_word(SET)? {
            if self.parse_word(NULL)? {
                let columns = if self.parse_token(TokenKind::LParen)? {
                    let mut columns = Vec::new();
                    loop {
                        columns.push(self.expect_name()?);
                        if !self.parse_token(TokenKind::Comma)? {
                            break;
                        }
                    }
                    self.expect_token(TokenKind::RParen)?;
                    Some(columns)
                } else {
                    None
                };
                Ok(ReferentialAction::SetNull { columns })
            } else if self.parse_word(DEFAULT)? {
                let columns = if self.parse_token(TokenKind::LParen)? {
                    let mut columns = Vec::new();
                    loop {
                        columns.push(self.expect_name()?);
                        if !self.parse_token(TokenKind::Comma)? {
                            break;
                        }
                    }
                    self.expect_token(TokenKind::RParen)?;
                    Some(columns)
                } else {
                    None
                };
                Ok(ReferentialAction::SetDefault { columns })
            } else {
                Err(self.expected_one_of(&[
                    NULL, DEFAULT
                ]))
            }
        } else {
            Err(self.expected_one_of(&[
                NO, ACTION, RESTRICT, CASCADE, SET, NULL, DEFAULT
            ]))
        }
    }

    fn parse_references(&mut self, column_match: &mut Option<ColumnMatch>, on_update: &mut Option<ReferentialAction>, on_delete: &mut Option<ReferentialAction>) -> Result<()> {
        if self.parse_word(MATCH)? {
            if self.parse_word(FULL)? {
                *column_match = Some(ColumnMatch::Full);
            } else if self.parse_word(PARTIAL)? {
                *column_match = Some(ColumnMatch::Partial);
            } else if self.parse_word(SIMPLE)? {
                *column_match = Some(ColumnMatch::Simple);
            } else {
                return Err(self.expected_one_of(&[
                    FULL, PARTIAL, SIMPLE
                ]));
            }
        }

        if self.parse_word(ON)? {
            if self.parse_word(DELETE)? {
                *on_delete = Some(self.parse_ref_action()?);

                if self.parse_word(ON)? {
                    self.expect_word(UPDATE)?;

                    *on_update = Some(self.parse_ref_action()?);
                }
            } else {
                self.expect_word(UPDATE)?;

                *on_update = Some(self.parse_ref_action()?);
            }
        }

        Ok(())
    }

    fn parse_value(&mut self) -> Result<Value> {
        let Some(token) = self.tokenizer.next()? else {
            return Err(Error::with_cursor(
                ErrorKind::UnexpectedEOF,
                Cursor::new(self.tokenizer.offset(), self.tokenizer.offset())
            ));
        };
        let source = self.tokenizer.get(token.cursor());
        match token.kind() {
            TokenKind::BinInt | TokenKind::OctInt | TokenKind::DecInt | TokenKind::HexInt => {
                let value = parse_int(&token, source)?;
                Ok(Value::Integer(value))
            },
            //TokenKind::Float => {
            //    let stripped = source.replace("_", "");
            //    let Ok(value) = stripped.parse() else {
            //        return Err(Error::with_message(
            //            ErrorKind::IllegalToken,
            //            *token.cursor(),
            //            format!("expected: <floating point number>, actual: {source}")
            //        ));
            //    };
            //    Ok(Value::Float(value))
            //},
            TokenKind::String => {
                let Some(value) = parse_string(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <string>, actual: {}", token.kind())
                    ));
                };
                Ok(Value::String(value.into()))
            },
            TokenKind::UString => {
                let start_offset = token.cursor().start_offset();
                let Some(value) = parse_ustring(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <unicode string>, actual: {}", token.kind())
                    ));
                };

                let escape = self.tokenizer.parse_uescape()?;
                let end_offset = self.tokenizer.offset();
                let Some(value) = uunescape(&value, escape) else {
                    let source = self.tokenizer.get(token.cursor());
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, end_offset),
                        format!("illegal escape sequence in: {source}")
                    ));
                };

                Ok(Value::String(value.into()))
            },
            TokenKind::EString => {
                let Some(value) = parse_estring(source) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <estring>, actual: {}", token.kind())
                    ));
                };
                Ok(Value::String(value.into()))
            },
            TokenKind::DollarString => {
                Ok(Value::String(strip_dollar_string(source).into()))
            },
            _ => {
                Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <string>, <integer>, or <float>, actual: {source}")
                ))
            }
        }
    }

    fn parse_type_params(&mut self) -> Result<Option<Rc<[Value]>>> {
        if self.peek_kind(TokenKind::LParen)? {
            self.tokenizer.next()?;
            let mut params = Vec::new();

            if !self.peek_kind(TokenKind::RParen)? {
                loop {
                    let value = self.parse_value()?;
                    params.push(value);

                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }
            }

            self.expect_token(TokenKind::RParen)?;
            Ok(Some(params.into()))
        } else {
            Ok(None)
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let data_type =
        if self.parse_word(BIGINT)? || self.parse_word(INT8)? {
            BasicType::Bigint
        } else if self.parse_word(BIGSERIAL)? || self.parse_word(SERIAL8)? {
            BasicType::BigSerial
        } else if self.parse_word(BIT)? {
            let mut var = false;
            if self.peek_word(VARYING)? {
                self.tokenizer.next()?;
                var = true;
            }
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            if var {
                BasicType::BitVarying(precision)
            } else {
                BasicType::Bit(precision)
            }
        } else if self.parse_word(VARBIT)? {
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            BasicType::BitVarying(precision)
        } else if self.parse_word(BOOLEAN)? || self.parse_word(BOOL)? {
            BasicType::Boolean
        } else if self.parse_word(BOX)? {
            BasicType::Box
        } else if self.parse_word(BYTEA)? {
            BasicType::ByteA
        } else if self.parse_word(CHARACTER)? {
            let mut var = false;
            if self.peek_word(VARYING)? {
                self.tokenizer.next()?;
                var = true;
            }
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            if var {
                BasicType::CharacterVarying(precision)
            } else {
                BasicType::Character(precision)
            }
        } else if self.parse_word(CHAR)? {
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            BasicType::Character(precision)
        } else if self.parse_word(VARCHAR)? {
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            BasicType::CharacterVarying(precision)
        } else if self.parse_word(CIDR)? {
            BasicType::CIDR
        } else if self.parse_word(CIRCLE)? {
            BasicType::Circle
        } else if self.parse_word(DATE)? {
            BasicType::Date
        } else if self.parse_word(DOUBLE)? {
            self.expect_word(PRECISION)?;

            BasicType::DoublePrecision
        } else if self.parse_word(FLOAT8)? {
            BasicType::DoublePrecision
        } else if self.parse_word(INET)? {
            BasicType::INet
        } else if self.parse_word(INTEGER)? || self.parse_word(INT)? || self.parse_word(INT4)? {
            BasicType::Integer
        } else if self.parse_word(INTERVAL)? {
            let mut precision = None;
            let mut fields = None;

            if self.parse_word(YEAR)? {
                if self.parse_word(TO)? {
                    self.expect_word(MONTH)?;
                    fields = Some(IntervalFields::YearToMonth);
                } else {
                    fields = Some(IntervalFields::Year);
                }
            } else if self.parse_word(MONTH)? {
                fields = Some(IntervalFields::Month);
            } else if self.parse_word(DAY)? {
                if self.parse_word(TO)? {
                    if self.parse_word(HOUR)? {
                        fields = Some(IntervalFields::DayToHour);
                    } else if self.parse_word(MINUTE)? {
                        fields = Some(IntervalFields::DayToMinute);
                    } else if self.parse_word(SECOND)? {
                        fields = Some(IntervalFields::DayToSecond);
                    } else {
                        return Err(self.expected_one_of(&[
                            HOUR, MINUTE, SECOND
                        ]));
                    }
                } else {
                    fields = Some(IntervalFields::Day);
                }
            } else if self.parse_word(HOUR)? {
                if self.parse_word(TO)? {
                    if self.parse_word(MINUTE)? {
                        fields = Some(IntervalFields::HourToMinute);
                    } else if self.parse_word(SECOND)? {
                        fields = Some(IntervalFields::HourToSecond);
                    } else {
                        return Err(self.expected_one_of(&[
                            HOUR, MINUTE, SECOND
                        ]));
                    }
                } else {
                    fields = Some(IntervalFields::Hour);
                }
            } else if self.parse_word(MINUTE)? {
                if self.parse_word(TO)? {
                    self.expect_word(SECOND)?;
                    fields = Some(IntervalFields::MinuteToSecond);
                } else {
                    fields = Some(IntervalFields::Minute);
                }
            } else if self.parse_word(SECOND)? {
                fields = Some(IntervalFields::Second);
            }

            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }
            BasicType::Interval { fields, precision }
        } else if self.parse_word(JSON)? {
            BasicType::JSON
        } else if self.parse_word(JSONB)? {
            BasicType::JSONB
        } else if self.parse_word(LINE)? {
            BasicType::Line
        } else if self.parse_word(LSEG)? {
            BasicType::LSeg
        } else if self.parse_word(MACADDR)? {
            BasicType::MacAddr
        } else if self.parse_word(MACADDR8)? {
            BasicType::MacAddr8
        } else if self.parse_word(MONEY)? {
            BasicType::Money
        } else if self.parse_word(NUMERIC)? {
            let mut params = None;
            if self.parse_token(TokenKind::LParen)? {
                let precision = self.parse_precision()?;
                let mut scale = 0;
                if self.parse_token(TokenKind::Comma)? {
                    scale = self.parse_int()?.1;
                }

                self.expect_token(TokenKind::RParen)?;

                params = Some((precision, scale));
            }

            BasicType::Numeric(params)
        } else if self.parse_word(PATH)? {
            BasicType::Path
        } else if self.parse_word(PG_LSN)? {
            BasicType::PgLSN
        } else if self.parse_word(PG_SNAPSHOT)? {
            BasicType::PgSnapshot
        } else if self.parse_word(POINT)? {
            BasicType::Point
        } else if self.parse_word(POLYGON)? {
            BasicType::Polygon
        } else if self.parse_word(REAL)? || self.parse_word(FLOAT4)? {
            BasicType::Real
        } else if self.parse_word(SMALLINT)? || self.parse_word(INT2)? {
            BasicType::SmallInt
        } else if self.parse_word(SMALLSERIAL)? || self.parse_word(SERIAL2)? {
            BasicType::SmallSerial
        } else if self.parse_word(SERIAL)? || self.parse_word(SERIAL4)? {
            BasicType::Serial
        } else if self.parse_word(TEXT)? {
            BasicType::Text
        } else if self.parse_word(TIME)? {
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            let mut with_time_zone = false;

            if self.parse_word(WITH)? {
                self.expect_word(TIME)?;
                self.expect_word(ZONE)?;
                with_time_zone = true;
            } else if self.parse_word(WITHOUT)? {
                self.expect_word(TIME)?;
                self.expect_word(ZONE)?;
                with_time_zone = false;
            }

            BasicType::Time { precision, with_time_zone }
        } else if self.parse_word(TIMETZ)? {
            BasicType::Time { precision: None, with_time_zone: true }
        } else if self.parse_word(TIMESTAMP)? {
            let mut precision = None;
            if self.parse_token(TokenKind::LParen)? {
                precision = Some(self.parse_precision()?);
                self.expect_token(TokenKind::RParen)?;
            }

            let mut with_time_zone = false;

            if self.parse_word(WITH)? {
                self.expect_word(TIME)?;
                self.expect_word(ZONE)?;
                with_time_zone = true;
            } else if self.parse_word(WITHOUT)? {
                self.expect_word(TIME)?;
                self.expect_word(ZONE)?;
                with_time_zone = false;
            }

            BasicType::Timestamp { precision, with_time_zone }
        } else if self.parse_word(TIMESTAMPTZ)? {
            BasicType::Timestamp { precision: None, with_time_zone: true }
        } else if self.parse_word(TSQUERY)? {
            BasicType::TsQuery
        } else if self.parse_word(TSVECTOR)? {
            BasicType::TsVector
        } else if self.parse_word(TXID_SNAPSHOT)? {
            BasicType::TxIdSnapshot
        } else if self.parse_word(UUID)? {
            BasicType::UUID
        } else if self.parse_word(XML)? {
            BasicType::XML
        } else {
            let first = self.expect_name()?;

            if self.parse_token(TokenKind::Period)? {
                let second = self.expect_name()?;

                if self.parse_operator("%")? {
                    self.expect_word(TYPE)?;

                    BasicType::ColumnType {
                        table_name: self.database.resolve_table_name(&first),
                        column_name: second
                    }
                } else if self.parse_token(TokenKind::Period)? {
                    let column_name = self.expect_name()?;

                    self.expect_operator("%")?;
                    self.expect_word(TYPE)?;

                    BasicType::ColumnType {
                        table_name: QName::new(
                            Some(first),
                            second,
                        ),
                        column_name
                    }
                } else {
                    let parameters = self.parse_type_params()?;
                    BasicType::UserDefined {
                        name: QName::new(
                            Some(first),
                            second
                        ),
                        parameters
                    }
                }
            } else {
                let parameters = self.parse_type_params()?;
                BasicType::UserDefined {
                    name: self.database.resolve_type_name(&first),
                    parameters
                }
            }
        };

        let mut array_dimensions: Option<Box<[Option<u32>]>> = None;
        if self.parse_word(ARRAY)? {
            let mut dims = None;
            if self.parse_token(TokenKind::LBracket)? {
                if !self.peek_kind(TokenKind::RBracket)? {
                    dims = Some(self.expect_uint()?);
                }
                self.expect_token(TokenKind::RBracket)?;
            }

            array_dimensions = Some(Box::new([dims]));
        } else if self.peek_kind(TokenKind::LBracket)? {
            let mut dims = Vec::new();
            while self.parse_token(TokenKind::LBracket)? {
                if !self.peek_kind(TokenKind::RBracket)? {
                    dims.push(Some(self.expect_uint()?));
                } else {
                    dims.push(None);
                }
                self.expect_token(TokenKind::RBracket)?;
            }
            array_dimensions = Some(dims.into_boxed_slice());
        }

        Ok(DataType::new(data_type, array_dimensions.map(Into::into)))
    }

    #[inline]
    fn parse_qual_name(&mut self) -> Result<QName> {
        let name = self.expect_name()?;
        if self.parse_token(TokenKind::Period)? {
            let actual_name = self.expect_name()?;
            Ok(QName::new(Some(name), actual_name))
        } else {
            Ok(QName::new(None, name))
        }
    }

    #[inline]
    fn parse_qual_name_default(&mut self) -> Result<QName> {
        let name = self.expect_name()?;
        if self.parse_token(TokenKind::Period)? {
            let actual_name = self.expect_name()?;
            Ok(QName::new(Some(name), actual_name))
        } else {
            Ok(QName::new(Some(self.database.default_schema().clone()), name))
        }
    }

    #[inline]
    fn parse_ref_table(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.database.resolve_table_name(&first))
        }
    }

    #[inline]
    fn parse_ref_type(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.database.resolve_type_name(&first))
        }
    }

    #[inline]
    fn parse_ref_function_for_trigger(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            let reference = FunctionRef::new(first.clone(), vec![]);
            Ok(self.database.resolve_function_reference(&reference))
        }
    }

    #[inline]
    fn parse_ref_index(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.database.resolve_index_name(&first))
        }
    }

    #[inline]
    fn parse_ref_collation(&mut self) -> Result<QName> {
        self.parse_qual_name()
    }

    fn parse_operator(&mut self, op: &str) -> Result<bool> {
        let Some(token) = self.peek_token()? else {
            return Ok(false);
        };

        if token.kind() == TokenKind::Operator && self.get_source(token.cursor()) == op {
            self.tokenizer.next()?;
            return Ok(true);
        }

        Ok(false)
    }

    fn expect_operator(&mut self, op: &str) -> Result<Token> {
        let token = self.expect_token(TokenKind::Operator)?;
        let source = self.get_source(token.cursor());

        if source != op {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: {op}, actual: {source}")
            ));
        }
        
        Ok(token)
    }

    fn parse_table_constaint(&mut self) -> Result<TableConstraint> {
        let mut constraint_name = None;
        let constraint_data;

        if self.parse_word(CONSTRAINT)? {
            constraint_name = Some(self.expect_name()?);
        }

        if self.peek_word(CHECK)? {
            self.expect_some()?;

            self.expect_token(TokenKind::LParen)?;
            let expr = self.parse_token_list(false, false)?;
            self.expect_token(TokenKind::RParen)?;

            let mut inherit = true;
            if self.peek_word(NO)? {
                self.expect_some()?;
                self.expect_word(INHERIT)?;
                inherit = false;
            }

            constraint_data = TableConstraintData::Check { expr: expr.into(), inherit };
        } else if self.peek_word(UNIQUE)? {
            self.expect_some()?;

            let mut nulls_distinct = None;
            if self.parse_word(NULLS)? {
                if self.parse_word(NOT)? {
                    nulls_distinct = Some(false);
                } else {
                    nulls_distinct = Some(true);
                }
                self.expect_word(DISTINCT)?;
            }

            self.expect_token(TokenKind::LParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_name()?);
                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }
            self.expect_token(TokenKind::RParen)?;

            let index_parameters = self.parse_index_parameters()?;

            constraint_data = TableConstraintData::Unique { nulls_distinct, columns: columns.into(), index_parameters };
        } else if self.peek_word(PRIMARY)? {
            self.expect_some()?;
            self.expect_word(KEY)?;

            self.expect_token(TokenKind::LParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_name()?);
                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }
            self.expect_token(TokenKind::RParen)?;

            let index_parameters = self.parse_index_parameters()?;

            constraint_data = TableConstraintData::PrimaryKey { columns: columns.into(), index_parameters };
        } else if self.peek_word(FOREIGN)? {
            self.expect_some()?;
            self.expect_word(KEY)?;

            self.expect_token(TokenKind::LParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_name()?);
                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }
            self.expect_token(TokenKind::RParen)?;

            self.expect_word(REFERENCES)?;

            let ref_table = self.parse_ref_table()?;
            let mut ref_columns = None;

            if self.parse_token(TokenKind::LParen)? {
                let mut refcolumns = Vec::new();
                loop {
                    refcolumns.push(self.expect_name()?);
                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }
                self.expect_token(TokenKind::RParen)?;
                ref_columns = Some(refcolumns.into());
            }

            let mut column_match = None;
            let mut on_delete = None;
            let mut on_update = None;
            self.parse_references(
                &mut column_match, &mut on_update, &mut on_delete
            )?;

            constraint_data = TableConstraintData::ForeignKey {
                columns: columns.into(),
                ref_table,
                ref_columns,
                column_match,
                on_update,
                on_delete,
            };
        } else {
            let token = self.expect_some()?;
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: <column constraint>, actual: {}", token.kind())
            ));
        }

        let mut deferrable = None;
        if self.parse_word(DEFERRABLE)? {
            deferrable = Some(true);
        } else if self.parse_word(NOT)? {
            self.expect_word(DEFERRABLE)?;
            deferrable = Some(false);
        }

        let mut initially_deferred = None;
        if self.peek_word(INITIALLY)? {
            self.expect_some()?;

            if self.parse_word(DEFERRED)? {
                initially_deferred = Some(true);
            } else {
                self.expect_word(IMMEDIATE)?;
                initially_deferred = Some(false);
            }
        }

        Ok(TableConstraint::new(
            constraint_name,
            constraint_data,
            deferrable,
            initially_deferred
        ))
    }

    fn parse_storage(&mut self) -> Result<Storage> {
        if self.parse_word(PLAIN)? {
            Ok(Storage::Plain)
        } else if self.parse_word(EXTERNAL)? {
            Ok(Storage::External)
        } else if self.parse_word(EXTENDED)? {
            Ok(Storage::Extended)
        } else if self.parse_word(MAIN)? {
            Ok(Storage::Main)
        } else if self.parse_word(DEFAULT)? {
            Ok(Storage::Default)
        } else {
            Err(self.expected_one_of(&[
                PLAIN, EXTERNAL, EXTENDED, MAIN, DEFAULT
            ]))
        }
    }

    fn parse_index_parameters(&mut self) -> Result<IndexParameters> {
        let include = if self.parse_word(INCLUDE)? {
            let mut include = Vec::new();

            self.expect_token(TokenKind::LParen)?;

            loop {
                include.push(self.expect_name()?);

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            self.expect_token(TokenKind::RParen)?;

            Some(include.into())
        } else {
            None
        };

        let storage_parameters = if self.parse_word(WITH)? {
            let mut storage_parameters = OrderedHashMap::new();

            loop {
                let key = self.expect_name()?;

                let value = if self.parse_token(TokenKind::Equal)? {
                    Some(self.parse_token_list(true, true)?.into())
                } else {
                    None
                };

                storage_parameters.insert(key, value);

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            self.expect_token(TokenKind::RParen)?;

            Some(storage_parameters)
        } else {
            None
        };

        let tablespace = if self.parse_word(TABLESPACE)? {
            Some(self.expect_name()?)
        } else {
            None
        };

        Ok(IndexParameters::new(include, storage_parameters, tablespace))
    }

    fn parse_column(&mut self, table_name: &Name, mut table_constraints: Option<&mut OrderedHashMap<Name, Rc<TableConstraint>>>) -> Result<Rc<Column>> {
        let column_name = self.expect_name()?;
        let data_type = self.parse_data_type()?;

        let storage = if self.parse_word(STORAGE)? {
            self.parse_storage()?
        } else {
            Storage::Default
        };

        let compression = if self.parse_word(COMPRESSION)? {
            if self.parse_word(DEFAULT)? {
                None
            } else {
                Some(self.expect_name()?)
            }
        } else {
            None
        };

        let collation = if self.peek_word(COLLATE)? {
            self.expect_some()?;
            Some(self.parse_ref_collation()?)
        } else {
            None
        };

        let mut column_constraints = Vec::new();

        loop {
            let mut constraint_name = None;
            if self.peek_word(CONSTRAINT)? {
                self.expect_some()?;
                constraint_name = Some(self.expect_name()?);
            }

            let constraint_data;

            if self.parse_word(NOT)? {
                self.expect_word(NULL)?;

                constraint_data = ColumnConstraintData::NotNull;
            } else if self.parse_word(NULL)? {
                constraint_data = ColumnConstraintData::Null;
            } else if self.parse_word(CHECK)? {
                self.expect_token(TokenKind::LParen)?;
                let expr = self.parse_token_list(false, false)?;
                self.expect_token(TokenKind::RParen)?;

                let mut inherit = true;
                if self.peek_word(NO)? {
                    self.expect_some()?;
                    self.expect_word(INHERIT)?;
                    inherit = false;
                }

                constraint_data = ColumnConstraintData::Check { expr: expr.into(), inherit };
            } else if self.parse_word(DEFAULT)? {
                let value = self.parse_expr()?;

                constraint_data = ColumnConstraintData::Default { value: value.into() };
            } else if self.parse_word(UNIQUE)? {
                let mut nulls_distinct = None;
                if self.parse_word(NULLS)? {
                    if self.parse_word(NOT)? {
                        nulls_distinct = Some(false);
                    } else {
                        nulls_distinct = Some(true);
                    }
                    self.expect_word(DISTINCT)?;
                }

                let index_parameters = self.parse_index_parameters()?;

                constraint_data = ColumnConstraintData::Unique { nulls_distinct, index_parameters };
            } else if self.parse_word(PRIMARY)? {
                self.expect_word(KEY)?;

                let index_parameters = self.parse_index_parameters()?;

                constraint_data = ColumnConstraintData::PrimaryKey { index_parameters };
            } else if self.parse_word(REFERENCES)? {
                let ref_table = self.parse_ref_table()?;

                let mut ref_column = None;
                if self.parse_token(TokenKind::LParen)? {
                    ref_column = Some(self.expect_name()?);
                    self.expect_token(TokenKind::RParen)?;
                }

                let mut column_match = None;
                let mut on_delete = None;
                let mut on_update = None;
                self.parse_references(
                    &mut column_match, &mut on_update, &mut on_delete
                )?;

                constraint_data = ColumnConstraintData::References {
                    ref_table, ref_column,
                    column_match, on_update, on_delete,
                };
            } else {
                if constraint_name.is_some() {
                    let token = self.expect_some()?;
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <column constraint>, actual: {}", token.kind())
                    ));
                }
                break;
            }

            let mut deferrable = None;
            if self.parse_word(DEFERRABLE)? {
                deferrable = Some(true);
            } else if self.peek_word(NOT)? {
                let start_offset = self.expect_some()?.cursor().start_offset();

                // Might be NOT NULL!
                // I guess SQL needs more than one token lookahead!
                if self.parse_word(DEFERRABLE)? {
                    deferrable = Some(false);
                } else {
                    self.tokenizer.move_to(start_offset);
                }
            }

            let mut initially_deferred = None;
            if self.peek_word(INITIALLY)? {
                self.expect_some()?;

                if self.parse_word(DEFERRED)? {
                    initially_deferred = Some(true);
                } else {
                    self.expect_word(IMMEDIATE)?;
                    initially_deferred = Some(false);
                }
            }

            let constraint = ColumnConstraint::new(
                constraint_name,
                constraint_data,
                deferrable,
                initially_deferred,
            );

            if let Some(table_constraints) = &mut table_constraints {
                if let Some(mut table_constraint) = constraint.to_table_constraint(table_name, &column_name) {
                    let name = table_constraint.ensure_name(table_name, table_constraints);
                    table_constraints.insert(name.clone(), Rc::new(table_constraint));
                    continue;
                }
            }

            column_constraints.push(Rc::new(constraint));
        }

        Ok(Rc::new(Column::new(
            column_name,
            data_type,
            storage,
            compression,
            collation,
            column_constraints,
        )))
    }

    fn parse_schema_intern(&mut self) -> Result<()> {
        let if_not_exists = self.parse_if_not_exists()?;

        if self.parse_word(AUTHORIZATION)? {
            let start_offset = self.tokenizer.offset();
            let owner = self.parse_owner()?;
            let end_offset = self.tokenizer.offset();

            if self.peek_kind(TokenKind::SemiColon)? || self.peek_token()?.is_none() || self.peek_words(&[
                CREATE, GRANT
            ])?.is_some() {
                match owner {
                    Owner::User(name) => {
                        self.parse_schema_tail(start_offset, if_not_exists, name)?;
                    }
                    _ => {
                        return Err(Error::with_message(
                            ErrorKind::SyntaxError,
                            Cursor::new(start_offset, end_offset),
                            "Schema must have a defined name".to_owned()
                        ));
                    }
                }
            }
        }

        let start_offset = self.tokenizer.offset();
        let name = self.expect_name()?;
        self.parse_schema_tail(start_offset, if_not_exists, name)?;

        Ok(())
    }

    fn parse_schema_tail(&mut self, start_offset: usize, if_not_exists: bool, name: Name) -> Result<()> {
        Rc::make_mut(&mut self.database).create_schema(if_not_exists, name.clone()).map_err(|mut err| {
            *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
            err
        })?;

        if !if_not_exists && !self.peek_kind(TokenKind::SemiColon)? && self.peek_token()?.is_some() {
            // TODO: optional schema elements
            // See: https://www.postgresql.org/docs/17/sql-createschema.html
            let old_default_schema = Rc::make_mut(&mut self.database).set_default_schema(name.clone());
            let old_search_path = self.database.search_path().to_vec();
            Rc::make_mut(&mut self.database).set_default_search_path();

            let res = self.parse_schema_elements();

            let database = Rc::make_mut(&mut self.database);
            database.set_default_schema(old_default_schema);
            *database.search_path_mut() = old_search_path;

            res?;
        }

        Ok(())
    }

    fn parse_schema_elements(&mut self) -> Result<()> {
        while let Some(token) = self.tokenizer.peek()? {
            if token.kind() == TokenKind::SemiColon {
                break;
            }

            let start_offset = token.cursor().start_offset();
            if self.parse_word(CREATE)? {
                self.parse_create_intern(start_offset)?;
            } else if self.parse_word(GRANT)? {
                // TODO: GRANT...
                self.parse_token_list(false, true)?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse GRANT statements: {source}");
            } else {
                return Err(self.expected_one_of(&[
                    CREATE, GRANT
                ]));
            }
        }
        Ok(())
    }

    fn parse_create_intern(&mut self, start_offset: usize) -> Result<()> {
        // "CREATE" is already parsed
        if self.parse_word(TABLE)? {
            let table = self.parse_table_intern(true)?;
            Rc::make_mut(&mut self.database).create_table(table).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(UNLOGGED)? {
            self.expect_word(TABLE)?;
            let table = self.parse_table_intern(false)?;
            Rc::make_mut(&mut self.database).create_table(table).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(INDEX)? {
            let index = self.parse_index_intern(false)?;
            Rc::make_mut(&mut self.database).create_index(index).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(UNIQUE)? && self.parse_word(INDEX)? {
            let index = self.parse_index_intern(true)?;
            Rc::make_mut(&mut self.database).create_index(index).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(TYPE)? {
            let type_def = self.parse_type_def_intern()?;
            Rc::make_mut(&mut self.database).create_type(type_def).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(SEQUENCE)? {
            // TODO: CREATE SEQUENCE?
            self.parse_token_list(false, true)?;

            let end_offset = self.tokenizer.offset();
            let source = self.tokenizer.get_offset(start_offset, end_offset);

            eprintln!("TODO: parse CREATE SEQUENCE statements: {source}");
            Ok(())
        } else if self.parse_word(EXTENSION)? {
            // CREATE EXTENSION
            let if_not_exists = self.parse_if_not_exists()?;

            let name = self.expect_name()?;

            self.parse_word(WITH)?;

            let schema = if self.parse_word(SCHEMA)? {
                self.expect_name()?
            } else {
                self.database.search_path().first().unwrap_or(self.database.default_schema()).clone()
            };

            let version = if self.parse_word(VERSION)? {
                Some(self.parse_version()?)
            } else {
                None
            };

            let cascade = self.parse_word(CASCADE)?;

            let extension = CreateExtension::new(
                if_not_exists,
                Extension::new(
                    QName::new(Some(schema), name),
                    version,
                ),
                cascade
            );

            Rc::make_mut(&mut self.database).create_extension(extension).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(FUNCTION)? {
            // CREATE FUNCTION/PROCEDURE
            let function = self.parse_function_intern(false, true)?;
            Rc::make_mut(&mut self.database).create_function(function).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(PROCEDURE)? {
            // CREATE FUNCTION/PROCEDURE
            let function = self.parse_function_intern(true, true)?;
            Rc::make_mut(&mut self.database).create_function(function).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(OR)? {
            // CREATE OR REPLACE FUNCTION/PROCEDURE/TRIGGER
            self.expect_word(REPLACE)?;

            if self.parse_word(FUNCTION)? {
                let function = self.parse_function_intern(false, true)?;
                Rc::make_mut(&mut self.database).create_function(function).map_err(|mut err| {
                    *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                    err
                })
            } else if self.parse_word(PROCEDURE)? {
                let function = self.parse_function_intern(true, true)?;
                Rc::make_mut(&mut self.database).create_function(function).map_err(|mut err| {
                    *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                    err
                })
            } else if self.parse_word(CONSTRAINT)? {
                // CREATE CONSTRAINT TRIGGER
                self.expect_word(TRIGGER)?;
                let trigger = self.parse_trigger_intern(true, true)?;
                Rc::make_mut(&mut self.database).create_trigger(trigger).map_err(|mut err| {
                    *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                    err
                })
            } else if self.parse_word(TRIGGER)? {
                let trigger = self.parse_trigger_intern(true, false)?;
                Rc::make_mut(&mut self.database).create_trigger(trigger).map_err(|mut err| {
                    *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                    err
                })
            } else {
                Err(self.expected_one_of(&[
                    FUNCTION, PROCEDURE, "[CONSTRAINT] TRIGGER"
                ]))
            }
        } else if self.parse_word(TRIGGER)? {
            // CREATE TRIGGER
            let trigger = self.parse_trigger_intern(false, false)?;
            Rc::make_mut(&mut self.database).create_trigger(trigger).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else if self.parse_word(CONSTRAINT)? {
            // CREATE CONSTRAINT TRIGGER
            self.expect_word(TRIGGER)?;
            let trigger = self.parse_trigger_intern(false, true)?;
            Rc::make_mut(&mut self.database).create_trigger(trigger).map_err(|mut err| {
                *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                err
            })
        } else {
            Err(self.expected_one_of(&[
                TABLE, "[UNIQUE] INDEX", TYPE, EXTENSION, SEQUENCE, FUNCTION, PROCEDURE, "[CONSTRAINT] TRIGGER", SCHEMA
            ]))
        }
    }

    fn parse_table_intern(&mut self, logged: bool) -> Result<CreateTable> {
        // "CREATE TABLE" is already parsed

        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_qual_name_default()?;
        let mut columns = OrderedHashMap::new();
        let mut table_constraints = OrderedHashMap::new();

        self.expect_token(TokenKind::LParen)?;

        while !self.peek_kind(TokenKind::RParen)? {
            if self.peek_words(&[CONSTRAINT, CHECK, UNIQUE, PRIMARY, FOREIGN])?.is_some() {
                let mut constraint = self.parse_table_constaint()?;
                let constraint_name = constraint.ensure_name(table_name.name(), &table_constraints);
                table_constraints.insert(constraint_name.clone(), Rc::new(constraint));
            } else {
                let start_offset = self.tokenizer.offset();
                let column = self.parse_column(table_name.name(), Some(&mut table_constraints))?;
                if columns.contains_key(column.name()) {
                    return Err(Error::with_message(
                        ErrorKind::ColumnExists,
                        Cursor::new(start_offset, self.tokenizer.offset()),
                        format!("column {} already exists in table {}", column.name(), table_name)
                    ));
                }
                columns.insert(column.name().clone(), column);
            }

            if self.peek_kind(TokenKind::Comma)? {
                self.expect_some()?;
            } else {
                break;
            }
        }
        self.expect_token(TokenKind::RParen)?;

        let mut inherits = Vec::new();

        if self.parse_word(INHERITS)? {
            self.expect_token(TokenKind::LParen)?;

            loop {
                let super_table = self.parse_ref_table()?;
                if !inherits.contains(&super_table) {
                    inherits.push(super_table);
                }

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            self.expect_token(TokenKind::RParen)?;
        }

        let mut table = Table::new(
            table_name,
            logged,
            columns,
            table_constraints,
            OrderedHashMap::new(),
            inherits,
        );

        table.convert_serial();

        Ok(CreateTable::new(table, if_not_exists))
    }

    fn parse_index_item(&mut self) -> Result<IndexItem> {
        let data;
        if self.parse_token(TokenKind::LParen)? {
            let expr = self.parse_token_list(false, false)?;
            data = IndexItemData::Expr(expr.into());
            self.expect_token(TokenKind::RParen)?;
        } else {
            let column_name = self.expect_name()?;
            data = IndexItemData::Column(column_name);
        }

        let mut collation = None;
        if self.parse_word(COLLATE)? {
            collation = Some(self.parse_ref_collation()?);
        }

        let mut direction = None;
        if self.parse_word(ASC)? {
            direction = Some(Direction::Asc);
        } else if self.parse_word(DESC)? {
            direction = Some(Direction::Desc);
        }

        let mut nulls_position = None;
        if self.parse_word(NULLS)? {
            if self.parse_word(FIRST)? {
                nulls_position = Some(NullsPosition::First);
            } else if self.parse_word(LAST)? {
                nulls_position = Some(NullsPosition::Last);
            } else {
                return Err(self.expected_one_of(&[
                    FIRST, LAST
                ]));
            }
        }

        Ok(IndexItem::new(data, collation, direction, nulls_position))
    }

    fn parse_index_intern(&mut self, unique: bool) -> Result<CreateIndex> {
        // "CREATE [UNIQUE] INDEX" is already parsed"

        let concurrently = self.parse_word(CONCURRENTLY)?;

        let mut if_not_exists = false;
        let index_name = if self.parse_word(IF)? {
            self.expect_word(NOT)?;
            self.expect_word(EXISTS)?;
            if_not_exists = true;

            let name = self.expect_name()?;
            self.expect_word(ON)?;
            Some(name)
        } else if self.parse_word(ON)? {
            None
        } else if peek_token!(self, TokenKind::Word | TokenKind::QuotName | TokenKind::UName)?.is_some() {
            let name = self.expect_name()?;
            self.expect_word(ON)?;
            Some(name)
        } else {
            self.expect_word(ON)?;
            None
        };

        let table_name = self.parse_ref_table()?;

        let method = if self.parse_word(USING)? {
            Some(self.expect_name()?)
        } else {
            None
        };

        self.expect_token(TokenKind::LParen)?;
        let mut items = Vec::new();
        loop {
            items.push(self.parse_index_item()?);

            if !self.parse_token(TokenKind::Comma)? {
                break;
            }
        }
        self.expect_token(TokenKind::RParen)?;

        let mut nulls_distinct = None;
        if self.parse_word(NULLS)? {
            if self.parse_word(NOT)? {
                nulls_distinct = Some(false);
            } else {
                nulls_distinct = Some(true);
            }
            self.expect_word(DISTINCT)?;
        }

        let mut predicate = None;
        if self.parse_word(WHERE)? {
            predicate = Some(self.parse_token_list(false, true)?);
        }

        let index = Index::new(
            unique,
            index_name,
            table_name,
            method,
            items,
            nulls_distinct,
            predicate,
        );

        Ok(CreateIndex::new(index, concurrently, if_not_exists))
    }

    fn parse_composite_attribute(&mut self) -> Result<CompositeAttribute> {
        let name = self.expect_name()?;
        let data_type = self.parse_data_type()?;

        let collation = if self.parse_word(COLLATE)? {
            Some(self.parse_ref_collation()?)
        } else {
            None
        };

        Ok(CompositeAttribute::new(name, data_type, collation))
    }

    fn parse_type_def_intern(&mut self) -> Result<TypeDef> {
        // "CREATE TYPE" is already parsed
        // TODO: other types
        let type_name = self.parse_qual_name_default()?;
        self.expect_word(AS)?;

        if self.parse_word(ENUM)? {
            self.expect_token(TokenKind::LParen)?;

            let mut values = Vec::new();
            while !self.peek_kind(TokenKind::RParen)? {
                let value = self.expect_string()?;
                values.push(value);
                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            self.expect_token(TokenKind::RParen)?;

            Ok(TypeDef::create_enum(type_name, values))
        } else {
            self.expect_token(TokenKind::LParen)?;

            let mut attributes = OrderedHashMap::new();
            while !self.peek_kind(TokenKind::RParen)? {
                let attribute = self.parse_composite_attribute()?;
                attributes.insert(attribute.name().clone(), attribute);

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            self.expect_token(TokenKind::RParen)?;

            Ok(TypeDef::create_composite(type_name, attributes))
        }
    }

    fn parse_alter_table_intern(&mut self) -> Result<Rc<AlterTable>> {
        // "ALTER TABLE" is already parsed
        let if_exists = self.parse_if_exists()?;
        let only_offset = self.tokenizer.offset();
        let only = self.parse_word(ONLY)?;
        let table_name = self.parse_ref_table()?;
        let data;

        if self.parse_word(RENAME)? {
            if self.parse_word(TO)? {
                if only {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        Cursor::new(only_offset, self.tokenizer.offset()),
                        format!("illegal token {ONLY} for {RENAME} {COLUMN}")
                    ));
                }
                let new_name = self.expect_name()?;

                data = AlterTableData::RenameTable { if_exists, new_name };
            } else if self.parse_word(CONSTRAINT)? {
                let constraint_name = self.expect_name()?;
                self.expect_word(TO)?;
                let new_constraint_name = self.expect_name()?;

                data = AlterTableData::RenameConstraint { if_exists, only, constraint_name, new_constraint_name };
            } else {
                self.parse_word(COLUMN)?;
                let column_name = self.expect_name()?;
                self.expect_word(TO)?;
                let new_column_name = self.expect_name()?;

                data = AlterTableData::RenameColumn { if_exists, only, column_name, new_column_name };
            }
        } else if self.parse_word(SET)? {
            self.expect_word(SCHEMA)?;
            if only {
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    Cursor::new(only_offset, self.tokenizer.offset()),
                    format!("illegal token {ONLY} for {SET} {SCHEMA}")
                ));
            }
            let new_schema = self.expect_name()?;

            data = AlterTableData::SetSchema { if_exists, new_schema };
        } else {
            let mut actions = Vec::new();
            loop {
                let action = self.parse_alter_table_action(table_name.name())?;
                actions.push(action);

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            data = AlterTableData::Actions { if_exists, only, actions: actions.into() };
        }

        Ok(Rc::new(AlterTable::new(table_name, data)))
    }

    fn parse_if_exists(&mut self) -> Result<bool> {
        if self.parse_word(IF)? {
            self.expect_word(EXISTS)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        if self.parse_word(IF)? {
            self.expect_word(NOT)?;
            self.expect_word(EXISTS)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_alter_table_action(&mut self, table_name: &Name) -> Result<AlterTableAction> {
        if self.parse_word(ADD)? {
            // ADD COLUMN or CONSTRAINT
            if self.peek_words(&[CONSTRAINT, CHECK, UNIQUE, PRIMARY, EXCLUDE, FOREIGN])?.is_some() {
                let table_constraint = self.parse_table_constaint()?;

                Ok(AlterTableAction::AddConstraint {
                    constraint: table_constraint.into()
                })
            } else {
                self.parse_word(COLUMN)?;

                let if_not_exists = self.parse_if_not_exists()?;
                let column = self.parse_column(table_name, None)?;

                Ok(AlterTableAction::AddColumn { if_not_exists, column })
            }
        } else if self.parse_word(DROP)? {
            if self.parse_word(CONSTRAINT)? {
                let if_exists = self.parse_if_exists()?;
                let constraint_name = self.expect_name()?;
                let behavior = self.parse_drop_behavior()?;

                Ok(AlterTableAction::DropConstraint { if_exists, constraint_name, behavior })
            } else {
                self.parse_word(COLUMN)?;
                let if_exists = self.parse_if_exists()?;
                let column_name = self.expect_name()?;
                let behavior = self.parse_drop_behavior()?;

                Ok(AlterTableAction::DropColumn { if_exists, column_name, behavior })
            }
        } else if self.parse_word(ALTER)? {
            // ALTER COLUMN or CONSTRAINT
            if self.parse_word(CONSTRAINT)? {
                let constraint_name = self.expect_name()?;

                let mut deferrable = None;
                if self.parse_word(NOT)? {
                    self.expect_word(DEFERRABLE)?;
                    deferrable = Some(false);
                } else if self.parse_word(DEFERRABLE)? {
                    deferrable = Some(true);
                }

                let mut initially_deferred = None;
                if self.parse_word(INITIALLY)? {
                    if self.parse_word(DEFERRED)? {
                        initially_deferred = Some(true);
                    } else {
                        self.expect_word(IMMEDIATE)?;
                        initially_deferred = Some(false);
                    }
                }

                Ok(AlterTableAction::AlterConstraint { constraint_name, deferrable, initially_deferred })
            } else {
                self.parse_word(COLUMN)?;
                let column_name = self.expect_name()?;

                let data;
                if self.parse_word(SET)? {
                    if self.parse_word(DATA)? {
                        self.expect_word(TYPE)?;

                        let data_type = Rc::new(self.parse_data_type()?);
                        let collation = if self.parse_word(COLLATE)? {
                            Some(self.parse_ref_collation()?)
                        } else {
                            None
                        };
                        let using = if self.parse_word(USING)? {
                            Some(self.parse_expr()?.into())
                        } else {
                            None
                        };

                        data = AlterColumnData::Type { data_type, collation, using };
                    } else if self.parse_word(NOT)? {
                        self.expect_word(NULL)?;

                        data = AlterColumnData::SetNotNull;
                    } else if self.parse_word(DEFAULT)? {
                        let expr = self.parse_expr()?.into();
                        data = AlterColumnData::SetDefault { expr };
                    } else if self.parse_word(STORAGE)? {
                        let storage = self.parse_storage()?;
                        data = AlterColumnData::SetStorage { storage };
                    } else if self.parse_word(COMPRESSION)? {
                        let compression = if self.parse_word(NULL)? {
                            None // XXX: I don't think it can be NULL?
                        } else {
                            Some(self.expect_name()?)
                        };
                        data = AlterColumnData::SetCompression { compression };
                    } else {
                        return Err(self.expected_one_of(&[
                            DATA, NOT, DEFAULT, STORAGE, COMPRESSION
                        ]));
                    }
                } else if self.parse_word(TYPE)? {
                    let data_type = Rc::new(self.parse_data_type()?);
                    let collation = if self.parse_word(COLLATE)? {
                        Some(self.parse_ref_collation()?)
                    } else {
                        None
                    };
                    let using = if self.parse_word(USING)? {
                        Some(self.parse_expr()?.into())
                    } else {
                        None
                    };

                    data = AlterColumnData::Type { data_type, collation, using };
                } else if self.parse_word(DROP)? {
                    self.expect_word(NOT)?;
                    self.expect_word(NULL)?;
                    data = AlterColumnData::DropNotNull;
                } else {
                    return Err(self.expected_one_of(&[
                        SET, TYPE, DROP
                    ]));
                }

                Ok(AlterTableAction::AlterColumn { alter_column: AlterColumn::new(column_name, data) })
            }
        } else if self.parse_word(INHERIT)? {
            let table_name = self.parse_ref_table()?;
            Ok(AlterTableAction::Inherit { table_name })
        } else if self.parse_word(NO)? {
            self.expect_word(INHERIT)?;
            let table_name = self.parse_ref_table()?;
            Ok(AlterTableAction::NoInherit { table_name })
        } else if self.parse_word(SET)? {
            if self.parse_word(LOGGED)? {
                Ok(AlterTableAction::SetLogged { logged: true })
            } else if self.parse_word(UNLOGGED)? {
                Ok(AlterTableAction::SetLogged { logged: false })
            } else {
                Err(self.expected_one_of(&[
                    LOGGED, UNLOGGED
                ]))
            }
        } else if self.parse_word(OWNER)? {
            self.expect_word(TO)?;

            let new_owner = self.parse_owner()?;

            Ok(AlterTableAction::OwnerTo { new_owner })
        } else {
            return Err(self.expected_one_of(&[
                ADD, DROP, OWNER, SET, "[NO] INHERIT",
            ]))
        }
    }

    fn parse_owner(&mut self) -> Result<Owner> {
        if self.parse_word(CURRENT_ROLE)? {
            Ok(Owner::CurrentRole)
        } else if self.parse_word(CURRENT_USER)? {
            Ok(Owner::CurrentUser)
        } else if self.parse_word(SESSION_USER)? {
            Ok(Owner::SessionUser)
        } else {
            let user = self.expect_name()?;
            Ok(Owner::User(user))
        }
    }

    fn expected_one_of(&mut self, tokens: &[&str]) -> Error {
        match self.peek_token() {
            Ok(res) => {
                let mut msg = if tokens.len() > 1 {
                    "expected one of: "
                } else {
                    "expected: "
                }.to_owned();
                let mut iter = tokens.iter();
                if let Some(first) = iter.next() {
                    msg.push_str(first);
                    for token in iter {
                        msg.push_str(", ");
                        msg.push_str(token);
                    }
                } else {
                    msg.push_str("<EOF>");
                }
                msg.push_str(", actual: ");

                let cursor;
                if let Some(token) = res {
                    msg.push_str(self.get_source(token.cursor()));
                    cursor = token.into_cursor();
                } else {
                    msg.push_str("<EOF>");
                    cursor = Cursor::new(
                        self.tokenizer.offset(),
                        self.tokenizer.offset()
                    );
                }

                Error::with_message(
                    ErrorKind::UnexpectedToken,
                    cursor,
                    msg
                )
            }
            Err(err) => err
        }
    }

    fn parse_alter_type_intern(&mut self) -> Result<Rc<AlterType>> {
        // "ALTER TYPE" is already parsed
        let type_name = self.parse_ref_type()?;

        if self.parse_word(OWNER)? {
            self.expect_word(TO)?;

            let new_owner = if self.parse_word(CURRENT_ROLE)? {
                Owner::CurrentRole
            } else if self.parse_word(CURRENT_USER)? {
                Owner::CurrentUser
            } else {
                let username = self.expect_name()?;
                Owner::User(username)
            };

            Ok(AlterType::owner_to(type_name, new_owner))
        } else if self.parse_word(RENAME)? {
            if self.parse_word(TO)? {
                let new_name = self.expect_name()?;

                Ok(AlterType::rename(type_name, new_name))
            } else if self.parse_word(ATTRIBUTE)? {
                let attribute_name = self.expect_name()?;
                self.expect_word(TO)?;
                let new_attribute_name = self.expect_name()?;

                Ok(AlterType::rename_attribute(type_name, attribute_name, new_attribute_name))
            } else if self.parse_word(VALUE)? {
                let existing_value = self.expect_string()?;
                self.expect_word(TO)?;
                let new_value = self.expect_string()?;

                Ok(AlterType::rename_value(type_name, existing_value, new_value))
            } else {
                Err(self.expected_one_of(&[
                    TO, VALUE, ATTRIBUTE
                ]))
            }
        } else if self.parse_word(ADD)? {
            if self.parse_word(ATTRIBUTE)? {
                let mut actions = vec![self.parse_alter_type_add_attribute()?];

                if self.parse_token(TokenKind::Comma)? {
                    self.parse_alter_type_actions(&mut actions)?;
                }

                Ok(Rc::new(AlterType::new(type_name, AlterTypeData::Actions { actions })))
            } else if self.parse_word(VALUE)? {
                let if_not_exists = self.parse_if_not_exists()?;

                let value = self.expect_string()?;
                let mut position = None;

                if self.parse_word(BEFORE)? {
                    let other_value = self.expect_string()?;
                    position = Some(ValuePosition::Before(other_value));
                } else if self.parse_word(AFTER)? {
                    let other_value = self.expect_string()?;
                    position = Some(ValuePosition::After(other_value));
                }

                Ok(Rc::new(AlterType::new(type_name, AlterTypeData::AddValue { if_not_exists, value, position })))
            } else {
                Err(self.expected_one_of(&[
                    VALUE, ATTRIBUTE
                ]))
            }
        } else if self.parse_word(DROP)? {
            self.expect_word(ATTRIBUTE)?;

            let mut actions = vec![self.parse_alter_type_drop_attribute()?];

            if self.parse_token(TokenKind::Comma)? {
                self.parse_alter_type_actions(&mut actions)?;
            }

            Ok(Rc::new(AlterType::new(type_name, AlterTypeData::Actions { actions })))
        } else if self.parse_word(ALTER)? {
            self.expect_word(ATTRIBUTE)?;

            let mut actions = vec![self.parse_alter_type_alter_attribute()?];

            if self.parse_token(TokenKind::Comma)? {
                self.parse_alter_type_actions(&mut actions)?;
            }

            Ok(Rc::new(AlterType::new(type_name, AlterTypeData::Actions { actions })))
        } else if self.parse_word(SET)? {
            self.expect_word(SCHEMA)?;
            let new_schma = self.expect_name()?;

            Ok(AlterType::set_schema(type_name, new_schma))
        } else {
            Err(self.expected_one_of(&[
                OWNER, RENAME, ADD
            ]))
        }
    }

    fn parse_alter_type_add_attribute(&mut self) -> Result<AlterTypeAction> {
        // "ADD ATTRIBUTE" is already parsed
        let name = self.expect_name()?;
        let data_type = self.parse_data_type()?;
        let collation = if self.parse_word(COLLATE)? {
            Some(self.parse_ref_collation()?)
        } else {
            None
        };
        let behavior = self.parse_drop_behavior()?;

        Ok(AlterTypeAction::AddAttribute { name, data_type, collation, behavior })
    }

    fn parse_alter_type_drop_attribute(&mut self) -> Result<AlterTypeAction> {
        // "DROP ATTRIBUTE" is already parsed
        let if_exists = self.parse_if_exists()?;
        let name = self.expect_name()?;
        let behavior = self.parse_drop_behavior()?;

        Ok(AlterTypeAction::DropAttribute { if_exists, name, behavior })
    }

    fn parse_alter_type_alter_attribute(&mut self) -> Result<AlterTypeAction> {
        // "ALTER ATTRIBUTE" is already parsed
        let name = self.expect_name()?;
        if self.parse_word(SET)? {
            self.expect_word(DATA)?;
        }
        self.expect_word(TYPE)?;

        let data_type = self.parse_data_type()?;
        let collation = if self.parse_word(COLLATE)? {
            Some(self.parse_ref_collation()?)
        } else {
            None
        };
        let behavior = self.parse_drop_behavior()?;

        Ok(AlterTypeAction::AlterAttribute { name, data_type, collation, behavior })
    }

    fn parse_alter_type_actions(&mut self, actions: &mut Vec<AlterTypeAction>) -> Result<()> {
        loop {
            if self.parse_word(ADD)? {
                self.expect_word(ATTRIBUTE)?;
                actions.push(self.parse_alter_type_add_attribute()?);
            } else if self.parse_word(ALTER)? {
                self.expect_word(ATTRIBUTE)?;
                actions.push(self.parse_alter_type_alter_attribute()?);
            } else if self.parse_word(DROP)? {
                self.expect_word(ATTRIBUTE)?;
                actions.push(self.parse_alter_type_drop_attribute()?);
            } else {
                return Err(self.expected_one_of(&[
                    ADD, ALTER, DROP
                ]));
            }

            if !self.parse_token(TokenKind::Comma)? {
                break;
            }
        }
        Ok(())
    }

    fn parse_version(&mut self) -> Result<Version> {
        if peek_token!(self, TokenKind::Word | TokenKind::QuotName | TokenKind::UName)?.is_some() {
            Ok(Version::Name(self.expect_name()?))
        } else {
            Ok(Version::String(self.expect_string()?))
        }
    }

    fn parse_argmode(&mut self) -> Result<Argmode> {
        if self.parse_word(IN)? {
            Ok(Argmode::In)
        } else if self.parse_word(OUT)? {
            Ok(Argmode::Out)
        } else if self.parse_word(INOUT)? {
            Ok(Argmode::InOut)
        } else if self.parse_word(VARIADIC)? {
            Ok(Argmode::Variadic)
        } else {
            Ok(Argmode::In)
        }
    }

    fn parse_function_argument(&mut self) -> Result<Argument> {
        let mode = self.parse_argmode()?;

        let offset = self.tokenizer.offset();
        let mut name = Some(self.expect_name()?);

        if self.peek_word(DEFAULT)? || peek_token!(self, TokenKind::Equal | TokenKind::LParen | TokenKind::LBracket | TokenKind::RParen | TokenKind::Comma)?.is_some() {
            // backtracking: no arg name, just type!
            name = None;
            self.tokenizer.move_to(offset);
        }

        let data_type = self.parse_data_type()?;
        let default = if self.parse_word(DEFAULT)? || self.parse_token(TokenKind::Equal)? {
            Some(self.parse_expr()?.into())
        } else {
            None
        };

        Ok(Argument::new(mode, name, data_type, default))
    }

    fn parse_signature_argument(&mut self) -> Result<SignatureArgument> {
        let mode = self.parse_argmode()?;
        let name = Some(self.expect_name()?);
        let data_type = self.parse_data_type()?;

        Ok(SignatureArgument::new(mode, name, data_type))
    }

    fn parse_function_intern(&mut self, is_procedure: bool, or_replace: bool) -> Result<CreateFunction> {
        // "CREATE [OR REPLACE] {FUNCTION|PROCEDURE}" is already parsed
        let name = self.parse_qual_name_default()?;

        let mut arguments = Vec::new();
        self.expect_token(TokenKind::LParen)?;
        while !self.peek_kind(TokenKind::RParen)? {
            let arg = self.parse_function_argument()?;
            arguments.push(arg);

            if !self.parse_token(TokenKind::Comma)? {
                break;
            }
        }
        self.expect_token(TokenKind::RParen)?;

        let mut returns = None;
        if !is_procedure && self.parse_word(RETURNS)? {
            returns = Some(if self.parse_word(TABLE)? {
                let mut columns = Vec::new();
                self.expect_token(TokenKind::LParen)?;
                loop {
                    let column_name = self.expect_name()?;
                    let column_type = self.parse_data_type()?;
                    columns.push(function::Column::new(column_name, column_type));

                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }
                self.expect_token(TokenKind::RParen)?;
                ReturnType::Table { columns: columns.into() }
            } else if self.parse_word(TRIGGER)? {
                ReturnType::Trigger
            } else {
                ReturnType::Type(self.parse_data_type()?)
            });
        }

        let mut language = None;
        let mut transform = Vec::new();
        let mut window = false;
        let mut state = None;
        let mut leakproof = None;
        let mut null_input_handling = None;
        let mut security = None;
        let mut parallelism = None;
        let mut cost = None;
        let mut rows = None;
        let mut support = None;
        let mut configuration_parameters = Vec::new();
        let mut body = None;

        // function only: WINDOW, LEAKPROOF, STATE, NULL INPUT, PARALLEL, COST, ROWS, SUPPORT

        // TODO...
        while !self.peek_kind(TokenKind::SemiColon)? {
            if self.parse_word(LANGUAGE)? {
                language = Some(if self.peek_kind(TokenKind::String)? {
                    Name::new_quoted(self.expect_string()?)
                } else {
                    self.expect_name()?
                });
            } else if self.parse_word(TRANSFORM)? {
                loop {
                    self.expect_word(FOR)?;
                    self.expect_word(TYPE)?;
                    transform.push(self.parse_ref_type()?);

                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }
            } else if !is_procedure && self.parse_word(WINDOW)? {
                window = true;
            } else if !is_procedure && self.parse_word(IMMUTABLE)? {
                state = Some(function::State::Immutable);
            } else if !is_procedure && self.parse_word(STABLE)? {
                state = Some(function::State::Stable);
            } else if !is_procedure && self.parse_word(VOLATILE)? {
                state = Some(function::State::Volatile);
            } else if !is_procedure && self.parse_word(NOT)? {
                self.expect_word(LEAKPROOF)?;
                leakproof = Some(false);
            } else if !is_procedure && self.parse_word(LEAKPROOF)? {
                leakproof = Some(true);
            } else if !is_procedure && self.parse_word(CALLED)? {
                self.expect_word(ON)?;
                self.expect_word(NULL)?;
                self.expect_word(INPUT)?;
                null_input_handling = Some(function::NullInputHandling::Called);
            } else if !is_procedure && self.parse_word(RETURNS)? {
                self.expect_word(NULL)?;
                self.expect_word(ON)?;
                self.expect_word(NULL)?;
                self.expect_word(INPUT)?;
                null_input_handling = Some(function::NullInputHandling::ReturnsNull);
            } else if !is_procedure && self.parse_word(STRICT)? {
                null_input_handling = Some(function::NullInputHandling::Strict);
            } else if self.parse_word(EXTERNAL)? {
                self.expect_word(SECURITY)?;
                if self.parse_word(INVOKER)? {
                    security = Some(function::Security::Invoker { external: true });
                } else if self.parse_word(DEFINER)? {
                    security = Some(function::Security::Definer { external: true });
                } else {
                    return Err(self.expected_one_of(&[
                        INVOKER, DEFINER
                    ]));
                }
            } else if self.parse_word(SECURITY)? {
                if self.parse_word(INVOKER)? {
                    security = Some(function::Security::Invoker { external: false });
                } else if self.parse_word(DEFINER)? {
                    security = Some(function::Security::Definer { external: false });
                } else {
                    return Err(self.expected_one_of(&[
                        INVOKER, DEFINER
                    ]));
                }
            } else if !is_procedure && self.parse_word(PARALLEL)? {
                if self.parse_word(UNSAFE)? {
                    parallelism = Some(function::Parallelism::Unsafe);
                } else if self.parse_word(RESTRICTED)? {
                    parallelism = Some(function::Parallelism::Restricted);
                } else if self.parse_word(SAFE)? {
                    parallelism = Some(function::Parallelism::Safe);
                } else {
                    return Err(self.expected_one_of(&[
                        UNSAFE, RESTRICTED, SAFE
                    ]));
                }
            } else if !is_procedure && self.parse_word(COST)? {
                cost = Some(self.expect_uint()?);
            } else if !is_procedure && self.parse_word(ROWS)? {
                rows = Some(self.expect_uint()?);
            } else if !is_procedure && self.parse_word(SUPPORT)? {
                let mut function_name = self.parse_qual_name()?;

                if function_name.schema().is_none() {
                    // lookup possibly schema qualified function: supportfn(internal) returns internal
                    let reference = FunctionRef::new(function_name.name().clone(), vec![
                        DataType::new(BasicType::Internal, None)
                    ]);
                    function_name = self.database.resolve_function_reference(&reference);
                }

                support = Some(function_name);
            } else if self.parse_word(SET)? {
                let name = self.expect_name()?;
                let value = if self.parse_word(TO)? || self.parse_token(TokenKind::Equal)? {
                    ConfigurationValue::Value(self.parse_token_list(true, true)?.into())
                } else if self.parse_word(FROM)? {
                    self.expect_word(CURRENT)?;
                    ConfigurationValue::FromCurrent
                } else {
                    return Err(self.expected_one_of(&[
                        TO, "=", "FROM CURRENT"
                    ]));
                };

                configuration_parameters.push((name, value));
            } else if self.parse_word(AS)? {
                let first = self.expect_string()?;
                if self.parse_token(TokenKind::Comma)? {
                    let link_symbol = self.expect_string()?;
                    body = Some(FunctionBody::Object { object_file: first, link_symbol });
                } else {
                    body = Some(FunctionBody::Definition { source: first });
                }
            } else if self.parse_word(BEGIN)? {
                let mut statements = Vec::new();
                self.parse_word(ATOMIC)?;
                while !self.parse_word(END)? {
                    let mut statement = self.parse_token_list(false, true)?;
                    self.expect_token(TokenKind::SemiColon)?;
                    statement.push(ParsedToken::SemiColon);
                    statements.push(statement.into());
                }
                body = Some(FunctionBody::SqlBody { statements: statements.into() });
            } else if is_procedure {
                return Err(self.expected_one_of(&[
                    LANGUAGE, TRANSFORM, "[EXTERNAL] SECURITY", AS, BEGIN,
                ]));
            } else {
                return Err(self.expected_one_of(&[
                    LANGUAGE, TRANSFORM, WINDOW, IMMUTABLE, STABLE, VOLATILE, "[NOT] LEAKPROOF",
                    CALLED, RETURNS, STRICT, "[EXTERNAL] SECURITY", PARALLEL, COST, ROWS,
                    SUPPORT, AS, BEGIN,
                ]));
            }
        }

        let Some(body) = body else {
            return Err(Error::with_message(
                ErrorKind::SyntaxError,
                Cursor::new(self.tokenizer.offset(), self.tokenizer.offset() + 1),
                format!("function {} misses a body", name)
            ))
        };

        Ok(CreateFunction::new(or_replace,
            Function::new(
                name,
                arguments,
                returns,
                language,
                transform,
                window,
                state,
                leakproof,
                null_input_handling,
                security,
                parallelism,
                cost,
                rows,
                support,
                configuration_parameters,
                body,
            )
        ))
    }

    fn parse_trigger_event(&mut self) -> Result<Event> {
        if self.parse_word(INSERT)? {
            Ok(Event::Insert)
        } else if self.parse_word(UPDATE)? {
            let columns = if self.parse_word(OF)? {
                let mut columns = Vec::new();
                loop {
                    let column = self.expect_name()?;
                    columns.push(column);

                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }

                Some(columns.into())
            } else {
                None
            };

            Ok(Event::Update { columns })
        } else if self.parse_word(DELETE)? {
            Ok(Event::Delete)
        } else if self.parse_word(TRUNCATE)? {
            Ok(Event::Truncate)
        } else {
            Err(self.expected_one_of(&[
                INSERT, UPDATE, DELETE, TRUNCATE
            ]))
        }
    }

    fn parse_trigger_intern(&mut self, or_replace: bool, constraint: bool) -> Result<CreateTrigger> {
        // "CREATE [OR REPLACE] [CONSTRAINT] TRIGGER" is already parsed
        let name = self.expect_name()?;

        let when = if self.parse_word(BEFORE)? {
            When::Before
        } else if self.parse_word(AFTER)? {
            When::After
        } else if self.parse_word(INSTEAD)? {
            self.expect_word(OF)?;
            When::InsteadOf
        } else {
            return Err(self.expected_one_of(&[
                BEFORE, AFTER, "INSTEAD OF"
            ]))
        };

        let mut events = Vec::new();
        loop {
            let event = self.parse_trigger_event()?;
            events.push(event);

            if !self.parse_word(OR)? {
                break;
            }
        }

        self.expect_word(ON)?;

        let table_name = self.parse_ref_table()?;

        let ref_table = if self.parse_word(FROM)? {
            Some(self.parse_ref_table()?)
        } else {
            None
        };

        let deferrable;
        let mut initially_deferred = false;
        if self.parse_word(NOT)? {
            self.expect_word(DEFERRABLE)?;
            deferrable = false;
        } else {
            deferrable = self.parse_word(DEFERRABLE)?;

            if self.parse_word(INITIALLY)? {
                if self.parse_word(IMMEDIATE)? {
                    initially_deferred = false;
                } else if self.parse_word(DEFERRED)? {
                    initially_deferred = true;
                } else {
                    return Err(self.expected_one_of(&[
                        IMMEDIATE, DEFERRED
                    ]));
                }
            }
        }

        let mut referencing = Vec::new();
        while self.parse_word(REFERENCING)? {
            let life_cycle = if self.parse_word(OLD)? {
                LifeCycle::Old
            } else if self.parse_word(NEW)? {
                LifeCycle::New
            } else {
                return Err(self.expected_one_of(&[
                    OLD, NEW
                ]));
            };

            self.expect_word(TABLE)?;
            self.parse_word(AS)?;

            let transition_relation_name = self.expect_name()?;

            referencing.push(ReferencedTable::new(life_cycle, transition_relation_name));
        }

        let mut for_each_row = false;
        if self.parse_word(FOR)? {
            self.parse_word(EACH)?;

            if self.parse_word(ROW)? {
                for_each_row = true;
            } else if self.parse_word(STATEMENT)? {
                for_each_row = false;
            } else {
                return Err(self.expected_one_of(&[
                    ROW, STATEMENT
                ]));
            }
        }

        let predicate = if self.parse_word(WHEN)? {
            self.expect_token(TokenKind::LParen)?;
            let tokens = self.parse_token_list(false, false)?;
            self.expect_token(TokenKind::RParen)?;
            Some(tokens.into())
        } else {
            None
        };

        self.expect_word(EXECUTE)?;
        if !self.parse_word(FUNCTION)? && !self.parse_word(PROCEDURE)? {
            return Err(self.expected_one_of(&[
                FUNCTION, PROCEDURE
            ]));
        }

        let function_name = self.parse_ref_function_for_trigger()?;

        self.expect_token(TokenKind::LParen)?;
        let mut arguments = Vec::new();

        while !self.peek_kind(TokenKind::RParen)? {
            let Some(token) = self.peek_token()? else {
                break;
            };

            if token.is_name() {
                arguments.push(self.expect_name()?.into());
            } else if token.is_int() {
                arguments.push(self.expect_int::<i64>()?.to_string().into());
            } else if token.is_float() {
                let value = self.expect_float::<f64>()?;
                arguments.push(format!("{value:?}").into());
            } else if token.is_name() {
                arguments.push(self.expect_name()?.into());
            } else if token.is_string() {
                arguments.push(self.expect_string()?);
            } else {
                return Err(self.expected_one_of(&[
                    "<literal>", "<name>"
                ]));
            }

            if !self.parse_token(TokenKind::Comma)? {
                break;
            }
        }

        self.expect_token(TokenKind::RParen)?;

        Ok(CreateTrigger::new(
            or_replace,
            table_name,
            Trigger::new(
                constraint,
                name,
                when,
                events.into(),
                ref_table,
                referencing.into(),
                for_each_row,
                deferrable,
                initially_deferred,
                predicate,
                function_name,
                arguments.into()
            )
        ))
    }

    fn parse_comment_tail(&mut self) -> Result<Option<Rc<str>>> {
        self.expect_word(IS)?;

        if self.parse_word(NULL)? {
            Ok(None)
        } else {
            Ok(Some(self.expect_string()?))
        }
    }

    fn parse_comment_intern(&mut self, start_offset: usize) -> Result<()> {
        // "COMMENT" is already parsed
        self.expect_word(ON)?;

        if self.parse_word(COLUMN)? {
            let first = self.expect_name()?;
            self.expect_token(TokenKind::Period)?;
            let second = self.expect_name()?;

            let (table_name, column_name) = if self.parse_token(TokenKind::Period)? {
                let third = self.expect_name()?;
                (QName::new(Some(first), second), third)
            } else {
                (QName::new(None, first), second)
            };

            let comment = self.parse_comment_tail()?;

            let Some(table) = Rc::make_mut(&mut self.database).get_table_mut(&table_name) else {
                return Err(Error::with_message(
                    ErrorKind::TableNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("table {table_name} not found")
                ));
            };
            let Some(column) = Rc::make_mut(table).columns_mut().get_mut(&column_name) else {
                return Err(Error::with_message(
                    ErrorKind::ColumnNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("column {column_name} not found in table {table_name}")
                ));
            };
            Rc::make_mut(column).set_comment(comment);
        } else if self.parse_word(EXTENSION)? {
            let name = self.parse_qual_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(extension) = Rc::make_mut(&mut self.database).get_extension_mut(&name) else {
                return Err(Error::with_message(
                    ErrorKind::ExtensionNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("extension {name} not found")
                ));
            };

            Rc::make_mut(extension).set_comment(comment);
        } else if self.parse_word(INDEX)? {
            let name = self.parse_qual_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(index) = Rc::make_mut(&mut self.database).get_index_mut(&name) else {
                return Err(Error::with_message(
                    ErrorKind::IndexNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("index {name} not found")
                ));
            };

            Rc::make_mut(index).set_comment(comment);
        } else if self.parse_word(SCHEMA)? {
            let name = self.expect_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(schema) = Rc::make_mut(&mut self.database).schemas_mut().get_mut(&name) else {
                return Err(Error::with_message(
                    ErrorKind::SchemaNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("schema {name} not found")
                ));
            };

            schema.set_comment(comment);
        } else if self.parse_word(TABLE)? {
            let name = self.parse_qual_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(table) = Rc::make_mut(&mut self.database).get_table_mut(&name) else {
                return Err(Error::with_message(
                    ErrorKind::TableNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("table {name} not found")
                ));
            };

            Rc::make_mut(table).set_comment(comment);
        } else if self.parse_word(TYPE)? {
            let name = self.parse_qual_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(type_def) = Rc::make_mut(&mut self.database).get_type_mut(&name) else {
                return Err(Error::with_message(
                    ErrorKind::TypeNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("type {name} not found")
                ));
            };

            Rc::make_mut(type_def).set_comment(comment);
        } else if self.parse_word(TRIGGER)? {
            let trigger_name = self.expect_name()?;
            self.expect_word(ON)?;
            let table_name = self.parse_qual_name()?;
            let comment = self.parse_comment_tail()?;

            let Some(table) = Rc::make_mut(&mut self.database).get_table_mut(&table_name) else {
                return Err(Error::with_message(
                    ErrorKind::TableNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("table {table_name} not found")
                ));
            };
            let Some(trigger) = Rc::make_mut(table).triggers_mut().get_mut(&trigger_name) else {
                return Err(Error::with_message(
                    ErrorKind::TriggerNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("trigger {trigger_name} not found in table {table_name}")
                ));
            };
            Rc::make_mut(trigger).set_comment(comment);
        } else if self.parse_word(FUNCTION)? || self.parse_word(PROCEDURE)? || self.parse_word(ROUTINE)? {
            let name = self.parse_qual_name()?;
            let mut arguments = Vec::new();

            if self.parse_token(TokenKind::LParen)? {
                while !self.peek_kind(TokenKind::RParen)? {
                    arguments.push(self.parse_signature_argument()?);

                    if !self.parse_token(TokenKind::Comma)? {
                        break;
                    }
                }
                self.expect_token(TokenKind::RParen)?;
            }

            let sig = FunctionSignature::new(name, arguments);
            let comment = self.parse_comment_tail()?;

            let Some(function) = Rc::make_mut(&mut self.database).get_function_mut(&sig.to_qref()) else {
                return Err(Error::with_message(
                    ErrorKind::FunctionNotExists,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("function {sig} not found")
                ));
            };

            Rc::make_mut(function).set_comment(comment);
        } else if self.parse_word(CONSTRAINT)? {
            let name = self.expect_name()?;
            self.expect_word(ON)?;

            if self.parse_word(DOMAIN)? {
                self.parse_token_list(false, true)?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse more COMMENT statements: {source}");
            } else {
                let table_name = self.parse_qual_name()?;
                let comment = self.parse_comment_tail()?;

                let Some(table) = Rc::make_mut(&mut self.database).get_table_mut(&table_name) else {
                    return Err(Error::with_message(
                        ErrorKind::TableNotExists,
                        Cursor::new(start_offset, self.tokenizer.offset()),
                        format!("table {table_name} not found")
                    ));
                };

                let table = Rc::make_mut(table);
                let Some(constraint) = table.constraints_mut().get_mut(&name) else {
                    return Err(Error::with_message(
                        ErrorKind::ConstraintNotExists,
                        Cursor::new(start_offset, self.tokenizer.offset()),
                        format!("constraint {name} on table {table_name} not found")
                    ));
                };
                Rc::make_mut(constraint).set_comment(comment);
            }
        } else {
            self.parse_token_list(false, true)?;

            let end_offset = self.tokenizer.offset();
            let source = self.tokenizer.get_offset(start_offset, end_offset);

            eprintln!("TODO: parse more COMMENT statements: {source}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BracketType {
    Round,
    Square,
}

impl BracketType {
    #[inline]
    fn closing(self) -> char {
        match self {
            BracketType::Round => ')',
            BracketType::Square => ']',
        }
    }
}

impl<'a> Parser for PostgreSQLParser<'a> {
    #[inline]
    fn get_source(&self, cursor: &Cursor) -> &str {
        cursor.get(self.tokenizer.source())
    }

    #[inline]
    fn expect_some(&mut self) -> Result<Token> {
        let Some(token) = self.tokenizer.next()? else {
            return Err(Error::with_cursor(ErrorKind::UnexpectedEOF, Cursor::new(self.tokenizer.offset(), self.tokenizer.offset())));
        };

        Ok(token)
    }

    #[inline]
    fn peek_token(&mut self) -> Result<Option<Token>> {
        self.tokenizer.peek()
    }

    fn expect_name(&mut self) -> Result<Name> {
        let token = self.expect_some()?;
        match token.kind() {
            TokenKind::Word => {
                Ok(self.parse_name(token.cursor()))
            }
            TokenKind::QuotName => {
                self.parse_quot_name(token.cursor())
            }
            TokenKind::UName => {
                self.parse_uname(token.cursor())
            }
            _ => {
                Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <name>, actual: {}", token.kind())
                ))
            }
        }
    }

    fn expect_string(&mut self) -> Result<Rc<str>> {
        let token = self.expect_some()?;
        match token.kind() {
            TokenKind::String => {
                let Some(value) = parse_string(self.get_source(token.cursor())) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <string>, actual: {}", token.kind())
                    ));
                };
                Ok(value.into())
            }
            TokenKind::EString => {
                let Some(value) = parse_estring(self.get_source(token.cursor())) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <estring>, actual: {}", token.kind())
                    ));
                };
                Ok(value.into())
            }
            TokenKind::UString => {
                let Some(value) = parse_ustring(self.get_source(token.cursor())) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <unicode string>, actual: {}", token.kind())
                    ));
                };

                let escape = self.tokenizer.parse_uescape()?;

                let Some(value) = uunescape(&value, escape) else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *token.cursor(),
                        format!("illegal escape sequence in: {}", self.get_source(token.cursor()))
                    ));
                };
                Ok(value.into())
            }
            TokenKind::DollarString => {
                let value = self.get_source(token.cursor());
                Ok(strip_dollar_string(value).into())
            }
            _ => {
                Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <string>, actual: {}", token.kind())
                ))
            }
        }
    }

    fn parse(&mut self) -> Result<Rc<Database>> {
        Rc::make_mut(&mut self.database).clear();

        while let Some(token) = self.tokenizer.peek()? {
            let start_offset = token.cursor().start_offset();

            if self.parse_word(SET)? {
                let name = self.expect_name()?;
                if !self.parse_word(TO)? {
                    self.expect_token(TokenKind::Equal)?;
                }

                if name.name().eq_ignore_ascii_case("search_path") {
                    if self.parse_word(DEFAULT)? {
                        Rc::make_mut(&mut self.database).set_default_search_path();
                    } else {
                        let mut new_search_path = Vec::new();

                        loop {
                            let name = self.expect_name()?;
                            new_search_path.push(name);

                            if !self.parse_token(TokenKind::Comma)? {
                                break;
                            }
                        }

                        *Rc::make_mut(&mut self.database).search_path_mut() = new_search_path;
                    }
                } else {
                    // ignored. store it somewhere?
                    self.parse_token_list(false, true)?;
                }

            } else if self.parse_word(CREATE)? {
                if self.parse_word(SCHEMA)? {
                    self.parse_schema_intern()?;
                } else {
                    self.parse_create_intern(start_offset)?;
                }
            } else if self.parse_word(SELECT)? {
                // SELECT
                if self.parse_word("pg_catalog")? &&
                   self.parse_token(TokenKind::Period)? &&
                   self.parse_word("set_config")? &&
                   self.parse_token(TokenKind::LParen)? {
                    if let Ok(conf_var) = self.expect_string() {
                        if conf_var.eq_ignore_ascii_case("search_path") &&
                           self.parse_token(TokenKind::Comma)? {
                            if let Ok(value) = self.expect_string() {
                                if self.parse_token(TokenKind::Comma)? &&
                                   (self.parse_word(TRUE)? || self.parse_word(FALSE)?) &&
                                   self.parse_token(TokenKind::RParen)? &&
                                   (self.peek_kind(TokenKind::SemiColon)? || self.peek_token()?.is_none()) {
                                    // matched: SELECT pg_catalog.set_config('search_path', '...', true|false);
                                    // if true then it should be only for the current transaction
                                    let value = value.trim();
                                    let schema = Rc::make_mut(&mut self.database);
                                    if value.is_empty() {
                                        schema.set_default_search_path();
                                    } else {
                                        let search_path = schema.search_path_mut();
                                        search_path.clear();
                                        for schema_name in value.split(',') {
                                            search_path.push(Name::new(schema_name.trim()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                self.parse_token_list(false, true)?;
            } else if self.parse_word(ALTER)? {
                // TODO: ALTER
                // This is important if the output of pg_dump should be supported,
                // because it adds constraints at the end via ALTER statements.

                if self.parse_word(TABLE)? {
                    let alter_table = self.parse_alter_table_intern()?;
                    Rc::make_mut(&mut self.database).alter_table(&alter_table).map_err(|mut err| {
                        *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                        err
                    })?;
                } else if self.parse_word(TYPE)? {
                    let alter_type = self.parse_alter_type_intern()?;
                    Rc::make_mut(&mut self.database).alter_type(&alter_type).map_err(|mut err| {
                        *err.cursor_mut() = Some(Cursor::new(start_offset, self.tokenizer.offset()));
                        err
                    })?;
                } else {
                    // TODO: ALTER INDEX|FUNCTION|TRIGGER|...?
                    self.parse_token_list(false, true)?;

                    let end_offset = self.tokenizer.offset();
                    let source = self.tokenizer.get_offset(start_offset, end_offset);

                    eprintln!("TODO: parse ALTER statements: {source}");
                }
            } else if self.parse_word(COMMENT)? {
                // COMMENT
                self.parse_comment_intern(start_offset)?;
            } else if self.parse_word(DROP)? {
                // TODO: DROP...
                self.parse_token_list(false, true)?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse DROP statements: {source}");
            } else if self.parse_word(GRANT)? {
                // TODO: GRANT...
                self.parse_token_list(false, true)?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse GRANT statements: {source}");
            } else if self.parse_word(BEGIN)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
            } else if self.parse_word(START)? {
                self.expect_word(TRANSACTION)?;
            } else if self.parse_word(COMMIT)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
                if self.parse_word(AND)? {
                    self.parse_word(NO)?;
                    self.expect_word(CHAIN)?;
                }
            } else if self.parse_word(ROLLBACK)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
                if self.parse_word(AND)? {
                    self.parse_word(NO)?;
                    self.expect_word(CHAIN)?;
                }

                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    "transaction rollback is not supported".to_string()
                ));
            } else {
                return Err(self.expected_one_of(&[
                    CREATE, SET, SELECT, ALTER, DROP, COMMENT, BEGIN, START
                ]));
            }
            self.expect_semicolon_or_eof()?;
        }

        Ok(self.database.clone())
    }
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
