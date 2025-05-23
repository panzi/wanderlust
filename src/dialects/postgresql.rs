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
            types::{AlterType, AlterTypeData, ValuePosition},
            DropBehavior, Owner
        },
        column::{Column, ColumnConstraint, ColumnConstraintData, ColumnMatch, ReferentialAction},
        extension::{CreateExtension, Extension, Version},
        function::{self, Argmode, Argument, CreateFunction, Function, ReturnType},
        index::{CreateIndex, Direction, Index, IndexItem, IndexItemData, NullsPosition},
        integers::{Integer, SignedInteger, UnsignedInteger},
        name::{Name, QName},
        schema::Schema,
        syntax::{Cursor, Parser, SourceLocation, Tokenizer},
        table::{CreateTable, Table, TableConstraint, TableConstraintData},
        token::{ParsedToken, ToTokens, Token, TokenKind},
        types::{BasicType, DataType, IntervalFields, TypeDef, Value}
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
                        format!("actual: <EOF>, expected: */")
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
                format!("expected: <string>")
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find('\'') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    format!("expected: <string>, actual: <EOF>")
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

        return Ok(self.offset + len);
    }

    fn find_quot_name_end(&mut self) -> Result<usize> {
        let slice = &self.source[self.offset..];
        if !slice.starts_with('"') {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                Cursor::new(self.offset, self.offset + 1),
                format!("expected: <quoted name>")
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find('"') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    format!("expected: <quoted name>, actual: <EOF>")
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

        return Ok(self.offset + len);
    }

    fn find_estring_end(&mut self) -> Result<usize> {
        let slice = &self.source[self.offset..];
        if !slice.starts_with('\'') {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                Cursor::new(self.offset, self.offset + 1),
                format!("expected: <estring>")
            ));
        }

        let mut len = 1usize;
        let mut slice = &slice[1..];

        loop {
            let Some(index) = slice.find(|c: char| c == '\'' || c == '\\') else {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + len),
                    format!("expected: <estring>, actual: <EOF>")
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
                        format!("expected: <estring>, actual: <EOF>")
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

        return Ok(self.offset + len);
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
                        format!("expected: <single char string>, actual: <EOF>")
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

                if value.len() != 1 {
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
                    return Ok(Some(Token::new(
                        TokenKind::HexInt,
                        Cursor::new(start_offset, end_offset)
                    )));
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
                    return Ok(Some(Token::new(
                        TokenKind::OctInt,
                        Cursor::new(start_offset, end_offset)
                    )));
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
                    return Ok(Some(Token::new(
                        TokenKind::BinInt,
                        Cursor::new(start_offset, end_offset)
                    )));
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
                    return Ok(Some(Token::new(
                        if float { TokenKind::Float } else { TokenKind::DecInt },
                        Cursor::new(start_offset, end_offset)
                    )));
                }
            }
            'E' if self.source[self.offset + 1..].starts_with('\'') => {
                // E string
                let start_offset = self.offset;
                self.offset += 1;
                let end_offset = self.find_estring_end()?;
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::EString,
                    Cursor::new(start_offset, end_offset)
                )));
            }
            'U' if self.source[self.offset + 1..].starts_with("&'") => {
                // U& string
                let start_offset = self.offset;
                self.offset += 2;
                let end_offset = self.find_string_end()?;
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::UString,
                    Cursor::new(start_offset, end_offset)
                )));
            }
            'U' if self.source[self.offset + 1..].starts_with("&\"") => {
                // U& identifier
                let start_offset = self.offset;
                self.offset += 2;
                let end_offset = self.find_quot_name_end()?;
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::UName,
                    Cursor::new(start_offset, end_offset)
                )));
            }
            _ if ch.is_ascii_alphabetic() || ch == '_' => {
                // word
                let start_offset = self.offset;
                let end_offset = if let Some(index) = self.source[start_offset..].find(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
                    start_offset + index
                } else {
                    self.source.len()
                };
                self.offset = end_offset;
                return Ok(Some(Token::new(
                    TokenKind::Word,
                    Cursor::new(start_offset, end_offset))));
            }
            '\'' => {
                // string
                let start_offset = self.offset;
                let end_offset = self.find_string_end()?;
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::String,
                    Cursor::new(start_offset, end_offset)
                )));
            }
            '"' => {
                // quoted identifier
                let start_offset = self.offset;
                let end_offset = self.find_quot_name_end()?;
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::QuotName,
                    Cursor::new(start_offset, end_offset)
                )));
            }
            '$' => {
                // dollar string or single dollar?
                let start_offset = self.offset;
                self.offset += 1;
                if !self.source[start_offset..].starts_with(|c: char| c.is_ascii_alphabetic() || c == '_' || c == '$') {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, start_offset + 2),
                        format!("expected: <dollar string>")
                    ));
                }

                let end_offset = if let Some(index) = self.source[self.offset..].find(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
                    self.offset + index
                } else {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, self.source.len()),
                        format!("expected: <dollar string>")
                    ));
                };

                if !self.source[end_offset..].starts_with("$") {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        Cursor::new(start_offset, end_offset),
                        format!("expected: <dollar string>")
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
                        format!("expected: <dollar string>")
                    ));
                };
                let end_offset = end_offset + tag.len();
                self.offset = end_offset;

                return Ok(Some(Token::new(
                    TokenKind::DollarString,
                    Cursor::new(start_offset, end_offset)
                )));
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
                return Ok(Some(Token::new(
                    TokenKind::Colon,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            ',' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::Comma,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            ';' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::SemiColon,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            '.' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::Period,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            '(' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::LParen,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            ')' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::RParen,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            '[' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::LBracket,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            ']' => {
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::RBracket,
                    Cursor::new(start_offset, self.offset),
                )));
            }
            '=' if !self.source[self.offset + 1..].starts_with(|c: char| is_operator!(c)) => {
                // equal
                let start_offset = self.offset;
                self.offset += 1;
                return Ok(Some(Token::new(
                    TokenKind::Equal,
                    Cursor::new(start_offset, self.offset))
                ));
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
                return Ok(Some(Token::new(
                    TokenKind::Operator,
                    Cursor::new(start_offset, self.offset))
                ));
            }
            _ => {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + 1),
                    format!("unexpected: {ch}")
                ));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostgreSQLParser<'a> {
    tokenizer: PostgreSQLTokenizer<'a>,
    default_schema: Name,
    schema: Rc<Schema>,
}

impl<'a> PostgreSQLParser<'a> {
    #[inline]
    pub fn new(source: &'a str) -> Self {
        let default_schema = Name::new("public");
        Self {
            tokenizer: PostgreSQLTokenizer::new(source),
            default_schema: default_schema.clone(),
            schema: Rc::new(Schema::new(default_schema)),
        }
    }

    #[inline]
    pub fn default_schema(&self) -> &Name {
        &self.default_schema
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
                format!("expected: <expression>, actual: <EOF>")));
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
                    format!("expected: <expression>, actual: {actual}")));
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
                            format!("expected: expression, actual: <EOF>")));
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

    fn parse_int<I: SignedInteger>(&mut self) -> Result<(Token, I)> {
        let token = self.expect_some()?;
        let source = self.get_source(token.cursor());
        let value = parse_int(&token, source)?;

        Ok((token, value))
    }

    fn parse_precision(&mut self) -> Result<NonZeroU32> {
        let (token, value) = self.parse_uint()?;
        let Some(value) = NonZeroU32::new(value) else {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("precision may not be zero")
            ));
        };
        Ok(value)
    }

    fn parse_drop_option(&mut self) -> Result<Option<DropBehavior>> {
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
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <string>, <integer>, or <float>, actual: {source}")
                ));
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
                        table_name: self.schema.resolve_table_name(&first),
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
                    name: self.schema.resolve_type_name(&first),
                    parameters
                }
            }
        };

        let mut array_dimensions: Option<Box<[Option<u32>]>> = None;
        if self.parse_word(ARRAY)? {
            let mut dims = None;
            if self.parse_token(TokenKind::LBracket)? {
                if !self.peek_kind(TokenKind::RBracket)? {
                    dims = Some(self.parse_uint()?.1);
                }
                self.expect_token(TokenKind::RBracket)?;
            }

            array_dimensions = Some(Box::new([dims]));
        } else if self.peek_kind(TokenKind::LBracket)? {
            let mut dims = Vec::new();
            while self.parse_token(TokenKind::LBracket)? {
                if !self.peek_kind(TokenKind::RBracket)? {
                    dims.push(Some(self.parse_uint()?.1));
                } else {
                    dims.push(None);
                }
                self.expect_token(TokenKind::RBracket)?;
            }
            array_dimensions = Some(dims.into_boxed_slice());
        }

        Ok(DataType::new(data_type, array_dimensions))
    }

    #[inline]
    fn parse_qual_name(&mut self) -> Result<QName> {
        let name = self.expect_name()?;
        self.parse_qual_name_tail(name)
    }

    #[inline]
    fn parse_ref_table(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.schema.resolve_table_name(&first))
        }
    }

    #[inline]
    fn parse_ref_type(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.schema.resolve_type_name(&first))
        }
    }

    #[inline]
    fn parse_ref_index(&mut self) -> Result<QName> {
        let first = self.expect_name()?;

        if self.parse_token(TokenKind::Period)? {
            let second = self.expect_name()?;
            Ok(QName::new(Some(first), second))
        } else {
            Ok(self.schema.resolve_index_name(&first))
        }
    }

    fn parse_qual_name_tail(&mut self, mut name: Name) -> Result<QName> {
        let schema;

        if self.parse_token(TokenKind::Period)? {
            let mut temp = self.expect_name()?;
            std::mem::swap(&mut temp, &mut name);
            schema = Some(temp);
        } else {
            schema = Some(self.schema.search_path().first().unwrap_or(&self.default_schema).clone());
        }

        Ok(QName::new(schema, name))
    }

    fn parse_qual_name_with_default_schema(&mut self, default_schema: Option<&Name>) -> Result<QName> {
        let mut name = self.expect_name()?;
        let schema;

        if self.parse_token(TokenKind::Period)? {
            let mut temp = self.expect_name()?;
            std::mem::swap(&mut temp, &mut name);
            schema = Some(temp);
        } else {
            schema = Some(default_schema.unwrap_or(self.schema.search_path().first().unwrap_or(&self.default_schema)).clone());
        }

        Ok(QName::new(schema, name))
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

            constraint_data = TableConstraintData::Unique { nulls_distinct, columns: columns.into() };
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

            constraint_data = TableConstraintData::PrimaryKey { columns: columns.into() };
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

    fn parse_column(&mut self) -> Result<Rc<Column>> {
        let column_name = self.expect_name()?;
        let data_type = self.parse_data_type()?;
        let mut collation = None;

        if self.peek_word(COLLATE)? {
            self.expect_some()?;
            collation = Some(self.expect_name()?);
        }

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

                constraint_data = ColumnConstraintData::Unique { nulls_distinct };
            } else if self.parse_word(PRIMARY)? {
                self.expect_word(KEY)?;

                constraint_data = ColumnConstraintData::PrimaryKey;
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

            column_constraints.push(
                Rc::new(ColumnConstraint::new(
                    constraint_name,
                    constraint_data,
                    deferrable,
                    initially_deferred,
                ))
            );
        }

        Ok(Rc::new(Column::new(
            column_name, data_type, collation, column_constraints,
        )))
    }

    fn parse_table_intern(&mut self) -> Result<CreateTable> {
        // "CREATE TABLE" is already parsed

        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_qual_name()?;
        let mut columns = OrderedHashMap::new();
        let mut table_constraints = OrderedHashMap::new();

        self.expect_token(TokenKind::LParen)?;

        while !self.peek_kind(TokenKind::RParen)? {
            if self.peek_words(&[CONSTRAINT, CHECK, UNIQUE, PRIMARY, FOREIGN])?.is_some() {
                let mut constraint = self.parse_table_constaint()?;
                let constraint_name = constraint.ensure_name();
                table_constraints.insert(constraint_name.clone(), Rc::new(constraint));
            } else {
                let start_offset = self.tokenizer.offset();
                let column = self.parse_column()?;
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
        self.expect_semicolon_or_eof()?;

        let table = Table::new(
            table_name,
            columns,
            table_constraints,
            OrderedHashMap::new()
        );

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
            collation = Some(self.expect_name()?);
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
            Some(self.expect_name()?)
        } else if peek_token!(self, TokenKind::Word | TokenKind::QuotName | TokenKind::UName)?.is_some() {
            Some(self.expect_name()?)
        } else {
            None
        };

        self.expect_word(ON)?;

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

        self.expect_semicolon_or_eof()?;

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

    fn parse_type_def_intern(&mut self) -> Result<TypeDef> {
        // "CREATE TYPE" is already parsed
        // TODO: other types
        let type_name = self.parse_qual_name()?;
        self.expect_word(AS)?;
        self.expect_word(ENUM)?;
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
        self.expect_semicolon_or_eof()?;

        Ok(TypeDef::create_enum(type_name, values))
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
                // TODO: can new_name be qualified? What schema does it default to, the
                //       same as the old schema or the `public` schema?
                let new_name = self.parse_qual_name_with_default_schema(table_name.schema())?;

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
                let action = self.parse_alter_table_action()?;
                actions.push(action);

                if !self.parse_token(TokenKind::Comma)? {
                    break;
                }
            }

            data = AlterTableData::Actions { if_exists, only, actions: actions.into() };
        }

        self.expect_semicolon_or_eof()?;

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

    fn parse_alter_table_action(&mut self) -> Result<AlterTableAction> {
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
                let column = self.parse_column()?;

                Ok(AlterTableAction::AddColumn { if_not_exists, column })
            }
        } else if self.parse_word(DROP)? {
            if self.parse_word(CONSTRAINT)? {
                let if_exists = self.parse_if_exists()?;
                let constraint_name = self.expect_name()?;
                let behavior = self.parse_drop_option()?;

                Ok(AlterTableAction::DropConstraint { if_exists, constraint_name, behavior })
            } else {
                self.parse_word(COLUMN)?;
                let if_exists = self.parse_if_exists()?;
                let column_name = self.expect_name()?;
                let behavior = self.parse_drop_option()?;

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
                            Some(self.expect_name()?)
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
                    } else {
                        return Err(self.expected_one_of(&[
                            DATA, NOT, DEFAULT
                        ]));
                    }
                } else if self.parse_word(TYPE)? {
                    let data_type = Rc::new(self.parse_data_type()?);
                    let collation = if self.parse_word(COLLATE)? {
                        Some(self.expect_name()?)
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
                    ]))
                }

                Ok(AlterTableAction::AlterColumn { alter_column: AlterColumn::new(column_name, data) })
            }
        } else {
            self.expect_word(OWNER)?;
            self.expect_word(TO)?;

            let new_owner = if self.parse_word(CURRENT_ROLE)? {
                Owner::CurrentRole
            } else if self.parse_word(CURRENT_USER)? {
                Owner::CurrentUser
            } else if self.parse_word(SESSION_USER)? {
                Owner::SessionUser
            } else {
                let user = self.expect_name()?;
                Owner::User(user)
            };

            Ok(AlterTableAction::OwnerTo { new_owner })
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

            self.expect_semicolon_or_eof()?;

            Ok(AlterType::owner_to(type_name, new_owner))
        } else if self.parse_word(RENAME)? {
            if self.parse_word(TO)? {
                let new_name = self.parse_qual_name()?;
                self.expect_semicolon_or_eof()?;

                Ok(AlterType::rename(type_name, new_name))
            } else {
                self.expect_word(VALUE)?;

                let existing_value = self.expect_string()?;
                self.expect_word(TO)?;
                let new_value = self.expect_string()?;

                self.expect_semicolon_or_eof()?;

                Ok(AlterType::rename_value(type_name, existing_value, new_value))
            }
        } else if self.parse_word(ADD)? {
            self.expect_word(VALUE)?;

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

            self.expect_semicolon_or_eof()?;

            Ok(Rc::new(AlterType::new(type_name, AlterTypeData::AddValue { if_not_exists, value, position })))
        } else if self.parse_word(SET)? {
            self.expect_word(SCHEMA)?;
            let new_schma = self.expect_name()?;

            self.expect_semicolon_or_eof()?;

            Ok(AlterType::set_schema(type_name, new_schma))
        } else {
            Err(self.expected_one_of(&[
                OWNER, RENAME, ADD
            ]))
        }
    }

    fn parse_version(&mut self) -> Result<Version> {
        if peek_token!(self, TokenKind::Word | TokenKind::QuotName | TokenKind::UName)?.is_some() {
            Ok(Version::Name(self.expect_name()?))
        } else {
            Ok(Version::String(self.expect_string()?))
        }
    }

    fn parse_function_argument(&mut self) -> Result<Argument> {
        let mode = if self.parse_word(IN)? {
            Argmode::In
        } else if self.parse_word(OUT)? {
            Argmode::Out
        } else if self.parse_word(INOUT)? {
            Argmode::InOut
        } else if self.parse_word(VARIADIC)? {
            Argmode::Variadic
        } else {
            Argmode::In
        };

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

    fn parse_function_intern(&mut self, is_procedure: bool, or_replace: bool) -> Result<CreateFunction> {
        // "CREATE [OR REPLACE] {FUNCTION|PROCEDURE}" is already parsed
        let name = self.parse_qual_name()?;

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
            } else {
                ReturnType::Type(self.parse_data_type()?)
            });
        }

        let body = self.parse_token_list(false, true)?;

        self.expect_semicolon_or_eof()?;

        Ok(CreateFunction::new(or_replace, Function::new(name, arguments, returns, body)))
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

    fn parse(&mut self) -> Result<Rc<Schema>> {
        Rc::make_mut(&mut self.schema).clear();

        while let Some(token) = self.tokenizer.peek()? {
            let start_offset = token.cursor().start_offset();

            if self.parse_word(SET)? {
                let name = self.expect_name()?;
                if !self.parse_word(TO)? {
                    self.expect_token(TokenKind::Equal)?;
                }

                if name.name().eq_ignore_ascii_case("search_path") {
                    if self.parse_word(DEFAULT)? {
                        let search_path = Rc::make_mut(&mut self.schema).search_path_mut();
                        search_path.clear();
                        search_path.push(self.default_schema.clone());
                    } else {
                        let mut new_search_path = Vec::new();

                        loop {
                            let name = self.expect_name()?;
                            new_search_path.push(name);

                            if !self.parse_token(TokenKind::Comma)? {
                                break;
                            }
                        }

                        *Rc::make_mut(&mut self.schema).search_path_mut() = new_search_path;
                    }
                } else {
                    // ignored. store it somewhere?
                    self.parse_token_list(false, true)?;
                }

                self.expect_semicolon_or_eof()?;
            } else if self.parse_word(CREATE)? {
                if self.parse_word(TABLE)? {
                    let table = self.parse_table_intern()?;
                    if !Rc::make_mut(&mut self.schema).create_table(table) {
                        return Err(Error::with_cursor(
                            ErrorKind::TableExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(INDEX)? {
                    let index = self.parse_index_intern(false)?;
                    if !Rc::make_mut(&mut self.schema).create_index(index) {
                        return Err(Error::with_cursor(
                            ErrorKind::IndexExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(UNIQUE)? && self.parse_word(INDEX)? {
                    let index = self.parse_index_intern(true)?;
                    if !Rc::make_mut(&mut self.schema).create_index(index) {
                        return Err(Error::with_cursor(
                            ErrorKind::IndexExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(TYPE)? {
                    let type_def = self.parse_type_def_intern()?;
                    if !Rc::make_mut(&mut self.schema).create_type(type_def) {
                        return Err(Error::with_cursor(
                            ErrorKind::TypeExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(SEQUENCE)? {
                    // TODO: CREATE SEQUENCE?
                    self.parse_token_list(false, true)?;
                    self.expect_semicolon_or_eof()?;

                    let end_offset = self.tokenizer.offset();
                    let source = self.tokenizer.get_offset(start_offset, end_offset);

                    eprintln!("TODO: parse CREATE SEQUENCE statements: {source}");
                } else if self.parse_word(EXTENSION)? {
                    // CREATE EXTENSION
                    let if_not_exists = self.parse_if_not_exists()?;

                    let name = self.expect_name()?;

                    self.parse_word(WITH)?;

                    let schema = if self.parse_word(SCHEMA)? {
                        self.expect_name()?
                    } else {
                        self.schema.search_path().first().unwrap_or(&self.default_schema).clone()
                    };

                    let version = if self.parse_word(VERSION)? {
                        Some(self.parse_version()?)
                    } else {
                        None
                    };

                    let cascade = self.parse_word(CASCADE)?;

                    self.expect_semicolon_or_eof()?;

                    let extension = CreateExtension::new(
                        if_not_exists,
                        Extension::new(
                            QName::new(Some(schema), name),
                            version,
                        ),
                        cascade
                    );

                    if !Rc::make_mut(&mut self.schema).create_extension(extension) {
                        return Err(Error::with_cursor(
                            ErrorKind::ExtensionExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(FUNCTION)? {
                    // CREATE FUNCTION/PROCEDURE
                    let function = self.parse_function_intern(false, true)?;
                    if !Rc::make_mut(&mut self.schema).create_function(function) {
                        return Err(Error::with_cursor(
                            ErrorKind::FunctionExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(PROCEDURE)? {
                    // CREATE FUNCTION/PROCEDURE
                    let function = self.parse_function_intern(true, true)?;
                    if !Rc::make_mut(&mut self.schema).create_function(function) {
                        return Err(Error::with_cursor(
                            ErrorKind::FunctionExists,
                            Cursor::new(start_offset, self.tokenizer.offset())
                        ));
                    }
                } else if self.parse_word(OR)? {
                    // CREATE OR REPLACE FUNCTION/PROCEDURE
                    self.expect_word(REPLACE)?;

                    if self.parse_word(FUNCTION)? {
                        let function = self.parse_function_intern(false, true)?;
                        if !Rc::make_mut(&mut self.schema).create_function(function) {
                            return Err(Error::with_cursor(
                                ErrorKind::FunctionExists,
                                Cursor::new(start_offset, self.tokenizer.offset())
                            ));
                        }
                    } else if self.parse_word(PROCEDURE)? {
                        let function = self.parse_function_intern(true, true)?;
                        if !Rc::make_mut(&mut self.schema).create_function(function) {
                            return Err(Error::with_cursor(
                                ErrorKind::FunctionExists,
                                Cursor::new(start_offset, self.tokenizer.offset())
                            ));
                        }
                    } else {
                        return Err(self.expected_one_of(&[
                            FUNCTION, PROCEDURE
                        ]));
                    }
                } else if self.parse_word(TRIGGER)? {
                    // TODO: CREATE TRIGGER?
                    self.parse_token_list(false, true)?;
                    self.expect_semicolon_or_eof()?;

                    let end_offset = self.tokenizer.offset();
                    let source = self.tokenizer.get_offset(start_offset, end_offset);

                    eprintln!("TODO: parse CREATE TRIGGER statements: {source}");
                } else {
                    return Err(self.expected_one_of(&[
                        TABLE, "[UNIQUE] INDEX", TYPE, EXTENSION, SEQUENCE, FUNCTION, PROCEDURE, TRIGGER
                    ]));
                }
            } else if self.parse_word(SELECT)? {
                // ignore SELECT
                self.parse_token_list(false, true)?;
                self.expect_semicolon_or_eof()?;
            } else if self.parse_word(ALTER)? {
                // TODO: ALTER
                // This is important if the output of pg_dump should be supported,
                // because it adds constraints at the end via ALTER statements.

                if self.parse_word(TABLE)? {
                    let alter_table = self.parse_alter_table_intern()?;
                    Rc::make_mut(&mut self.schema).alter_table(&alter_table)?;
                } else if self.parse_word(TYPE)? {
                    let alter_type = self.parse_alter_type_intern()?;
                    Rc::make_mut(&mut self.schema).alter_type(&alter_type)?;
                } else {
                    // TODO: ALTER INDEX|FUNCTION|TRIGGER|...?
                    self.parse_token_list(false, true)?;
                    self.expect_semicolon_or_eof()?;

                    let end_offset = self.tokenizer.offset();
                    let source = self.tokenizer.get_offset(start_offset, end_offset);

                    eprintln!("TODO: parse ALTER statements: {source}");
                }
            } else if self.parse_word(COMMENT)? {
                // ignore COMMENT?
                self.parse_token_list(false, true)?;
                self.expect_semicolon_or_eof()?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse COMMENT statements: {source}");
            } else if self.parse_word(DROP)? {
                // TODO: DROP...
                self.parse_token_list(false, true)?;
                self.expect_semicolon_or_eof()?;

                let end_offset = self.tokenizer.offset();
                let source = self.tokenizer.get_offset(start_offset, end_offset);

                eprintln!("TODO: parse DROP statements: {source}");
            } else if self.parse_word(BEGIN)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
                self.expect_semicolon_or_eof()?;
            } else if self.parse_word(START)? {
                self.expect_word(TRANSACTION)?;
                self.expect_semicolon_or_eof()?;
            } else if self.parse_word(COMMIT)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
                if self.parse_word(AND)? {
                    self.parse_word(NO)?;
                    self.expect_word(CHAIN)?;
                }
                self.expect_semicolon_or_eof()?;
            } else if self.parse_word(ROLLBACK)? {
                let _ = self.parse_word(TRANSACTION)? || self.parse_word(WORK)?;
                if self.parse_word(AND)? {
                    self.parse_word(NO)?;
                    self.expect_word(CHAIN)?;
                }
                self.expect_semicolon_or_eof()?;

                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    Cursor::new(start_offset, self.tokenizer.offset()),
                    format!("transaction rollback is not supported")
                ));
            } else {
                return Err(self.expected_one_of(&[
                    CREATE, SET, SELECT, ALTER, DROP, COMMENT, BEGIN, START
                ]));
            }
        }

        Ok(self.schema.clone())
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

    let value = parse_int_intern(&token, source)?;

    Ok(value)
}

pub fn parse_int<I: SignedInteger>(token: &Token, mut source: &str) -> Result<I> {
    if source.starts_with('-') {
        source = &source[1..];
        return parse_neg_int_intern(token, source);
    } else if source.starts_with('+') {
        source = &source[1..];
    }

    parse_int_intern(&token, source)
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

    value.push_str(&source);
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

    value.push_str(&source);
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

    value.push_str(&source);
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

    value.push_str(&source);
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
                    let Some(slice) = source.get(..4) else {
                        return None;
                    };
                    let Ok(ch) = u32::from_str_radix(slice, 16) else {
                        return None;
                    };
                    value.push(char::from_u32(ch)?);
                    source = &source[4..];
                }
                'U' => {
                    let Some(slice) = source.get(..8) else {
                        return None;
                    };
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

    value.push_str(&source);
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
