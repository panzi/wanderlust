// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// https://www.postgresql.org/docs/current/sql-createtable.html
// https://www.postgresql.org/docs/current/sql-createindex.html
// https://www.postgresql.org/docs/current/sql-createtype.html

use std::{num::NonZeroU32, rc::Rc};

use crate::{error::{Error, ErrorKind, Result}, model::{column::{Column, ColumnConstraint, ColumnConstraintData, ColumnMatch, ReferentialAction}, ddl::DDL, index::{CreateIndex, Direction, Index, IndexItem, IndexItemData, NullsPosition}, integers::{Integer, SignedInteger, UnsignedInteger}, name::{Name, QName}, syntax::{Cursor, Parser, SourceLocation, Tokenizer}, table::{CreateTable, Table, TableConstraint, TableConstraintData}, token::{ParsedToken, Token, TokenKind}, types::{ColumnDataType, DataType, IntervalFields, TypeDef}}, peek_token};
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
                        format!("actual: EOF, expected: */")
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
                    format!("expected: <string>, actual: EOF")
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
                    format!("expected: <quoted name>, actual: EOF")
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
                    format!("expected: <estring>, actual: EOF")
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
                        format!("expected: <estring>, actual: EOF")
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
                        format!("expected: <single char string>, actual: EOF")
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
                        format!("expected: <string>, actual: {:?}", token.kind())
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
                        format!("expected: <unicode string>, actual: {:?}", token.kind())
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
                        format!("expected: <estring>, actual: {:?}", token.kind())
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
                        format!("expected: <quoted name>, actual: {:?}", token.kind())
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
                        format!("expected: <unicode name>, actual: {:?}", token.kind())
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
                        start_offset + index
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
                            start_offset + index
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
                            start_offset + index
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgreSQLParser<'a> {
    tokenizer: PostgreSQLTokenizer<'a>,
    // TODO: search_path: Vec<Name>,
}

impl<'a> PostgreSQLParser<'a> {
    #[inline]
    pub fn new(source: &'a str) -> Self {
        Self { tokenizer: PostgreSQLTokenizer::new(source) }
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
                format!("expected: <expression>, actual: EOF")));
        };

        match token.kind() {
            TokenKind::LParen => {
                return self.parse_token_list(true);
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
                TokenKind::Word if !matches!(token.kind(), TokenKind::Period | TokenKind::Operator | TokenKind::DoubleColon) => {
                    break;
                }
                TokenKind::LParen | TokenKind::LBracket => {
                    expr.push(self.tokenizer.parse()?);
                    self.parse_token_list_into(&mut expr, false)?;

                    token = if let Some(token) = self.tokenizer.peek()? {
                        token
                    } else {
                        return Err(Error::with_message(
                            ErrorKind::UnexpectedEOF,
                            Cursor::new(self.tokenizer.offset(), self.tokenizer.offset()),
                            format!("expected: expression, actual: EOF")));
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

    fn parse_token_list(&mut self, early_stop: bool) -> Result<Vec<ParsedToken>> {
        let mut tokens = Vec::new();

        self.parse_token_list_into(&mut tokens, early_stop)?;

        Ok(tokens)
    }

    fn parse_token_list_into(&mut self, tokens: &mut Vec<ParsedToken>, early_stop: bool) -> Result<()> {
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
                TokenKind::SemiColon | TokenKind::Comma if early_stop && stack.is_empty() => {
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
                format!("expected: {}, actual: EOF", br.closing())
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
                let token = self.expect_some()?;
                let actual = self.get_source(token.cursor());
                Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected one of: {NULL}, {DEFAULT}, actual: {actual}")
                ))
            }
        } else {
            let token = self.expect_some()?;
            let actual = self.get_source(token.cursor());
            Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected one of: {NO} {ACTION}, {RESTRICT}, {CASCADE}, {SET} {NULL}, {SET} {DEFAULT}, actual: {actual}")
            ))
        }
    }

    fn parse_references(&mut self, column_match: &mut Option<ColumnMatch>, on_update: &mut Option<ReferentialAction>, on_delete: &mut Option<ReferentialAction>) -> Result<()> {
        if self.parse_word(MATCH)? {
            let token = self.expect_token(TokenKind::Word)?;
            let word = self.get_source(token.cursor());

            if word.eq_ignore_ascii_case(FULL) {
                *column_match = Some(ColumnMatch::Full);
            } else if word.eq_ignore_ascii_case(PARTIAL) {
                *column_match = Some(ColumnMatch::Partial);
            } else if word.eq_ignore_ascii_case(SIMPLE) {
                *column_match = Some(ColumnMatch::Simple);
            } else {
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected one of: {FULL}, {PARTIAL}, {SIMPLE}, actual: {word}")
                ));
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

    fn parse_column_data_type(&mut self) -> Result<ColumnDataType> {
        let token = self.expect_some()?;

        let data_type = match token.kind() {
            TokenKind::Word => {
                let word = self.get_source(token.cursor());

                if word.eq_ignore_ascii_case(BIGINT) || word.eq_ignore_ascii_case(INT8) {
                    DataType::Bigint
                } else if word.eq_ignore_ascii_case(BIGSERIAL) || word.eq_ignore_ascii_case(SERIAL8) {
                    DataType::BigSerial
                } else if word.eq_ignore_ascii_case(BIT) {
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
                        DataType::BitVarying(precision)
                    } else {
                        DataType::Bit(precision)
                    }
                } else if word.eq_ignore_ascii_case(VARBIT) {
                    let mut precision = None;
                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }

                    DataType::BitVarying(precision)
                } else if word.eq_ignore_ascii_case(BOOLEAN) || word.eq_ignore_ascii_case(BOOL) {
                    DataType::Boolean
                } else if word.eq_ignore_ascii_case(BOX) {
                    DataType::Box
                } else if word.eq_ignore_ascii_case(BYTEA) {
                    DataType::ByteA
                } else if word.eq_ignore_ascii_case(CHARACTER) {
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
                        DataType::CharacterVarying(precision)
                    } else {
                        DataType::Character(precision)
                    }
                } else if word.eq_ignore_ascii_case(CHAR) {
                    let mut precision = None;
                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }

                    DataType::Character(precision)
                } else if word.eq_ignore_ascii_case(VARCHAR) {
                    let mut precision = None;
                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }

                    DataType::CharacterVarying(precision)
                } else if word.eq_ignore_ascii_case(CIDR) {
                    DataType::CIDR
                } else if word.eq_ignore_ascii_case(CIRCLE) {
                    DataType::Circle
                } else if word.eq_ignore_ascii_case(DATE) {
                    DataType::Date
                } else if word.eq_ignore_ascii_case(DOUBLE) {
                    self.expect_word(PRECISION)?;

                    DataType::DoublePrecision
                } else if word.eq_ignore_ascii_case(FLOAT8) {
                    DataType::DoublePrecision
                } else if word.eq_ignore_ascii_case(INET) {
                    DataType::INet
                } else if word.eq_ignore_ascii_case(INTEGER) || word.eq_ignore_ascii_case(INT) || word.eq_ignore_ascii_case(INT4) {
                    DataType::Integer
                } else if word.eq_ignore_ascii_case(INTERVAL) {
                    let mut precision = None;
                    let mut fields = None;

                    if let Some(token) = self.peek_token()? {
                        if token.kind() == TokenKind::Word {
                            let word = self.get_source(token.cursor());

                            if word.eq_ignore_ascii_case(YEAR) {
                                self.tokenizer.next()?;

                                if self.peek_word(TO)? {
                                    self.tokenizer.next()?;
                                    self.expect_word(MONTH)?;
                                    fields = Some(IntervalFields::YearToMonth);
                                } else {
                                    fields = Some(IntervalFields::Year);
                                }
                            } else if word.eq_ignore_ascii_case(MONTH) {
                                self.tokenizer.next()?;

                                fields = Some(IntervalFields::Month);
                            } else if word.eq_ignore_ascii_case(DAY) {
                                self.tokenizer.next()?;
                                
                                if self.peek_word(TO)? {
                                    self.tokenizer.next()?;

                                    let token = self.expect_token(TokenKind::Word)?;
                                    let word = self.get_source(token.cursor());

                                    if word.eq_ignore_ascii_case(HOUR) {
                                        self.tokenizer.next()?;
                                        fields = Some(IntervalFields::DayToHour);
                                    } else if word.eq_ignore_ascii_case(MINUTE) {
                                        self.tokenizer.next()?;
                                        fields = Some(IntervalFields::DayToMinute);
                                    } else if word.eq_ignore_ascii_case(SECOND) {
                                        self.tokenizer.next()?;
                                        fields = Some(IntervalFields::DayToSecond);
                                    } else {
                                        return Err(Error::with_message(
                                            ErrorKind::UnexpectedToken,
                                            *token.cursor(),
                                            format!("expected one of: {HOUR}, {MINUTE}, {SECOND}, actual: {word}")
                                        ));
                                    }
                                } else {
                                    fields = Some(IntervalFields::Day);
                                }
                            } else if word.eq_ignore_ascii_case(HOUR) {
                                self.tokenizer.next()?;
                                if self.peek_word(TO)? {
                                    self.tokenizer.next()?;

                                    let token = self.expect_token(TokenKind::Word)?;
                                    let word = self.get_source(token.cursor());

                                    if word.eq_ignore_ascii_case(MINUTE) {
                                        self.tokenizer.next()?;
                                        fields = Some(IntervalFields::HourToMinute);
                                    } else if word.eq_ignore_ascii_case(SECOND) {
                                        self.tokenizer.next()?;
                                        fields = Some(IntervalFields::HourToSecond);
                                    } else {
                                        return Err(Error::with_message(
                                            ErrorKind::UnexpectedToken,
                                            *token.cursor(),
                                            format!("expected one of: {HOUR}, {MINUTE}, {SECOND}, actual: {word}")
                                        ));
                                    }
                                } else {
                                    fields = Some(IntervalFields::Hour);
                                }
                            } else if word.eq_ignore_ascii_case(MINUTE) {
                                self.tokenizer.next()?;
                                if self.peek_word(TO)? {
                                    self.tokenizer.next()?;
                                    self.expect_word(SECOND)?;
                                    fields = Some(IntervalFields::MinuteToSecond);
                                } else {
                                    fields = Some(IntervalFields::Minute);
                                }
                            } else if word.eq_ignore_ascii_case(SECOND) {
                                self.tokenizer.next()?;
                                fields = Some(IntervalFields::Second);
                            }
                        }
                    }

                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }
                    DataType::Interval { fields, precision }
                } else if word.eq_ignore_ascii_case(JSON) {
                    DataType::JSON
                } else if word.eq_ignore_ascii_case(JSONB) {
                    DataType::JSONB
                } else if word.eq_ignore_ascii_case(LINE) {
                    DataType::Line
                } else if word.eq_ignore_ascii_case(LSEG) {
                    DataType::LSeg
                } else if word.eq_ignore_ascii_case(MACADDR) {
                    DataType::MacAddr
                } else if word.eq_ignore_ascii_case(MACADDR8) {
                    DataType::MacAddr8
                } else if word.eq_ignore_ascii_case(MONEY) {
                    DataType::Money
                } else if word.eq_ignore_ascii_case(NUMERIC) {
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

                    DataType::Numeric(params)
                } else if word.eq_ignore_ascii_case(PATH) {
                    DataType::Path
                } else if word.eq_ignore_ascii_case(PG_LSN) {
                    DataType::PgLSN
                } else if word.eq_ignore_ascii_case(PG_SNAPSHOT) {
                    DataType::PgSnapshot
                } else if word.eq_ignore_ascii_case(POINT) {
                    DataType::Point
                } else if word.eq_ignore_ascii_case(POLYGON) {
                    DataType::Polygon
                } else if word.eq_ignore_ascii_case(REAL) || word.eq_ignore_ascii_case(FLOAT4) {
                    DataType::Real
                } else if word.eq_ignore_ascii_case(SMALLINT) || word.eq_ignore_ascii_case(INT2) {
                    DataType::SmallInt
                } else if word.eq_ignore_ascii_case(SMALLSERIAL) || word.eq_ignore_ascii_case(SERIAL2) {
                    DataType::SmallSerial
                } else if word.eq_ignore_ascii_case(SERIAL) || word.eq_ignore_ascii_case(SERIAL4) {
                    DataType::Serial
                } else if word.eq_ignore_ascii_case(TEXT) {
                    DataType::Text
                } else if word.eq_ignore_ascii_case(TIME) {
                    let mut precision = None;
                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }

                    let token = self.expect_token(TokenKind::Word)?;
                    let word = self.get_source(token.cursor());
                    let mut with_time_zone = false;

                    if word.eq_ignore_ascii_case(WITH) {
                        self.tokenizer.next()?;
                        self.expect_word(TIME)?;
                        self.expect_word(ZONE)?;
                        with_time_zone = true;
                    } else if word.eq_ignore_ascii_case(WITHOUT) {
                        self.tokenizer.next()?;
                        self.expect_word(TIME)?;
                        self.expect_word(ZONE)?;
                        with_time_zone = false;
                    }

                    DataType::Time { precision, with_time_zone }
                } else if word.eq_ignore_ascii_case(TIMETZ) {
                    DataType::Time { precision: None, with_time_zone: true }
                } else if word.eq_ignore_ascii_case(TIMESTAMP) {
                    let mut precision = None;
                    if self.parse_token(TokenKind::LParen)? {
                        precision = Some(self.parse_precision()?);
                        self.expect_token(TokenKind::RParen)?;
                    }

                    let token = self.expect_token(TokenKind::Word)?;
                    let word = self.get_source(token.cursor());
                    let mut with_time_zone = false;

                    if word.eq_ignore_ascii_case(WITH) {
                        self.tokenizer.next()?;
                        self.expect_word(TIME)?;
                        self.expect_word(ZONE)?;
                        with_time_zone = true;
                    } else if word.eq_ignore_ascii_case(WITHOUT) {
                        self.tokenizer.next()?;
                        self.expect_word(TIME)?;
                        self.expect_word(ZONE)?;
                        with_time_zone = false;
                    }

                    DataType::Timestamp { precision, with_time_zone }
                } else if word.eq_ignore_ascii_case(TIMESTAMPTZ) {
                    DataType::Timestamp { precision: None, with_time_zone: true }
                } else if word.eq_ignore_ascii_case(TSQUERY) {
                    DataType::TsQuery
                } else if word.eq_ignore_ascii_case(TSVECTOR) {
                    DataType::TsVector
                } else if word.eq_ignore_ascii_case(TXID_SNAPSHOT) {
                    DataType::TxIdSnapshot
                } else if word.eq_ignore_ascii_case(UUID) {
                    DataType::UUID
                } else if word.eq_ignore_ascii_case(XML) {
                    DataType::XML
                } else {
                    let name = Name::new_unquoted(word);
                    let name = self.parse_qual_name_tail(name)?;

                    DataType::UserDefined { name }
                }
            }
            TokenKind::QuotName => {
                let name = self.parse_quot_name(token.cursor())?;
                let name = self.parse_qual_name_tail(name)?;

                DataType::UserDefined { name }
            }
            TokenKind::UName => {
                let name = self.parse_uname(token.cursor())?;
                let name = self.parse_qual_name_tail(name)?;

                DataType::UserDefined { name }
            }
            _ => {
                let actual = self.get_source(token.cursor());

                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    *token.cursor(),
                    format!("expected: <column data type>, actual: {actual}")
                ));
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

        Ok(ColumnDataType::new(data_type, array_dimensions))
    }

    #[inline]
    fn parse_qual_name(&mut self) -> Result<QName> {
        let name = self.expect_name()?;
        self.parse_qual_name_tail(name)
    }

    fn parse_qual_name_tail(&mut self, mut name: Name) -> Result<QName> {
        let mut schema = None;

        if self.parse_token(TokenKind::Period)? {
            let mut temp = self.expect_name()?;
            std::mem::swap(&mut temp, &mut name);
            schema = Some(temp);
        }

        Ok(QName::new(schema, name))
    }

    fn parse_table_intern(&mut self) -> Result<CreateTable> {
        // "CREATE TABLE" is already parsed

        let mut if_not_exists = false;
        if self.parse_word(IF)? {
            self.expect_word(NOT)?;
            self.expect_word(EXISTS)?;
            if_not_exists = true;
        }
        let table_name = self.parse_qual_name()?;
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        self.expect_token(TokenKind::LParen)?;

        while !self.peek_kind(TokenKind::RParen)? {
            if let Some(word) = self.peek_words(&[CONSTRAINT, CHECK, UNIQUE, PRIMARY, FOREIGN])? {
                let mut constraint_name = None;
                let constraint_data;

                if word == CONSTRAINT {
                    self.expect_some()?;
                    constraint_name = Some(self.expect_name()?);
                }

                if self.peek_word(CHECK)? {
                    self.expect_some()?;

                    self.expect_token(TokenKind::LParen)?;
                    let expr = self.parse_token_list(false)?;
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

                    let ref_table = self.parse_qual_name()?;
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
                        format!("expected: <column constraint>, actual: {:?}", token.kind())
                    ));
                }

                let mut deferrable = None;
                if self.peek_word(DEFERRABLE)? {
                    self.expect_some()?;
                    deferrable = Some(true);
                } else if self.peek_word(NOT)? {
                    self.expect_some()?;
                    self.expect_word(DEFERRABLE)?;
                    deferrable = Some(false);
                }

                let mut initially_deferred = None;
                if self.peek_word(INITIALLY)? {
                    self.expect_some()?;

                    if self.peek_word(DEFERRED)? {
                        self.expect_some()?;
                        initially_deferred = Some(true);
                    } else {
                        self.expect_word(IMMEDIATE)?;
                        initially_deferred = Some(false);
                    }
                }

                table_constraints.push(
                    Rc::new(TableConstraint::new(
                        constraint_name,
                        constraint_data,
                        deferrable,
                        initially_deferred
                    ))
                );
            } else {
                let column_name = self.expect_name()?;
                let data_type = self.parse_column_data_type()?;
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

                    if self.peek_word(NOT)? {
                        self.expect_some()?;
                        self.expect_word(NULL)?;

                        constraint_data = ColumnConstraintData::NotNull;
                    } else if self.peek_word(NULL)? {
                        self.expect_some()?;

                        constraint_data = ColumnConstraintData::Null;
                    } else if self.peek_word(CHECK)? {
                        self.expect_some()?;

                        self.expect_token(TokenKind::LParen)?;
                        let expr = self.parse_token_list(false)?;
                        self.expect_token(TokenKind::RParen)?;

                        let mut inherit = true;
                        if self.peek_word(NO)? {
                            self.expect_some()?;
                            self.expect_word(INHERIT)?;
                            inherit = false;
                        }

                        constraint_data = ColumnConstraintData::Check { expr: expr.into(), inherit };
                    } else if self.peek_word(DEFAULT)? {
                        self.expect_some()?;
                        let value = self.parse_expr()?;

                        constraint_data = ColumnConstraintData::Default { value: value.into() };
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

                        constraint_data = ColumnConstraintData::Unique { nulls_distinct };
                    } else if self.peek_word(PRIMARY)? {
                        self.expect_some()?;
                        self.expect_word(KEY)?;

                        constraint_data = ColumnConstraintData::PrimaryKey;
                    } else if self.peek_word(REFERENCES)? {
                        self.expect_some()?;
                        let ref_table = self.parse_qual_name()?;

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
                                format!("expected: <column constraint>, actual: {:?}", token.kind())
                            ));
                        }
                        break;
                    }

                    let mut deferrable = None;
                    if self.peek_word(DEFERRABLE)? {
                        self.expect_some()?;
                        deferrable = Some(true);
                    } else if self.peek_word(NOT)? {
                        self.expect_some()?;
                        self.expect_word(DEFERRABLE)?;
                        deferrable = Some(false);
                    }

                    let mut initially_deferred = None;
                    if self.peek_word(INITIALLY)? {
                        self.expect_some()?;

                        if self.peek_word(DEFERRED)? {
                            self.expect_some()?;
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

                columns.push(Rc::new(Column::new(
                    column_name, data_type, collation, column_constraints,
                )));
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
            table_constraints
        );

        Ok(CreateTable::new(table, if_not_exists))
    }

    fn parse_index_item(&mut self) -> Result<IndexItem> {
        let data;
        if self.parse_token(TokenKind::LParen)? {
            let expr = self.parse_token_list(false)?;
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
                let token = self.expect_some()?;
                let actual = self.get_source(token.cursor());

                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected one of: {FIRST}, {LAST}, actual: {actual}")
                ));
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
            Some(self.parse_qual_name()?)
        } else if peek_token!(self, TokenKind::Word | TokenKind::QuotName | TokenKind::UName)?.is_some() {
            Some(self.parse_qual_name()?)
        } else {
            None
        };

        self.expect_word(ON)?;

        let table_name = self.parse_qual_name()?;

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
            predicate = Some(self.parse_token_list(true)?);
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
                    format!("expected: <name>, actual: {:?}", token.kind())
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
                        format!("expected: <string>, actual: {:?}", token.kind())
                    ));
                };
                Ok(value.into())
            }
            TokenKind::EString => {
                let Some(value) = parse_estring(self.get_source(token.cursor())) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <estring>, actual: {:?}", token.kind())
                    ));
                };
                Ok(value.into())
            }
            TokenKind::UString => {
                let Some(value) = parse_ustring(self.get_source(token.cursor())) else {
                    return Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *token.cursor(),
                        format!("expected: <unicode string>, actual: {:?}", token.kind())
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
                    format!("expected: <string>, actual: {:?}", token.kind())
                ))
            }

        }
    }

    fn parse(&mut self) -> Result<DDL> {
        let mut ddl = DDL::new();

        while self.tokenizer.peek()?.is_some() {
            let start_offset = self.expect_word(CREATE)?.cursor().start_offset();

            if self.parse_word(TABLE)? {
                if !ddl.create_table(self.parse_table_intern()?) {
                    return Err(Error::with_cursor(
                        ErrorKind::TableExists,
                        Cursor::new(start_offset, self.tokenizer.offset())
                    ));
                }
            } else if self.parse_word(INDEX)? {
                if !ddl.create_index(self.parse_index_intern(false)?) {
                    return Err(Error::with_cursor(
                        ErrorKind::IndexExists,
                        Cursor::new(start_offset, self.tokenizer.offset())
                    ));
                }
            } else if self.parse_word(UNIQUE)? && self.parse_word(INDEX)? {
                if !ddl.create_index(self.parse_index_intern(true)?) {
                    return Err(Error::with_cursor(
                        ErrorKind::IndexExists,
                        Cursor::new(start_offset, self.tokenizer.offset())
                    ));
                }
            } else if self.parse_word(TYPE)? {
                if !ddl.create_type(self.parse_type_def_intern()?) {
                    return Err(Error::with_cursor(
                        ErrorKind::TypeExists,
                        Cursor::new(start_offset, self.tokenizer.offset())
                    ));
                }
            } else if let Some(token) = self.tokenizer.next()? {
                let actual = self.get_source(token.cursor());
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected one of: {TABLE}, [{UNIQUE}] {INDEX}, {TYPE}, actual: {actual}")
                ));
            } else {
                return Err(Error::with_message(
                    ErrorKind::UnexpectedEOF,
                    Cursor::new(self.tokenizer.offset(), self.tokenizer.offset() + 1),
                    format!("expected one of: {TABLE}, [{UNIQUE}] {INDEX}, {TYPE}, actual: EOF")
                ));
            }
        }

        Ok(ddl)
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
    let value = &value[..index];
    value
}
