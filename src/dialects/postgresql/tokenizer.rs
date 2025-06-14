use crate::{
    dialects::postgresql::{
        parse_estring, parse_int, parse_quot_name, parse_string, parse_uname, parse_ustring,
        strip_dollar_string, uunescape,
    },
    error::{Error, ErrorKind, Result},
    model::{
        name::Name,
        syntax::{Cursor, SourceLocation, Tokenizer},
        token::{ParsedToken, Token, TokenKind},
    },
};

use crate::model::words::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgreSQLTokenizer<'a> {
    source: &'a str,
    offset: usize,
    peeked: Option<Token>,
}

impl<'a> PostgreSQLTokenizer<'a> {
    pub fn parse_all(source: &'a str) -> Result<Vec<ParsedToken>> {
        let mut tokenizer = Self::new(source);
        tokenizer.parse_all()
    }

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
    pub(crate) fn move_to(&mut self, offset: usize) {
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

    pub(crate) fn parse_uescape(&mut self) -> Result<char> {
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

