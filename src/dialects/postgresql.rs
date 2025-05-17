// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// https://www.postgresql.org/docs/current/sql-createtable.html
// https://www.postgresql.org/docs/current/sql-createindex.html
// https://www.postgresql.org/docs/current/sql-createtype.html

use crate::{error::{Error, ErrorKind, Result}, model::{column::{Column, ColumnConstraint, ColumnConstraintData}, ddl::DDL, name::Name, syntax::{Cursor, Parser, SourceLocation, Tokenizer}, token::{Token, TokenKind}}};

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
                        TokenKind::Int,
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
                        TokenKind::Int,
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
                        TokenKind::Int,
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
                        if float { TokenKind::Float } else { TokenKind::Int },
                        Cursor::new(start_offset, end_offset)
                    )));
                }
            }
            'E' if self.source[self.offset + 1..].starts_with('\'') => {
                // E string
                let start_offset = self.offset;
                self.offset += 1;
                let end_offset = self.find_string_end()?;
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
                unimplemented!()
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
                unimplemented!() // TODO
            }
            '$' => {
                // dollar string or single dollar?
                unimplemented!() // TODO
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
                return Ok(Some(Token::new(TokenKind::Operator, Cursor::new(start_offset, self.offset))));
            }
            _ => {
                return Err(Error::with_message(
                    ErrorKind::IllegalToken,
                    Cursor::new(self.offset, self.offset + 1),
                    format!("unexpected: {ch}")));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgreSQLParser<'a> {
    tokenizer: PostgreSQLTokenizer<'a>,
}

impl<'a> PostgreSQLParser<'a> {
    #[inline]
    pub fn new(source: &'a str) -> Self {
        Self { tokenizer: PostgreSQLTokenizer::new(source) }
    }

    fn parse_qname(&self, cursor: &Cursor) -> Name {
        // assumes that the quoted name is syntactically correct
        let source = cursor.get(self.tokenizer.source());
        let mut source = &source[1..source.len() - 1];
        let mut name = String::with_capacity(source.len());

        while let Some(index) = source.find('"') {
            name.push_str(&source[..index + 1]);
            source = &source[index + 2..];
        }

        name.push_str(&source);
        name.shrink_to_fit();

        Name::new_quoted(name)
    }

    fn parse_uname(&mut self, cursor: &Cursor) -> Result<Name> {
        // parse U&"..." and U&"..." UESCAPE '.'
        // assumes that the quoted name is syntactically correct
        let source = cursor.get(self.tokenizer.source());
        let mut source = &source[1..source.len() - 1];
        let mut name = String::with_capacity(source.len());

        while let Some(index) = source.find('"') {
            name.push_str(&source[..index + 1]);
            source = &source[index + 2..];
        }

        name.push_str(&source);

        let mut escape = '\\';
        if let Some(next) = self.tokenizer.peek()? {
            if next.kind() == TokenKind::Word && self.tokenizer.get(next.cursor()).eq_ignore_ascii_case("UESCAPE") {
                self.tokenizer.next()?;
                let next = self.expect_token(TokenKind::String)?;
                let source = self.tokenizer.get(next.cursor());
                let value = self.parse_string(source);

                if value.len() != 1 {
                    return Err(Error::with_message(
                        ErrorKind::IllegalToken,
                        *next.cursor(),
                        format!("epected: <single char string>\nactual: {source}")
                    ));
                }

                escape = value.chars().next().unwrap();
            }
        }

        let Some(name) = uunescape(&name, escape) else {
            return Err(Error::with_message(
                ErrorKind::IllegalToken,
                *cursor,
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

    fn parse_string(&self, source: &str) -> String {
        // assumes that the quoted string is syntactically correct
        let mut source = &source[1..source.len() - 1];
        let mut value = String::with_capacity(source.len());

        while let Some(index) = source.find('\'') {
            value.push_str(&source[..index + 1]);
            source = &source[index + 2..];
        }

        value.push_str(&source);
        value.shrink_to_fit();

        value
    }

    fn parse_expr(&mut self) -> Result<Vec<Token>> {
        let Some(mut token) = self.tokenizer.peek()? else {
            return Err(Error::with_message(
                ErrorKind::UnexpectedEOF,
                Cursor::new(self.tokenizer.offset(), self.tokenizer.offset()),
                format!("expected: <expression>, actual: EOF")));
        };

        match token.kind() {
            TokenKind::Comma | TokenKind::SemiColon | TokenKind::Colon | TokenKind::DoubleColon | TokenKind::Period | TokenKind::RParen | TokenKind::RBracket => {
                let actual = self.tokenizer.get(token.cursor());
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: <expression>, actual: {actual}")));
            }
            _ => {}
        }

        let mut expr = vec![token];
        self.tokenizer.next()?;

        while let Some(next) = self.tokenizer.peek()? {
            match next.kind() {
                TokenKind::Comma | TokenKind::SemiColon | TokenKind::RParen | TokenKind::RBracket => {
                    break;
                }
                TokenKind::Word if token.kind() == TokenKind::UString && self.tokenizer.get(next.cursor()).eq_ignore_ascii_case("UESCAPE") => {
                    expr.push(next);
                    self.tokenizer.next()?;

                    token = self.expect_token(TokenKind::String)?;
                    expr.push(token);
                }
                TokenKind::Word if !matches!(token.kind(), TokenKind::Period | TokenKind::Operator | TokenKind::DoubleColon) => {
                    break;
                }
                TokenKind::LParen | TokenKind::LBracket => {
                    expr.push(next);
                    self.tokenizer.next()?;
                    self.parse_token_list_into(&mut expr, false)?;

                    token = if let Some(token) = self.tokenizer.next()? {
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

                    expr.push(token);
                }
                _ => {
                    expr.push(next);
                    self.tokenizer.next()?;
                    token = next;
                }
            }
        }

        Ok(expr)
    }

    fn parse_token_list(&mut self, early_stop: bool) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        self.parse_token_list_into(&mut tokens, early_stop)?;

        Ok(tokens)
    }

    fn parse_token_list_into(&mut self, tokens: &mut Vec<Token>, early_stop: bool) -> Result<()> {
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

            tokens.push(token);
            self.tokenizer.next()?;
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
            TokenKind::QIdent => {
                Ok(self.parse_qname(token.cursor()))
            }
            TokenKind::UIdent => {
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

    fn expect_string(&mut self) -> Result<String> {
        let token = self.expect_some()?;
        match token.kind() {
            TokenKind::String => {
                Ok(self.parse_string(self.get_source(token.cursor())))
            }
            TokenKind::EString => {
                unimplemented!()
            }
            TokenKind::UString => {
                unimplemented!()
            }
            TokenKind::DollarString => {
                unimplemented!()
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
            self.expect_word("CREATE")?;

            let token = self.expect_token(TokenKind::Word)?;
            let word = self.get_source(token.cursor());

            if word.eq_ignore_ascii_case("TABLE") {
                if self.parse_word("IF")? {
                    self.expect_word("NOT")?;
                    self.expect_word("EXISTS")?;
                }
                let table_name = self.expect_name()?;
                let mut columns = Vec::new();
                //let mut table_constraints = Vec::new();

                self.expect_token(TokenKind::LParen)?;

                while !self.peek_kind(TokenKind::RParen)? {
                    let start_offset = self.tokenizer.offset();

                    if let Some(word) = self.peek_words(&["CONSTRAINT", "CHECK", "UNIQUE", "PRIMARY", "FOREIGN"])? {
                        let mut constraint_name = None;
                        if word == "CONSTRAINT" {
                            self.expect_some()?;
                            constraint_name = Some(self.expect_name()?);
                        }

                        if self.peek_word("CHECK")? {
                            // TODO
                        } else if self.peek_word("UNIQUE")? {
                            // TODO
                        } else if self.peek_word("PRIMARAY")? {
                            // TODO
                        } else if self.peek_word("FOREIGN")? {
                            // TODO
                        } else {
                            // ERROR
                        }
                    } else {
                        let column_name = self.expect_name()?;
                        let data_type = self.expect_name()?;
                        let mut collation = None;

                        if self.peek_word("COLLATION")? {
                            self.expect_some()?;
                            collation = Some(self.expect_string()?);
                        }

                        let mut column_constraints = Vec::new();

                        loop {
                            let mut constraint_name = None;
                            let constraint_start_offset = self.tokenizer.offset();
                            if self.peek_word("CONSTRAINT")? {
                                self.expect_some()?;
                                constraint_name = Some(self.expect_name()?);
                            }

                            let constraint_data;

                            if self.peek_word("NOT")? {
                                self.expect_some()?;
                                self.expect_word("NULL")?;

                                constraint_data = ColumnConstraintData::NotNull;
                            } else if self.peek_word("NULL")? {
                                self.expect_some()?;

                                constraint_data = ColumnConstraintData::Null;
                            } else if self.peek_word("CHECK")? {
                                self.expect_some()?;

                                self.expect_token(TokenKind::LParen)?;
                                let expr = self.parse_expr()?;
                                self.expect_token(TokenKind::RParen)?;

                                let mut inherit = true;
                                if self.peek_word("NO")? {
                                    self.expect_some()?;
                                    self.expect_word("INHERIT")?;
                                    inherit = false;
                                }

                                constraint_data = ColumnConstraintData::Check { expr, inherit };
                            } else if self.peek_word("DEFAULT")? {
                                self.expect_some()?;
                                let value = self.parse_expr()?;

                                constraint_data = ColumnConstraintData::Default { value };
                            } else if self.peek_word("UNIQUE")? {
                                unimplemented!() // TODO
                            } else if self.peek_word("PRIMARY")? {
                                unimplemented!() // TODO
                            } else if self.peek_word("REFERENCES")? {
                                unimplemented!() // TODO
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

                            let mut deferrable = false;
                            if self.peek_word("DEFERRABLE")? {
                                self.expect_some()?;
                                deferrable = true;
                            } else if self.peek_word("NOT")? {
                                self.expect_some()?;
                                self.expect_word("DEFERRABLE")?;
                                deferrable = false;
                            }

                            let mut initially_deferred = false;
                            if self.peek_word("INITIALLY")? {
                                self.expect_some()?;

                                if self.peek_word("DEFERRED")? {
                                    self.expect_some()?;
                                    initially_deferred = true;
                                } else {
                                    self.expect_word("IMMEDIATE")?;
                                    initially_deferred = false;
                                }
                            }

                            column_constraints.push(
                                ColumnConstraint::new(
                                    Cursor::new(constraint_start_offset, self.tokenizer.offset()),
                                    constraint_name,
                                    constraint_data,
                                    deferrable,
                                    initially_deferred,
                                )
                            );
                        }

                        columns.push(Column::new(
                            Cursor::new(start_offset, self.tokenizer.offset()),
                            column_name, data_type, collation, column_constraints,
                        ));
                    }

                    if self.peek_kind(TokenKind::Colon)? {
                        self.expect_some()?;
                    } else {
                        break;
                    }
                }
                self.expect_token(TokenKind::RParen)?;
                self.expect_token(TokenKind::SemiColon)?;
                unimplemented!()
            } else if word.eq_ignore_ascii_case("INDEX") {
                unimplemented!()
            } else if word.eq_ignore_ascii_case("TYPE") {
                unimplemented!()
            } else {
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected one of: TABLE, INDEX, TYPE, actual: {word}")));
            }
        }

        Ok(ddl)
    }
}
