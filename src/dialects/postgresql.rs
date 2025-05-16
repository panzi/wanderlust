// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// https://www.postgresql.org/docs/current/sql-createtable.html
// https://www.postgresql.org/docs/current/sql-createindex.html
// https://www.postgresql.org/docs/current/sql-createtype.html

use crate::{error::{Error, ErrorKind, Result}, model::{ddl::DDL, name::Name, syntax::{Cursor, Parser, SourceLocation, Tokenizer}, token::{Token, TokenKind}}};

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
}

macro_rules! is_operator {
    ($ch:expr) => {
        matches!($ch, '+' | '-' | '*' | '/' | '<' | '>' | '=' | '~' | '!' | '@' | '#' | '%' | '^' | '&' | '|' | '`' | '?')
    };
}

impl<'a> Tokenizer for PostgreSQLTokenizer<'a> {
    #[inline]
    fn get_offset(&self, start_offset: usize, end_offset: usize) -> &str {
        &self.source[start_offset..end_offset]
    }

    fn peek(&mut self) -> Result<Option<crate::model::token::Token>> {
        if let Some(token) = &self.peeked {
            return Ok(Some(*token));
        }

        self.peeked = self.next()?;

        Ok(self.peeked)
    }

    fn next(&mut self) -> Result<Option<crate::model::token::Token>> {
        if let Some(token) = self.peeked {
            self.peeked = None;
            return Ok(Some(token));
        }

        self.skip_ws()?;

        let Some(ch) = self.peek_char() else {
            return Ok(None);
        };

        if ch >= '0' && ch <= '9' {
            // number
            unimplemented!() // TODO
        } else if ch.is_ascii_alphabetic() || ch == '_' {
            // word (or U string or E string)
            unimplemented!() // TODO
        } else if ch == '\'' {
            // string
            unimplemented!() // TODO
        } else if ch == '"' {
            // quoted word
            unimplemented!() // TODO
        } else if ch == '$' {
            // dollar string
            unimplemented!() // TODO
        } else if is_operator!(ch) {
            // operator
            let start_offset = self.offset;
            self.offset += 1;
            self.offset = if let Some(index) = self.source[self.offset..].find(|c| !is_operator!(c)) {
                self.offset + index
            } else {
                self.source.len()
            };
            return Ok(Some(Token::new(TokenKind::Operator, Cursor::new(start_offset, self.offset))));
        } else {
            return Err(Error::with_message(
                ErrorKind::IllegalToken,
                Cursor::new(self.offset, self.offset + 1),
                format!("unexpected: {ch}")));
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
        // assumes that the quoted name is already syntactically correct
        let source = cursor.get(self.tokenizer.source());
        let mut source = &source[1..source.len() - 1];
        let mut name = String::new();

        loop {
            let Some(index) = source.find('"') else {
                break;
            };

            name.push_str(&source[..index + 1]);
            source = &source[index + 2..];
        }

        name.push_str(&source);

        Name::new_quoted(name)
    }

    #[inline]
    fn parse_name(&self, cursor: &Cursor) -> Name {
        let source = cursor.get(self.tokenizer.source());
        Name::new_unquoted(source)
    }

    fn parse_token_list(&mut self) -> Result<Vec<Token>> {
        let mut stack = Vec::new();
        let mut tokens = Vec::new();

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
                        return Err(Error::with_cursor(
                            ErrorKind::UnexpectedToken,
                            *token.cursor(),
                        ));
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
                        return Err(Error::with_cursor(
                            ErrorKind::UnexpectedToken,
                            *token.cursor(),
                        ));
                    }
                }
                TokenKind::SemiColon | TokenKind::Comma => {
                    return Ok(tokens);
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

        Ok(tokens)
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

    fn parse(&mut self) -> Result<DDL> {
        let mut ddl = DDL::new();

        while self.tokenizer.peek()?.is_some() {
            self.expect_word("CREATE")?;

            let token = self.expect_token(TokenKind::Word)?;
            let word = self.get_source(token.cursor());

            if word.eq_ignore_ascii_case("TABLE") {
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
