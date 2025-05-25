use std::rc::Rc;

use crate::error::{Error, ErrorKind, Result};

use super::{schema::Schema, name::Name, token::{ParsedToken, Token, TokenKind}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    start_offset: usize,
    end_offset: usize,
}

#[inline]
fn get_width(text: &str) -> usize {
    // TODO: propper unicode width?
    text.chars().count()
}

#[inline]
fn get_last_line_start(source: &str) -> usize {
    let Some(mut start_index) = source.rfind('\n') else {
        return 0;
    };

    start_index += 1;

    if source[start_index..].starts_with('\r') {
        start_index += 1;
    }

    start_index
}

impl Cursor {
    #[inline]
    pub fn new(start_offset: usize, end_offset: usize) -> Self {
        Self { start_offset, end_offset }
    }

    #[inline]
    pub fn start_offset(&self) -> usize {
        self.start_offset
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.end_offset
    }

    #[inline]
    pub fn set_start_offset(&mut self, start_offset: usize) {
        self.start_offset = start_offset;
    }

    #[inline]
    pub fn set_end_offset(&mut self, end_offset: usize) {
        self.end_offset = end_offset;
    }

    #[inline]
    pub fn set_offsets(&mut self, start_offset: usize, end_offset: usize) {
        self.start_offset = start_offset;
        self.end_offset   = end_offset;
    }

    #[inline]
    pub fn get<'a>(&self, source: &'a str) -> &'a str {
        &source[self.start_offset..self.end_offset]
    }

    #[inline]
    pub fn locate_start(&self, source: &str) -> SourceLocation {
        SourceLocation::from_offset(self.start_offset, source)
    }

    #[inline]
    pub fn locate_end(&self, source: &str) -> SourceLocation {
        SourceLocation::from_offset(self.end_offset, source)
    }

    pub fn locate(&self, source: &str) -> SourceSpan {
        let head = &source[..self.start_offset];
        let line_start_index = get_last_line_start(head);

        let start_lineno = head.chars().filter(|c| *c == '\n').count() + 1;
        let tail = &head[line_start_index..];

        let start_column = get_width(tail);

        let slice = &source[self.start_offset..self.end_offset];
        let end_lineno = start_lineno + slice.chars().filter(|c| *c == '\n').count();

        let head = &source[..self.end_offset];
        let line_end_index = get_last_line_start(head);
        let tail = &head[line_end_index..];

        let end_column = get_width(tail);

        SourceSpan::new(
            SourceLocation::new(start_lineno, start_column),
            SourceLocation::new(end_lineno, end_column),
        )
    }

    pub fn write(&self, source: &str, write: &mut impl std::fmt::Write) -> std::fmt::Result {
        let span = self.locate(source);
        let head = &source[..self.start_offset];

        let start_lineno = span.start().lineno();
        let end_lineno = span.end().lineno();
        let lineno_width = get_digits(end_lineno);

        let mut line_start_index = get_last_line_start(head);
        let mut lineno = span.start().lineno();

        while line_start_index < self.end_offset {
            let line_end_index = line_start_index + source[line_start_index..].find('\n').unwrap_or(source.len());
            let line = &source[line_start_index..line_end_index];

            writeln!(write, "{: ^width$} │ {}", lineno, line, width = lineno_width)?;
            write!(write, "{: ^width$} │ ", "", width = lineno_width)?;

            let start_column = if lineno == start_lineno {
                span.start().column()
            } else {
                0
            };

            let end_column = if lineno == end_lineno {
                span.end().column()
            } else {
                get_width(line)
            };

            writeln!(write, "{: ^before$}{:^^underline$}", "", "", before = start_column, underline = (end_column - start_column).max(1))?;

            line_start_index = line_end_index;
            if line_start_index < source.len() && source[line_start_index..].starts_with('\r') {
                line_start_index += 1;
            }
            lineno += 1;
        }

        Ok(())
    }

    #[inline]
    pub fn format(&self, source: &str) -> String {
        let mut result = String::new();
        let _ = self.write(source, &mut result);
        result
    }
}

#[inline]
fn get_digits(mut value: usize) -> usize {
    let mut count = 1;

    while value >= 10 {
        count += 1;
        value /= 10;
    }

    count
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLocation {
    lineno: usize,
    column: usize,
}

impl SourceLocation {
    #[inline]
    pub fn new(lineno: usize, column: usize) -> Self {
        Self { lineno, column }
    }

    pub fn from_offset(offset: usize, source: &str) -> SourceLocation {
        let head = &source[..offset];
        let Some(nl_index) = head.rfind('\n') else {
            return SourceLocation::new(1, get_width(head));
        };

        let lineno = head.chars().filter(|c| *c == '\n').count() + 1;
        let mut tail = &head[nl_index + 1..];

        if tail.starts_with('\r') {
            tail = &tail[1..];
        }

        SourceLocation::new(lineno, get_width(tail))
    }

    #[inline]
    pub fn lineno(&self) -> usize {
        self.lineno
    }

    #[inline]
    pub fn column(&self) -> usize {
        self.column
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    start: SourceLocation,
    end: SourceLocation,
}

impl SourceSpan {
    #[inline]
    pub fn new(start: SourceLocation, end: SourceLocation) -> Self {
        Self { start, end }
    }

    #[inline]
    pub fn start(&self) -> &SourceLocation {
        &self.start
    }

    #[inline]
    pub fn end(&self) -> &SourceLocation {
        &self.end
    }
}

pub trait Locatable {
    fn cursor(&self) -> &Cursor;
}

impl Locatable for Cursor {
    #[inline]
    fn cursor(&self) -> &Cursor {
        self
    }
}

pub trait Tokenizer {
    fn get(&self, cursor: &Cursor) -> &str {
        self.get_offset(cursor.start_offset(), cursor.end_offset())
    }

    fn get_offset(&self, start_offset: usize, end_offset: usize) -> &str;

    fn peek(&mut self) -> Result<Option<Token>>;
    fn next(&mut self) -> Result<Option<Token>>;
    fn parse(&mut self) -> Result<ParsedToken>;
}

#[macro_export]
macro_rules! expect_token {
    ($parser:expr, $($kinds:tt)*) => {
        match $parser.expect_some() {
            Ok(_token) => {
                if matches!(_token.kind(), $($kinds)*) {
                    Ok(_token)
                } else {
                    let _actual = $parser.get_source(_token.cursor());
                    let mut _msg = "expected one of:".to_owned();
                    expect_token!(@msg _msg $($kinds)*);
                    let _ = write!(_msg, ", actual: {_actual}");
                    Err(Error::with_message(
                        ErrorKind::UnexpectedToken,
                        *_token.cursor(),
                        _msg
                    ))
                }
            },
            Err(_err) => Err(_err)
        }
    };
    (@msg $msg:ident) => {};
    (@msg $msg:ident $tp:ident :: $val:ident) => {
        let _ = write!($msg, " {}", $tp::$val);
    };
    (@msg $msg:ident $tp:ident :: $val:ident | $($tail:tt)*) => {
        let _ = write!($msg, " {}", $tp::$val);
        expect_token!(@msg $msg $($tail)*);
    };
}

#[macro_export]
macro_rules! peek_token {
    ($parser:expr, $($kinds:tt)*) => {
        match $parser.peek_token() {
            Ok(Some(_token)) => {
                if matches!(_token.kind(), $($kinds)*) {
                    Ok(Some(_token))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(_err) => Err(_err)
        }
    }
}

pub trait Parser {
    fn get_source(&self, cursor: &Cursor) -> &str;

    fn parse(&mut self) -> Result<Rc<Schema>>;

    fn expect_some(&mut self) -> Result<Token>;

    fn expect_token(&mut self, kind: TokenKind) -> Result<Token> {
        let token = self.expect_some()?;

        if token.kind() != kind {
            let actual = self.get_source(token.cursor());
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: {kind}, actual: {actual}")
            ));
        }

        Ok(token)
    }

    fn expect_semicolon_or_eof(&mut self) -> Result<()> {
        if let Some(token) = self.peek_token()? {
            if token.kind() != TokenKind::SemiColon {
                let actual = self.get_source(token.cursor());
                return Err(Error::with_message(
                    ErrorKind::UnexpectedToken,
                    *token.cursor(),
                    format!("expected: ; or <EOF>, actual: {actual}")
                ));
            }
            self.expect_some()?;
        }
        Ok(())
    }

    fn expect_word(&mut self, word: &str) -> Result<Token> {
        let token = self.expect_token(TokenKind::Word)?;
        let actual = self.get_source(token.cursor());

        if !actual.eq_ignore_ascii_case(word) {
            return Err(Error::with_message(
                ErrorKind::UnexpectedToken,
                *token.cursor(),
                format!("expected: {word}, actual: {actual}")
            ));
        }

        Ok(token)
    }

    fn peek_token(&mut self) -> Result<Option<Token>>;

    #[inline]
    fn peek_integer(&mut self) -> Result<Option<Token>> {
        peek_token!(self, TokenKind::BinInt | TokenKind::OctInt | TokenKind::DecInt | TokenKind::HexInt)
    }

    #[inline]
    fn peek_kind(&mut self, kind: TokenKind) -> Result<bool> {
        let Some(token) = self.peek_token()? else {
            return Ok(false);
        };

        return Ok(token.kind() == kind);
    }

    #[inline]
    fn parse_token(&mut self, kind: TokenKind) -> Result<bool> {
        let Some(token) = self.peek_token()? else {
            return Ok(false);
        };

        if token.kind() == kind {
            self.expect_some()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn peek_word(&mut self, word: &str) -> Result<bool> {
        let Some(token) = self.peek_token()? else {
            return Ok(false);
        };

        if token.kind() != TokenKind::Word {
            return Ok(false);
        }

        let actual = self.get_source(token.cursor());
        if !actual.eq_ignore_ascii_case(word) {
            return Ok(false);
        }

        Ok(true)
    }

    fn peek_words<'a>(&mut self, words: &[&'a str]) -> Result<Option<&'a str>> {
        let Some(token) = self.peek_token()? else {
            return Ok(None);
        };

        if token.kind() != TokenKind::Word {
            return Ok(None);
        }

        let actual = self.get_source(token.cursor());
        let Some(word) = words.iter().find(|word| word.eq_ignore_ascii_case(actual)) else {
            return Ok(None);
        };

        Ok(Some(*word))
    }

    #[inline]
    fn parse_word(&mut self, word: &str) -> Result<bool> {
        if self.peek_word(word)? {
            self.expect_some()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn expect_name(&mut self) -> Result<Name>;
    fn expect_string(&mut self) -> Result<Rc<str>>;
}
