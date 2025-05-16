use crate::error::{Error, ErrorKind, Result};

use super::{ddl::DDL, token::{Token, TokenKind}};

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

            let line_end_index = self.start_offset + source[line_start_index..].find('\n').unwrap_or(source.len());
            let line = &source[line_start_index..line_end_index];

            write!(write, "{: ^width$} │ {}\n", lineno, line, width = lineno_width)?;
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

            write!(write, "{: ^before$}{:^^underline$}\n", "", "", before = start_column, underline = end_column - start_column)?;

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

pub trait Tokenizer {
    fn get(&self, cursor: &Cursor) -> &str {
        self.get_offset(cursor.start_offset(), cursor.end_offset())
    }

    fn get_offset(&self, start_offset: usize, end_offset: usize) -> &str;

    fn peek(&mut self) -> Result<Option<Token>>;
    fn next(&mut self) -> Result<Option<Token>>;
}

pub trait Parser {
    fn get_source(&self, cursor: &Cursor) -> &str;

    fn parse(&mut self) -> Result<DDL>;

    fn expect_some(&mut self) -> Result<Token>;

    fn expect_token(&mut self, kind: TokenKind) -> Result<Token> {
        let token = self.expect_some()?;

        if token.kind() != kind {
            let actual = self.get_source(token.cursor());
            return Err(Error::with_message(ErrorKind::UnexpectedToken, *token.cursor(), format!("expected: {kind:?}, actual: {actual}")));
        }

        Ok(token)
    }

    fn expect_word(&mut self, word: &str) -> Result<Token> {
        let token = self.expect_token(TokenKind::Word)?;
        let actual = self.get_source(token.cursor());

        if !actual.eq_ignore_ascii_case(word) {
            return Err(Error::with_message(ErrorKind::UnexpectedToken, *token.cursor(), format!("expected: {word}, actual: {actual}")));
        }

        Ok(token)
    }
}
