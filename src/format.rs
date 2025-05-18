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

pub fn write_token_list(tokens: &[ParsedToken], f: &mut impl std::fmt::Write) -> std::fmt::Result {
    let mut iter = tokens.iter();
    if let Some(first) = iter.next() {
        write!(f, "{first}")?;
        for token in iter {
            write!(f, " {token}")?;
        }
    }
    Ok(())
}

pub fn format_iso_string(mut write: impl std::fmt::Write, value: &str) -> std::fmt::Result {
    if value.contains(|c: char| c < ' ' || c > '~') {
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

        return write.write_str("'");
    }

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
