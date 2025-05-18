pub struct FmtWriter<W: std::io::Write>(W);

impl<W: std::io::Write> FmtWriter<W> {
    #[inline]
    pub fn new(write: W) -> Self {
        Self(write)
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
