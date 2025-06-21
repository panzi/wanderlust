pub trait Float:
    Sized +
    Default +
    std::str::FromStr
{
    const BITS: u32;
    const ZERO: Self;

    fn from_u64(value: u64) -> Self;
}

impl Float for f32 {
    const BITS: u32 = 32;
    const ZERO: Self = 0f32;

    fn from_u64(value: u64) -> Self {
        value as f32
    }
}

impl Float for f64 {
    const BITS: u32 = 64;
    const ZERO: Self = 0f64;

    fn from_u64(value: u64) -> Self {
        value as f64
    }
}
