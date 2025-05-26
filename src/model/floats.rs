pub trait Float:
    Sized +
    Default +
    std::str::FromStr
{
    const BITS: u32;
    const ZERO: Self;
}

impl Float for f32 {
    const BITS: u32 = 32;
    const ZERO: Self = 0f32;
}

impl Float for f64 {
    const BITS: u32 = 64;
    const ZERO: Self = 0f64;
}
