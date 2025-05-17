use std::ops::{AddAssign, BitOrAssign, MulAssign, Neg, ShlAssign, SubAssign};

pub trait Integer:
    Sized +
    Default +
    AddAssign +
    SubAssign +
    ShlAssign +
    BitOrAssign +
    MulAssign
{
    const BITS: u32;
    const ZERO: Self;
    const EIGHT: Self;
    const TEN: Self;
    const SIXTEEN: Self;
    const ONE: Self;
    const TWO: Self;
    const MAX: Self;

    fn value_of(ch: char) -> Self;
    fn hex_value_of(ch: char) -> Self;
}

pub trait UnsignedInteger: Integer {}

pub trait SignedInteger: Integer + Neg<Output = Self> {
    const MINUS_ONE: Self;
}

macro_rules! make_int {
    ($int:ty) => {
        impl Integer for $int {
            const BITS: u32 = Self::BITS;
            const ZERO: Self = 0;
            const ONE: Self = 1;
            const TWO: Self = 2;
            const EIGHT: Self = 8;
            const TEN: Self = 10;
            const SIXTEEN: Self = 16;
            const MAX: Self = Self::MAX;

            #[inline]
            fn value_of(ch: char) -> Self {
                (ch as u32 - '0' as u32) as $int
            }

            #[inline]
            fn hex_value_of(ch: char) -> Self {
                if ch >= 'a' && ch <= 'f' {
                    (ch as u32 - 'a' as u32) as $int + 10
                } else if ch >= 'A' && ch <= 'F' {
                    (ch as u32 - 'A' as u32) as $int + 10
                } else {
                    (ch as u32 - '0' as u32) as $int
                }
            }
        }
    };
    () => {};
    ($int:ty, $($more:ty),+) => {
        make_int!($int);
        make_int!($($more),+);
    };
}

make_int!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128);

macro_rules! make_sint {
    ($int:ty) => {
        impl SignedInteger for $int {
            const MINUS_ONE: Self = -1;
        }
    };
    () => {};
    ($int:ty, $($more:ty),+) => {
        make_sint!($int);
        make_sint!($($more),+);
    };
}

make_sint!(i8, i16, i32, i64, i128);

macro_rules! make_uint {
    ($int:ty) => {
        impl UnsignedInteger for $int {}
    };
    () => {};
    ($int:ty, $($more:ty),+) => {
        make_uint!($int);
        make_uint!($($more),+);
    };
}

make_uint!(u8, u16, u32, u64, u128);
