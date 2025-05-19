use std::num::NonZeroU32;
use std::rc::Rc;

use crate::format::format_iso_string;

use super::name::Name;
use super::words::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    name: Name,
    data: TypeData,
}

impl std::fmt::Display for TypeDef {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} {TYPE} {} {};", self.name, self.data)
    }
}

impl TypeDef {
    #[inline]
    pub fn new(name: Name, data: TypeData) -> Self {
        Self { name, data }
    }

    #[inline]
    pub fn create_enum(name: Name, values: impl Into<Rc<[Rc<str>]>>) -> Self {
        Self { name, data: TypeData::create_enum(values) }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn data(&self) -> &TypeData {
        &self.data
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeData {
    Enum { values: Rc<[Rc<str>]> },
    // TODO: more types
}

impl TypeData {
    #[inline]
    pub fn create_enum(values: impl Into<Rc<[Rc<str>]>>) -> Self {
        Self::Enum { values: values.into() }
    }
}

impl std::fmt::Display for TypeData {
    #[inline]
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enum { values } => {
                write!(f, "{AS} (")?;

                let mut iter = values.iter();
                if let Some(first) = iter.next() {
                    format_iso_string(&mut f, first)?;

                    for value in iter {
                        f.write_str(", ")?;
                        format_iso_string(&mut f, value)?;
                    }
                }

                f.write_str(")")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalFields {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
}

impl std::fmt::Display for IntervalFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Year           => f.write_str(YEAR),
            Self::Month          => f.write_str(MONTH),
            Self::Day            => f.write_str(DAY),
            Self::Hour           => f.write_str(HOUR),
            Self::Minute         => f.write_str(MINUTE),
            Self::Second         => f.write_str(SECOND),
            Self::YearToMonth    => f.write_str("YEAR TO MONTH"),
            Self::DayToHour      => f.write_str("DAY TO HOUR"),
            Self::DayToMinute    => f.write_str("DAY TO MINUTE"),
            Self::DayToSecond    => f.write_str("DAY TO SECOND"),
            Self::HourToMinute   => f.write_str("HOUR TO MINUTE"),
            Self::HourToSecond   => f.write_str("HOUR TO SECOND"),
            Self::MinuteToSecond => f.write_str("MINUTE TO SECOND"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bigint,
    BigSerial,
    Bit(Option<NonZeroU32>),
    BitVarying(Option<NonZeroU32>),
    Boolean,
    Box,
    ByteA,
    Character(Option<NonZeroU32>),
    CharacterVarying(Option<NonZeroU32>),
    CIDR,
    Circle,
    Date,
    DoublePrecision,
    INet,
    Integer,
    Interval { fields: Option<IntervalFields>, precision: Option<NonZeroU32> },
    JSON,
    JSONB,
    Line,
    LSeg,
    MacAddr,
    MacAddr8,
    Money,
    Numeric(Option<(NonZeroU32, i32)>),
    Path,
    PgLSN,
    PgSnapshot,
    Point,
    Polygon,
    Real,
    SmallInt,
    SmallSerial,
    Serial,
    Text,
    Time { precision: Option<NonZeroU32>, with_time_zone: bool },
    Timestamp { precision: Option<NonZeroU32>, with_time_zone: bool },
    TsQuery,
    TsVector,
    TxIdSnapshot,
    UUID,
    XML,
    UserDefined { name: Name },
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bigint => BIGINT.fmt(f),
            Self::BigSerial => BIGSERIAL.fmt(f),
            Self::Bit(p) => {
                if let Some(p) = p {
                    write!(f, "{BIT} ({p})")
                } else {
                    BIT.fmt(f)
                }
            }
            Self::BitVarying(p) => {
                if let Some(p) = p {
                    write!(f, "{BIT} {VARYING} ({p})")
                } else {
                    write!(f, "{BIT} {VARYING}")
                }
            }
            Self::Boolean => BOOLEAN.fmt(f),
            Self::Box => BOX.fmt(f),
            Self::ByteA => BYTEA.fmt(f),
            Self::Character(p) => {
                if let Some(p) = p {
                    write!(f, "{CHARACTER} ({p})")
                } else {
                    CHARACTER.fmt(f)
                }
            }
            Self::CharacterVarying(p) => {
                if let Some(p) = p {
                    write!(f, "{CHARACTER} {VARYING} ({p})")
                } else {
                    write!(f, "{CHARACTER} {VARYING}")
                }
            }
            Self::CIDR => CIDR.fmt(f),
            Self::Circle => CIRCLE.fmt(f),
            Self::Date => DATE.fmt(f),
            Self::DoublePrecision => write!(f, "{DOUBLE} {PRECISION}"),
            Self::INet => INET.fmt(f),
            Self::Integer => INTEGER.fmt(f),
            Self::Interval { fields, precision } => {
                INTERVAL.fmt(f)?;

                if let Some(fields) = fields {
                    f.write_str(" ")?;
                    fields.fmt(f)?;
                }

                if let Some(precision) = precision {
                    write!(f, " ({precision})")?;
                }

                Ok(())
            }
            Self::JSON => JSON.fmt(f),
            Self::JSONB => JSONB.fmt(f),
            Self::Line => LINE.fmt(f),
            Self::LSeg => LSEG.fmt(f),
            Self::MacAddr => MACADDR.fmt(f),
            Self::MacAddr8 => MACADDR8.fmt(f),
            Self::Money => MONEY.fmt(f),
            Self::Numeric(p) => {
                if let Some((p, s)) = p {
                    write!(f, "{NUMERIC} ({p}, {s})")
                } else {
                    f.write_str(NUMERIC)
                }
            }
            Self::Path => PATH.fmt(f),
            Self::PgLSN => PG_LSN.fmt(f),
            Self::PgSnapshot => PG_SNAPSHOT.fmt(f),
            Self::Point => POINT.fmt(f),
            Self::Polygon => POLYGON.fmt(f),
            Self::Real => REAL.fmt(f),
            Self::SmallInt => SMALLINT.fmt(f),
            Self::SmallSerial => SMALLSERIAL.fmt(f),
            Self::Serial => SERIAL.fmt(f),
            Self::Text => TEXT.fmt(f),
            Self::Time { precision, with_time_zone } => {
                f.write_str(TIME)?;

                if let Some(precision) = precision {
                    write!(f, " ({precision})")?;
                }

                if *with_time_zone {
                    write!(f, " {WITH} {TIME} {ZONE}")?;
                } else {
                    write!(f, " {WITHOUT} {TIME} {ZONE}")?;
                }

                Ok(())
            }
            Self::Timestamp { precision, with_time_zone } => {
                f.write_str(TIME)?;

                if let Some(precision) = precision {
                    write!(f, " ({precision})")?;
                }

                if *with_time_zone {
                    write!(f, " {WITH} {TIME} {ZONE}")?;
                } else {
                    write!(f, " {WITHOUT} {TIME} {ZONE}")?;
                }

                Ok(())
            }
            Self::TsQuery => TSQUERY.fmt(f),
            Self::TsVector => TSVECTOR.fmt(f),
            Self::TxIdSnapshot => TXID_SNAPSHOT.fmt(f),
            Self::UUID => UUID.fmt(f),
            Self::XML => XML.fmt(f),
            Self::UserDefined { name } => name.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDataType {
    data_type: DataType,
    array_dimensions: Option<Box<[Option<u32>]>>,
}

impl ColumnDataType {
    #[inline]
    pub fn new(data_type: DataType, array_dimensions: Option<Box<[Option<u32>]>>) -> Self {
        Self { data_type, array_dimensions }
    }

    #[inline]
    pub fn basic(data_type: DataType) -> Self {
        Self { data_type, array_dimensions: None }
    }

    #[inline]
    pub fn with_array(data_type: DataType, array_dimensions: Box<[Option<u32>]>) -> Self {
        Self { data_type, array_dimensions: Some(array_dimensions) }
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn array_dimensions(&self) -> Option<&[Option<u32>]> {
        self.array_dimensions.as_deref()
    }
}

impl std::fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data_type.fmt(f)?;
        if let Some(dims) = &self.array_dimensions {
            for dim in dims {
                if let Some(dim) = dim {
                    write!(f, "[{dim}]")?;
                } else {
                    f.write_str("[]")?;
                }
            }
        }
        Ok(())
    }
}
