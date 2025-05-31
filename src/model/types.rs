use std::num::NonZeroU32;
use std::ops::Deref;
use std::rc::Rc;

use crate::format::format_iso_string;
use crate::make_tokens;
use crate::ordered_hash_map::OrderedHashMap;

use super::alter::types::ValuePosition;
use super::name::{Name, QName};
use super::token::{ParsedToken, ToTokens};
use super::words::*;

#[derive(Debug, Clone, PartialEq)]
pub struct TypeDef {
    name: QName,
    data: TypeData,
    comment: Option<Rc<str>>,
}

impl std::fmt::Display for TypeDef {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} {TYPE} {} {};", self.name, self.data)
    }
}

impl TypeDef {
    #[inline]
    pub fn new(name: QName, data: TypeData) -> Self {
        Self { name, data, comment: None }
    }

    #[inline]
    pub fn with_name(&self, name: QName) -> Self {
        Self { name, data: self.data.clone(), comment: self.comment.clone() }
    }

    #[inline]
    pub fn create_enum(name: QName, values: impl Into<Rc<Vec<Rc<str>>>>) -> Self {
        Self { name, data: TypeData::create_enum(values), comment: None }
    }

    #[inline]
    pub fn create_composite(name: QName, attributes: impl Into<OrderedHashMap<Name, CompositeAttribute>>) -> Self {
        Self { name, data: TypeData::create_composite(attributes), comment: None }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn set_name(&mut self, name: QName) {
        self.name = name;
    }

    #[inline]
    pub fn name_mut(&mut self) -> &mut QName {
        &mut self.name
    }

    #[inline]
    pub fn data(&self) -> &TypeData {
        &self.data
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut TypeData {
        &mut self.data
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment;
    }

    pub fn missing_enum_values(&self, new_type_def: &TypeDef) -> Option<Vec<(Rc<str>, Option<ValuePosition>)>> {
        match (self.data(), new_type_def.data()) {
            (TypeData::Enum { values: old_values }, TypeData::Enum { values: new_values }) => {
                for old_value in old_values.deref() {
                    if !new_values.contains(old_value) {
                        return None;
                    }
                }

                let mut missing_values = Vec::new();

                for (index, new_value) in new_values.deref().iter().enumerate() {
                    if !old_values.contains(new_value) {
                        if index > 0 {
                            missing_values.push((
                                new_value.clone(),
                                Some(ValuePosition::after(new_values[index - 1].clone()))
                            ));
                        } else if let Some(new_first) = new_values.first() {
                            missing_values.push((
                                new_value.clone(),
                                Some(ValuePosition::before(new_first.clone()))
                            ));
                        } else {
                            missing_values.push((
                                new_value.clone(),
                                None
                            ));
                        }
                    }
                }

                Some(missing_values)
            }
            _ => None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeData {
    Composite { attributes: OrderedHashMap<Name, CompositeAttribute> },
    Enum { values: Rc<Vec<Rc<str>>> },
    // TODO: more types
}

impl TypeData {
    #[inline]
    pub fn create_enum(values: impl Into<Rc<Vec<Rc<str>>>>) -> Self {
        Self::Enum { values: values.into() }
    }

    #[inline]
    pub fn create_composite(attributes: impl Into<OrderedHashMap<Name, CompositeAttribute>>) -> Self {
        Self::Composite { attributes: attributes.into() }
    }
}

impl std::fmt::Display for TypeData {
    #[inline]
    fn fmt(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Composite { attributes } => {
                write!(f, "{AS} (")?;

                if attributes.len() > 1 {
                    let mut iter = attributes.values();
                    if let Some(first) = iter.next() {
                        write!(f, "\n    {first}")?;

                        for attribute in iter {
                            write!(f, ",\n    {attribute}")?;
                        }
                    }
                    f.write_str("\n")?;
                } else if let Some(first) = attributes.values_unordered().next() {
                    std::fmt::Display::fmt(first, f)?;
                }

                f.write_str(")")
            }
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

#[derive(Debug, Clone, PartialEq)]
pub struct CompositeAttribute {
    name: Name,
    data_type: DataType,
    collation: Option<QName>,
}

impl CompositeAttribute {
    #[inline]
    pub fn new(
        name: Name,
        data_type: DataType,
        collation: Option<QName>,
    ) -> Self {
        Self { name, data_type, collation }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn set_name(&mut self, name: Name) {
        self.name = name;
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn set_data_type(&mut self, data_type: DataType) {
        self.data_type = data_type;
    }

    #[inline]
    pub fn collation(&self) -> Option<&QName> {
        self.collation.as_ref()
    }

    #[inline]
    pub fn set_collation(&mut self, collation: Option<QName>) {
        self.collation = collation;
    }
}

impl std::fmt::Display for CompositeAttribute {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(collation) = &self.collation {
            write!(f, "{} {} {COLLATE} {collation}", self.name, self.data_type)
        } else {
            write!(f, "{} {}", self.name, self.data_type)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl ToTokens for IntervalFields {
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        match self {
            Self::Year           => make_tokens!(tokens, YEAR),
            Self::Month          => make_tokens!(tokens, MONTH),
            Self::Day            => make_tokens!(tokens, DAY),
            Self::Hour           => make_tokens!(tokens, HOUR),
            Self::Minute         => make_tokens!(tokens, MINUTE),
            Self::Second         => make_tokens!(tokens, SECOND),
            Self::YearToMonth    => make_tokens!(tokens, YEAR TO MONTH),
            Self::DayToHour      => make_tokens!(tokens, DAY TO HOUR),
            Self::DayToMinute    => make_tokens!(tokens, DAY TO MINUTE),
            Self::DayToSecond    => make_tokens!(tokens, DAY TO SECOND),
            Self::HourToMinute   => make_tokens!(tokens, HOUR TO MINUTE),
            Self::HourToSecond   => make_tokens!(tokens, HOUR TO SECOND),
            Self::MinuteToSecond => make_tokens!(tokens, MINUTE TO SECOND),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BasicType {
    Internal,
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
    UserDefined { name: QName, parameters: Option<Rc<[Value]>> },
    ColumnType { table_name: QName, column_name: Name },
}

impl BasicType {
    #[inline]
    pub fn is_serial(&self) -> bool {
        matches!(self, Self::Serial | Self::BigSerial | Self::SmallSerial)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    String(Rc<str>),
    Integer(i64),
    // Float(f64),
    // maybe names?
    // hope there can't be arrays
}

impl Value {
    #[inline]
    pub fn to_parsed_token(&self) -> ParsedToken {
        match self {
            Value::String(value) => ParsedToken::String(value.clone()),
            Value::Integer(value) => ParsedToken::Integer(*value),
            //Value::Float(value) => ParsedToken::Float(*value),
        }
    }

    #[inline]
    pub fn into_parsed_token(self) -> ParsedToken {
        match self {
            Value::String(value) => ParsedToken::String(value),
            Value::Integer(value) => ParsedToken::Integer(value),
            //Value::Float(value) => ParsedToken::Float(value),
        }
    }
}

impl std::fmt::Display for Value {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(value) => format_iso_string(f, value),
            Value::Integer(value) => write!(f, "{value}"),
            //Value::Float(value) => write!(f, "{value:?}"),
        }
    }
}

impl From<Value> for ParsedToken {
    #[inline]
    fn from(value: Value) -> Self {
        value.into_parsed_token()
    }
}

impl ToTokens for Value {
    #[inline]
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        tokens.push(self.to_parsed_token());
    }
}

impl ToTokens for BasicType {
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        match self {
            Self::Internal => make_tokens!(tokens, INTERNAL),
            Self::Bigint => make_tokens!(tokens, BIGINT),
            Self::BigSerial => make_tokens!(tokens, BIGSERIAL),
            Self::Bit(p) => {
                if let Some(p) = p {
                    make_tokens!(tokens, BIT ({p}));
                } else {
                    make_tokens!(tokens, BIT);
                }
            },
            Self::BitVarying(p) => {
                if let Some(p) = p {
                    make_tokens!(tokens, BIT VARYING ({p}));
                } else {
                    make_tokens!(tokens, BIT VARYING);
                }
            }
            Self::Boolean => make_tokens!(tokens, BOOLEAN),
            Self::Box => make_tokens!(tokens, BOX),
            Self::ByteA => make_tokens!(tokens, BYTEA),
            Self::Character(p) => {
                if let Some(p) = p {
                    make_tokens!(tokens, CHARACTER ({p}));
                } else {
                    make_tokens!(tokens, CHARACTER);
                }
            }
            Self::CharacterVarying(p) => {
                if let Some(p) = p {
                    make_tokens!(tokens, CHARACTER VARYING ({p}));
                } else {
                    make_tokens!(tokens, CHARACTER VARYING);
                }
            }
            Self::CIDR => make_tokens!(tokens, CIDR),
            Self::Circle => make_tokens!(tokens, CIRCLE),
            Self::Date => make_tokens!(tokens, DATE),
            Self::DoublePrecision => make_tokens!(tokens, DOUBLE PRECISION),
            Self::INet => make_tokens!(tokens, INET),
            Self::Integer => make_tokens!(tokens, INTEGER),
            Self::Interval { fields, precision } => {
                make_tokens!(tokens, INTERVAL);

                if let Some(fields) = fields {
                    fields.to_tokens_into(tokens);
                }

                if let Some(precision) = precision {
                    make_tokens!(tokens, ({precision}));
                }

            }
            Self::JSON => make_tokens!(tokens, JSON),
            Self::JSONB => make_tokens!(tokens, JSONB),
            Self::Line => make_tokens!(tokens, LINE),
            Self::LSeg => make_tokens!(tokens, LSEG),
            Self::MacAddr => make_tokens!(tokens, MACADDR),
            Self::MacAddr8 => make_tokens!(tokens, MACADDR8),
            Self::Money => make_tokens!(tokens, MONEY),
            Self::Numeric(p) => {
                if let Some((p, s)) = p {
                    make_tokens!(tokens, NUMERIC ({p}, {*s}));
                } else {
                    make_tokens!(tokens, NUMERIC);
                }
            }
            Self::Path => make_tokens!(tokens, PATH),
            Self::PgLSN => make_tokens!(tokens, PG_LSN),
            Self::PgSnapshot => make_tokens!(tokens, PG_SNAPSHOT),
            Self::Point => make_tokens!(tokens, POINT),
            Self::Polygon => make_tokens!(tokens, POLYGON),
            Self::Real => make_tokens!(tokens, REAL),
            Self::SmallInt => make_tokens!(tokens, SMALLINT),
            Self::SmallSerial => make_tokens!(tokens, SMALLSERIAL),
            Self::Serial => make_tokens!(tokens, SERIAL),
            Self::Text => make_tokens!(tokens, TEXT),
            Self::Time { precision, with_time_zone } => {
                make_tokens!(tokens, TIME);

                if let Some(precision) = precision {
                    make_tokens!(tokens, ({precision}));
                }

                if *with_time_zone {
                    make_tokens!(tokens, WITH TIME ZONE);
                } else {
                    make_tokens!(tokens, WITHOUT TIME ZONE);
                }
            }
            Self::Timestamp { precision, with_time_zone } => {
                make_tokens!(tokens, TIMESTAMP);

                if let Some(precision) = precision {
                    make_tokens!(tokens, ({precision}));
                }

                if *with_time_zone {
                    make_tokens!(tokens, WITH TIME ZONE);
                } else {
                    make_tokens!(tokens, WITHOUT TIME ZONE);
                }
            }
            Self::TsQuery => make_tokens!(tokens, TSQUERY),
            Self::TsVector => make_tokens!(tokens, TSVECTOR),
            Self::TxIdSnapshot => make_tokens!(tokens, TXID_SNAPSHOT),
            Self::UUID => make_tokens!(tokens, UUID),
            Self::XML => make_tokens!(tokens, XML),
            Self::UserDefined { name, parameters } => {
                make_tokens!(tokens, {name});
                if let Some(parameters) = parameters {
                    tokens.push(ParsedToken::LParen);
                    let mut iter = parameters.iter();
                    if let Some(first) = iter.next() {
                        make_tokens!(tokens, {first});

                        for value in iter {
                            make_tokens!(tokens, , {value});
                        }
                    }
                    tokens.push(ParsedToken::RParen);
                }
            }
            Self::ColumnType { table_name, column_name } => {
                make_tokens!(tokens, {table_name}.{column_name}%TYPE)
            }
        }
    }
}

impl std::fmt::Display for BasicType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal => f.write_str(INTERNAL),
            Self::Bigint => f.write_str(BIGINT),
            Self::BigSerial => f.write_str(BIGSERIAL),
            Self::Bit(p) => {
                if let Some(p) = p {
                    write!(f, "{BIT} ({p})")
                } else {
                    f.write_str(BIT)
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
                f.write_str(TIMESTAMP)?;

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
            Self::UserDefined { name, parameters } => {
                name.fmt(f)?;

                if let Some(parameters) = parameters {
                    write!(f, "(")?;
                    let mut iter = parameters.iter();
                    if let Some(first) = iter.next() {
                        first.fmt(f)?;
                        for value in iter {
                            write!(f, ", ")?;
                            value.fmt(f)?;
                        }
                    }
                    write!(f, ")")?;
                }

                Ok(())
            }
            Self::ColumnType { table_name, column_name } => {
                write!(f, "{table_name}.{column_name}%{TYPE}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    basic_type: BasicType,
    array_dimensions: Option<Rc<[Option<u32>]>>,
}

impl DataType {
    #[inline]
    pub fn new(data_type: BasicType, array_dimensions: Option<Rc<[Option<u32>]>>) -> Self {
        Self { basic_type: data_type, array_dimensions }
    }

    #[inline]
    pub fn basic(data_type: BasicType) -> Self {
        Self { basic_type: data_type, array_dimensions: None }
    }

    #[inline]
    pub fn to_array(&self, dimensions: Option<u32>) -> Self {
        if let Some(array_dimensions) = &self.array_dimensions {
            let mut new_array_dimensions = Vec::with_capacity(array_dimensions.len() + 1);

            // not sure if front or back is correct
            new_array_dimensions.push(dimensions);
            new_array_dimensions.extend(array_dimensions.deref());

            Self {
                basic_type: self.basic_type.clone(),
                array_dimensions: Some(new_array_dimensions.into())
            }
        } else {
            Self {
                basic_type: self.basic_type.clone(),
                array_dimensions: Some([dimensions].into())
            }
        }
    }

    #[inline]
    pub fn basic_type(&self) -> &BasicType {
        &self.basic_type
    }

    #[inline]
    pub fn array_dimensions(&self) -> Option<&Rc<[Option<u32>]>> {
        self.array_dimensions.as_ref()
    }

    #[inline]
    pub fn with_data_type(&self, data_type: BasicType) -> Self {
        Self { basic_type: data_type, array_dimensions: self.array_dimensions.clone() }
    }

    #[inline]
    pub fn with_user_type(&self, type_name: QName, parameters: Option<Rc<[Value]>>) -> Self {
        Self {
            basic_type: BasicType::UserDefined { name: type_name, parameters },
            array_dimensions: self.array_dimensions.clone(),
        }
    }

    pub fn cast(&self, value: impl ToTokens) -> Vec<ParsedToken> {
        let mut expr = vec![ParsedToken::from_name(CAST), ParsedToken::LParen];
        value.to_tokens_into(&mut expr);
        expr.push(ParsedToken::from_name(AS));
        self.to_tokens_into(&mut expr);
        expr.push(ParsedToken::RParen);

        expr
    }
}

impl ToTokens for DataType {
    fn to_tokens_into(&self, tokens: &mut Vec<ParsedToken>) {
        self.basic_type.to_tokens_into(tokens);
        if let Some(array_dimensions) = &self.array_dimensions {
            for dim in array_dimensions.deref() {
                if let Some(dim) = dim {
                    make_tokens!(tokens, [{*dim}]);
                } else {
                    make_tokens!(tokens, []);
                }
            }
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.basic_type.fmt(f)?;
        if let Some(dims) = &self.array_dimensions {
            for dim in dims.deref() {
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
