use std::{ops::Deref, rc::Rc};

use super::{name::{Name, QName}, token::ParsedToken, types::DataType};

use crate::{format::{join_into, write_token_list, write_token_list_with_options}, model::words::*};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionSignature {
    name: QName,
    arguments: Rc<[DataType]>,
}

impl FunctionSignature {
    #[inline]
    pub fn new(name: QName, arguments: impl Into<Rc<[DataType]>>) -> Self {
        Self { name, arguments: arguments.into() }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn arguments(&self) -> &Rc<[DataType]> {
        &self.arguments
    }
}

impl std::fmt::Display for FunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;

        join_into(", ", &self.arguments, f)?;

        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropFunctionSignature {
    name: QName,
    arguments: Vec<DropArgument>,
}

impl DropFunctionSignature {
    #[inline]
    pub fn new(name: QName, arguments: impl Into<Vec<DropArgument>>) -> Self {
        Self { name, arguments: arguments.into() }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn arguments(&self) -> &[DropArgument] {
        &self.arguments
    }
}

impl std::fmt::Display for DropFunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (", self.name)?;

        let mut iter = self.arguments.iter();
        if let Some(first) = iter.next() {
            first.fmt(f)?;
            for arg in iter {
                write!(f, ", {arg}")?;
            }
        }

        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropArgument {
    mode: Argmode,
    name: Option<Name>,
    data_type: DataType,
}

impl DropArgument {
    #[inline]
    pub fn new(mode: Argmode, name: Option<Name>, data_type: DataType) -> Self {
        Self { mode, name, data_type }
    }

    #[inline]
    pub fn mode(&self) -> Argmode {
        self.mode
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl std::fmt::Display for DropArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.mode.fmt(f)?;

        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }

        write!(f, " {}", self.data_type)
    }
}

impl From<&Argument> for DropArgument {
    #[inline]
    fn from(value: &Argument) -> Self {
        Self::new(value.mode(), value.name().cloned(), value.data_type.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Argmode {
    In,
    Out,
    InOut,
    Variadic,
}

impl std::fmt::Display for Argmode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::In       => f.write_str(IN),
            Self::Out      => f.write_str(OUT),
            Self::InOut    => f.write_str(INOUT),
            Self::Variadic => f.write_str(VARIADIC),
        }
    }
}

impl Default for Argmode {
    #[inline]
    fn default() -> Self {
        Argmode::In
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    name: QName,
    arguments: Rc<[Argument]>,
    returns: Option<ReturnType>,
    body: Rc<[ParsedToken]>,
}

impl Function {
    #[inline]
    pub fn new(name: QName, arguments: impl Into<Rc<[Argument]>>, returns: Option<ReturnType>, body: impl Into<Rc<[ParsedToken]>>) -> Self {
        Self { name, arguments: arguments.into(), returns, body: body.into() }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn arguments(&self) -> &Rc<[Argument]> {
        &self.arguments
    }

    #[inline]
    pub fn returns(&self) -> Option<&ReturnType> {
        self.returns.as_ref()
    }

    #[inline]
    pub fn body(&self) -> &Rc<[ParsedToken]> {
        &self.body
    }

    pub fn signature(&self) -> FunctionSignature {
        let mut args = Vec::new();
        for arg in self.arguments.deref() {
            match arg.mode() {
                Argmode::In | Argmode::InOut => {
                    args.push(arg.data_type.clone());
                }
                Argmode::Variadic => {
                    // XXX: not sure if this is correct
                    args.push(arg.data_type.to_array(None));
                }
                Argmode::Out => {}
            }
        }

        FunctionSignature::new(self.name.clone(), args)
    }

    /// Signature as wanted by DROP FUNCTION
    pub fn drop_signature(&self) -> DropFunctionSignature {
        DropFunctionSignature::new(
            self.name.clone(),
            self.arguments.iter().map(Into::into).collect::<Vec<_>>()
        )
    }

    pub fn write(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.returns.is_none() {
            f.write_str(PROCEDURE)?;
        } else {
            f.write_str(FUNCTION)?;
        }

        write!(f, " {} (", self.name)?;

        let mut iter = self.arguments.iter();
        if let Some(first) = iter.next() {
            std::fmt::Display::fmt(first, f)?;
            for arg in iter {
                write!(f, ", {arg}")?;
            }
        }

        f.write_str(")\n")?;

        if let Some(returns) = &self.returns {
            writeln!(f, "{RETURNS} {returns}")?;
        }

        write_token_list_with_options(&self.body, f, true)?;

        f.write_str(";\n")
    }
}

impl std::fmt::Display for Function {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} ")?;
        self.write(f)
    }
}

impl From<&Function> for FunctionSignature {
    #[inline]
    fn from(value: &Function) -> Self {
        value.signature()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Argument {
    mode: Argmode,
    name: Option<Name>,
    data_type: DataType,
    default: Option<Rc<[ParsedToken]>>,
}

impl Argument {
    #[inline]
    pub fn new(mode: Argmode, name: Option<Name>, data_type: DataType, default: Option<Rc<[ParsedToken]>>) -> Self {
        Self { mode, name, data_type, default }
    }

    #[inline]
    pub fn mode(&self) -> Argmode {
        self.mode
    }

    #[inline]
    pub fn name(&self) -> Option<&Name> {
        self.name.as_ref()
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn default(&self) -> Option<&Rc<[ParsedToken]>> {
        self.default.as_ref()
    }
}

impl std::fmt::Display for Argument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.mode.fmt(f)?;
        if let Some(name) = &self.name {
            write!(f, " {name} {}", self.data_type)?;
        } else {
            self.data_type.fmt(f)?;
        }

        if let Some(default) = &self.default {
            write!(f, " {DEFAULT} ")?;
            write_token_list(default, f)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnType {
    Trigger,
    Type(DataType),
    Table { columns: Rc<[Column]> }
}

impl std::fmt::Display for ReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trigger => f.write_str(TRIGGER),
            Self::Type(data_type) => data_type.fmt(f),
            Self::Table { columns } => {
                write!(f, "{TABLE} (")?;

                let mut iter = columns.iter();
                if let Some(first) = iter.next() {
                    first.fmt(f)?;
                    for column in iter {
                        write!(f, ", {column}")?;
                    }
                }

                f.write_str(")")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    name: Name,
    data_type: DataType,
}

impl Column {
    #[inline]
    pub fn new(name: Name, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl std::fmt::Display for Column {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunction {
    or_replace: bool,
    function: Rc<Function>,
}

impl CreateFunction {
    #[inline]
    pub fn new(or_replace: bool, function: impl Into<Rc<Function>>) -> Self {
        Self { or_replace, function: function.into() }
    }

    #[inline]
    pub fn or_replace(&self) -> bool {
        self.or_replace
    }

    #[inline]
    pub fn function(&self) -> &Rc<Function> {
        &self.function
    }

    #[inline]
    pub fn into_function(self) -> Rc<Function> {
        self.function
    }
}

impl std::fmt::Display for CreateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CREATE)?;

        if self.or_replace {
            write!(f, " {OR} {REPLACE}")?;
        }

        write!(f, " {}", self.function)
    }
}

impl From<CreateFunction> for Rc<Function> {
    #[inline]
    fn from(value: CreateFunction) -> Rc<Function> {
        value.into_function()
    }
}
