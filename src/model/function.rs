use std::{ops::Deref, rc::Rc};

use super::{name::{Name, QName}, token::ParsedToken, types::DataType};

use crate::format::{format_string_for_functions, join_into, write_token_list, IsoString};

use crate::model::words::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QFunctionRef {
    name: QName,
    /// only input arguments
    arguments: Rc<[DataType]>,
}

impl QFunctionRef {
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

    #[inline]
    pub fn to_unqualifed(&self) -> FunctionRef {
        FunctionRef::new(self.name.name().clone(), self.arguments.clone())
    }

    #[inline]
    pub fn into_unqualifed(self) -> FunctionRef {
        FunctionRef::new(self.name.into_name(), self.arguments)
    }
}

impl From<QFunctionRef> for FunctionRef {
    #[inline]
    fn from(value: QFunctionRef) -> Self {
        value.into_unqualifed()
    }
}

impl From<&QFunctionRef> for FunctionRef {
    #[inline]
    fn from(value: &QFunctionRef) -> Self {
        value.to_unqualifed()
    }
}

impl std::fmt::Display for QFunctionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;

        join_into(", ", &self.arguments, f)?;

        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionRef {
    name: Name,
    /// only input arguments
    arguments: Rc<[DataType]>,
}

impl FunctionRef {
    #[inline]
    pub fn new(name: Name, arguments: impl Into<Rc<[DataType]>>) -> Self {
        Self { name, arguments: arguments.into() }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn arguments(&self) -> &Rc<[DataType]> {
        &self.arguments
    }

    #[inline]
    pub fn to_qualified(&self, schema: Name) -> QFunctionRef {
        QFunctionRef::new(QName::new(Some(schema), self.name.clone()), self.arguments.clone())
    }

    #[inline]
    pub fn into_qualified(self, schema: Name) -> QFunctionRef {
        QFunctionRef::new(QName::new(Some(schema), self.name), self.arguments)
    }
}

impl std::fmt::Display for FunctionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;

        join_into(", ", &self.arguments, f)?;

        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionSignature {
    name: QName,
    arguments: Vec<SignatureArgument>,
}

impl FunctionSignature {
    #[inline]
    pub fn new(name: QName, arguments: impl Into<Vec<SignatureArgument>>) -> Self {
        Self { name, arguments: arguments.into() }
    }

    #[inline]
    pub fn name(&self) -> &QName {
        &self.name
    }

    #[inline]
    pub fn arguments(&self) -> &[SignatureArgument] {
        &self.arguments
    }

    fn ref_args(&self) -> Vec<DataType> {
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
        args
    }

    #[inline]
    pub fn to_qref(&self) -> QFunctionRef {
        QFunctionRef::new(self.name.clone(), self.ref_args())
    }

    #[inline]
    pub fn to_ref(&self) -> FunctionRef {
        FunctionRef::new(self.name.name().clone(), self.ref_args())
    }
}

impl From<&FunctionSignature> for QFunctionRef {
    #[inline]
    fn from(value: &FunctionSignature) -> Self {
        value.to_qref()
    }
}

impl std::fmt::Display for FunctionSignature {
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

/// Doesn't contain DEFAULT value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignatureArgument {
    mode: Argmode,
    name: Option<Name>,
    data_type: DataType,
}

impl SignatureArgument {
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

impl std::fmt::Display for SignatureArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.mode.fmt(f)?;

        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }

        write!(f, " {}", self.data_type)
    }
}

impl From<&Argument> for SignatureArgument {
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
    language: Option<Name>,
    transform: Vec<QName>,
    window: bool,
    state: Option<State>,
    leakproof: Option<bool>,
    null_input_handling: Option<NullInputHandling>,
    security: Option<Security>,
    parallelism: Option<Parallelism>,
    cost: Option<u32>,
    rows: Option<u32>,
    support: Option<QName>,
    configuration_parameters: Vec<(Name, ConfigurationValue)>,
    body: FunctionBody,
    comment: Option<Rc<str>>,
}

impl Function {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn new(
            name: QName,
            arguments: impl Into<Rc<[Argument]>>,
            returns: Option<ReturnType>,
            language: Option<Name>,
            transform: Vec<QName>,
            window: bool,
            state: Option<State>,
            leakproof: Option<bool>,
            null_input_handling: Option<NullInputHandling>,
            security: Option<Security>,
            parallelism: Option<Parallelism>,
            cost: Option<u32>,
            rows: Option<u32>,
            support: Option<QName>,
            configuration_parameters: Vec<(Name, ConfigurationValue)>,
            body: FunctionBody,
    ) -> Self {
        Self {
            name,
            arguments: arguments.into(),
            returns,
            language,
            transform,
            window,
            state,
            leakproof,
            null_input_handling,
            security,
            parallelism,
            cost,
            rows,
            support,
            configuration_parameters,
            body,
            comment: None,
        }
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
    pub fn language(&self) -> Option<&Name> {
        self.language.as_ref()
    }

    #[inline]
    pub fn transform(&self) -> &[QName] {
        &self.transform
    }

    #[inline]
    pub fn window(&self) -> bool {
        self.window
    }

    #[inline]
    pub fn state(&self) -> Option<State> {
        self.state
    }

    #[inline]
    pub fn leakproof(&self) -> Option<bool> {
        self.leakproof
    }

    #[inline]
    pub fn null_input_handling(&self) -> Option<NullInputHandling> {
        self.null_input_handling
    }

    #[inline]
    pub fn security(&self) -> Option<&Security> {
        self.security.as_ref()
    }

    #[inline]
    pub fn parallelism(&self) -> Option<Parallelism> {
        self.parallelism
    }

    #[inline]
    pub fn cost(&self) -> Option<u32> {
        self.cost
    }

    #[inline]
    pub fn rows(&self) -> Option<u32> {
        self.rows
    }

    #[inline]
    pub fn support(&self) -> Option<&QName> {
        self.support.as_ref()
    }

    #[inline]
    pub fn configuration_parameters(&self) -> &[(Name, ConfigurationValue)] {
        &self.configuration_parameters
    }

    #[inline]
    pub fn body(&self) -> &FunctionBody {
        &self.body
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment;
    }

    fn ref_args(&self) -> Vec<DataType> {
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
        args
    }

    #[inline]
    pub fn to_qref(&self) -> QFunctionRef {
        QFunctionRef::new(self.name.clone(), self.ref_args())
    }

    #[inline]
    pub fn to_ref(&self) -> FunctionRef {
        FunctionRef::new(self.name.name().clone(), self.ref_args())
    }

    pub fn to_signature(&self) -> FunctionSignature {
        FunctionSignature::new(
            self.name.clone(),
            self.arguments.iter().map(Into::into).collect::<Vec<_>>()
        )
    }

    pub fn write(&self, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

        if let Some(language) = &self.language {
            writeln!(f, "{LANGUAGE} {language}")?;
        }

        {
            let mut iter = self.transform.iter();
            if let Some(first) = iter.next() {
                write!(f, "{TRANSFORM} {FOR} {TYPE} {first}")?;
                for type_name in iter {
                    write!(f, ", {FOR} {TYPE} {type_name}")?;
                }
                f.write_str("\n")?;
            }
        }

        if self.window {
            writeln!(f, "{WINDOW}")?;
        }

        if let Some(state) = self.state {
            writeln!(f, "{state}")?;
        }

        if let Some(leakproof) = self.leakproof {
            if leakproof {
                writeln!(f, "{LEAKPROOF}")?;
            } else {
                writeln!(f, "{NOT} {LEAKPROOF}")?;
            }
        }

        if let Some(null_input_handling) = self.null_input_handling {
            writeln!(f, "{null_input_handling}")?;
        }

        if let Some(security) = self.security {
            writeln!(f, "{security}")?;
        }

        if let Some(parallelism) = self.parallelism {
            writeln!(f, "{parallelism}")?;
        }

        if let Some(cost) = self.cost {
            writeln!(f, "{COST} {cost}")?;
        }

        if let Some(rows) = self.rows {
            writeln!(f, "{ROWS} {rows}")?;
        }

        if let Some(support) = &self.support {
            writeln!(f, "{SUPPORT} {support}")?;
        }

        for (name, value) in &self.configuration_parameters {
            match value {
                ConfigurationValue::FromCurrent => {
                    writeln!(f, "{SET} {name} {FROM} {CURRENT}")?;
                }
                ConfigurationValue::Value(value) => {
                    write!(f, "{SET} {name} {TO} ")?;
                    write_token_list(value, f)?;
                    f.write_str("\n")?;
                }
            }
        }

        match &self.body {
            FunctionBody::Definition { source } => {
                write!(f, "{AS} ")?;
                format_string_for_functions(&mut f, source)?;
                f.write_str("\n")?;
            }
            FunctionBody::Object { object_file, link_symbol } => {
                writeln!(f, "{AS} {}, {}", IsoString(object_file), IsoString(link_symbol))?;
            }
            FunctionBody::SqlBody { statements } => {
                writeln!(f, "{BEGIN} {ATOMIC}")?;
                for statement in statements.deref() {
                    f.write_str("    ")?;
                    write_token_list(statement, f)?;
                    f.write_str("\n")?;
                }
                writeln!(f, "{END}")?;
            }
        }

        f.write_str(";\n")
    }

    pub fn eq_content(&self, other: &Function) -> bool {
        // arguments need to be compared because out arguments and
        // default values aren't part of the signature
        self.arguments == other.arguments &&
        self.returns == other.returns &&
        self.transform == other.transform &&
        self.window == other.window &&
        self.state == other.state &&
        self.leakproof == other.leakproof &&
        self.null_input_handling == other.null_input_handling &&
        self.parallelism == other.parallelism &&
        self.cost == other.cost &&
        self.rows == other.rows &&
        self.support == other.support &&
        self.configuration_parameters == other.configuration_parameters &&
        self.body == other.body
    }
}

impl std::fmt::Display for Function {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CREATE} ")?;
        self.write(f)
    }
}

impl From<&Function> for QFunctionRef {
    #[inline]
    fn from(value: &Function) -> Self {
        value.to_qref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionBody {
    Definition { source: Rc<str> },
    Object { object_file: Rc<str>, link_symbol: Rc<str> },
    SqlBody { statements: Rc<[Rc<[ParsedToken]>]> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Parallelism {
    Unsafe,
    Restricted,
    Safe,
}

impl std::fmt::Display for Parallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsafe     => write!(f, "{PARALLEL} {UNSAFE}"),
            Self::Restricted => write!(f, "{PARALLEL} {RESTRICTED}"),
            Self::Safe       => write!(f, "{PARALLEL} {SAFE}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Immutable,
    Stable,
    Volatile,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Immutable => f.write_str(IMMUTABLE),
            Self::Stable    => f.write_str(STABLE),
            Self::Volatile  => f.write_str(VOLATILE),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NullInputHandling {
    Called,
    ReturnsNull,
    Strict,
}

impl std::fmt::Display for NullInputHandling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Called => write!(f, "{CALLED} {ON} {NULL} {INPUT}"),
            Self::ReturnsNull => write!(f, "{RETURNS} {NULL} {ON} {NULL} {INPUT}"),
            Self::Strict => write!(f, "{STRICT}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Security {
    Invoker { external: bool },
    Definer { external: bool },
}

impl std::fmt::Display for Security {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invoker { external } => {
                if *external {
                    write!(f, "{EXTERNAL} ")?;
                }
                write!(f, "{SECURITY} {INVOKER}")
            }
            Self::Definer { external } => {
                if *external {
                    write!(f, "{EXTERNAL} ")?;
                }
                write!(f, "{SECURITY} {DEFINER}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigurationValue {
    FromCurrent,
    Value(Rc<[ParsedToken]>),
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

impl ReturnType {
    #[inline]
    pub fn is_user_defined(&self, type_name: &QName) -> bool {
        if let Self::Type(type_def) = self {
            type_def.basic_type().is_user_defined(type_name)
        } else {
            false
        }
    }
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
