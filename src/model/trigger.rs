use std::{ops::Deref, rc::Rc};

use super::{name::{Name, QName}, token::ParsedToken};

use crate::{format::{format_iso_string, join_into, write_token_list}, model::words::*};

#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    constraint: bool,
    name: Name, // NOTE: Triggers are scoped to their table
    when: When,
    events: Rc<[Event]>,
    table_name: QName,
    ref_table: Option<QName>,
    referencing: Rc<[ReferencedTable]>,
    for_each_row: bool,
    deferrable: bool,
    initially_deferred: bool,
    predicate: Option<Rc<[ParsedToken]>>,
    function_name: QName,
    arguments: Rc<[Rc<str>]>,
    comment: Option<Rc<str>>,
}

impl Trigger {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn new(
        constraint: bool,
        name: Name,
        when: When,
        events: Rc<[Event]>,
        table_name: QName,
        ref_table: Option<QName>,
        referencing: Rc<[ReferencedTable]>,
        for_each_row: bool,
        deferrable: bool,
        initially_deferred: bool,
        predicate: Option<Rc<[ParsedToken]>>,
        function_name: QName,
        arguments: Rc<[Rc<str>]>,
    ) -> Self {
        Self {
            constraint,
            name,
            when,
            events,
            table_name,
            ref_table,
            referencing,
            for_each_row,
            deferrable,
            initially_deferred,
            predicate,
            function_name,
            arguments,
            comment: None,
        }
    }

    #[inline]
    pub fn constraint(&self) -> bool {
        self.constraint
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn when(&self) -> When {
        self.when
    }

    #[inline]
    pub fn events(&self) -> &Rc<[Event]> {
        &self.events
    }

    #[inline]
    pub fn table_name(&self) -> &QName {
        &self.table_name
    }

    #[inline]
    pub fn referencing(&self) -> &Rc<[ReferencedTable]> {
        &self.referencing
    }

    #[inline]
    pub fn for_each_row(&self) -> bool {
        self.for_each_row
    }

    #[inline]
    pub fn ref_table(&self) -> Option<&QName> {
        self.ref_table.as_ref()
    }

    #[inline]
    pub fn deferrable(&self) -> bool {
        self.deferrable
    }

    #[inline]
    pub fn initially_deferred(&self) -> bool {
        self.initially_deferred
    }

    #[inline]
    pub fn predicate(&self) -> Option<&Rc<[ParsedToken]>> {
        self.predicate.as_ref()
    }

    #[inline]
    pub fn function_name(&self) -> &QName {
        &self.function_name
    }

    #[inline]
    pub fn arguments(&self) -> &Rc<[Rc<str>]> {
        &self.arguments
    }

    #[inline]
    pub fn comment(&self) -> Option<&Rc<str>> {
        self.comment.as_ref()
    }

    #[inline]
    pub fn set_comment(&mut self, comment: Option<Rc<str>>) {
        self.comment = comment.into();
    }

    fn write(&self, or_replace: bool, mut f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CREATE)?;

        if or_replace {
            write!(f, " {OR} {REPLACE}")?;
        }

        if self.constraint {
            write!(f, " {CONSTRAINT}")?;
        }

        write!(f, " {TRIGGER} {} {} ", self.name, self.when)?;

        join_into(" OR ", self.events.deref(), f)?;
        
        write!(f, "\n{ON} {}", self.table_name)?;

        if let Some(ref_table) = &self.ref_table {
            write!(f, "\n{FROM} {}", ref_table)?;
        }

        if self.deferrable {
            write!(f, " {DEFERRABLE}")?;

            if self.initially_deferred {
                write!(f, " {INITIALLY} {DEFERRED}")?;
            } else {
                write!(f, " {INITIALLY} {IMMEDIATE}")?;
            }
        } else {
            write!(f, " {NOT} {DEFERRABLE}")?;
        }

        for item in self.referencing.deref() {
            write!(f, "\n{REFERENCING} {item}")?;
        }

        if self.for_each_row {
            write!(f, "\n{FOR} {EACH} {ROW}")?;
        } else {
            write!(f, "\n{FOR} {EACH} {STATEMENT}")?;
        }

        if let Some(predicate) = &self.predicate {
            write!(f, "\n{WHEN} (")?;
            write_token_list(predicate.deref(), f)?;
            f.write_str(")")?;
        }

        write!(f, "\n{EXECUTE} {FUNCTION} {} (", self.function_name)?;

        let mut iter = self.arguments.iter();
        if let Some(first) = iter.next() {
            format_iso_string(&mut f, first)?;
            for arg in iter {
                f.write_str(", ")?;
                format_iso_string(&mut f, arg)?;
            }
        }
        f.write_str(");\n")?;

        Ok(())
    }

    pub fn eq_no_comment(&self, other: &Trigger) -> bool {
        self.constraint == other.constraint &&
        self.when == other.when &&
        self.events == other.events &&
        self.ref_table == other.ref_table &&
        self.referencing == other.referencing &&
        self.for_each_row == other.for_each_row &&
        self.deferrable == other.deferrable &&
        self.initially_deferred == other.initially_deferred &&
        self.predicate == other.predicate &&
        self.function_name == other.function_name &&
        self.arguments == other.arguments
    }
}

impl std::fmt::Display for Trigger {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write(false, f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Before,
    After,
    InsteadOf,
}

impl std::fmt::Display for When {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Before    => f.write_str(BEFORE),
            Self::After     => f.write_str(AFTER),
            Self::InsteadOf => write!(f, "{INSTEAD} {OF}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Insert,
    Update { columns: Option<Rc<[Name]>> },
    Delete,
    Truncate,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert => f.write_str(INSERT),
            Self::Update { columns } => {
                f.write_str(UPDATE)?;
                if let Some(columns) = columns {
                    write!(f, " {ON} ")?;
                    join_into(", ", columns, f)?;
                }
                Ok(())
            }
            Self::Delete => f.write_str(DELETE),
            Self::Truncate => f.write_str(TRUNCATE),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReferencedTable {
    life_cycle: LifeCycle,
    transition_relation_name: Name,
}

impl ReferencedTable {
    #[inline]
    pub fn new(life_cycle: LifeCycle, transition_relation_name: Name) -> Self {
        Self { life_cycle, transition_relation_name }
    }

    #[inline]
    pub fn life_cycle(&self) -> LifeCycle {
        self.life_cycle
    }

    #[inline]
    pub fn transition_relation_name(&self) -> &Name {
        &self.transition_relation_name
    }
}

impl std::fmt::Display for ReferencedTable {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {TABLE} {AS} {}", self.life_cycle, self.transition_relation_name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifeCycle {
    Old,
    New,
}

impl std::fmt::Display for LifeCycle {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old => f.write_str(OLD),
            Self::New => f.write_str(NEW),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger {
    or_replace: bool,
    trigger: Rc<Trigger>,
}

impl CreateTrigger {
    #[inline]
    pub fn new(or_replace: bool, trigger: impl Into<Rc<Trigger>>) -> Self {
        Self { or_replace, trigger: trigger.into() }
    }

    #[inline]
    pub fn or_replace(&self) -> bool {
        self.or_replace
    }

    #[inline]
    pub fn trigger(&self) -> &Rc<Trigger> {
        &self.trigger
    }

    #[inline]
    pub fn into_trigger(self) -> Rc<Trigger> {
        self.trigger
    }
}

impl std::fmt::Display for CreateTrigger {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.trigger.write(self.or_replace, f)
    }
}

impl From<CreateTrigger> for Rc<Trigger> {
    #[inline]
    fn from(value: CreateTrigger) -> Self {
        value.into_trigger()
    }
}
