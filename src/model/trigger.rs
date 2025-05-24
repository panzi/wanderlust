use std::rc::Rc;

use super::{name::{Name, QName}, token::ParsedToken};

#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    constraint: bool,
    name: Name, // NOTE: Triggers are scoped to their table
    when: When,
    events: Rc<[Event]>,
    table_name: QName,
    referencing: Option<Rc<[ReferencedTable]>>,
    for_each_row: bool,
    ref_table: Option<QName>,
    deferrable: bool,
    initially_deferred: bool,
    predicate: Option<Rc<[ParsedToken]>>,
    function_name: QName,
    arguments: Rc<[Rc<str>]>,
}

impl Trigger {
    #[inline]
    pub fn new(
        constraint: bool,
        name: Name,
        when: When,
        events: Rc<[Event]>,
        table_name: QName,
        referencing: Option<Rc<[ReferencedTable]>>,
        for_each_row: bool,
        ref_table: Option<QName>,
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
            referencing,
            for_each_row,
            ref_table,
            deferrable,
            initially_deferred,
            predicate,
            function_name,
            arguments
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
    pub fn referencing(&self) -> Option<&Rc<[ReferencedTable]>> {
        self.referencing.as_ref()
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Insert,
    Update { columns: Option<Rc<[Name]>> },
    Delete,
    Truncate,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifeCycle {
    Old,
    New,
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
}
