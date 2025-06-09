-- before
create type an_enum as enum ('foo', 'bar', 'baz');

create table a_table (
    col1 text,
    col2 character varying (32),
    col3 integer,
    col4 an_enum
);
-- after
create type an_enum as enum ('foo', 'bar', 'baz');

create table a_table (
    col1 an_enum,
    col2 text,
    col3 double precision,
    col4 text collate pg_catalog."de-AT-x-icu"
);
