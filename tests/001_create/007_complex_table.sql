-- before
-- after
create table super_table1 (
    foo text
);

create table super_table2 (
    bar text
);

create table sub_table (
    id serial primary key
) inherits (super_table1, super_table2);

create table other_table (
    id serial primary key
);

create table rel_table (
    sub_table_id integer references sub_table (id),
    other_table_id integer references other_table (id),
    primary key (sub_table_id, other_table_id)
);

create table rel_table_child (
    sub_table_id integer,
    other_table_id integer,
    position integer,
    foreign key (sub_table_id, other_table_id) references rel_table (sub_table_id, other_table_id),
    primary key (sub_table_id, other_table_id, position)
);
