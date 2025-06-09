-- before
-- after
create table users (
    id serial primary key,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    username text not null,
    password_hash text,
    email text
);
