-- before
create table users (
    id serial primary key,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    username text null,
    password_hash text,
    email text,
    bio text
);
-- after
create table users (
    id serial primary key,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    username text not null unique check (char_length(username) > 2),
    password_hash text,
    email text check (email is null or email like '%@%') deferrable initially deferred,
    bio text not null default ''::text,
    check (updated_at >= created_at)
);
