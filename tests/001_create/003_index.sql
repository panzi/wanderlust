-- before
-- after
create table "User" (
    id serial primary key,
    "createdAt" timestamp with time zone not null check ("createdAt" >= "2000-01-01T00:00:00+0000"::timestamp with time zone),
    "updatedAt" timestamp with time zone not null,
    username text not null,
    "passwordHash" text null,
    email text null unique,
    bio text not null default '',
    check ("updatedAt" >= "createdAt")
);

create unique index on "User" (username) where (email is not null);
create index on "User" ("createdAt", "updatedAt");
