-- before
-- after
create table "User" (
    id serial primary key,
    "createdAt" timestamp with time zone not null check ("createdAt" >= "2000-01-01T00:00:00+0000"::timestamp with time zone),
    "updatedAt" timestamp with time zone not null,
    username text not null,
    "passwordHash" text null,
    email text null,
    bio text not null default '',
    check ("updatedAt" >= "createdAt")
);

create table "Event" (
    id BIGSERIAL PRIMARY KEY,
    "createdAt" timestamp with time zone not null,
    "userId" integer references "User" ("id"),
    "eventType" text not null
);
