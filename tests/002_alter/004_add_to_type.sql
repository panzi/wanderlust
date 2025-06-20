-- before
create type "AccountStatus" as enum ('active', 'verification_pending', 'deleted');

create type "RecordDemo" as (
    "timestamp" timestamp with time zone
);

create table "User" (
    id serial primary key,
    "createdAt" timestamp with time zone not null check (("createdAt" >= '2000-01-01 00:00:00+00'::timestamp with time zone)),
    "updatedAt" timestamp with time zone not null,
    username text not null,
    "passwordHash" text null,
    email text null unique,
    bio text not null default '',
    "accountStatus" "AccountStatus" not null default 'verification_pending'::public."AccountStatus",
    "recordDemo" "RecordDemo",
    check ("updatedAt" >= "createdAt")
);
-- after
create type "AccountStatus" as enum ('active', 'verification_pending', 'deleted', 'banned');

create type "RecordDemo" as (
    "timestamp" timestamp with time zone,
    "text" text
);

create table "User" (
    id serial primary key,
    "createdAt" timestamp with time zone not null check (("createdAt" >= '2000-01-01 00:00:00+00'::timestamp with time zone)),
    "updatedAt" timestamp with time zone not null,
    username text not null,
    "passwordHash" text null,
    email text null unique,
    bio text not null default '',
    "accountStatus" "AccountStatus" not null default 'verification_pending'::public."AccountStatus",
    "recordDemo" "RecordDemo",
    check ("updatedAt" >= "createdAt")
);
