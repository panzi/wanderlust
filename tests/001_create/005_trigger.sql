-- before
-- after
create table "User" (
    id serial primary key,
    "createdAt" timestamp with time zone not null check ("createdAt" >= '2000-01-01 00:00:00+00'::timestamp with time zone),
    "updatedAt" timestamp with time zone not null,
    username text not null,
    "passwordHash" text null,
    email text null unique,
    bio text not null default '',
    check ("updatedAt" >= "createdAt")
);

create table logs (
    id bigserial primary key,
    "createdAt" timestamp with time zone not null,
    "tableName" text not null,
    "operation" text not null,
    "old" jsonb default null,
    "new" jsonb default null
);

create function record_log () returns trigger
security definer
language plpgsql
as $body$
begin
    case TG_OP
    when 'INSERT'
        insert into logs ("createdAt", "tableName", "oepration", "old", "new")
        values (now(), TG_TABLE_NAME::text, TG_OP::text, null, row_to_json(new));

        return new;

    when 'UPDATE'
        insert into logs ("createdAt", "tableName", "oepration", "old", "new")
        values (now(), TG_TABLE_NAME::text, TG_OP::text, row_to_json(old), row_to_json(new));

        return new;

    when 'DELETE'
        insert into logs ("createdAt", "tableName", "oepration", "old", "new")
        values (now(), TG_TABLE_NAME::text, TG_OP::text, row_to_json(old), null);

        return null;

    else
        return null;
    end
end;
$body$;

create trigger record_log_users
after insert or update or delete
on "User"
for each row
execute function record_log ();
