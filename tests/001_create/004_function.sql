-- before
-- after
create function add (in a integer, b integer)
returns integer
language plpgsql
as $$
begin
    return a + b;
end;
$$;

create function retset (in a integer = 123)
returns table (foo integer, bar text)
stable
leakproof
returns null on null input
cost 50.5
rows 2.5
language plpgsql
as $$
begin
    return;
end;
$$;
