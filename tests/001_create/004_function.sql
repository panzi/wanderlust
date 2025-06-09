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
