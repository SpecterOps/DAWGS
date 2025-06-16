-- Creates a new kind definition if it does not exist and returns the resulting ID. If the
-- kind already exists then the kind's assigned ID is returned.
with
  existing as (
    select id from kind where kind.name = @name
  ),
  inserted as (
    insert into kind (name) values (@name) on conflict (name) do nothing returning id
  )
select * from existing
union
select * from inserted;
