-- Selects the ID of a given Kind by name
select id
from kind
where name = @name;
