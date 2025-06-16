-- Selects the ID of a graph with the given name.
select id
from graph
where name = @name;
