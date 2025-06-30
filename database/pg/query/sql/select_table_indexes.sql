-- List all indexes for a given table name.
select indexdef
from pg_indexes
where schemaname = 'public'
  and tablename = @tablename;
