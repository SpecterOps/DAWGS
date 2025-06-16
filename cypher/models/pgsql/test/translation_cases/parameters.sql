-- case: match (n) where n.objectid ends with $p0 and not (n:NodeKind1 or n:NodeKind2) return n
-- pgsql_params:{"pi0":null}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> 'objectid') like '%' || @pi0 and not (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] or n0.kind_ids operator (pg_catalog.&&) array [2]::int2[]))) select s0.n0 as n from s0;

-- case: match (n) where n.objectid starts with $p0 and not (n:NodeKind1 or n:NodeKind2) return n
-- pgsql_params:{"pi0":null}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> 'objectid') like @pi0 || '%' and not (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] or n0.kind_ids operator (pg_catalog.&&) array [2]::int2[]))) select s0.n0 as n from s0;

-- case: match (n) where n.objectid contains $p0 and not (n:NodeKind1 or n:NodeKind2) return n
-- pgsql_params:{"pi0":null}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> 'objectid') like '%' || @pi0 || '%' and not (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] or n0.kind_ids operator (pg_catalog.&&) array [2]::int2[]))) select s0.n0 as n from s0;

