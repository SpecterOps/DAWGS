-- case: match (s:NodeKind1) return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]) select s0.n0 as s from s0;

