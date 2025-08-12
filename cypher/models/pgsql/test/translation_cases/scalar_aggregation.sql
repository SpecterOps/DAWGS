-- Test cases for scalar aggregation functions with GROUP BY support
-- Tests verify the new aggregate functions (sum, avg, min, max) work correctly
-- and that GROUP BY clauses are properly generated for mixed scalar/aggregate queries

-- Simple sum aggregate
-- case: MATCH (n) RETURN sum(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum(((s0.n0).properties ->> 'age'))::numeric from s0;

-- Simple average aggregate  
-- case: MATCH (n) RETURN avg(n.salary)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select avg(((s0.n0).properties ->> 'salary'))::numeric from s0;

-- Simple min aggregate
-- case: MATCH (n) RETURN min(n.created_date)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select min(((s0.n0).properties ->> 'created_date')) from s0;

-- Simple max aggregate
-- case: MATCH (n) RETURN max(n.updated_date)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select max(((s0.n0).properties ->> 'updated_date')) from s0;

-- Sum with GROUP BY (single property)
-- case: MATCH (n) RETURN n.department, sum(n.salary)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), sum(((s0.n0).properties ->> 'salary'))::numeric from s0 group by ((s0.n0).properties -> 'department');

-- Average with GROUP BY (single property)
-- case: MATCH (n) RETURN n.department, avg(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), avg(((s0.n0).properties ->> 'age'))::numeric from s0 group by ((s0.n0).properties -> 'department');

-- Multiple aggregates (no GROUP BY needed)
-- case: MATCH (n) RETURN count(n), sum(n.age), avg(n.age), min(n.age), max(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8, sum(((s0.n0).properties ->> 'age'))::numeric, avg(((s0.n0).properties ->> 'age'))::numeric, min(((s0.n0).properties ->> 'age')), max(((s0.n0).properties ->> 'age')) from s0;

-- Pure scalar queries (no MATCH clause needed)
select 42;

-- case: RETURN 'hello world'
select 'hello world';

-- case: RETURN 2 + 3
select 2 + 3;