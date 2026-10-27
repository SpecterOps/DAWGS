-- Copyright 2026 Specter Ops, Inc.
--
-- Licensed under the Apache License, Version 2.0
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- SPDX-License-Identifier: Apache-2.0

-- case: MATCH (n:NodeKind1) WITH n ORDER BY n.x DESC RETURN n.x
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 order by ((s1.n0).properties -> E'x') desc) select ((s0.n0).properties -> E'x') as "n.x" from s0;

-- case: MATCH (n:NodeKind1) WITH n SKIP 1 RETURN n
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 offset 1) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WITH n LIMIT 2 RETURN n
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 limit 2) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WITH n ORDER BY n.x DESC SKIP 1 LIMIT 2 RETURN n.x
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 order by ((s1.n0).properties -> E'x') desc offset 1 limit 2) select ((s0.n0).properties -> E'x') as "n.x" from s0;

-- case: MATCH (n:NodeKind1) WITH n AS item ORDER BY item.x DESC LIMIT 2 RETURN item
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n1 from s1 order by ((s1.n0).properties -> E'x') desc limit 2) select s0.n1 as item from s0;

-- case: MATCH (n:NodeKind1) WITH n.x AS value ORDER BY value + 1 DESC LIMIT 2 RETURN value
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'x') as i0 from s1 order by cypher_arithmetic(nullif((((s1.n0).properties -> E'x'))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((1)::int8)::jsonb, @__strlit1::text)::jsonb desc limit 2) select s0.i0 as value from s0;

-- case: MATCH (n:NodeKind1) WITH DISTINCT n.x AS value ORDER BY value DESC LIMIT 2 RETURN value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select distinct ((s1.n0).properties -> E'x') as i0 from s1 order by ((s1.n0).properties -> E'x') desc limit 2) select s0.i0 as value from s0;

-- case: MATCH (n:NodeKind1) WITH n.x AS value, count(n) AS total ORDER BY total DESC, value LIMIT 2 RETURN value, total
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'x') as i0, count(s1.n0)::int8 as i1 from s1 group by ((s1.n0).properties -> E'x') order by count(s1.n0)::int8 desc, ((s1.n0).properties -> E'x') limit 2) select s0.i0 as value, s0.i1 as total from s0;

-- case: MATCH (n:NodeKind1) WHERE n.x > 0 WITH n ORDER BY n.x DESC LIMIT 1 WHERE n.x < 3 RETURN n.x
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_compare(nullif((n0.properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb)::int4 > 0) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select s1.n0 as n0, (cypher_compare(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, to_jsonb((3)::int8)::jsonb)::int4 < 0) as s0_where from s1 order by ((s1.n0).properties -> E'x') desc limit 1) select s0.n0 from s0 where s0.s0_where) select ((s0.n0).properties -> E'x') as "n.x" from s0;

-- case: MATCH (n:NodeKind1) WITH n.x AS value ORDER BY value DESC LIMIT 1 WHERE value < 3 RETURN value
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select ((s1.n0).properties -> E'x') as i0, (cypher_compare(nullif((((s1.n0).properties -> E'x'))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((3)::int8)::jsonb)::int4 < 0) as s0_where from s1 order by ((s1.n0).properties -> E'x') desc limit 1) select s0.i0 from s0 where s0.s0_where) select s0.i0 as value from s0;

-- case: UNWIND [3, 1, 2] AS x WITH x ORDER BY x SKIP 1 LIMIT 1 RETURN x
with s0 as (select i0 as i0 from unnest(array [3, 1, 2]::int8[]) as i0 order by i0 offset 1 limit 1) select s0.i0 as x from s0;

-- case: UNWIND [5, 1, 4, 2, 3] AS x WITH x ORDER BY x DESC LIMIT 4 WITH x ORDER BY x SKIP 1 LIMIT 2 RETURN x
with s0 as (select i0 as i0 from unnest(array [5, 1, 4, 2, 3]::int8[]) as i0 order by i0 desc limit 4), s1 as (select s0.i0 as i0 from s0 order by s0.i0 offset 1 limit 2) select s1.i0 as x from s1;

-- case: MATCH (n:NodeKind1) WITH 1 AS value LIMIT 2 RETURN count(value)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select 1 as i0 from s1 limit 2) select count(s0.i0)::int8 as "count(value)" from s0;

-- case: MATCH (n:NodeKind1) WITH n ORDER BY n.x DESC LIMIT 2 SET n.hit = true RETURN n
-- pgsql_params:{"__strlit0":"hit"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 order by ((s1.n0).properties -> E'x') desc limit 2), s2 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s3 as (select case when (s0.n0).id = (s2.n1).id then s2.n1 else s0.n0 end as n0 from s0 left outer join s2 on (s0.n0).id = (s2.n1).id) select s3.n0 as n from s3;

-- case: MATCH ()-[r:EdgeKind1]->() WITH r ORDER BY r.x DESC SKIP 1 LIMIT 2 SET r.hit = true
-- pgsql_params:{"__strlit0":"hit"}
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s1.e0 as e0 from s1 order by ((s1.e0).properties -> E'x') desc offset 1 limit 2), s2 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s3 as (select case when (s0.e0).id = (s2.e1).id then s2.e1 else s0.e0 end as e0 from s0 left outer join s2 on (s0.e0).id = (s2.e1).id) select 1;

-- case: MATCH (n:NodeKind1) WITH n, n.x AS value ORDER BY value DESC LIMIT 1 WHERE value < 3 SET n.hit = true
-- pgsql_params:{"__strlit0":"null","__strlit1":"hit"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select s1.n0 as n0, ((s1.n0).properties -> E'x') as i0, (cypher_compare(nullif((((s1.n0).properties -> E'x'))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((3)::int8)::jsonb)::int4 < 0) as s0_where from s1 order by ((s1.n0).properties -> E'x') desc limit 1) select s0.n0, s0.i0 from s0 where s0.s0_where), s2 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit1::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s3 as (select s0.i0 as i0, case when (s0.n0).id = (s2.n1).id then s2.n1 else s0.n0 end as n0 from s0 left outer join s2 on (s0.n0).id = (s2.n1).id) select 1;

-- case: MATCH (n:NodeKind1) WITH n ORDER BY n.x DESC SKIP 1 LIMIT 2 CREATE (c:NodeKind2 {value: n.x})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 order by ((s1.n0).properties -> E'x') desc offset 1 limit 2), s2 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, ((s2.n0).properties ->> E'x'))::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.n0 as n0, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select 1;
