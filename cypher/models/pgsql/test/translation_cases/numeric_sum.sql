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

-- case: MATCH (n) RETURN sum(n.big)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_sum(nullif(((s0.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as "sum(n.big)" from s0;

-- case: MATCH (n) RETURN sum(DISTINCT n.big)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_sum(distinct nullif(((s0.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as "sum(distinct n.big)" from s0;

-- case: MATCH (n) WITH n.big AS x RETURN sum(x)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s1.n0).properties -> E'big') as i0 from s1) select cypher_sum(to_jsonb(s0.i0)::jsonb)::jsonb as "sum(x)" from s0;

-- case: MATCH (n) WITH sum(n.big) AS total RETURN total / 2
-- pgsql_params:{"__strlit0":"null","__strlit1":"/"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_sum(nullif(((s1.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as i0 from s1) select cypher_arithmetic(nullif((s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((2)::int8)::jsonb, @__strlit1::text)::jsonb as "total / 2" from s0;

-- case: MATCH (n) WHERE n.name = 'absent' RETURN sum(n.big), avg(n.big), count(n)
-- pgsql_params:{"__strlit0":"string","__strlit1":"absent","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') = @__strlit1::text))) select cypher_sum(nullif(((s0.n0).properties -> E'big'), (@__strlit2::text)::jsonb)::jsonb)::jsonb as "sum(n.big)", avg((((s0.n0).properties ->> E'big'))::float8)::numeric as "avg(n.big)", count(s0.n0)::int8 as "count(n)" from s0;

-- case: MATCH (n) RETURN n.group, sum(n.big)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> E'group') as "n.group", cypher_sum(nullif(((s0.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as "sum(n.big)" from s0 group by ((s0.n0).properties -> E'group');

-- case: RETURN sum(3.0)
select cypher_sum(to_jsonb((3.0)::numeric + 0.0)::jsonb)::jsonb as "sum(3.0)";

-- case: WITH 1.0 AS x RETURN sum(x)
with s0 as (select 1.0 as i0) select cypher_sum(to_jsonb(s0.i0)::jsonb)::jsonb as "sum(x)" from s0;

-- case: UNWIND [9007199254740993, 2] AS x RETURN sum(x)
select cypher_sum(to_jsonb(i0)::jsonb)::jsonb as "sum(x)" from unnest(array [9007199254740993, 2]::int8[]) as i0;

-- case: MATCH ()-[r]->() RETURN sum(r.big)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select cypher_sum(nullif(((s0.e0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as "sum(r.big)" from s0;

-- case: MATCH (n:NodeKind1) WITH sum(n.big) AS total MATCH (t:NodeKind2) SET t.total = total RETURN t
-- pgsql_params:{"__strlit0":"null","__strlit1":"total"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_sum(nullif(((s1.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]), s3 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit1::text, i0)::jsonb from s2 where (s2.n1).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2), s4 as (select s2.i0 as i0, case when (s2.n1).id = (s3.n2).id then s3.n2 else s2.n1 end as n1 from s2 left outer join s3 on (s2.n1).id = (s3.n2).id) select s4.n1 as t from s4;

-- case: MATCH (n:NodeKind1) WITH sum(DISTINCT n.big) AS total MATCH ()-[r:EdgeKind1]->() SET r.total = total
-- pgsql_params:{"__strlit0":"null","__strlit1":"total"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_sum(distinct nullif(((s1.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s3 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit1::text, i0)::jsonb from s2 where (s2.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s4 as (select case when (s2.e0).id = (s3.e1).id then s3.e1 else s2.e0 end as e0, s2.i0 as i0 from s2 left outer join s3 on (s2.e0).id = (s3.e1).id) select 1;

-- case: MATCH (n:NodeKind1) WITH sum(n.big) AS total CREATE (c:NodeKind2 {total:total})
-- pgsql_params:{"__strlit0":"null","__strlit1":"node","__strlit2":"id","__strlit3":"total"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_sum(nullif(((s1.n0).properties -> E'big'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as i0 from s1), s2 as (select s0.i0 as i0, nextval(pg_get_serial_sequence(@__strlit1::text, @__strlit2::text))::int8 as n1_id from s0), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit3::text, i0)::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.i0 as i0, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select 1;
