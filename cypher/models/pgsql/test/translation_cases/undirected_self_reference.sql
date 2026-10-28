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

-- case: MATCH (x)-[r:EdgeKind1]-(x) RETURN x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s0.n0 as x from s0;

-- case: MATCH (x)-[r:EdgeKind1 {marker:'yes'}]-(x:NodeKind1 {eligible:true}) RETURN x.name
-- pgsql_params:{"__strlit0":"string","__strlit1":"yes"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and ((n0.properties -> E'eligible'))::jsonb = to_jsonb((true)::bool)::jsonb and n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[]) and (jsonb_typeof((e0.properties -> E'marker')) = @__strlit0::text and (e0.properties ->> E'marker') = @__strlit1::text)) select ((s0.n0).properties -> E'name') as "x.name" from s0;

-- case: MATCH p = (x:NodeKind1)-[:EdgeKind1]-(x) RETURN p
with s0 as (select e0.id as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select case when (s0.n0).id is null or s0.e0 is null or (s0.n0).id is null then null else ordered_edge_ids_to_path(0, s0.n0, array [s0.e0]::int8[], array [s0.n0, s0.n0]::nodecomposite[])::pathcomposite end as p from s0;

-- case: MATCH (x)-[:EdgeKind1]-(x) RETURN count(*) AS c
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select count(*)::int8 as c from s0;

-- case: UNWIND [1, 1] AS row WITH row MATCH (x)-[r:EdgeKind1]-(x) RETURN row, x
with s0 as (select i0 as i0 from unnest(array [1, 1]::int8[]) as i0), s1 as (select s0.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s0, edge e0 join node n0 on n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s1.i0 as row, s1.n0 as x from s1;

-- case: MATCH (seed:NodeKind1) MATCH (x)-[r:EdgeKind1]-(x) RETURN seed, x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, edge e0 join node n1 on n1.id = e0.start_id and n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s1.n0 as seed, s1.n1 as x from s1;

-- case: MATCH (seed:NodeKind1) OPTIONAL MATCH (x)-[r:EdgeKind1]-(x) RETURN seed, x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as materialized (select s0.n0 as n0, row_number() over () as i0 from s0), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s1.i0 as i0, s1.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1, edge e0 join node n1 on n1.id = e0.start_id and n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s3 as (select s1.n0 as n0, s2.e0 as e0, s2.n1 as n1 from s1 left outer join s2 on s1.i0 = s2.i0) select s3.n0 as seed, s3.n1 as x from s3;

-- case: MATCH (x:NodeKind1) MATCH (x)-[r:EdgeKind1]-(x) RETURN x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0 from s0 join edge e0 on (((s0.n0).id = e0.start_id and (s0.n0).id = e0.end_id) or ((s0.n0).id = e0.start_id and (s0.n0).id = e0.end_id)) where e0.kind_id = any (array [3]::int2[])) select s1.n0 as x from s1;

-- case: MATCH (x)-[r:EdgeKind1]-(x) SET x.visited = true, r.visited = true RETURN x
-- pgsql_params:{"__strlit0":"visited"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select s0.e0 as e0, case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id), s3 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s2 where (s2.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s4 as (select case when (s2.e0).id = (s3.e1).id then s3.e1 else s2.e0 end as e0, s2.n0 as n0 from s2 left outer join s3 on (s2.e0).id = (s3.e1).id) select s4.n0 as x from s4;

-- case: MATCH (x)-[r:EdgeKind1]-(x) CREATE (:NodeKind2 {target:x.name})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"target"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id and n0.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s2 as (insert into node (graph_id, id, kind_ids, properties) select 0, s1.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, ((s1.n0).properties -> E'name'))::jsonb from s1 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s3 as (select s1.n0 as n0, s2.n1 as n1 from s1, s2 where s2.n1_id = s1.n1_id) select 1;
