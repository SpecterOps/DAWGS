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

-- case: OPTIONAL MATCH (n:NodeKind1) RETURN n
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0) select s3.n0 as n from s3;

-- case: OPTIONAL MATCH (n:NodeKind1) WHERE n.name='missing' RETURN n,n.name
-- pgsql_params:{"__strlit0":"string","__strlit1":"missing"}
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where ((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') = @__strlit1::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0) select s3.n0 as n, ((s3.n0).properties -> E'name') as "n.name" from s3;

-- case: OPTIONAL MATCH (a:NodeKind1)-[r:EdgeKind1]->(b:NodeKind2) RETURN a,r,b
with s1 as materialized (select row_number() over () as i0), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1, edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s3 as (select s2.e0 as e0, s2.n0 as n0, s2.n1 as n1 from s1 left outer join s2 on s1.i0 = s2.i0) select s3.n0 as a, s3.e0 as r, s3.n1 as b from s3;

-- case: OPTIONAL MATCH (a:NodeKind1) OPTIONAL MATCH (a)-[r:EdgeKind1]->(b:NodeKind2) RETURN a,r,b
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as materialized (select s3.n0 as n0, row_number() over () as i1 from s3), s5 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s4.i1 as i1, s4.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s4 join edge e0 on (s4.n0).id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s6 as (select s4.n0 as n0, s5.e0 as e0, s5.n1 as n1 from s4 left outer join s5 on s4.i1 = s5.i1) select s6.n0 as a, s6.e0 as r, s6.n1 as b from s6;

-- case: OPTIONAL MATCH (n:NodeKind1) RETURN *
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0) select s3.n0 as n from s3;

-- case: OPTIONAL MATCH (n:NodeKind1) RETURN count(*) AS rows, count(n) AS nodes
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0) select count(*)::int8 as rows, count(s3.n0)::int8 as nodes from s3;

-- case: OPTIONAL MATCH (n:NodeKind1) WHERE n.name='missing' SET n.selected=true RETURN n
-- pgsql_params:{"__strlit0":"string","__strlit1":"missing","__strlit2":"selected"}
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where ((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') = @__strlit1::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s3 where (s3.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s5 as (select case when (s3.n0).id = (s4.n1).id then s4.n1 else s3.n0 end as n0 from s3 left outer join s4 on (s3.n0).id = (s4.n1).id) select s5.n0 as n from s5;

-- case: OPTIONAL MATCH (n:NodeKind1) CREATE (m:NodeKind2 {name:'made'}) RETURN m.name
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"name","__strlit3":"made"}
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as (select s3.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s3), s5 as (insert into node (graph_id, id, kind_ids, properties) select 0, s4.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, @__strlit3::text)::jsonb from s4 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s6 as (select s4.n0 as n0, s5.n1 as n1 from s4, s5 where s5.n1_id = s4.n1_id) select ((s6.n1).properties -> E'name') as "m.name" from s6;

-- case: OPTIONAL MATCH (n:NodeKind1) CREATE (m:NodeKind2 {name:'made'})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"name","__strlit3":"made"}
with s1 as materialized (select row_number() over () as i0), s2 as (select s1.i0 as i0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from s1, node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s3 as (select s2.n0 as n0 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as (select s3.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s3), s5 as (insert into node (graph_id, id, kind_ids, properties) select 0, s4.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, @__strlit3::text)::jsonb from s4 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s6 as (select s4.n0 as n0, s5.n1 as n1 from s4, s5 where s5.n1_id = s4.n1_id) select 1;
