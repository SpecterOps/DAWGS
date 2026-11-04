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

-- case: MATCH (n:`NodeKind1`) RETURN count(n)
select count(*)::int8 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[];

-- case: MATCH (n) WHERE NOT n:`NodeKind1` RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not n0.kind_ids operator (pg_catalog.@>) array [1]::int2[])) select s0.n0 as n from s0;

-- case: MATCH ()-[r:`EdgeKind1`|`EdgeKind2`]->() RETURN count(r)
select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[]);

-- case: MATCH (n:`NodeKind1`) SET n:`NodeKind2` RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set kind_ids = uniq(sort(n1.kind_ids || array [2]::int2[])::int2[])::int2[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: MATCH (n:`NodeKind1`) REMOVE n:`NodeKind2` RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set kind_ids = n1.kind_ids - array [2]::int2[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: CREATE (n:`NodeKind1`)-[:`EdgeKind1`]->(m:`NodeKind2`) RETURN n
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"edge"}
with s0 as (select nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n0_id), s1 as (insert into node (graph_id, id, kind_ids, properties) select 0, s0.n0_id, array [1]::int2[], jsonb_build_object()::jsonb from s0 returning id as n0_id, (id, kind_ids, properties)::nodecomposite as n0), s2 as (select s1.n0 as n0 from s0, s1 where s1.n0_id = s0.n0_id), s3 as (select s2.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s2), s4 as (insert into node (graph_id, id, kind_ids, properties) select 0, s3.n1_id, array [2]::int2[], jsonb_build_object()::jsonb from s3 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s5 as (select s3.n0 as n0, s4.n1 as n1 from s3, s4 where s4.n1_id = s3.n1_id), s6 as (select s5.n0 as n0, s5.n1 as n1, nextval(pg_get_serial_sequence(@__strlit2::text, @__strlit1::text))::int8 as e0_id from s5), s7 as (insert into edge (graph_id, id, start_id, end_id, kind_id, properties) select 0, s6.e0_id, (s6.n0).id, (s6.n1).id, 3, jsonb_build_object()::jsonb from s6 returning id as e0_id, (id, start_id, end_id, kind_id, properties)::edgecomposite as e0), s8 as (select s6.n0 as n0, s6.n1 as n1, s7.e0 as e0 from s6, s7 where s7.e0_id = s6.e0_id) select s8.n0 as n from s8;

-- case: MATCH (n:```Base```) RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [135]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:`has space`) WHERE n:`has``tick` RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.@>) array [134]::int2[]) and n0.kind_ids operator (pg_catalog.@>) array [133]::int2[]) select s0.n0 as n from s0;
