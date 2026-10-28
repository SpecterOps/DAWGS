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

-- case: match (b:NodeKind1) match (c {name: 'c1'}) where (b)-[:EdgeKind1]->(c) return b, c
-- pgsql_params:{"__strlit0":"string","__strlit1":"c1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (jsonb_typeof((n1.properties -> E'name')) = @__strlit0::text and (n1.properties ->> E'name') = @__strlit1::text)) select s1.n0 as b, s1.n1 as c from s1 where ((with s2 as (select s1.n0 as n0, s1.n1 as n1 from edge e0 where e0.kind_id = any (array [3]::int2[]) and (s1.n1).id = e0.end_id and (s1.n0).id = e0.start_id) select count(*) > 0 from s2));

-- case: match (b:NodeKind1) match (c {name: 'c1'}) where not (b)-[:EdgeKind1]->(c) return b, c
-- pgsql_params:{"__strlit0":"string","__strlit1":"c1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (jsonb_typeof((n1.properties -> E'name')) = @__strlit0::text and (n1.properties ->> E'name') = @__strlit1::text)) select s1.n0 as b, s1.n1 as c from s1 where (not (with s2 as (select s1.n0 as n0, s1.n1 as n1 from edge e0 where e0.kind_id = any (array [3]::int2[]) and (s1.n1).id = e0.end_id and (s1.n0).id = e0.start_id) select count(*) > 0 from s2));

-- case: match (b:NodeKind1) match (c {name: 'c1'}) where (b)-[:EdgeKind1]->(c) set b.seen = true return b, c
-- pgsql_params:{"__strlit0":"string","__strlit1":"c1","__strlit2":"seen"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (jsonb_typeof((n1.properties -> E'name')) = @__strlit0::text and (n1.properties ->> E'name') = @__strlit1::text)), s3 as (select s1.n0 as n0, s1.n1 as n1 from s1 where ((with s2 as (select s1.n0 as n0, s1.n1 as n1 from edge e0 where e0.kind_id = any (array [3]::int2[]) and (s1.n1).id = e0.end_id and (s1.n0).id = e0.start_id) select count(*) > 0 from s2))), s4 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s3 where (s3.n0).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2), s5 as (select case when (s3.n0).id = (s4.n2).id then s4.n2 else s3.n0 end as n0, case when (s3.n1).id = (s4.n2).id then s4.n2 else s3.n1 end as n1 from s3 left outer join s4 on (s3.n0).id = (s4.n2).id) select s5.n0 as b, s5.n1 as c from s5;

-- case: match (b:NodeKind1) match (c {name: 'c1'}) where (b)-[:EdgeKind1]->(c) create (:NodeKind2 {name: b.name}) return b, c
-- pgsql_params:{"__strlit0":"string","__strlit1":"c1","__strlit2":"node","__strlit3":"id","__strlit4":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (jsonb_typeof((n1.properties -> E'name')) = @__strlit0::text and (n1.properties ->> E'name') = @__strlit1::text)), s3 as (select s1.n0 as n0, s1.n1 as n1 from s1 where ((with s2 as (select s1.n0 as n0, s1.n1 as n1 from edge e0 where e0.kind_id = any (array [3]::int2[]) and (s1.n1).id = e0.end_id and (s1.n0).id = e0.start_id) select count(*) > 0 from s2))), s4 as (select s3.n0 as n0, s3.n1 as n1, nextval(pg_get_serial_sequence(@__strlit2::text, @__strlit3::text))::int8 as n2_id from s3), s5 as (insert into node (graph_id, id, kind_ids, properties) select 0, s4.n2_id, array [2]::int2[], jsonb_build_object(@__strlit4::text, ((s4.n0).properties -> E'name'))::jsonb from s4 returning id as n2_id, (id, kind_ids, properties)::nodecomposite as n2), s6 as (select s4.n0 as n0, s4.n1 as n1, s5.n2 as n2 from s4, s5 where s5.n2_id = s4.n2_id) select s6.n0 as b, s6.n1 as c from s6;
