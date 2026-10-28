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

-- case: MATCH (n:NodeKind1) CREATE (c:NodeKind2 {score:n.score, flag:n.flag, text:n.text}) RETURN c
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"flag","__strlit3":"score","__strlit4":"text"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s2 as (insert into node (graph_id, id, kind_ids, properties) select 0, s1.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, ((s1.n0).properties -> E'flag'), @__strlit3::text, ((s1.n0).properties -> E'score'), @__strlit4::text, ((s1.n0).properties -> E'text'))::jsonb from s1 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s3 as (select s1.n0 as n0, s2.n1 as n1 from s1, s2 where s2.n1_id = s1.n1_id) select s3.n1 as c from s3;

-- case: MATCH (n:NodeKind1) WITH n ORDER BY n.score DESC LIMIT 2 CREATE (c:NodeKind2 {score:(n.score)})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"score"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1 order by ((s1.n0).properties -> E'score') desc limit 2), s2 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, ((s2.n0).properties -> E'score'))::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.n0 as n0, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select 1;

-- case: MATCH (n:NodeKind1) WITH n.score AS score CREATE (c:NodeKind2 {score:score}) RETURN c.score
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"score"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'score') as i0 from s1), s2 as (select s0.i0 as i0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, i0)::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.i0 as i0, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select ((s4.n1).properties -> E'score') as "c.score" from s4;

-- case: MATCH (n:NodeKind1)-[r:EdgeKind1]->(m:NodeKind2) CREATE (n)-[c:EdgeKind2 {score:r.score}]->(m) RETURN c.score
-- pgsql_params:{"__strlit0":"edge","__strlit1":"id","__strlit2":"score"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, s0.n0 as n0, s0.n1 as n1, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as e1_id from s0), s2 as (insert into edge (graph_id, id, start_id, end_id, kind_id, properties) select 0, s1.e1_id, (s1.n0).id, (s1.n1).id, 4, jsonb_build_object(@__strlit2::text, ((s1.e0).properties -> E'score'))::jsonb from s1 returning id as e1_id, (id, start_id, end_id, kind_id, properties)::edgecomposite as e1), s3 as (select s1.e0 as e0, s1.n0 as n0, s1.n1 as n1, s2.e1 as e1 from s1, s2 where s2.e1_id = s1.e1_id) select ((s3.e1).properties -> E'score') as "c.score" from s3;
