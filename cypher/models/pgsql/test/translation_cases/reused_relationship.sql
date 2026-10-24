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

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (a)-[r]->(c) RETURN count(*) AS c
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0 from s0 join edge e1 on (s0.n0).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id = (s0.e0).id) select count(*)::int8 as c from s1;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (a)-[r]->(b) RETURN r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1 from s0 join edge e1 on (s0.n0).id = e1.start_id and (s0.n1).id = e1.end_id where e1.id = (s0.e0).id) select s1.e0 as r from s1;

-- case: MATCH (a)-[r:EdgeKind1]->(b) WITH r MATCH (c)-[r]->(d) RETURN c, d
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s1.e0 as e0 from s1), s2 as (select s0.e0 as e0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2, (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n3 from s0, edge e1 join node n2 on n2.id = e1.start_id join node n3 on n3.id = e1.end_id where e1.id = (s0.e0).id) select s2.n2 as c, s2.n3 as d from s2;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (c)<-[r]-(a) RETURN c
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n0).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id = (s0.e0).id) select s1.n2 as c from s1;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (a)-[r]-(c) RETURN c
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on ((s0.n0).id = e1.end_id or (s0.n0).id = e1.start_id) join node n2 on (n2.id = e1.end_id or n2.id = e1.start_id) where (((s0.n0).id = e1.start_id and n2.id = e1.end_id) or (n2.id = e1.start_id and (s0.n0).id = e1.end_id)) and e1.id = (s0.e0).id) select s1.n2 as c from s1;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH p = (a)-[r]->(c) RETURN p
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n0).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id = (s0.e0).id) select case when (s1.n0).id is null or (s1.e1).id is null or (s1.n2).id is null then null else (array [s1.n0, s1.n2]::nodecomposite[], array [s1.e1]::edgecomposite[])::pathcomposite end as p from s1;

-- case: MATCH (a)-[r:EdgeKind1]->(b) WITH a, r AS carried MATCH (a)-[carried]->(c) RETURN c
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select s1.n0 as n0, s1.e0 as e1 from s1), s2 as (select s0.e1 as e1, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e2 on (s0.n0).id = e2.start_id join node n2 on n2.id = e2.end_id where e2.id = (s0.e1).id) select s2.n2 as c from s2;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (c)<-[:EdgeKind2]-(d)<-[r]-(e) RETURN e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, e1.id as e1, n3.id as n3 from s0, edge e1 join node n2 on n2.id = e1.end_id join node n3 on n3.id = e1.start_id where e1.kind_id = any (array [4]::int2[])), s2 as (select s1.e0 as e0, s1.e1 as e1, (e2.id, e2.start_id, e2.end_id, e2.kind_id, e2.properties)::edgecomposite as e2, s1.n3 as n3, (n4.id, n4.kind_ids, n4.properties)::nodecomposite as n4 from s1 join edge e2 on s1.n3 = e2.end_id join node n4 on n4.id = e2.start_id where e2.id = (s1.e0).id and e2.id != s1.e1) select s2.n4 as e from s2;

-- case: MATCH (a)-[r:EdgeKind1]->(b) WHERE (a)-[r]->(b) RETURN count(*) AS c
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select count(*)::int8 as c from s0 where ((with s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1 from edge e1 where e1.id = (s0.e0).id and (s0.n1).id = e1.end_id and (s0.n0).id = e1.start_id) select count(*) > 0 from s1));

-- case: MATCH (a)-[r:EdgeKind1]->(b) OPTIONAL MATCH (a)-[r]->(c) SET c.visited = true RETURN c
-- pgsql_params:{"__strlit0":"visited"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as materialized (select s0.e0 as e0, s0.n0 as n0, row_number() over () as i0 from s0), s2 as (select s1.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s1.i0 as i0, s1.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s1 join edge e1 on (s1.n0).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id = (s1.e0).id), s3 as (select s1.e0 as e0, s1.n0 as n0, s2.e1 as e1, s2.n2 as n2 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as (update node n3 set properties = n3.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s3 where (s3.n2).id = n3.id returning (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n3), s5 as (select s3.e0 as e0, s3.e1 as e1, case when (s3.n0).id = (s4.n3).id then s4.n3 else s3.n0 end as n0, case when (s3.n2).id = (s4.n3).id then s4.n3 else s3.n2 end as n2 from s3 left outer join s4 on (s3.n2).id = (s4.n3).id) select s5.n2 as c from s5;

-- case: MATCH (a)-[r:EdgeKind1]->(b) MATCH (a)-[r]->(c) CREATE (audit:NodeKind1 {target:c.name})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"target"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n0).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id = (s0.e0).id), s2 as (select s1.e0 as e0, s1.e1 as e1, s1.n0 as n0, s1.n2 as n2, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n3_id from s1), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n3_id, array [1]::int2[], jsonb_build_object(@__strlit2::text, ((s2.n2).properties ->> E'name'))::jsonb from s2 returning id as n3_id, (id, kind_ids, properties)::nodecomposite as n3), s4 as (select s2.e0 as e0, s2.e1 as e1, s2.n0 as n0, s2.n2 as n2, s3.n3 as n3 from s2, s3 where s3.n3_id = s2.n3_id) select 1;
