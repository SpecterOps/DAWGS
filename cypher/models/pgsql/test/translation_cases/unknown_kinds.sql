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

-- case: MATCH (n:DAWGS023NeverRegistered) RETURN count(n) AS c
select count(*)::int8 as c from node n0 where false;

-- case: MATCH (n:NodeKind1:DAWGS023NeverRegistered) RETURN count(n) AS c
select count(*)::int8 as c from node n0 where false;

-- case: MATCH ()-[r:DAWGS023NeverRegistered|EdgeKind1]->() RETURN count(r) AS c
select count(*)::int8 as c from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[]);

-- case: MATCH (n:NodeKind1)-[r:DAWGS023NeverRegistered*1..2]->(t) RETURN count(t) AS c
with s0 as (with recursive s1_seed(root_id) as not materialized (select n0.id as root_id from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id, e0.end_id, 1, false, false, array [e0.id] from s1_seed join edge e0 on e0.start_id = s1_seed.root_id where e0.kind_id = any (array []::int2[]) union all select s1.root_id, e0.end_id, s1.depth + 1, false, false, s1.path || e0.id from s1 join lateral (select e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties from edge e0 where e0.start_id = s1.next_id and e0.id != all (s1.path) and e0.kind_id = any (array []::int2[]) offset 0) e0 on true where s1.depth < 2 and not s1.is_cycle) select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1 join lateral (select n0.id, n0.kind_ids, n0.properties from node n0 where n0.id = s1.root_id offset 0) n0 on true join lateral (select n1.id, n1.kind_ids, n1.properties from node n1 where n1.id = s1.next_id offset 0) n1 on true) select count(s0.n1)::int8 as c from s0;

-- case: MATCH (n:NodeKind1) WHERE NOT n:DAWGS023NeverRegistered RETURN count(n) AS c
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not n0.kind_ids operator (pg_catalog.&&) array []::int2[]) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select count(s0.n0)::int8 as c from s0;

-- case: MATCH (n:NodeKind1) RETURN n:NodeKind1:DAWGS023NeverRegistered AS x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select (s0.n0).kind_ids operator (pg_catalog.&&) array []::int2[] as x from s0;

-- case: MATCH (n:DAWGS023NeverRegistered) SET n.touched = true RETURN count(*) AS c
-- pgsql_params:{"__strlit0":"touched"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where false), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select count(*)::int8 as c from s2;

-- case: MATCH ()-[r:DAWGS023NeverRegistered]->() DELETE r RETURN count(*) AS c
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array []::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select count(*)::int8 as c from s0;

-- case: MATCH (n:DAWGS023NeverRegistered) CREATE (:NodeKind1 {name:"unexpected"}) RETURN count(*) AS c
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"name","__strlit3":"unexpected"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where false), s1 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s2 as (insert into node (graph_id, id, kind_ids, properties) select 0, s1.n1_id, array [1]::int2[], jsonb_build_object(@__strlit2::text, @__strlit3::text)::jsonb from s1 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s3 as (select s1.n0 as n0, s2.n1 as n1 from s1, s2 where s2.n1_id = s1.n1_id) select count(*)::int8 as c from s3;
