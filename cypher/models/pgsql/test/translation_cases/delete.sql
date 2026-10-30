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

-- case: match (s:NodeKind1) detach delete s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select 1;

-- case: match ()-[r:EdgeKind1]->() delete r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[]->()-[r:EdgeKind1]->() delete r
with s0 as (select e0.id as e0, n1.id as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n1 as n1 from s0 join edge e1 on s0.n1 = e1.start_id join node n2 on n2.id = e1.end_id where e1.kind_id = any (array [3]::int2[]) and e1.id != s0.e0), s2 as (delete from edge e2 using s1 where (s1.e1).id = e2.id) select 1;

-- case: match (s)-[*1..]->(mid)-[]->(e) delete mid
with s0 as (with recursive s1(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id, e0.end_id, 1, true, false, array [e0.id] from edge e0 join node n1 on n1.id = e0.end_id where dawgs_admit_traversal_v1(1, e0.id) union all select s1.root_id, e0.end_id, s1.depth + 1, true, false, s1.path || e0.id from s1 join lateral (select e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties from edge e0 where e0.start_id = s1.next_id and e0.id != all (s1.path) offset 0) e0 on true join node n1 on n1.id = e0.end_id where not s1.is_cycle and dawgs_admit_traversal_v1(s1.depth + 1, e0.id)) select s1.path as ep0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1 join lateral (select n0.id, n0.kind_ids, n0.properties from node n0 where n0.id = s1.root_id offset 0) n0 on true join lateral (select n1.id, n1.kind_ids, n1.properties from node n1 where n1.id = s1.next_id offset 0) n1 on true where s1.satisfied and exists (select 1 from edge e1 join node n2 on n2.id = e1.end_id where n1.id = e1.start_id)), s2 as materialized (select s0.ep0 as ep0, s0.n0 as n0, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id != all (s0.ep0)), s3 as (delete from node n3 using s2 where (s2.n1).id = n3.id and cypher_assert_node_deletable(0, (s2.n1).id, array []::int8[], array []::int8[])::bool) select 1;

-- case: match (n) delete n
with s0 as materialized (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[], array []::int8[])::bool) select 1;

-- case: match (n)-[r]->(m) delete n, r
with s0 as materialized (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id), s2 as (delete from node n2 using s0 where (s0.n0).id = n2.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[] || array(select (s0.e0).id from s0 where (s0.e0).id is not null), array []::int8[])::bool) select 1;

-- case: match (n)-[r]->(m) delete r, n
with s0 as materialized (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (delete from node n2 using s0 where (s0.n0).id = n2.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[] || array(select (s0.e0).id from s0 where (s0.e0).id is not null), array []::int8[])::bool), s2 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (n)-[r]->(m) detach delete n, m
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (delete from node n2 using s0 where (s0.n1).id = n2.id), s2 as (delete from node n3 using s0 where (s0.n0).id = n3.id) select 1;

-- case: match (n)-[r]->(m) delete n detach delete m
with s0 as materialized (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (delete from node n2 using s0 where (s0.n0).id = n2.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[], array []::int8[] || array(select (s0.n1).id from s0 where (s0.n1).id is not null))::bool), s2 as (delete from node n3 using s0 where (s0.n1).id = n3.id) select 1;

-- case: match (n) optional match (n)-[r]->() delete n, r
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as materialized (select s0.n0 as n0, row_number() over () as i0 from s0), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s1.i0 as i0, s1.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1 join edge e0 on (s1.n0).id = e0.start_id join node n1 on n1.id = e0.end_id), s3 as materialized (select s1.n0 as n0, s2.e0 as e0, s2.n1 as n1 from s1 left outer join s2 on s1.i0 = s2.i0), s4 as (delete from edge e1 using s3 where (s3.e0).id = e1.id), s5 as (delete from node n2 using s3 where (s3.n0).id = n2.id and cypher_assert_node_deletable(0, (s3.n0).id, array []::int8[] || array(select (s3.e0).id from s3 where (s3.e0).id is not null), array []::int8[])::bool) select 1;

-- case: match (n) where n.name = 'isolated' with n delete n
-- pgsql_params:{"__strlit0":"string","__strlit1":"isolated"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') = @__strlit1::text))) select s1.n0 as n0 from s1), s2 as (delete from node n1 using s0 where (s0.n0).id = n1.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[], array []::int8[])::bool) select 1;

-- case: match (n) delete n return count(*) as c
with s0 as materialized (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[], array []::int8[])::bool) select count(*)::int8 as c from s0;

-- case: match (n) detach delete n return 1 as x
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select 1 as x from s0;

-- case: match (n)-[r]->(m) delete r, n return count(*) as c
with s0 as materialized (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (delete from node n2 using s0 where (s0.n0).id = n2.id and cypher_assert_node_deletable(0, (s0.n0).id, array []::int8[] || array(select (s0.e0).id from s0 where (s0.e0).id is not null), array []::int8[])::bool), s2 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select count(*)::int8 as c from s0;

-- case: match (n) with n, n.name as name detach delete n return name
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s1.n0 as n0, ((s1.n0).properties -> E'name') as i0 from s1), s2 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select s0.i0 as name from s0;
