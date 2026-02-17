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

-- case: MATCH (n) RETURN sum(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum(((s0.n0).properties ->> 'age'))::numeric from s0;

-- case: MATCH (n) RETURN avg(n.salary)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select avg(((s0.n0).properties ->> 'salary'))::numeric from s0;

-- case: MATCH (n) RETURN min(n.created_date)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select min(((s0.n0).properties ->> 'created_date')) from s0;

-- case: MATCH (n) RETURN max(n.updated_date)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select max(((s0.n0).properties ->> 'updated_date')) from s0;

-- case: MATCH (n) RETURN n.department, sum(n.salary)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), sum(((s0.n0).properties ->> 'salary'))::numeric from s0 group by ((s0.n0).properties -> 'department');

-- case: MATCH (n) RETURN n.department, avg(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), avg(((s0.n0).properties ->> 'age'))::numeric from s0 group by ((s0.n0).properties -> 'department');

-- case: MATCH (n) RETURN count(n), sum(n.age), avg(n.age), min(n.age), max(n.age)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8, sum(((s0.n0).properties ->> 'age'))::numeric, avg(((s0.n0).properties ->> 'age'))::numeric, min(((s0.n0).properties ->> 'age')), max(((s0.n0).properties ->> 'age')) from s0;

-- case: RETURN 'hello world'
select 'hello world';

-- case: RETURN 2 + 3
select 2 + 3;

-- case: MATCH (n) RETURN n.department, collect(n.name)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), array_remove(coalesce(array_agg(((s0.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray from s0 group by ((s0.n0).properties -> 'department');

-- case: MATCH (n) RETURN collect(n.name)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(((s0.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray from s0;

-- case: MATCH (n) RETURN n.department, collect(n.name), count(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> 'department'), array_remove(coalesce(array_agg(((s0.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray, count(s0.n0)::int8 from s0 group by ((s0.n0).properties -> 'department');

-- case: MATCH (n) RETURN size(n.tags)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select jsonb_array_length(((s0.n0).properties -> 'tags'))::int from s0;

-- case: MATCH (n) RETURN size(collect(n.name))
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_length(array_remove(coalesce(array_agg(((s0.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray, 1)::int from s0;

-- case: MATCH (n) WHERE size(n.permissions) > 2 RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (jsonb_array_length((n0.properties -> 'permissions'))::int > 2)) select s0.n0 as n from s0;

-- case: MATCH (n) WITH n, collect(n.prop) as props WHERE size(props) > 1 RETURN n, props
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s1.n0 as n0, array_remove(coalesce(array_agg(((s1.n0).properties ->> 'prop'))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s1 group by n0) select s0.n0 as n, s0.i0 as props from s0 where (array_length(s0.i0, 1)::int > 1);

-- case: MATCH (n) WITH n, count(n) as node_count WHERE node_count > 1 RETURN n, node_count
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s1.n0 as n0, count(s1.n0)::int8 as i0 from s1 group by n0) select s0.n0 as n, s0.i0 as node_count from s0 where (s0.i0 > 1);

-- case: MATCH (n) WITH sum(n.age) as total_age, count(n) as total_count RETURN total_age / total_count as avg_age
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum(((s1.n0).properties ->> 'age'))::numeric as i0, count(s1.n0)::int8 as i1 from s1) select s0.i0 / s0.i1 as avg_age from s0;

-- case: MATCH (n) WITH count(n) as cnt WHERE cnt > 1 RETURN cnt
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s1.n0)::int8 as i0 from s1) select s0.i0 as cnt from s0 where (s0.i0 > 1);

-- case: MATCH (n) WITH count(n) as lim MATCH (o) RETURN o
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s1.n0)::int8 as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select s2.n1 as o from s2;

