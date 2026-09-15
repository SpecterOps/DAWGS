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
-- pgsql_params:{"__strlit0":"age"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum((((s0.n0).properties ->> @__strlit0::text))::float8)::numeric from s0;

-- case: MATCH (n) RETURN avg(n.salary)
-- pgsql_params:{"__strlit0":"salary"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select avg((((s0.n0).properties ->> @__strlit0::text))::float8)::numeric from s0;

-- case: MATCH (n) RETURN min(n.created_date)
-- pgsql_params:{"__strlit0":"created_date"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_min(((s0.n0).properties -> @__strlit0::text))::jsonb from s0;

-- case: MATCH (n) RETURN max(n.updated_date)
-- pgsql_params:{"__strlit0":"updated_date"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_max(((s0.n0).properties -> @__strlit0::text))::jsonb from s0;

-- case: MATCH (n) RETURN min(n.name)
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_min(((s0.n0).properties -> @__strlit0::text))::jsonb from s0;

-- case: MATCH (n) RETURN max(n.name)
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cypher_max(((s0.n0).properties -> @__strlit0::text))::jsonb from s0;

-- case: MATCH (n) RETURN n.department, sum(n.salary)
-- pgsql_params:{"__strlit0":"department","__strlit1":"salary"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> @__strlit0::text), sum((((s0.n0).properties ->> @__strlit1::text))::float8)::numeric from s0 group by ((s0.n0).properties -> @__strlit0::text);

-- case: MATCH (n) RETURN n.department, avg(n.age)
-- pgsql_params:{"__strlit0":"department","__strlit1":"age"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> @__strlit0::text), avg((((s0.n0).properties ->> @__strlit1::text))::float8)::numeric from s0 group by ((s0.n0).properties -> @__strlit0::text);

-- case: MATCH (n) RETURN count(n), sum(n.age), avg(n.age), min(n.age), max(n.age)
-- pgsql_params:{"__strlit0":"age"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8, sum((((s0.n0).properties ->> @__strlit0::text))::float8)::numeric, avg((((s0.n0).properties ->> @__strlit0::text))::float8)::numeric, cypher_min(((s0.n0).properties -> @__strlit0::text))::jsonb, cypher_max(((s0.n0).properties -> @__strlit0::text))::jsonb from s0;

-- case: RETURN 'hello world'
-- pgsql_params:{"__strlit0":"hello world"}
select @__strlit0::text;

-- case: RETURN 2 + 3
select 2 + 3;

-- case: MATCH (n) RETURN n.department, collect(n.name)
-- pgsql_params:{"__strlit0":"department","__strlit1":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> @__strlit0::text), array_remove(coalesce(array_agg(((s0.n0).properties ->> @__strlit1::text))::anyarray, array []::text[])::anyarray, null)::anyarray from s0 group by ((s0.n0).properties -> @__strlit0::text);

-- case: MATCH (n) RETURN collect(n.name)
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(((s0.n0).properties ->> @__strlit0::text))::anyarray, array []::text[])::anyarray, null)::anyarray from s0;

-- case: MATCH (n) RETURN n.department, collect(n.name), count(n)
-- pgsql_params:{"__strlit0":"department","__strlit1":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s0.n0).properties -> @__strlit0::text), array_remove(coalesce(array_agg(((s0.n0).properties ->> @__strlit1::text))::anyarray, array []::text[])::anyarray, null)::anyarray, count(s0.n0)::int8 from s0 group by ((s0.n0).properties -> @__strlit0::text);

-- case: MATCH (n) RETURN size(n.tags)
-- pgsql_params:{"__strlit0":"tags"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select jsonb_array_length(((s0.n0).properties -> @__strlit0::text))::int from s0;

-- case: MATCH (n) RETURN size(collect(n.name))
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select cardinality(array_remove(coalesce(array_agg(((s0.n0).properties ->> @__strlit0::text))::anyarray, array []::text[])::anyarray, null)::anyarray)::int from s0;

-- case: MATCH (n) WITH collect(labels(n)) as label_sets RETURN size(label_sets)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(to_jsonb((array(select _kind.name from generate_subscripts((s1.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s1.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[])::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1) select cardinality(s0.i0)::int from s0;

-- case: MATCH (n) WHERE size(n.permissions) > 2 RETURN n
-- pgsql_params:{"__strlit0":"permissions"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (jsonb_array_length((n0.properties -> @__strlit0::text))::int > 2)) select s0.n0 as n from s0;

-- case: MATCH (n) WITH n, collect(n.prop) as props WHERE size(props) > 1 RETURN n, props
-- pgsql_params:{"__strlit0":"prop"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s1.n0 as n0, array_remove(coalesce(array_agg(((s1.n0).properties ->> @__strlit0::text))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s1 group by n0) select s0.n0 as n, s0.i0 as props from s0 where (cardinality(s0.i0)::int > 1);

-- case: MATCH (n) WITH n, count(n) as node_count WHERE node_count > 1 RETURN n, node_count
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s1.n0 as n0, count(s1.n0)::int8 as i0 from s1 group by n0) select s0.n0 as n, s0.i0 as node_count from s0 where (s0.i0 > 1);

-- case: MATCH (n) WITH sum(n.age) as total_age, count(n) as total_count RETURN total_age / total_count as avg_age
-- pgsql_params:{"__strlit0":"age"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum((((s1.n0).properties ->> @__strlit0::text))::float8)::numeric as i0, count(s1.n0)::int8 as i1 from s1) select s0.i0 / s0.i1 as avg_age from s0;

-- case: MATCH (n) WITH count(n) as cnt WHERE cnt > 1 RETURN cnt
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s1.n0)::int8 as i0 from s1) select s0.i0 as cnt from s0 where (s0.i0 > 1);

-- case: MATCH (n) WITH count(n) as lim MATCH (o) RETURN o
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s1.n0)::int8 as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select s2.n1 as o from s2;

-- case: MATCH (n) RETURN count(n) + count(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8 + count(s0.n0)::int8 from s0;

-- case: MATCH (n) RETURN count(n) * 2
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8 * 2 from s0;

-- case: MATCH (n) RETURN count(n) AS total ORDER BY total DESC
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s0.n0)::int8 as total from s0 order by total desc;

-- case: MATCH (n) RETURN toInteger(n.value) + count(n)
-- pgsql_params:{"__strlit0":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (((s0.n0).properties ->> @__strlit0::text))::int8 + count(s0.n0)::int8 from s0 group by (((s0.n0).properties ->> @__strlit0::text))::int8;

-- case: MATCH (n) WITH toInteger(n.value) AS value, count(n) AS node_count RETURN value + node_count
-- pgsql_params:{"__strlit0":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (((s1.n0).properties ->> @__strlit0::text))::int8 as i0, count(s1.n0)::int8 as i1 from s1 group by (((s1.n0).properties ->> @__strlit0::text))::int8) select s0.i0 + s0.i1 from s0;

-- case: MATCH (n) WITH toInteger(n.value) + count(n) AS score RETURN score
-- pgsql_params:{"__strlit0":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (((s1.n0).properties ->> @__strlit0::text))::int8 + count(s1.n0)::int8 as i0 from s1 group by (((s1.n0).properties ->> @__strlit0::text))::int8) select s0.i0 as score from s0;

-- case: MATCH (n) WITH toInteger(n.value) AS value, count(n) AS node_count WITH value + node_count AS score RETURN score
-- pgsql_params:{"__strlit0":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (((s1.n0).properties ->> @__strlit0::text))::int8 as i0, count(s1.n0)::int8 as i1 from s1 group by (((s1.n0).properties ->> @__strlit0::text))::int8), s2 as (select s0.i0 + s0.i1 as i2 from s0) select s2.i2 as score from s2;

-- case: MATCH (n) WITH count(n) > 1 AS many RETURN many
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select count(s1.n0)::int8 > 1 as i0 from s1) select s0.i0 as many from s0;

-- case: MATCH (n) WITH sum(n.age) / count(n) AS avg_age RETURN avg_age
-- pgsql_params:{"__strlit0":"age"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select sum((((s1.n0).properties ->> @__strlit0::text))::float8)::numeric / count(s1.n0)::int8 as i0 from s1) select s0.i0 as avg_age from s0;

