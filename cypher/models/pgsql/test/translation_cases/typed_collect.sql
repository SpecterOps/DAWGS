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

-- case: MATCH (n) RETURN collect(n.x)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(n.x)" from s0;

-- case: MATCH ()-[r]->() RETURN collect(DISTINCT r.x)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select array_remove(coalesce(array_agg(distinct nullif(((s0.e0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(distinct r.x)" from s0;

-- case: MATCH (n) WITH n.x AS x RETURN collect(x)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((s1.n0).properties -> E'x') as i0 from s1) select array_remove(coalesce(array_agg(nullif(s0.i0, (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(x)" from s0;

-- case: MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN x + 1
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1) select cypher_arithmetic(nullif((i1)::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((1)::int8)::jsonb, @__strlit1::text)::jsonb as "x + 1" from s0, unnest(i0) as i1;

-- case: MATCH (n) WITH collect(n.flag) AS xs UNWIND xs AS x RETURN x AND true
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'flag'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1) select (nullif(i1, (@__strlit0::text)::jsonb)::jsonb)::bool and true as "x and true" from s0, unnest(i0) as i1;

-- case: MATCH (n) WITH collect(n.x) AS xs RETURN collect(xs)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1) select array_remove(coalesce(array_agg(to_jsonb(s0.i0)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(xs)" from s0;

-- case: RETURN collect(null)
select array_remove(coalesce(array_agg((null)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(null)";

-- case: RETURN collect(true)
select array_remove(coalesce(array_agg(to_jsonb((true)::bool)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect(true)";

-- case: RETURN collect([])
select array_remove(coalesce(array_agg(to_jsonb(array []::int8[])::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as "collect([])";

-- case: MATCH (n) WITH collect(n.name) AS xs, ['tail'] AS ys RETURN xs + ys
-- pgsql_params:{"__strlit0":"null","__strlit1":"tail"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'name'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0, array [@__strlit1::text]::text[] as i1 from s1) select (s0.i0)::jsonb[] || (array(select to_jsonb((unnest((s0.i1)::text[])::text)::text)::jsonb))::jsonb[] as "xs + ys" from s0;

-- case: MATCH (n:NodeKind1) WITH collect(n.x) AS xs MATCH (t:NodeKind2) SET t.xs = xs RETURN t
-- pgsql_params:{"__strlit0":"null","__strlit1":"xs"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]), s3 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit1::text, i0)::jsonb from s2 where (s2.n1).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2), s4 as (select s2.i0 as i0, case when (s2.n1).id = (s3.n2).id then s3.n2 else s2.n1 end as n1 from s2 left outer join s3 on (s2.n1).id = (s3.n2).id) select s4.n1 as t from s4;

-- case: MATCH (n:NodeKind1) WITH collect(n.x) AS xs MATCH ()-[r:EdgeKind1]->() SET r.xs = xs
-- pgsql_params:{"__strlit0":"null","__strlit1":"xs"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s3 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit1::text, i0)::jsonb from s2 where (s2.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s4 as (select case when (s2.e0).id = (s3.e1).id then s3.e1 else s2.e0 end as e0, s2.i0 as i0 from s2 left outer join s3 on (s2.e0).id = (s3.e1).id) select 1;

-- case: MATCH (n:NodeKind1) WITH collect(n.x) AS xs UNWIND xs AS x CREATE (c:NodeKind2 {x: x + 1})
-- pgsql_params:{"__strlit0":"null","__strlit1":"node","__strlit2":"id","__strlit3":"x","__strlit4":"+"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1), s2 as (select s0.i0 as i0, i1 as i1, nextval(pg_get_serial_sequence(@__strlit1::text, @__strlit2::text))::int8 as n1_id from s0, unnest(i0) as i1), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit3::text, cypher_arithmetic(nullif((s2.i1)::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((1)::int8)::jsonb, @__strlit4::text)::jsonb)::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.i0 as i0, s2.i1 as i1, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select 1;

-- case: MATCH (n) WITH collect(n.x) AS xs RETURN 2 IN xs
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1) select to_jsonb((2)::int8)::jsonb = any ((s0.i0)::jsonb[]) as "2 in xs" from s0;

-- case: MATCH (n) RETURN 2 IN collect(n.x)
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select to_jsonb((2)::int8)::jsonb = any (array_remove(coalesce(array_agg(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[]) as "2 in collect(n.x)" from s0;

-- case: MATCH (n) RETURN head(collect(n.x))
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((array_remove(coalesce(array_agg(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[])[1])::jsonb as "head(collect(n.x))" from s0;

-- case: MATCH (n) WITH collect(n.x) AS xs MATCH (m) WHERE m.x IN xs RETURN m
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (nullif((n1.properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb = any ((s0.i0)::jsonb[]))) select s2.n1 as m from s2;

-- case: MATCH (n) WITH collect(n.text) AS xs MATCH (m) WHERE NOT m.text IN xs SET m.hit = true RETURN m
-- pgsql_params:{"__strlit0":"null","__strlit1":"hit"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(nullif(((s1.n0).properties -> E'text'), (@__strlit0::text)::jsonb)::jsonb)::jsonb[], array []::jsonb[])::jsonb[], null)::jsonb[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (not nullif((n1.properties -> E'text'), (@__strlit0::text)::jsonb)::jsonb = any ((s0.i0)::jsonb[]))), s3 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit1::text, true)::jsonb from s2 where (s2.n1).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2), s4 as (select s2.i0 as i0, case when (s2.n1).id = (s3.n2).id then s3.n2 else s2.n1 end as n1 from s2 left outer join s3 on (s2.n1).id = (s3.n2).id) select s4.n1 as m from s4;

