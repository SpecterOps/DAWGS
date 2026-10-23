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

-- case: unwind [1,2,3] as x return x, x > 1 and x < 3 as mid order by x
select i0 as x, i0 > 1 and i0 < 3 as mid from unnest(array [1, 2, 3]::int8[]) as i0 order by x;

-- case: unwind [1,2,3] as x with x return x, x > 0 and x < 3 and x <> 1 as mid order by x
with s0 as (select i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0) select s0.i0 as x, s0.i0 > 0 and s0.i0 < 3 and s0.i0 <> 1 as mid from s0 order by s0.i0;

-- case: unwind [1,2,3] as x with x return x, x > 1 and x < 3 as mid, x = 1 or x = 3 as outer order by x
with s0 as (select i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0) select s0.i0 as x, s0.i0 > 1 and s0.i0 < 3 as mid, s0.i0 = 1 or s0.i0 = 3 as outer from s0 order by s0.i0;

-- case: unwind [1,2,3] as x with x, x > 1 and x < 3 as mid where x >= 1 and x <= 3 return x, mid order by x
with s0 as (select i0 as i0, i0 > 1 and i0 < 3 as i1 from unnest(array [1, 2, 3]::int8[]) as i0 where (i0 >= 1 and i0 <= 3)) select s0.i0 as x, s0.i1 as mid from s0 order by s0.i0;

-- case: unwind [1,2,3] as x with x where coalesce(x > 1 and x < 3, false) return x order by x
with s0 as (select i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 where (coalesce(i0 > 1 and i0 < 3, false)::bool)) select s0.i0 as x from s0 order by s0.i0;

-- case: match (n:NodeKind1) return n.k, n.k > 0 and n.k < 2 as mid
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k", (((s0.n0).properties ->> E'k'))::int8 > 0 and (((s0.n0).properties ->> E'k'))::int8 < 2 as mid from s0;

-- case: match (n:NodeKind1) set n.mid = n.k > 0 and n.k < 2 return n
-- pgsql_params:{"__strlit0":"mid"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, (((s0.n0).properties ->> E'k'))::int8 > 0 and (((s0.n0).properties ->> E'k'))::int8 < 2)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: match ()-[r:EdgeKind1]->() set r.mid = r.k > 0 and r.k < 2 return r
-- pgsql_params:{"__strlit0":"mid"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, (((s0.e0).properties ->> E'k'))::int8 > 0 and (((s0.e0).properties ->> E'k'))::int8 < 2)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s2 as (select case when (s0.e0).id = (s1.e1).id then s1.e1 else s0.e0 end as e0 from s0 left outer join s1 on (s0.e0).id = (s1.e1).id) select s2.e0 as r from s2;

-- case: match (n:NodeKind1) set n.mid = coalesce(n.k > 0 and n.k < 2, false) return n
-- pgsql_params:{"__strlit0":"mid"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, coalesce((((s0.n0).properties ->> E'k'))::int8 > 0 and (((s0.n0).properties ->> E'k'))::int8 < 2, false)::bool)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: match (n:NodeKind1)-[r:EdgeKind1]->() return n.active and r.active as both, n.active or r.active as either
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select (((s0.n0).properties ->> E'active'))::bool and (((s0.e0).properties ->> E'active'))::bool as both, (((s0.n0).properties ->> E'active'))::bool or (((s0.e0).properties ->> E'active'))::bool as either from s0;
