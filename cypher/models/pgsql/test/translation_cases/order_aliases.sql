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

-- case: match (n:NodeKind1) return n.k as x order by x + 0
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as x from s0 order by cypher_arithmetic(nullif(((((s0.n0).properties -> E'k')))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb, @__strlit1::text)::jsonb;

-- case: match (n:NodeKind1) return n.k as x order by coalesce(x, 0) desc
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as x from s0 order by coalesce(nullif(to_jsonb((((s0.n0).properties -> E'k')))::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((0)::int8)::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb desc;

-- case: unwind [1, 2, 3] as x return 10-x as i0 order by x
select 10 - i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 order by i0.i0;

-- case: unwind [1, 2, 3] as x return 10-x as i0 order by i0 + 0
select 10 - i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 order by (10 - i0) + 0;

-- case: unwind [1, 2, 3] as x return 10-x as i0 order by toString(i0)
select 10 - i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 order by (10 - i0)::text;

-- case: match (n:NodeKind1) return distinct n.k as i0 order by i0 + 0 desc skip 1 limit 1
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.i0 as i0 from (select distinct ((s0.n0).properties -> E'k') as i0, cypher_arithmetic(nullif(((((s0.n0).properties -> E'k')))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb, @__strlit1::text)::jsonb as i1 from s0) as s1 order by s1.i1 desc offset 1 limit 1;

-- case: unwind [1, 1, 2] as x return distinct 10-x as i2 order by i2 + 0
select s1.i2 as i2 from (select distinct 10 - i0 as i2, (10 - i0) + 0 as i3 from unnest(array [1, 1, 2]::int8[]) as i0) as s1 order by s1.i3;

-- case: match (n:NodeKind1) return sum(n.k) as i0 order by i0 + 0
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_sum(nullif(((s0.n0).properties -> E'k'), (@__strlit0::text)::jsonb)::jsonb)::jsonb as i0 from s0 order by cypher_arithmetic(nullif(((cypher_sum(nullif(((s0.n0).properties -> E'k'), (@__strlit0::text)::jsonb)::jsonb)::jsonb))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb, @__strlit1::text)::jsonb;

-- case: match (n:NodeKind1) with n, n.k as i0 order by i0 + 0 desc limit 1 set n.seen = true return n.k as s0 order by s0 + 0
-- pgsql_params:{"__strlit0":"null","__strlit1":"+","__strlit2":"seen"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0, ((s1.n0).properties -> E'k') as i0 from s1 order by cypher_arithmetic(nullif((((s1.n0).properties -> E'k'))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb, @__strlit1::text)::jsonb desc limit 1), s2 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s3 as (select s0.i0 as i0, case when (s0.n0).id = (s2.n1).id then s2.n1 else s0.n0 end as n0 from s0 left outer join s2 on (s0.n0).id = (s2.n1).id) select ((s3.n0).properties -> E'k') as s0 from s3 order by cypher_arithmetic(nullif(((((s3.n0).properties -> E'k')))::jsonb, (@__strlit0::text)::jsonb)::jsonb, to_jsonb((0)::int8)::jsonb, @__strlit1::text)::jsonb;
