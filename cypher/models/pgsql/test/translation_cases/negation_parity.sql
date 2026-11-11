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

-- case: match (n:NodeKind1) where not not n.k = 1 return n.k
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k" from s0;

-- case: match (n:NodeKind1) where not not not n.k = 1 return n.k
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k" from s0;

-- case: match (n:NodeKind1) where not (not n.k = 1) return n.k
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k" from s0;

-- case: match (n:NodeKind1) where not n.k = 1 return n.k
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k" from s0;

-- case: match (n:NodeKind1) where n.j = 2 and not not n.k = 1 return n.k
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'j'))::jsonb = to_jsonb((2)::int8)::jsonb and ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'k') as "n.k" from s0;

-- case: match (n:NodeKind1) where not not n.name starts with 'a' return n.name
-- pgsql_params:{"__strlit0":"string","__strlit1":"a%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') like @__strlit1::text) or (not (jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text) and null))) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;

-- case: match (n:NodeKind1) where not not n.name is null return n.name
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((not n0.properties ? E'name' or (n0.properties -> E'name') = (@__strlit0::text)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;

-- case: return not not true as value
select true as value;

-- case: unwind [1,2,3] as x with x where not not x > 1 return x order by x
with s0 as (select i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 where ((i0)::int8 > 1)) select s0.i0 as x from s0 order by s0.i0;

-- case: unwind [1,2,3] as x return x, not not x > 1 as again order by x
select i0 as x, (i0)::int8 > 1 as again from unnest(array [1, 2, 3]::int8[]) as i0 order by x;

-- case: match (n:NodeKind1) set n.flag = not not n.k > 1 return n
-- pgsql_params:{"__strlit0":"flag","__strlit1":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, cypher_compare(nullif(((s0.n0).properties -> E'k'), (@__strlit1::text)::jsonb)::jsonb, to_jsonb((1)::int8)::jsonb)::int4 > 0)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;
