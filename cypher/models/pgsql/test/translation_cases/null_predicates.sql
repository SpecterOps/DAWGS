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

-- case: RETURN null IS NULL AS value
select null is null as value;

-- case: RETURN null IS NOT NULL AS value
select null is not null as value;

-- case: RETURN 1 IS NULL AS value
select 1 is null as value;

-- case: RETURN 'a' IS NOT NULL AS value
-- pgsql_params:{"__strlit0":"a"}
select @__strlit0::text is not null as value;

-- case: UNWIND [1] AS x RETURN null IS NULL AS value
select null is null as value from unnest(array [1]::int8[]) as i0;

-- case: WITH null AS n RETURN n IS NULL AS value
with s0 as (select null as i0) select s0.i0 is null as value from s0;

-- case: MATCH (n:NodeKind1) WHERE null IS NULL RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (null is null) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE 1 IS NULL RETURN n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (1 is null) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE n IS NOT NULL RETURN count(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0 is not null) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select count(s0.n0)::int8 as "count(n)" from s0;

-- case: MATCH (n:NodeKind1) WHERE n.name IS NOT NULL AND null IS NULL RETURN n
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (null is null and (n0.properties ? E'name' and not (n0.properties -> E'name') = (@__strlit0::text)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE n.name IS NULL RETURN n
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((not n0.properties ? E'name' or (n0.properties -> E'name') = (@__strlit0::text)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE n.name IS NOT NULL RETURN n
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ? E'name' and not (n0.properties -> E'name') = (@__strlit0::text)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) RETURN n.name IS NULL AS value
-- pgsql_params:{"__strlit0":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select (not (s0.n0).properties ? E'name' or ((s0.n0).properties -> E'name') = (@__strlit0::text)::jsonb) as value from s0;

-- case: UNWIND [1, 2] AS x WITH x WHERE x IS NOT NULL RETURN x
with s0 as (select i0 as i0 from unnest(array [1, 2]::int8[]) as i0 where (i0 is not null)) select s0.i0 as x from s0;
