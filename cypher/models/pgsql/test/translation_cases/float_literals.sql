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

-- case: return 3.0 / 2.0 as a
select 3.0 / 2.0 as a;

-- case: return 3.0 as a
select 3.0 as a;

-- case: return 1e3 as b
select 1000.0 as b;

-- case: return toString(1.0) as s
select (1.0)::text as s;

-- case: unwind [1.0, 2.5] as x return x
select i0 as x from unnest(array [1.0, 2.5]::float8[]) as i0;

-- case: match (n:NodeKind1) where n.score in [-1.0, 0.0] return n.name
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'score'))::float8 = any (array [- 1.0, 0.0]::float8[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;

-- case: match (n:NodeKind1) where n.score = 2.0 return n.name
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'score'))::jsonb = to_jsonb((2.0)::float8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;

-- case: return 3 / 2 as a
select 3 / 2 as a;
