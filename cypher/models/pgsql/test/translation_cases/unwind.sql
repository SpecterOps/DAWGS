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

-- case: with [1, 2, 3] as ids unwind ids as x return x
with s0 as (select array [1, 2, 3]::int8[] as i0) select i1 as x from s0, unnest(i0) as i1;

-- case: match (n:NodeKind1) with collect(n.name) as names unwind names as name return name
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(((s1.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s1) select i1 as name from s0, unnest(i0) as i1;

-- case: match (n:NodeKind1) with collect(n.name) as names unwind names as name with name where name starts with 'test' return name
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(((s1.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s1), s2 as (select i1 as i1 from s0, unnest(i0) as i1 where (i1 like 'test%')) select s2.i1 as name from s2;

-- case: with ['a', 'b', 'c'] as names unwind names as name return name
with s0 as (select array ['a', 'b', 'c']::text[] as i0) select i1 as name from s0, unnest(i0) as i1;

-- case: with [1, 2, 3] as ids unwind ids as x return x order by x desc
with s0 as (select array [1, 2, 3]::int8[] as i0) select i1 as x from s0, unnest(i0) as i1 order by x desc;

-- case: with [1, 2, 3, 1, 2] as ids unwind ids as x return distinct x
with s0 as (select array [1, 2, 3, 1, 2]::int8[] as i0) select distinct i1 as x from s0, unnest(i0) as i1;

-- case: with [1, 2, 3] as ids unwind ids as x return count(x)
with s0 as (select array [1, 2, 3]::int8[] as i0) select count(i1)::int8 from s0, unnest(i0) as i1;

-- case: match (n:NodeKind1) with collect(n.name) as names unwind names as name match (m:NodeKind2) where m.name = name return m
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select array_remove(coalesce(array_agg(((s1.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s1), s2 as (select s0.i0 as i0, i1 as i1, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, unnest(i0) as i1, node n1 where ((n1.properties ->> 'name') = i1) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s2.n1 as m from s2;

-- case: with [1, 2, 3] as ids unwind ids as x with x where x > 1 return x
with s0 as (select array [1, 2, 3]::int8[] as i0), s1 as (select i1 as i1 from s0, unnest(i0) as i1 where (i1 > 1)) select s1.i1 as x from s1;

-- case: with [1, 2, 3] as ids unwind ids as x return x limit 2
with s0 as (select array [1, 2, 3]::int8[] as i0) select i1 as x from s0, unnest(i0) as i1 limit 2;

-- case: unwind [1, 2, 3] as x return x
select i0 as x from unnest(array [1, 2, 3]::int8[]) as i0;

-- case: MATCH (n) WHERE n.environmentid = '1234' UNWIND labels(n) AS kind RETURN kind, count(n) AS count
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'environmentid')) = 'string' and (n0.properties ->> 'environmentid') = '1234'))) select i0 as kind, count(s0.n0)::int8 as count from s0, unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 group by i0;

-- case: MATCH (n) UNWIND labels(n) AS label RETURN label, count(n) AS count
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select i0 as label, count(s0.n0)::int8 as count from s0, unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 group by i0;

-- case: MATCH (n) UNWIND labels(n) AS label RETURN label, count(n) AS count ORDER BY count DESC
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select i0 as label, count(s0.n0)::int8 as count from s0, unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 group by i0 order by count desc;

-- case: MATCH (n) UNWIND labels(n) AS label RETURN label, count(n) AS count ORDER BY label
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select i0 as label, count(s0.n0)::int8 as count from s0, unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 group by i0 order by label;

-- case: MATCH (n) UNWIND labels(n) AS label RETURN label AS kind, count(n) AS count ORDER BY kind
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select i0 as kind, count(s0.n0)::int8 as count from s0, unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 group by i0 order by kind;

-- case: match (au:AZUser) where au.name contains "ADMIN" match (u:User) where u.name contains "ADMIN" WITH COLLECT(u.name) + COLLECT(au.azname) as temp UNWIND temp as usernames return usernames
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> 'name') like '%ADMIN%') and n0.kind_ids operator (pg_catalog.@>) array [32]::int2[]), s2 as (select s1.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s1, node n1 where ((n1.properties ->> 'name') like '%ADMIN%') and n1.kind_ids operator (pg_catalog.@>) array [6]::int2[]) select array_remove(coalesce(array_agg(((s2.n1).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray || array_remove(coalesce(array_agg(((s2.n0).properties ->> 'azname'))::anyarray, array []::text[])::anyarray, null)::anyarray as i0 from s2) select i1 as usernames from s0, unnest(i0) as i1;

-- case: MATCH (n) WITH collect(n.name) + ['tail'] AS names UNWIND names AS name RETURN name
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(((s1.n0).properties ->> 'name'))::anyarray, array []::text[])::anyarray, null)::anyarray || array ['tail']::text[] as i0 from s1) select i1 as name from s0, unnest(i0) as i1;

