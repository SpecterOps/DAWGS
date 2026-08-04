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

-- case: match (n) where not n:RegressionKind03 and (n.lastseen is null or n.lastseen < datetime($threshold)) return id(n)
-- cypher_params: {"threshold":"2026-01-02T03:04:05Z"}
-- pgsql_params:{"pi0":"2026-01-02T03:04:05Z"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not n0.kind_ids operator (pg_catalog.@>) array [35]::int2[] and ((not n0.properties ? 'lastseen' or (n0.properties -> 'lastseen') = ('null')::jsonb) or ((n0.properties ->> 'lastseen'))::timestamp with time zone < (@pi0::text)::timestamp with time zone))) select (s0.n0).id from s0;

-- case: match ()-[r]->() where not r:RegressionKind45 and r.lastseen < datetime($threshold) return id(r)
-- cypher_params: {"threshold":"2026-01-03T00:00:00Z"}
-- pgsql_params:{"pi0":"2026-01-03T00:00:00Z"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (not e0.kind_id = any (array [77]::int2[]) and ((e0.properties ->> 'lastseen'))::timestamp with time zone < (@pi0::text)::timestamp with time zone)) select (s0.e0).id from s0;

-- case: match ()-[r]->() where not (r:RegressionKind45 or r:RegressionKind46) and r.lastseen < datetime($threshold) return id(r)
-- cypher_params: {"threshold":"2026-01-03T00:00:00Z"}
-- pgsql_params:{"pi0":"2026-01-03T00:00:00Z"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (not (e0.kind_id = any (array [77]::int2[]) or e0.kind_id = any (array [78]::int2[])) and ((e0.properties ->> 'lastseen'))::timestamp with time zone < (@pi0::text)::timestamp with time zone)) select (s0.e0).id from s0;

-- case: match ()-[r:HasSession]->() where r.lastseen is null or r.lastseen < datetime($threshold) return id(r)
-- cypher_params: {"threshold":"2026-01-03T00:00:00Z"}
-- pgsql_params:{"pi0":"2026-01-03T00:00:00Z"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where ((not e0.properties ? 'lastseen' or (e0.properties -> 'lastseen') = ('null')::jsonb) or ((e0.properties ->> 'lastseen'))::timestamp with time zone < (@pi0::text)::timestamp with time zone) and e0.kind_id = any (array [7]::int2[])) select (s0.e0).id from s0;

-- case: match (n) where not (n:RegressionKind48 or n:RegressionKind49) and (n.lastseen is null or n.lastseen < datetime($threshold)) return id(n)
-- cypher_params: {"threshold":"2026-01-03T00:00:00Z"}
-- pgsql_params:{"pi0":"2026-01-03T00:00:00Z"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (n0.kind_ids operator (pg_catalog.@>) array [80]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [81]::int2[]) and ((not n0.properties ? 'lastseen' or (n0.properties -> 'lastseen') = ('null')::jsonb) or ((n0.properties ->> 'lastseen'))::timestamp with time zone < (@pi0::text)::timestamp with time zone))) select (s0.n0).id from s0;

-- case: match (n) where not (n:RegressionKind48 or n:RegressionKind49) and n.name is null and n.objectid starts with $sid_prefix return id(n)
-- cypher_params: {"sid_prefix":"S-1-5"}
-- pgsql_params:{"pi0":"S-1-5"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (n0.kind_ids operator (pg_catalog.@>) array [80]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [81]::int2[]) and (not n0.properties ? 'name' or (n0.properties -> 'name') = ('null')::jsonb) and cypher_starts_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool)) select (s0.n0).id from s0;

