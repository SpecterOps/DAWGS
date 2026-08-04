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

