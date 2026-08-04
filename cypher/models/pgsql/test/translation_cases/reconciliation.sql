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

-- case: match (s)-[r]->(e) where (id(s) = $forward_start and id(e) = $forward_end and r:RegressionKind01) or (id(s) = $forward_end and id(e) = $forward_start and r:RegressionKind02) return id(r)
-- cypher_params: {"forward_end":202,"forward_start":101}
-- pgsql_params:{"pi0":101,"pi1":202}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on n1.id = e0.end_id join node n0 on n0.id = e0.start_id where ((n0.id = @pi0::float8 and n1.id = @pi1::float8 and e0.kind_id = any (array [33]::int2[])) or (n0.id = @pi1::float8 and n1.id = @pi0::float8 and e0.kind_id = any (array [34]::int2[])))) select (s0.e0).id from s0;

-- case: match (s:RegressionKind03)-[r:RegressionKind04]->(e:RegressionKind03) where r.lastseen < s.lastcollected or r.lastseen < e.lastcollected return id(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [35]::int2[] and n0.id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [35]::int2[] and n1.id = e0.end_id where ((e0.properties -> 'lastseen') < (n0.properties -> 'lastcollected') or (e0.properties -> 'lastseen') < (n1.properties -> 'lastcollected')) and e0.kind_id = any (array [36]::int2[])) select (s0.e0).id from s0;

-- case: match (s:RegressionKind05)-[r:RegressionKind06]->(e:RegressionKind07) where e.objectid = $object_id and r.shoulddelete = $should_delete delete r
-- cypher_params: {"object_id":"delete-edge","should_delete":true}
-- pgsql_params:{"pi0":"delete-edge","pi1":true}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [39]::int2[] and n1.id = e0.end_id join node n0 on n0.kind_ids operator (pg_catalog.@>) array [37]::int2[] and n0.id = e0.start_id where (((e0.properties -> 'shoulddelete'))::jsonb = to_jsonb((@pi1::bool)::bool)::jsonb) and e0.kind_id = any (array [38]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (n:RegressionKind08) where n.objectid = $object_id detach delete n
-- cypher_params: {"object_id":"delete-node"}
-- pgsql_params:{"pi0":"delete-node"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [40]::int2[]), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select 1;

-- case: match ()-[r:RegressionKind09]->(e) return r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [41]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match ()-[r:RegressionKind09]->(e) return id(e), labels(e), id(r), type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [41]::int2[])) select (s0.n1).id, (array(select _kind.name from generate_subscripts((s0.n1).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n1).kind_ids)[_kind_idx] order by _kind_idx))::text[], (s0.e0).id, kind_name((s0.e0).kind_id)::text from s0;

-- case: match (s)-[r:RegressionKind09]->(e) return s, r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [41]::int2[])) select s0.n0 as s, s0.e0 as r, s0.n1 as e from s0;

-- case: match ()-[r:RegressionKind09]->() return id(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [41]::int2[])) select (s0.e0).id from s0;

-- case: match ()-[r:RegressionKind09]->() return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [41]::int2[])) select s0.e0 as r from s0;

