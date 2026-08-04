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

-- case: match ()-[r]->() return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select s0.e0 as r from s0;

-- case: match ()-[r]->() where type(r) = 'EdgeKind1' return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.kind_id = 3)) select s0.e0 as r from s0;

-- case: match ()-[r]->() return type(r) order by type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select kind_name((s0.e0).kind_id)::text from s0 order by kind_name((s0.e0).kind_id)::text;

-- case: match ()-[r]->() where type(r) <> 'EdgeKind1' return type(r) order by type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.kind_id <> 3)) select kind_name((s0.e0).kind_id)::text from s0 order by kind_name((s0.e0).kind_id)::text;

-- case: match ()-[r]->() where type(r) in ['EdgeKind2'] return type(r) order by type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (kind_name(e0.kind_id)::text = any (array ['EdgeKind2']::text[]))) select kind_name((s0.e0).kind_id)::text from s0 order by kind_name((s0.e0).kind_id)::text;

-- case: match ()-[r]->() where type(r) STARTS WITH 'EdgeKind' return type(r) order by type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (kind_name(e0.kind_id)::text like 'EdgeKind%')) select kind_name((s0.e0).kind_id)::text from s0 order by kind_name((s0.e0).kind_id)::text;

-- case: match ()-[r]->() where 'EdgeKind1' = type(r) return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (3 = e0.kind_id)) select s0.e0 as r from s0;

-- case: match (n), ()-[r]->() return n, r
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.n0 as n0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id) select s1.n0 as n, s1.e0 as r from s1;

-- case: match ()-[r]->(), ()-[e]->() return r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1 from s0, edge e1 join node n2 on n2.id = e1.start_id join node n3 on n3.id = e1.end_id) select s1.e0 as r, s1.e1 as e from s1;

-- case: match p = (:NodeKind1)-[:EdgeKind1|EdgeKind2]->(c:NodeKind2) where '123' in c.prop2 or '243' in c.prop2 or size(c.prop2) = 0 return p limit 10
with s0 as (select e0.id as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on ('123' = any (jsonb_to_text_array((n1.properties -> 'prop2'))::text[]) or '243' = any (jsonb_to_text_array((n1.properties -> 'prop2'))::text[]) or jsonb_array_length((n1.properties -> 'prop2'))::int = 0) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[]) limit 10) select case when (s0.n0).id is null or s0.e0 is null or (s0.n1).id is null then null else ordered_edges_to_path(s0.n0, (select coalesce(array_agg((_edge.id, _edge.start_id, _edge.end_id, _edge.kind_id, _edge.properties)::edgecomposite order by _path.ordinality), array []::edgecomposite[]) from unnest(array [s0.e0]::int8[]) with ordinality as _path(id, ordinality) join edge _edge on _edge.id = _path.id), array [s0.n0, s0.n1]::nodecomposite[])::pathcomposite end as p from s0 limit 10;

-- case: match ()-[r:EdgeKind1]->() return count(r) as the_count
select count(*)::int8 as the_count from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[]);

-- case: match ()-[r:EdgeKind1]->() return count(r) as the_count limit 1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select count(s0.e0)::int8 as the_count from s0 limit 1;

-- case: match ()-[r:EdgeKind1]->({name: "123"}) return count(r) as the_count
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n1 on (jsonb_typeof((n1.properties -> 'name')) = 'string' and (n1.properties ->> 'name') = '123') and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [3]::int2[])) select count(s0.e0)::int8 as the_count from s0;

-- case: match (s)-[r:RegressionKind01]->(e) where id(s) = $start_id return r, e
-- cypher_params: {"start_id":101}
-- pgsql_params:{"pi0":101}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01]->(e) where id(s) in $start_ids return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01]->(e) where id(e) = $end_id return r, s
-- cypher_params: {"end_id":202}
-- pgsql_params:{"pi0":202}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = @pi0::float8) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02]->(e) where id(s) in $start_ids return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02]->(e) where id(e) in $end_ids return r, s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05]->(e) where id(s) in $start_ids return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34, 35, 36, 37]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05]->(e) where id(e) in $end_ids return r, s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34, 35, 36, 37]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09]->(e) where id(s) in $start_ids return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09]->(e) where id(e) in $end_ids return r, s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09|RegressionKind10|RegressionKind11|RegressionKind12|RegressionKind13|RegressionKind14|RegressionKind15|RegressionKind16|RegressionKind17|RegressionKind18|RegressionKind19|RegressionKind20|RegressionKind21|RegressionKind22|RegressionKind23|RegressionKind24|RegressionKind25|RegressionKind26|RegressionKind27|RegressionKind28|RegressionKind29|RegressionKind30]->(e) where id(s) in $start_ids return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09|RegressionKind10|RegressionKind11|RegressionKind12|RegressionKind13|RegressionKind14|RegressionKind15|RegressionKind16|RegressionKind17|RegressionKind18|RegressionKind19|RegressionKind20|RegressionKind21|RegressionKind22|RegressionKind23|RegressionKind24|RegressionKind25|RegressionKind26|RegressionKind27|RegressionKind28|RegressionKind29|RegressionKind30]->(e) where id(e) in $end_ids return r, s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind51]->(e) where id(s) in $start_ids and (e:RegressionKind52 or e:RegressionKind53) return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on ((n1.kind_ids operator (pg_catalog.@>) array [84]::int2[] or n1.kind_ids operator (pg_catalog.@>) array [85]::int2[])) and n1.id = e0.end_id where e0.kind_id = any (array [83]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind54]->(e) where id(s) = $start_id and id(e) in $end_ids return r, e
-- cypher_params: {"end_ids":[202,303],"start_id":101}
-- pgsql_params:{"pi0":101,"pi1":[202,303]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on (n1.id = any (@pi1::float8[])) and n1.id = e0.end_id where e0.kind_id = any (array [86]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind55]->(e) where id(s) = $start_id and e.enabled = $enabled and e.score = $score and e.name = $name and e.isassignabletorole = $role_value return r, e
-- cypher_params: {"enabled":true,"name":"target","role_value":"true","score":7,"start_id":101}
-- pgsql_params:{"pi0":101,"pi1":true,"pi2":7,"pi3":"target","pi4":"true"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (((n1.properties -> 'enabled'))::jsonb = to_jsonb((@pi1::bool)::bool)::jsonb and ((n1.properties -> 'score'))::jsonb = to_jsonb((@pi2::float8)::float8)::jsonb and (jsonb_typeof((n1.properties -> 'name')) = 'string' and (n1.properties ->> 'name') = @pi3::text) and (jsonb_typeof((n1.properties -> 'isassignabletorole')) = 'string' and (n1.properties ->> 'isassignabletorole') = @pi4::text)) and n1.id = e0.end_id join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id where e0.kind_id = any (array [87]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind56]->(e:RegressionKind57) where id(s) = $start_id and ((e.requiresmanagerapproval = false and e.schemaversion > 1 and e.authorizedsignatures = 0 and e.authenticationenabled = true) or (e.requiresmanagerapproval = false and e.schemaversion = 1 and e.authenticationenabled = true)) return r, e
-- cypher_params: {"start_id":101}
-- pgsql_params:{"pi0":101}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on (((((n1.properties -> 'requiresmanagerapproval'))::jsonb = to_jsonb((false)::bool)::jsonb and ((n1.properties ->> 'schemaversion'))::int8 > 1 and ((n1.properties -> 'authorizedsignatures'))::jsonb = to_jsonb((0)::int8)::jsonb and ((n1.properties -> 'authenticationenabled'))::jsonb = to_jsonb((true)::bool)::jsonb) or (((n1.properties -> 'requiresmanagerapproval'))::jsonb = to_jsonb((false)::bool)::jsonb and ((n1.properties -> 'schemaversion'))::jsonb = to_jsonb((1)::int8)::jsonb and ((n1.properties -> 'authenticationenabled'))::jsonb = to_jsonb((true)::bool)::jsonb))) and n1.kind_ids operator (pg_catalog.@>) array [89]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [88]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind58]->(e) where id(s) = $start_id and (e.schannelauthenticationenabled = true or size(e.effectiveekus) = 0 or $eku in e.effectiveekus) return r, e
-- cypher_params: {"eku":"1.3.6.1.5.5.7.3.2","start_id":101}
-- pgsql_params:{"pi0":101,"pi1":"1.3.6.1.5.5.7.3.2"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on ((((n1.properties -> 'schannelauthenticationenabled'))::jsonb = to_jsonb((true)::bool)::jsonb or jsonb_array_length((n1.properties -> 'effectiveekus'))::int = 0 or @pi1::text = any (jsonb_to_text_array((n1.properties -> 'effectiveekus'))::text[]))) and n1.id = e0.end_id where e0.kind_id = any (array [90]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind59]->(e) where id(s) in $start_ids and id(e) in $end_ids return r, e
-- cypher_params: {"end_ids":[303,404],"start_ids":[101,202]}
-- pgsql_params:{"pi0":[101,202],"pi1":[303,404]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on (n1.id = any (@pi1::float8[])) and n1.id = e0.end_id where e0.kind_id = any (array [91]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind60]->(e:RegressionKind52) where id(s) in $start_ids and e.active = true return r, e
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on (((n1.properties -> 'active'))::jsonb = to_jsonb((true)::bool)::jsonb) and n1.kind_ids operator (pg_catalog.@>) array [84]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [92]::int2[])) select s0.e0 as r, s0.n1 as e from s0;

-- case: match (s:RegressionKind51)-[r:RegressionKind60]->(e) where id(e) in $end_ids and s.active = true return r, s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on (((n0.properties -> 'active'))::jsonb = to_jsonb((true)::bool)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [83]::int2[] and n0.id = e0.start_id where e0.kind_id = any (array [92]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind60]->(e) where id(e) in $end_ids return s
-- cypher_params: {"end_ids":[202]}
-- pgsql_params:{"pi0":[202]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [92]::int2[])) select s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind60]->(e) where id(s) in $start_ids return id(e), r
-- cypher_params: {"start_ids":[101]}
-- pgsql_params:{"pi0":[101]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = any (@pi0::float8[])) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [92]::int2[])) select (s0.n1).id, s0.e0 as r from s0;

-- case: match (s)-[r]->(e) where id(e) = $a and not (id(s) = $b) and (r:EdgeKind1 or r:EdgeKind2) and not (s.objectid ends with $c or e.objectid ends with $d) return distinct id(s), id(r), id(e)
-- cypher_params: {"a":1,"b":2,"c":"123","d":"456"}
-- pgsql_params:{"pi0":1,"pi1":2,"pi2":"123","pi3":"456"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on n1.id = e0.end_id join node n0 on (not (n0.id = @pi1::float8)) and n0.id = e0.start_id where ((e0.kind_id = any (array [3]::int2[]) or e0.kind_id = any (array [4]::int2[]))) and (not (cypher_ends_with((n0.properties ->> 'objectid'), (@pi2::text)::text)::bool or cypher_ends_with((n1.properties ->> 'objectid'), (@pi3::text)::text)::bool) and n1.id = @pi0::float8)) select distinct (s0.n0).id, (s0.e0).id, (s0.n1).id from s0;

-- case: match (s)-[r]->(e) where s.name = '123' and e:NodeKind1 and not r.property return s, r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '123')) and n0.id = e0.start_id join node n1 on (n1.kind_ids operator (pg_catalog.@>) array [1]::int2[]) and n1.id = e0.end_id where (not ((e0.properties ->> 'property'))::bool)) select s0.n0 as s, s0.e0 as r, s0.n1 as e from s0;

-- case: match ()-[r]->() where r.value = 42 return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties -> 'value'))::jsonb = to_jsonb((42)::int8)::jsonb)) select s0.e0 as r from s0;

-- case: match ()-[r]->() where r.bool_prop return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties ->> 'bool_prop'))::bool)) select s0.e0 as r from s0;

-- case: match (n)-[r]->() where n.name = '123' return n, r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '123')) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select s0.n0 as n, s0.e0 as r from s0;

-- case: match (n:NodeKind1)-[r]->() where n.name = '123' or n.name = '321' or n.name = '222' or n.name = '333' return n, r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '123') or (jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '321') or (jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '222') or (jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '333')) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select s0.n0 as n, s0.e0 as r from s0;

-- case: match (s)-[r]->(e) where s.name = '123' and e.name = '321' return s, r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = '123')) and n0.id = e0.start_id join node n1 on ((jsonb_typeof((n1.properties -> 'name')) = 'string' and (n1.properties ->> 'name') = '321')) and n1.id = e0.end_id) select s0.n0 as s, s0.e0 as r, s0.n1 as e from s0;

-- case: match (f), (s)-[r]->(e) where not f.bool_field and s.name = '123' and e.name = '321' return f, s, r, e
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties ->> 'bool_field'))::bool)), s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0, edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'name')) = 'string' and (n1.properties ->> 'name') = '123')) and n1.id = e0.start_id join node n2 on ((jsonb_typeof((n2.properties -> 'name')) = 'string' and (n2.properties ->> 'name') = '321')) and n2.id = e0.end_id) select s1.n0 as f, s1.n1 as s, s1.e0 as r, s1.n2 as e from s1;

-- case: match ()-[e0]->(n)<-[e1]-() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.end_id join node n2 on n2.id = e1.start_id where e1.id != (s0.e0).id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match ()-[e0]->(n)-[e1]->() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id != (s0.e0).id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match ()<-[e0]-(n)<-[e1]-() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.end_id join node n1 on n1.id = e0.start_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.end_id join node n2 on n2.id = e1.start_id where e1.id != (s0.e0).id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match (s)<-[r:EdgeKind1|EdgeKind2]-(e) return s.name, e.name
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.end_id join node n1 on n1.id = e0.start_id where e0.kind_id = any (array [3, 4]::int2[])) select ((s0.n0).properties -> 'name'), ((s0.n1).properties -> 'name') from s0;

-- case: match (s)-[:EdgeKind1|EdgeKind2]->(e)-[:EdgeKind1]->() return s.name as s_name, e.name as e_name
with s0 as (select e0.id as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[])), s1 as (select s0.e0 as e0, s0.n0 as n0, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.kind_id = any (array [3]::int2[]) and e1.id != s0.e0) select ((s1.n0).properties -> 'name') as s_name, ((s1.n1).properties -> 'name') as e_name from s1;

-- case: match (s:NodeKind1)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2) return s.name, e.name
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[])) select ((s0.n0).properties -> 'name'), ((s0.n1).properties -> 'name') from s0;

-- case: match (s)-[r:EdgeKind1]->() where (s)-[r {prop: 'a'}]->() return s
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (jsonb_typeof((e0.properties -> 'prop')) = 'string' and (e0.properties ->> 'prop') = 'a') and e0.kind_id = any (array [3]::int2[])) select s0.n0 as s from s0 where ((with s1 as (select s0.e0 as e0, s0.n0 as n0 from edge e0 join node n2 on n2.id = (s0.e0).end_id where (s0.n0).id = (s0.e0).start_id) select count(*) > 0 from s1));

-- case: match (s)-[r:EdgeKind1]->(e) where not (s.system_tags contains 'admin_tier_0') and id(e) = 1 return id(s), labels(s), id(r), type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = 1) and n1.id = e0.end_id join node n0 on (not (coalesce((n0.properties ->> 'system_tags'), '')::text like '%admin\_tier\_0%')) and n0.id = e0.start_id where e0.kind_id = any (array [3]::int2[])) select (s0.n0).id, (array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[], (s0.e0).id, kind_name((s0.e0).kind_id)::text from s0;

-- case: match (s)-[r]->(e) where s:NodeKind1 and toLower(s.name) starts with 'test' and r:EdgeKind1 and id(e) in [1, 2] return r limit 1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and lower((n0.properties ->> 'name'))::text like 'test%') and n0.id = e0.start_id join node n1 on (n1.id = any (array [1, 2]::int8[])) and n1.id = e0.end_id where (e0.kind_id = any (array [3]::int2[])) limit 1) select s0.e0 as r from s0 limit 1;

-- case: match (n1)-[]->(n2) where n1 <> n2 return n2
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n0.id <> n1.id)) select s0.n1 as n2 from s0;

-- case: match (n1)-[]->(n2) where n2 <> n1 return n2
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.id <> n0.id)) select s0.n1 as n2 from s0;

-- case: match ()-[r]->()-[e]->(n) where r <> e return n
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where ((s0.e0).id <> e1.id) and e1.id != (s0.e0).id) select s1.n2 as n from s1;

-- case: match (s:NodeKind1:NodeKind2)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2:NodeKind1) return s.name, e.name
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [1, 2]::int2[] and n0.id = e0.start_id join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2, 1]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[])) select ((s0.n0).properties -> 'name'), ((s0.n1).properties -> 'name') from s0;

