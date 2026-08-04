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

-- case: match ()-[r:RegressionKind01]->(e:RegressionKind31) where e.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-01"}
-- pgsql_params:{"pi0":"rec-01"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind01|RegressionKind02]->(e:RegressionKind31) where e.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-01"}
-- pgsql_params:{"pi0":"rec-01"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09]->(e:RegressionKind31) where e.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-01"}
-- pgsql_params:{"pi0":"rec-01"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09|RegressionKind10|RegressionKind11|RegressionKind12|RegressionKind13|RegressionKind14|RegressionKind15|RegressionKind16|RegressionKind17|RegressionKind18|RegressionKind19|RegressionKind20|RegressionKind21|RegressionKind22|RegressionKind23|RegressionKind24|RegressionKind25|RegressionKind26|RegressionKind27|RegressionKind28|RegressionKind29|RegressionKind30]->(e:RegressionKind31) where e.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-01"}
-- pgsql_params:{"pi0":"rec-01"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind31)-[r:RegressionKind01]->() where s.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-02"}
-- pgsql_params:{"pi0":"rec-02"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind31)-[r:RegressionKind01|RegressionKind02]->() where s.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-02"}
-- pgsql_params:{"pi0":"rec-02"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind31)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09]->() where s.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-02"}
-- pgsql_params:{"pi0":"rec-02"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind31)-[r:RegressionKind01|RegressionKind02|RegressionKind03|RegressionKind04|RegressionKind05|RegressionKind06|RegressionKind07|RegressionKind08|RegressionKind09|RegressionKind10|RegressionKind11|RegressionKind12|RegressionKind13|RegressionKind14|RegressionKind15|RegressionKind16|RegressionKind17|RegressionKind18|RegressionKind19|RegressionKind20|RegressionKind21|RegressionKind22|RegressionKind23|RegressionKind24|RegressionKind25|RegressionKind26|RegressionKind27|RegressionKind28|RegressionKind29|RegressionKind30]->() where s.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-02"}
-- pgsql_params:{"pi0":"rec-02"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind32]->(e:RegressionKind31) where e.objectid = $object_id and r.isprimarygroup = $flag delete r
-- cypher_params: {"flag":false,"object_id":"rec-03-in"}
-- pgsql_params:{"pi0":"rec-03-in","pi1":false}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where (((e0.properties -> 'isprimarygroup'))::jsonb = to_jsonb((@pi1::bool)::bool)::jsonb) and e0.kind_id = any (array [64]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind31)-[r:RegressionKind32]->() where s.objectid = $object_id and r.isprimarygroup = $flag delete r
-- cypher_params: {"flag":true,"object_id":"rec-03-out"}
-- pgsql_params:{"pi0":"rec-03-out","pi1":true}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties -> 'isprimarygroup'))::jsonb = to_jsonb((@pi1::bool)::bool)::jsonb) and e0.kind_id = any (array [64]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind32]->(e:RegressionKind31) where e.objectid in $object_ids delete r
-- cypher_params: {"object_ids":["rec-04-a","rec-04-b"]}
-- pgsql_params:{"pi0":["rec-04-a","rec-04-b"]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((n1.properties ->> 'objectid') = any (@pi0::text[])) and n1.kind_ids operator (pg_catalog.@>) array [63]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [64]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind34]->(e:RegressionKind33) where e.objectid in $object_ids delete r
-- cypher_params: {"object_ids":["rec-04-azure-a","rec-04-azure-b"]}
-- pgsql_params:{"pi0":["rec-04-azure-a","rec-04-azure-b"]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((n1.properties ->> 'objectid') = any (@pi0::text[])) and n1.kind_ids operator (pg_catalog.@>) array [65]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [66]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (s:RegressionKind35)-[r:RegressionKind36]->(e) where e.objectid in $ca_ids return r, s
-- cypher_params: {"ca_ids":["ca-a","ca-b"]}
-- pgsql_params:{"pi0":["ca-a","ca-b"]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((n1.properties ->> 'objectid') = any (@pi0::text[])) and n1.id = e0.end_id join node n0 on n0.kind_ids operator (pg_catalog.@>) array [67]::int2[] and n0.id = e0.start_id where e0.kind_id = any (array [68]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match ()-[r:RegressionKind37]->(e:RegressionKind35) where id(e) in $template_ids delete r
-- cypher_params: {"template_ids":[101,202]}
-- pgsql_params:{"pi0":[101,202]}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.kind_ids operator (pg_catalog.@>) array [67]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [69]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match ()-[r:RegressionKind39]->(e:RegressionKind38) where e.objectid = $object_id delete r
-- cypher_params: {"object_id":"rec-07"}
-- pgsql_params:{"pi0":"rec-07"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on ((jsonb_typeof((n1.properties -> 'objectid')) = 'string' and (n1.properties ->> 'objectid') = @pi0::text)) and n1.kind_ids operator (pg_catalog.@>) array [70]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [71]::int2[])), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;

-- case: match (n:RegressionKind31) where n.objectid in $object_ids detach delete n
-- cypher_params: {"object_ids":["rec-08-a","rec-08-b"]}
-- pgsql_params:{"pi0":["rec-08-a","rec-08-b"]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> 'objectid') = any (@pi0::text[])) and n0.kind_ids operator (pg_catalog.@>) array [63]::int2[]), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select 1;

