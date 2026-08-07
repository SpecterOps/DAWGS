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

-- case: match (s)-[r:RegressionKind63]->(e) where (s:RegressionKind61 or s:RegressionKind62) and (e:RegressionKind61 or e:RegressionKind62) return id(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on ((n0.kind_ids operator (pg_catalog.@>) array [93]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [94]::int2[])) and n0.id = e0.start_id join node n1 on ((n1.kind_ids operator (pg_catalog.@>) array [93]::int2[] or n1.kind_ids operator (pg_catalog.@>) array [94]::int2[])) and n1.id = e0.end_id where e0.kind_id = any (array [95]::int2[])) select (s0.e0).id as "id(r)" from s0;

-- case: match (s)-[r:RegressionKind66|RegressionKind67]->(e) where not (s:RegressionKind64 or s:RegressionKind65) and not (e:RegressionKind64 or e:RegressionKind65) return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (not (n0.kind_ids operator (pg_catalog.@>) array [96]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [97]::int2[])) and n0.id = e0.start_id join node n1 on (not (n1.kind_ids operator (pg_catalog.@>) array [96]::int2[] or n1.kind_ids operator (pg_catalog.@>) array [97]::int2[])) and n1.id = e0.end_id where e0.kind_id = any (array [98, 99]::int2[])) select s0.e0 as r from s0;

-- case: match (s)-[r:RegressionKind68]->(e) where not (s:RegressionKind64 or s:RegressionKind65) and r.lastseen is not null and not (e:RegressionKind64 or e:RegressionKind65) return id(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (not (n0.kind_ids operator (pg_catalog.@>) array [96]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [97]::int2[])) and n0.id = e0.start_id join node n1 on (not (n1.kind_ids operator (pg_catalog.@>) array [96]::int2[] or n1.kind_ids operator (pg_catalog.@>) array [97]::int2[])) and n1.id = e0.end_id where ((e0.properties ? 'lastseen' and not (e0.properties -> 'lastseen') = ('null')::jsonb)) and e0.kind_id = any (array [100]::int2[])) select (s0.e0).id as "id(r)" from s0;

-- case: match (s:RegressionKind69)-[r:RegressionKind70]->() return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [101]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [102]::int2[])) select s0.e0 as r from s0;

-- case: match (s:RegressionKind69)-[r:RegressionKind71]->() return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.kind_ids operator (pg_catalog.@>) array [101]::int2[] and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [103]::int2[])) select s0.e0 as r from s0;

-- case: match (s:RegressionKind69)-[r:RegressionKind72]->(e) where id(e) = $end_id return r, s
-- cypher_params: {"end_id":202}
-- pgsql_params:{"pi0":202}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = @pi0::float8) and n1.id = e0.end_id join node n0 on n0.kind_ids operator (pg_catalog.@>) array [101]::int2[] and n0.id = e0.start_id where e0.kind_id = any (array [104]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s:RegressionKind69)-[r:RegressionKind72|RegressionKind73|RegressionKind74|RegressionKind75|RegressionKind76|RegressionKind77|RegressionKind78|RegressionKind79|RegressionKind80]->(e) where id(e) = $end_id return r, s
-- cypher_params: {"end_id":202}
-- pgsql_params:{"pi0":202}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = @pi0::float8) and n1.id = e0.end_id join node n0 on n0.kind_ids operator (pg_catalog.@>) array [101]::int2[] and n0.id = e0.start_id where e0.kind_id = any (array [104, 105, 106, 107, 108, 109, 110, 111, 112]::int2[])) select s0.e0 as r, s0.n0 as s from s0;

-- case: match (s)-[r:RegressionKind82]->(e:RegressionKind81) return id(s), id(r), type(r), id(e)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, n0.id as n0, n1.id as n1 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [113]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id where e0.kind_id = any (array [114]::int2[])) select s0.n0 as "id(s)", (s0.e0).id as "id(r)", kind_name((s0.e0).kind_id)::text as "type(r)", s0.n1 as "id(e)" from s0;

-- case: match (s)-[r:RegressionKind83]->(e) return id(s), id(e)
with s0 as (select n0.id as n0, n1.id as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [115]::int2[])) select s0.n0 as "id(s)", s0.n1 as "id(e)" from s0;

-- case: match (s)-[r:RegressionKind83|RegressionKind84]->(e) return id(s), id(e)
with s0 as (select n0.id as n0, n1.id as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [115, 116]::int2[])) select s0.n0 as "id(s)", s0.n1 as "id(e)" from s0;

-- case: match (s)-[r:RegressionKind87|RegressionKind88|RegressionKind89|RegressionKind90|RegressionKind91|RegressionKind92]->(e) where (s:RegressionKind85 or s:RegressionKind86 or s:RegressionKind81) and id(e) in $end_ids return id(s)
-- cypher_params: {"end_ids":[202,303]}
-- pgsql_params:{"pi0":[202,303]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.id = e0.end_id join node n0 on ((n0.kind_ids operator (pg_catalog.@>) array [117]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [118]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [113]::int2[])) and n0.id = e0.start_id where e0.kind_id = any (array [119, 120, 121, 122, 123, 124]::int2[])) select (s0.n0).id as "id(s)" from s0;

-- case: match (s)-[r:RegressionKind87|RegressionKind88|RegressionKind89|RegressionKind90|RegressionKind91]->(e:RegressionKind81) where (s:RegressionKind85 or s:RegressionKind86 or s:RegressionKind81) and id(e) in $end_ids return id(s)
-- cypher_params: {"end_ids":[202,303]}
-- pgsql_params:{"pi0":[202,303]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = any (@pi0::float8[])) and n1.kind_ids operator (pg_catalog.@>) array [113]::int2[] and n1.id = e0.end_id join node n0 on ((n0.kind_ids operator (pg_catalog.@>) array [117]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [118]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [113]::int2[])) and n0.id = e0.start_id where e0.kind_id = any (array [119, 120, 121, 122, 123]::int2[])) select (s0.n0).id as "id(s)" from s0;

-- case: match (n) where n:RegressionKind85 or n:RegressionKind86 return id(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.@>) array [117]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [118]::int2[])) select (s0.n0).id as "id(n)" from s0;

-- case: match (n:RegressionKind93) return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [125]::int2[]) select s0.n0 as n from s0;

-- case: match (n:RegressionKind81) where n.objectid = $objectid return n limit 1
-- cypher_params: {"objectid":"S-1-5-21"}
-- pgsql_params:{"pi0":"S-1-5-21"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text)) and n0.kind_ids operator (pg_catalog.@>) array [113]::int2[]) select s0.n0 as n from s0 limit 1;

-- case: match (n) where n.name = $name and n.enabled = $enabled return id(n)
-- cypher_params: {"enabled":true,"name":"dc.example.test"}
-- pgsql_params:{"pi0":"dc.example.test","pi1":true}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = @pi0::text) and ((n0.properties -> 'enabled'))::jsonb = to_jsonb((@pi1::bool)::bool)::jsonb)) select (s0.n0).id as "id(n)" from s0;

-- case: match (n:RegressionKind81) where n.hasura = true return id(n), n.hasura
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> 'hasura'))::jsonb = to_jsonb((true)::bool)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [113]::int2[]) select (s0.n0).id as "id(n)", ((s0.n0).properties -> 'hasura') as "n.hasura" from s0;

-- case: match (n:RegressionKind94) where n.distinguishedname starts with $prefix and n.domainsid = $domain return n
-- cypher_params: {"domain":"S-1-5-21","prefix":"CN=ADMINSDHOLDER,CN=SYSTEM,"}
-- pgsql_params:{"pi0":"CN=ADMINSDHOLDER,CN=SYSTEM,","pi1":"S-1-5-21"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_starts_with((n0.properties ->> 'distinguishedname'), (@pi0::text)::text)::bool and (jsonb_typeof((n0.properties -> 'domainsid')) = 'string' and (n0.properties ->> 'domainsid') = @pi1::text)) and n0.kind_ids operator (pg_catalog.@>) array [126]::int2[]) select s0.n0 as n from s0;

-- case: match (n:RegressionKind85) where n.objectid ends with $suffix_a or n.objectid ends with $suffix_b return id(n)
-- cypher_params: {"suffix_a":"-S-1","suffix_b":"-S-2"}
-- pgsql_params:{"pi0":"-S-1","pi1":"-S-2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_ends_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool or cypher_ends_with((n0.properties ->> 'objectid'), (@pi1::text)::text)::bool) and n0.kind_ids operator (pg_catalog.@>) array [117]::int2[]) select (s0.n0).id as "id(n)" from s0;

-- case: match (n) where toLower(n.name) starts with $prefix return id(n)
-- cypher_params: {"prefix":"remote desktop users%_"}
-- pgsql_params:{"pi0":"remote desktop users%_"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_starts_with((lower((n0.properties ->> 'name'))::text)::text, (@pi0::text)::text)::bool)) select (s0.n0).id as "id(n)" from s0;

-- case: match (n) where toLower(n.objectid) contains $fragment return n
-- cypher_params: {"fragment":"approver_guid"}
-- pgsql_params:{"pi0":"approver_guid"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_contains((lower((n0.properties ->> 'objectid'))::text)::text, (@pi0::text)::text)::bool)) select s0.n0 as n from s0;

-- case: match (n) where (n:RegressionKind85 or n:RegressionKind86) and n:RegressionKind69 and n.objectid ends with $suffix and n.domainsid = $domain return n
-- cypher_params: {"domain":"S-1-5-21","suffix":"-512"}
-- pgsql_params:{"pi0":"-512","pi1":"S-1-5-21"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.kind_ids operator (pg_catalog.@>) array [117]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [118]::int2[]) and n0.kind_ids operator (pg_catalog.@>) array [101]::int2[] and cypher_ends_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool and (jsonb_typeof((n0.properties -> 'domainsid')) = 'string' and (n0.properties ->> 'domainsid') = @pi1::text))) select s0.n0 as n from s0;

-- case: match (n:RegressionKind69) where not (n:RegressionKind85 or n:RegressionKind98) and n.objectid ends with $suffix return n
-- cypher_params: {"suffix":"-512"}
-- pgsql_params:{"pi0":"-512"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (n0.kind_ids operator (pg_catalog.@>) array [117]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [130]::int2[]) and cypher_ends_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool) and n0.kind_ids operator (pg_catalog.@>) array [101]::int2[]) select s0.n0 as n from s0;

-- case: match (n) where n.name is null return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((not n0.properties ? 'name' or (n0.properties -> 'name') = ('null')::jsonb))) select s0.n0 as n from s0;

-- case: match (n:RegressionKind95) where n.tenantid = $tenant and n.approvalrequired = true and (n.userapprovers is not null or n.groupapprovers is not null) return n
-- cypher_params: {"tenant":"tenant-1"}
-- pgsql_params:{"pi0":"tenant-1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'tenantid')) = 'string' and (n0.properties ->> 'tenantid') = @pi0::text) and ((n0.properties -> 'approvalrequired'))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties ? 'userapprovers' and not (n0.properties -> 'userapprovers') = ('null')::jsonb) or (n0.properties ? 'groupapprovers' and not (n0.properties -> 'groupapprovers') = ('null')::jsonb))) and n0.kind_ids operator (pg_catalog.@>) array [127]::int2[]) select s0.n0 as n from s0;

-- case: match (n) where id(n) in $ids return n
-- cypher_params: {"ids":[101,202,101]}
-- pgsql_params:{"pi0":[101,202,101]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = any (@pi0::float8[]))) select s0.n0 as n from s0;

-- case: match (n:RegressionKind86) where not (n.gmsa is not null and n.gmsa = true) and not (n.msa is not null and n.msa = true) and id(n) in $ids return n
-- cypher_params: {"ids":[101,202]}
-- pgsql_params:{"pi0":[101,202]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties ? 'gmsa' and not (n0.properties -> 'gmsa') = ('null')::jsonb) and ((n0.properties -> 'gmsa'))::jsonb = to_jsonb((true)::bool)::jsonb) and not ((n0.properties ? 'msa' and not (n0.properties -> 'msa') = ('null')::jsonb) and ((n0.properties -> 'msa'))::jsonb = to_jsonb((true)::bool)::jsonb) and n0.id = any (@pi0::float8[])) and n0.kind_ids operator (pg_catalog.@>) array [118]::int2[]) select s0.n0 as n from s0;

-- case: match (s)-[:RegressionKind97]->(e) where id(s) = $tenant_id and (e:RegressionKind95 or e:RegressionKind96) and e.roletemplateid in $role_ids return e
-- cypher_params: {"role_ids":["role-a","role-b"],"tenant_id":101}
-- pgsql_params:{"pi0":101,"pi1":["role-a","role-b"]}
with s0 as (select n0.id as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on ((n1.kind_ids operator (pg_catalog.@>) array [127]::int2[] or n1.kind_ids operator (pg_catalog.@>) array [128]::int2[]) and (n1.properties ->> 'roletemplateid') = any (@pi1::text[])) and n1.id = e0.end_id where e0.kind_id = any (array [129]::int2[])) select s0.n1 as e from s0;

-- case: match (s)-[:RegressionKind97]->(e:RegressionKind95) where id(s) = $tenant_id and e.enabled = true return e
-- cypher_params: {"tenant_id":101}
-- pgsql_params:{"pi0":101}
with s0 as (select n0.id as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on (((n1.properties -> 'enabled'))::jsonb = to_jsonb((true)::bool)::jsonb) and n1.kind_ids operator (pg_catalog.@>) array [127]::int2[] and n1.id = e0.end_id where e0.kind_id = any (array [129]::int2[])) select s0.n1 as e from s0;

-- case: match (s)-[r:RegressionKind83]->(e) where id(s) = $start_id and id(e) = $end_id return r limit 1
-- cypher_params: {"end_id":202,"start_id":101}
-- pgsql_params:{"pi0":101,"pi1":202}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, n0.id as n0, n1.id as n1 from edge e0 join node n0 on (n0.id = @pi0::float8) and n0.id = e0.start_id join node n1 on (n1.id = @pi1::float8) and n1.id = e0.end_id where e0.kind_id = any (array [115]::int2[]) limit 1) select s0.e0 as r from s0 limit 1;

-- case: match (s)-[:RegressionKind82]->(e) where s.objectid ends with $suffix and id(e) = $end_id return s
-- cypher_params: {"end_id":202,"suffix":"-555"}
-- pgsql_params:{"pi0":"-555","pi1":202}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = @pi1::float8) and n1.id = e0.end_id join node n0 on (cypher_ends_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool) and n0.id = e0.start_id where e0.kind_id = any (array [114]::int2[])) select s0.n0 as s from s0;

-- case: match (s)-[:RegressionKind82]->(e) where s.objectid ends with $suffix and id(e) = $end_id return id(s)
-- cypher_params: {"end_id":202,"suffix":"-555"}
-- pgsql_params:{"pi0":"-555","pi1":202}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, n1.id as n1 from edge e0 join node n1 on (n1.id = @pi1::float8) and n1.id = e0.end_id join node n0 on (cypher_ends_with((n0.properties ->> 'objectid'), (@pi0::text)::text)::bool) and n0.id = e0.start_id where e0.kind_id = any (array [114]::int2[])) select (s0.n0).id as "id(s)" from s0;

-- case: match (n:RegressionKind99) return n order by n.name desc
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [131]::int2[]) select s0.n0 as n from s0 order by ((s0.n0).properties -> 'name') desc;

-- case: match (n:RegressionKind81) where n.domainsid = $domain and n.isdc = true and n.ldapavailable = true and n.ldapsigning = false return id(n)
-- cypher_params: {"domain":"S-1-5-21"}
-- pgsql_params:{"pi0":"S-1-5-21"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'domainsid')) = 'string' and (n0.properties ->> 'domainsid') = @pi0::text) and ((n0.properties -> 'isdc'))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties -> 'ldapavailable'))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties -> 'ldapsigning'))::jsonb = to_jsonb((false)::bool)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [113]::int2[]) select (s0.n0).id as "id(n)" from s0;

-- case: match (n) where n.domainsid = $domain and n.isdc = true and n.ldapsavailable = true and n.epa = false return n
-- cypher_params: {"domain":"S-1-5-21"}
-- pgsql_params:{"pi0":"S-1-5-21"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'domainsid')) = 'string' and (n0.properties ->> 'domainsid') = @pi0::text) and ((n0.properties -> 'isdc'))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties -> 'ldapsavailable'))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties -> 'epa'))::jsonb = to_jsonb((false)::bool)::jsonb)) select s0.n0 as n from s0;

