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

-- case: MATCH (n:NodeKind1) WHERE toLower(n.name) STARTS WITH 'a_' RETURN n
-- pgsql_params:{"__strlit0":"a\\_%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'name') ilike @__strlit0::text and lower((n0.properties ->> E'name'))::text like @__strlit0::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE toLower(n.name) ENDS WITH 'a%' RETURN n
-- pgsql_params:{"__strlit0":"%a\\%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'name') ilike @__strlit0::text and lower((n0.properties ->> E'name'))::text like @__strlit0::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) WHERE toLower(n.name) CONTAINS 'a_\\' RETURN n
-- pgsql_params:{"__strlit0":"%a\\_\\\\%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'name') ilike @__strlit0::text and lower((n0.properties ->> E'name'))::text like @__strlit0::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) RETURN toUpper(n.name) STARTS WITH 'A_' AS value
-- pgsql_params:{"__strlit0":"A\\_%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select upper(((s0.n0).properties ->> E'name'))::text like @__strlit0::text as value from s0;

-- case: MATCH (n:NodeKind1) RETURN coalesce(n.name, '') CONTAINS '%' AS value
-- pgsql_params:{"__strlit0":"null","__strlit1":"","__strlit2":"string","__strlit3":"%\\%%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((jsonb_typeof(coalesce(nullif(nullif(((s0.n0).properties -> E'name'), (@__strlit0::text)::jsonb)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((@__strlit1::text)::text)::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb) = @__strlit2::text and coalesce(nullif(nullif(((s0.n0).properties -> E'name'), (@__strlit0::text)::jsonb)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((@__strlit1::text)::text)::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb #>> array []::text[] like @__strlit3::text) or (not (jsonb_typeof(coalesce(nullif(nullif(((s0.n0).properties -> E'name'), (@__strlit0::text)::jsonb)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((@__strlit1::text)::text)::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb) = @__strlit2::text) and null)) as value from s0;

-- case: MATCH (n:NodeKind1) RETURN NOT toString(n.name) ENDS WITH '_' AS value
-- pgsql_params:{"__strlit0":"null","__strlit1":"toString","__strlit2":"%\\_"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select not (cypher_text(nullif(((s0.n0).properties -> E'name'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::text)::text like @__strlit2::text as value from s0;

-- case: MATCH (n:NodeKind1) WITH n, toLower(n.name) AS value WHERE value STARTS WITH 'a_' RETURN n
-- pgsql_params:{"__strlit0":"a\\_%"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0, lower(((s1.n0).properties ->> E'name'))::text as i0 from s1) select s0.n0 as n from s0 where ((s0.i0)::text like @__strlit0::text);

-- case: MATCH (n:NodeKind1) WHERE toLower(n.name) STARTS WITH $needle RETURN n
-- cypher_params: {"needle":"a_%\\"}
-- pgsql_params:{"__strlit0":"%","pi0":"a_%\\"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'name') ilike (cypher_like_escape((@pi0::text)::text)::text || @__strlit0::text)::text and (lower((n0.properties ->> E'name'))::text)::text like (cypher_like_escape((@pi0::text)::text)::text || @__strlit0::text)::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: MATCH (n:NodeKind1) RETURN toLower(n.name) =~ 'a_%' AS value
-- pgsql_params:{"__strlit0":"a_%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select lower(((s0.n0).properties ->> E'name'))::text ~ @__strlit0::text as value from s0;

-- case: MATCH (n:NodeKind1) WHERE n.name CONTAINS 'a_%\\' RETURN n
-- pgsql_params:{"__strlit0":"string","__strlit1":"%a\\_\\%\\\\%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text and (n0.properties ->> E'name') like @__strlit1::text) or (not (jsonb_typeof((n0.properties -> E'name')) = @__strlit0::text) and null))) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: RETURN toLower('A_') STARTS WITH 'a_' AS value
-- pgsql_params:{"__strlit0":"A_","__strlit1":"a\\_%"}
select lower(@__strlit0::text)::text like @__strlit1::text as value;

-- case: MATCH (n:NodeKind1) WHERE toLower(n.name) STARTS WITH 'a_' SET n.hit = true RETURN n
-- pgsql_params:{"__strlit0":"a\\_%","__strlit1":"hit"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'name') ilike @__strlit0::text and lower((n0.properties ->> E'name'))::text like @__strlit0::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit1::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: MATCH ()-[r:EdgeKind1]->() WHERE toLower(r.name) ENDS WITH 'a%' SET r.hit = true
-- pgsql_params:{"__strlit0":"%a\\%","__strlit1":"hit"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties ->> E'name') ilike @__strlit0::text and lower((e0.properties ->> E'name'))::text like @__strlit0::text)) and e0.kind_id = any (array [3]::int2[])), s1 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit1::text, true)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s2 as (select case when (s0.e0).id = (s1.e1).id then s1.e1 else s0.e0 end as e0 from s0 left outer join s1 on (s0.e0).id = (s1.e1).id) select 1;

-- case: MATCH (n:NodeKind1) WHERE NOT toLower(n.name) CONTAINS '_' CREATE (c:NodeKind2 {name: n.name})
-- pgsql_params:{"__strlit0":"%\\_%","__strlit1":"node","__strlit2":"id","__strlit3":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties ->> E'name') ilike @__strlit0::text and lower((n0.properties ->> E'name'))::text like @__strlit0::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit1::text, @__strlit2::text))::int8 as n1_id from s0), s2 as (insert into node (graph_id, id, kind_ids, properties) select 0, s1.n1_id, array [2]::int2[], jsonb_build_object(@__strlit3::text, ((s1.n0).properties ->> E'name'))::jsonb from s1 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s3 as (select s1.n0 as n0, s2.n1 as n1 from s1, s2 where s2.n1_id = s1.n1_id) select 1;
