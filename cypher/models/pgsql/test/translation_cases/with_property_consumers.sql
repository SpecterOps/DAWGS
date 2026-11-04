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

-- case: match (n:NodeKind1) with n.name as x return toString(x) as value
-- pgsql_params:{"__strlit0":"toString"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select (cypher_text(s0.i0, @__strlit0::text)::text)::text as value from s0;

-- case: match (n:NodeKind1) with n.name as x return toLower(x) as value
-- pgsql_params:{"__strlit0":"toLower"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select lower(cypher_text(s0.i0, @__strlit0::text)::text)::text as value from s0;

-- case: match (n:NodeKind1) with n.name as x return toUpper(x) as value
-- pgsql_params:{"__strlit0":"toUpper"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select upper(cypher_text(s0.i0, @__strlit0::text)::text)::text as value from s0;

-- case: match (n:NodeKind1) with n.name as x return split(x, ',') as value
-- pgsql_params:{"__strlit0":"split","__strlit1":","}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select string_to_array(cypher_text(s0.i0, @__strlit0::text)::text, @__strlit1::text)::text[] as value from s0;

-- case: match (n:NodeKind1) with n.name as x return split('a,b', x) as value
-- pgsql_params:{"__strlit0":"a,b","__strlit1":"split"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select string_to_array(@__strlit0::text, cypher_text(s0.i0, @__strlit1::text)::text)::text[] as value from s0;

-- case: match (n:NodeKind1) with n.name as x return size(x) as value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select cypher_size(s0.i0)::int8 as value from s0;

-- case: match (n:NodeKind1) with n.name as x return coalesce(x, 'fallback') as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"fallback"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select coalesce(nullif(to_jsonb(s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((@__strlit1::text)::text)::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.name as x return x = 'Alice' as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"Alice"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select nullif((s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb = to_jsonb((@__strlit1::text)::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.name as x return x <> 'Alice' as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"Alice"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select nullif((s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb <> to_jsonb((@__strlit1::text)::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.name as x return x is null as value
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select nullif((s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb is null as value from s0;

-- case: match (n:NodeKind1) with n.name as x return toInteger(x) as value
-- pgsql_params:{"__strlit0":"number"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select case when jsonb_typeof(s0.i0) = @__strlit0::text then trunc((i0)::numeric)::int8 else (s0.i0 #>> array []::text[])::int8 end as value from s0;

-- case: match (n:NodeKind1) with n.name as x return date(x) as value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1) select (s0.i0 #>> array []::text[])::date as value from s0;

-- case: match (n:NodeKind1) with n, n.name as x where toString(x) = 'Alice' set n.value = toString(x) return n
-- pgsql_params:{"__strlit0":"toString","__strlit1":"Alice","__strlit2":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select s1.n0 as n0, ((s1.n0).properties -> E'name') as i0, ((cypher_text(((s1.n0).properties -> E'name'), @__strlit0::text)::text)::text = @__strlit1::text) as s0_where from s1) select s0.n0, s0.i0 from s0 where s0.s0_where), s2 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, (cypher_text(s0.i0, @__strlit0::text)::text)::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s3 as (select s0.i0 as i0, case when (s0.n0).id = (s2.n1).id then s2.n1 else s0.n0 end as n0 from s0 left outer join s2 on (s0.n0).id = (s2.n1).id) select s3.n0 as n from s3;

-- case: match ()-[r:EdgeKind1]->() with r, r.name as x where toString(x) = 'Alice' set r.value = toUpper(x)
-- pgsql_params:{"__strlit0":"toString","__strlit1":"Alice","__strlit2":"value","__strlit3":"toUpper"}
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s0 as (select s1.e0 as e0, ((s1.e0).properties -> E'name') as i0, ((cypher_text(((s1.e0).properties -> E'name'), @__strlit0::text)::text)::text = @__strlit1::text) as s0_where from s1) select s0.e0, s0.i0 from s0 where s0.s0_where), s2 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit2::text, upper(cypher_text(s0.i0, @__strlit3::text)::text)::text)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s3 as (select case when (s0.e0).id = (s2.e1).id then s2.e1 else s0.e0 end as e0, s0.i0 as i0 from s0 left outer join s2 on (s0.e0).id = (s2.e1).id) select 1;

-- case: match (n:NodeKind1) with n.name as x where toString(x) = 'Alice' create (c:NodeKind2 {value: toString(x)})
-- pgsql_params:{"__strlit0":"toString","__strlit1":"Alice","__strlit2":"node","__strlit3":"id","__strlit4":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select ((s1.n0).properties -> E'name') as i0, ((cypher_text(((s1.n0).properties -> E'name'), @__strlit0::text)::text)::text = @__strlit1::text) as s0_where from s1) select s0.i0 from s0 where s0.s0_where), s2 as (select s0.i0 as i0, nextval(pg_get_serial_sequence(@__strlit2::text, @__strlit3::text))::int8 as n1_id from s0), s3 as (insert into node (graph_id, id, kind_ids, properties) select 0, s2.n1_id, array [2]::int2[], jsonb_build_object(@__strlit4::text, (cypher_text(s2.i0, @__strlit0::text)::text)::text)::jsonb from s2 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s4 as (select s2.i0 as i0, s3.n1 as n1 from s2, s3 where s3.n1_id = s2.n1_id) select 1;

-- case: match (n:NodeKind1) with n.name as x with toString(x) as x return toString(x) as value
-- pgsql_params:{"__strlit0":"toString"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1), s2 as (select (cypher_text(s0.i0, @__strlit0::text)::text)::text as i1 from s0) select (i1)::text as value from s2;

-- case: match (n:NodeKind1) with n.name as x with size(x) as x return x + 1 as value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'name') as i0 from s1), s2 as (select cypher_size(s0.i0)::int8 as i1 from s0) select s2.i1 + 1 as value from s2;

-- case: match (n:NodeKind1) with n as x with x.name as x return toString(x) as value
-- pgsql_params:{"__strlit0":"toString"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n1 from s1), s2 as (select ((s0.n1).properties -> E'name') as i0 from s0) select (cypher_text(s2.i0, @__strlit0::text)::text)::text as value from s2;

-- case: match (n:NodeKind1) with n, n.name as x with n, toString(x) as x where x = 'Alice' set n.value = x
-- pgsql_params:{"__strlit0":"toString","__strlit1":"Alice","__strlit2":"value"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0, ((s1.n0).properties -> E'name') as i0 from s1), s2 as (with s2 as (select s0.n0 as n0, (cypher_text(s0.i0, @__strlit0::text)::text)::text as i1, ((cypher_text(s0.i0, @__strlit0::text)::text)::text = @__strlit1::text) as s2_where from s0) select s2.n0, s2.i1 from s2 where s2.s2_where), s3 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, i1)::jsonb from s2 where (s2.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s4 as (select s2.i1 as i1, case when (s2.n0).id = (s3.n1).id then s3.n1 else s2.n0 end as n0 from s2 left outer join s3 on (s2.n0).id = (s3.n1).id) select 1;

-- case: match (n:NodeKind1) with n.items as x return head(x) as value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'items') as i0 from s1) select ((cypher_list(s0.i0)::jsonb -> 0))::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.items as x return tail(x) as value
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'items') as i0 from s1) select (cypher_list(s0.i0)::jsonb - 0)::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.items as x return head(coalesce(x, ['fallback'])) as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"fallback"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'items') as i0 from s1) select ((cypher_list(coalesce(nullif(to_jsonb(s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((array [@__strlit1::text]::text[])::text[])::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb)::jsonb -> 0))::jsonb as value from s0;

-- case: match (n:NodeKind1) with n.items as x return tail(coalesce(x, ['fallback'])) as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"fallback"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'items') as i0 from s1) select (cypher_list(coalesce(nullif(to_jsonb(s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((array [@__strlit1::text]::text[])::text[])::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb)::jsonb - 0)::jsonb as value from s0;

-- case: match (n:NodeKind1) with n, n.items as x where head(x) = 'one' set n.items = tail(x)
-- pgsql_params:{"__strlit0":"one","__strlit1":"items"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s0 as (select s1.n0 as n0, ((s1.n0).properties -> E'items') as i0, (((cypher_list(((s1.n0).properties -> E'items'))::jsonb -> 0))::jsonb = to_jsonb((@__strlit0::text)::text)::jsonb) as s0_where from s1) select s0.n0, s0.i0 from s0 where s0.s0_where), s2 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit1::text, (cypher_list(s0.i0)::jsonb - 0)::jsonb)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s3 as (select s0.i0 as i0, case when (s0.n0).id = (s2.n1).id then s2.n1 else s0.n0 end as n0 from s0 left outer join s2 on (s0.n0).id = (s2.n1).id) select 1;

-- case: match (n:NodeKind1) with n.items as x return size(coalesce(x, [])) as value
-- pgsql_params:{"__strlit0":"null"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'items') as i0 from s1) select cypher_size(coalesce(nullif(to_jsonb(s0.i0)::jsonb, (@__strlit0::text)::jsonb)::jsonb, nullif(to_jsonb((array []::int8[])::int8[])::jsonb, (@__strlit0::text)::jsonb)::jsonb)::jsonb)::int8 as value from s0;
