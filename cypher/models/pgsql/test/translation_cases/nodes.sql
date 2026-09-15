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

-- case: match (n) return labels(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[] from s0;

-- case: match (n) where 'NodeKind1' in labels(n) return n
-- pgsql_params:{"__strlit0":"NodeKind1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 where (@__strlit0::text = any ((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]));

-- case: match (n) where labels(n) = ['NodeKind1', 'NodeKind2'] return n
-- pgsql_params:{"__strlit0":"NodeKind1","__strlit1":"NodeKind2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 where ((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[] = array [@__strlit0::text, @__strlit1::text]::text[]);

-- case: match (n) where n.name = 'n3' with labels(n) as labels return labels, size(labels)
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n3"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select (array(select _kind.name from generate_subscripts((s1.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s1.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[] as i0 from s1) select s0.i0 as labels, cardinality(s0.i0)::int from s0;

-- case: match (n) with 1 as _kind_idx, n return labels(n), _kind_idx
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select 1 as i0, s1.n0 as n0 from s1) select (array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[], s0.i0 as _kind_idx from s0;

-- case: match (n:NodeKind1) return n.name as displayname order by displayname
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s0.n0).properties -> @__strlit0::text) as displayname from s0 order by displayname;

-- case: match (n) where any(label in labels(n) where label = 'NodeKind2') return n
-- pgsql_params:{"__strlit0":"NodeKind2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 where (((select count(*)::int from unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 where (i0 = @__strlit0::text)) >= 1)::bool);

-- case: match (n) where none(label in labels(n) where label = 'NodeKind2') return n
-- pgsql_params:{"__strlit0":"NodeKind2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 where (((select count(*)::int from unnest((array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[]) as i0 where (i0 = @__strlit0::text)) = 0 and (array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[] is not null)::bool);

-- case: match (n) where ID(n) = 1 return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = 1)) select s0.n0 as n from s0;

-- case: match (n) where coalesce(n.name, '') = '1234' return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"","__strlit2":"1234"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text = @__strlit2::text)) select s0.n0 as n from s0;

-- case: match (n) where n.name = '1234' return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"1234"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.`a-aaa` = "123" return n
-- pgsql_params:{"__strlit0":"a-aaa","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.`b_bbb` = "123" return n
-- pgsql_params:{"__strlit0":"b_bbb","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.`has``tick` = "123" return n
-- pgsql_params:{"__strlit0":"has`tick","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.`'` = "123" return n
-- pgsql_params:{"__strlit0":"'","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.```starts-tick` = "123" return n
-- pgsql_params:{"__strlit0":"`starts-tick","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.```super-wrapped``` = "123" return n
-- pgsql_params:{"__strlit0":"`super-wrapped`","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where n.```` = "123" return n
-- pgsql_params:{"__strlit0":"`","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (n) where (n).`a-aaa` = "123" return n
-- pgsql_params:{"__strlit0":"a-aaa","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 where ((jsonb_typeof((((s0.n0)).properties -> @__strlit0::text)) = @__strlit1::text and (((s0.n0)).properties ->> @__strlit0::text) = @__strlit2::text));

-- case: match ()-[r]-() where startNode(r).`something` = "abc" return r
-- pgsql_params:{"__strlit0":"something","__strlit1":"string","__strlit2":"abc"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on (n0.id = e0.end_id or n0.id = e0.start_id) join node n1 on (n1.id = e0.end_id or n1.id = e0.start_id) where (n0.id <> n1.id)) select s0.e0 as r from s0 where ((jsonb_typeof(((start_node(((s0.e0).id, (s0.e0).start_id, (s0.e0).end_id, (s0.e0).kind_id, (s0.e0).properties)::edgecomposite)::nodecomposite).properties -> @__strlit0::text)) = @__strlit1::text and ((start_node(((s0.e0).id, (s0.e0).start_id, (s0.e0).end_id, (s0.e0).kind_id, (s0.e0).properties)::edgecomposite)::nodecomposite).properties ->> @__strlit0::text) = @__strlit2::text));

-- case: match (n:NodeKind1 {name: "SOME NAME"}) return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"SOME NAME"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and (jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)) select s0.n0 as n from s0;

-- case: match (n:NodeKind1 {`'`: 'value'}) return n
-- pgsql_params:{"__strlit0":"'","__strlit1":"string","__strlit2":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and (jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)) select s0.n0 as n from s0;

-- case: match (n) where n.objectid in $p return n
-- cypher_params: {"p":["1","2","3"]}
-- pgsql_params:{"__strlit0":"objectid","pi0":["1","2","3"]}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) = any (@pi0::text[]))) select s0.n0 as n from s0;

-- case: match (s) where s.name = $myParam return s
-- cypher_params: {"myParam":"123"}
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","pi0":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @pi0::text))) select s0.n0 as s from s0;

-- case: match (s) return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0;

-- case: match (s) where s.prop = [1, 2, 3] return s
-- pgsql_params:{"__strlit0":"prop"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (jsonb_to_text_array((n0.properties -> @__strlit0::text))::int8[] = array [1, 2, 3]::int8[])) select s0.n0 as s from s0;

-- case: match (s) where (s:NodeKind1 or s:NodeKind2) return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [2]::int2[]))) select s0.n0 as s from s0;

-- case: match (n:NodeKind1), (e) where n.name = e.name return n
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (((s0.n0).properties -> @__strlit0::text) = (n1.properties -> @__strlit0::text))) select s1.n0 as n from s1;

-- case: match (s), (e) where id(s) in e.captured_ids return s, e
-- pgsql_params:{"__strlit0":"captured_ids"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((s0.n0).id = any (jsonb_to_text_array((n1.properties -> @__strlit0::text))::int8[]))) select s1.n0 as s, s1.n1 as e from s1;

-- case: match (s) where s:NodeKind1 and s:NodeKind2 return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.kind_ids operator (pg_catalog.@>) array [2]::int2[])) select s0.n0 as s from s0;

-- case: match (s) where s.name = '1234' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"1234"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as s from s0;

-- case: match (s:NodeKind1), (e:NodeKind2) where s.selected or s.tid = e.tid and e.enabled return s, e
-- pgsql_params:{"__strlit0":"selected","__strlit1":"tid","__strlit2":"enabled"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((((s0.n0).properties ->> @__strlit0::text))::bool or ((s0.n0).properties -> @__strlit1::text) = (n1.properties -> @__strlit1::text) and ((n1.properties ->> @__strlit2::text))::bool) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n0 as s, s1.n1 as e from s1;

-- case: match (s) where s.value + 2 / 3 > 10 return s
-- pgsql_params:{"__strlit0":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::int8 + 2 / 3 > 10)) select s0.n0 as s from s0;

-- case: match (s), (e) where s.name = 'n1' return s, e.name as othername
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select s1.n0 as s, ((s1.n1).properties -> @__strlit0::text) as othername from s1;

-- case: match (s) where s.name in ['option 1', 'option 2'] return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"option 1","__strlit2":"option 2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) = any (array [@__strlit1::text, @__strlit2::text]::text[]))) select s0.n0 as s from s0;

-- case: match (s) where toLower(s.name) = '1234' return distinct s
-- pgsql_params:{"__strlit0":"name","__strlit1":"1234"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (lower((n0.properties ->> @__strlit0::text))::text = @__strlit1::text)) select distinct s0.n0 as s from s0;

-- case: match (s:NodeKind1), (e:NodeKind2) where s.name = e.name return s, e
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (((s0.n0).properties -> @__strlit0::text) = (n1.properties -> @__strlit0::text)) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n0 as s, s1.n1 as e from s1;

-- case: match (n) where n.system_tags is not null and not (n:NodeKind1 or n:NodeKind2) return id(n)
-- pgsql_params:{"__strlit0":"system_tags","__strlit1":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ? @__strlit0::text and not (n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb) and not (n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] or n0.kind_ids operator (pg_catalog.@>) array [2]::int2[]))) select (s0.n0).id from s0;

-- case: match (s), (e) where s.name = '1234' and e.other = 1234 return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"1234","__strlit3":"other"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (((n1.properties -> @__strlit3::text))::jsonb = to_jsonb((1234)::int8)::jsonb)) select s1.n0 as s from s1;

-- case: match (s), (e) where s.name = '1234' or e.other = 1234 return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"1234","__strlit3":"other"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((jsonb_typeof(((s0.n0).properties -> @__strlit0::text)) = @__strlit1::text and ((s0.n0).properties ->> @__strlit0::text) = @__strlit2::text) or ((n1.properties -> @__strlit3::text))::jsonb = to_jsonb((1234)::int8)::jsonb)) select s1.n0 as s from s1;

-- case: match (n), (k) where n.name = '1234' and k.name = '1234' match (e) where e.name = n.name return k, e
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"1234"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit2::text))), s2 as (select s1.n0 as n0, s1.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s1, node n2 where ((n2.properties -> @__strlit0::text) = ((s1.n0).properties -> @__strlit0::text))) select s2.n1 as k, s2.n2 as e from s2;

-- case: match (n) return n skip 5 limit 10
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as n from s0 offset 5 limit 10;

-- case: match (s) return s order by id(s) desc
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 order by (s0.n0).id desc;

-- case: match (s) return s order by s.name, s.other_prop desc
-- pgsql_params:{"__strlit0":"name","__strlit1":"other_prop"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 order by ((s0.n0).properties -> @__strlit0::text), ((s0.n0).properties -> @__strlit1::text) desc;

-- case: match (s) where s.created_at = localtime() return s
-- pgsql_params:{"__strlit0":"created_at"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::time without time zone = localtime(6)::time without time zone)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = localtime('4:4:4') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"4:4:4"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::time without time zone = (@__strlit1::text)::time without time zone)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = date() return s
-- pgsql_params:{"__strlit0":"created_at"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::date = current_date::date)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = date() - duration('P1D') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"P1D"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp without time zone = current_date::date - @__strlit1::interval)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = date() + duration('PT4H') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"PT4H"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp without time zone = current_date::date + @__strlit1::interval)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = date('2023-4-4') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"2023-4-4"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::date = (@__strlit1::text)::date)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = datetime() return s
-- pgsql_params:{"__strlit0":"created_at"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp with time zone = now()::timestamp with time zone)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = datetime('2019-06-01T18:40:32.142+0100') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"2019-06-01T18:40:32.142+0100"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp with time zone = (@__strlit1::text)::timestamp with time zone)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = localdatetime() return s
-- pgsql_params:{"__strlit0":"created_at"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp without time zone = localtimestamp(6)::timestamp without time zone)) select s0.n0 as s from s0;

-- case: match (s) where s.created_at = localdatetime('2019-06-01T18:40:32.142') return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"2019-06-01T18:40:32.142"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::timestamp without time zone = (@__strlit1::text)::timestamp without time zone)) select s0.n0 as s from s0;

-- case: match (s) where not (s.name = '123') return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)))) select s0.n0 as s from s0;

-- case: match (s) where s.isassignabletorole = 'true' return s
-- pgsql_params:{"__strlit0":"isassignabletorole","__strlit1":"string","__strlit2":"true"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as s from s0;

-- case: match (s) where s.isassignabletorole = true return s
-- pgsql_params:{"__strlit0":"isassignabletorole"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text))::jsonb = to_jsonb((true)::bool)::jsonb)) select s0.n0 as s from s0;

-- case: match (s) return s.value + 1
-- pgsql_params:{"__strlit0":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (((s0.n0).properties ->> @__strlit0::text))::int8 + 1 from s0;

-- case: match (s) return (s.value + 1) / 3
-- pgsql_params:{"__strlit0":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select ((((s0.n0).properties ->> @__strlit0::text))::int8 + 1) / 3 from s0;

-- case: match (s) where id(s) in [1, 2, 3, 4] return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = any (array [1, 2, 3, 4]::int8[]))) select s0.n0 as s from s0;

-- case: match (s) where s.name in ['option 1', 'option 2'] return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"option 1","__strlit2":"option 2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) = any (array [@__strlit1::text, @__strlit2::text]::text[]))) select s0.n0 as s from s0;

-- case: match (s) where s.created_at is null return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((not n0.properties ? @__strlit0::text or (n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb))) select s0.n0 as s from s0;

-- case: match (s) where s.created_at is not null return s
-- pgsql_params:{"__strlit0":"created_at","__strlit1":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ? @__strlit0::text and not (n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb))) select s0.n0 as s from s0;

-- case: match (s) where s.name starts with '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"123%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) like @__strlit1::text)) select s0.n0 as s from s0;

-- case: match (s) where not s.name starts with '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"","__strlit2":"123%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text like @__strlit2::text)) select s0.n0 as s from s0;

-- case: match (s) where s.name contains '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"%123%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) like @__strlit1::text)) select s0.n0 as s from s0;

-- case: match (s) where not s.name contains '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"","__strlit2":"%123%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text like @__strlit2::text)) select s0.n0 as s from s0;

-- case: match (s) where s.name ends with '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"%123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) like @__strlit1::text)) select s0.n0 as s from s0;

-- case: match (s) where not s.name ends with '123' return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"","__strlit2":"%123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text like @__strlit2::text)) select s0.n0 as s from s0;

-- case: match (s) where s.name starts with s.other return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"other"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_starts_with((n0.properties ->> @__strlit0::text), (n0.properties ->> @__strlit1::text))::bool)) select s0.n0 as s from s0;

-- case: match (s) where s.name contains s.other return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"other"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_contains((n0.properties ->> @__strlit0::text), (n0.properties ->> @__strlit1::text))::bool)) select s0.n0 as s from s0;

-- case: match (s) where s.name ends with s.other return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"other"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_ends_with((n0.properties ->> @__strlit0::text), (n0.properties ->> @__strlit1::text))::bool)) select s0.n0 as s from s0;

-- case: match (n) where n:NodeKind1 and toLower(n.tenantid) contains 'myid' and n.system_tags contains 'tag' return n
-- pgsql_params:{"__strlit0":"tenantid","__strlit1":"%myid%","__strlit2":"system_tags","__strlit3":"%tag%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and lower((n0.properties ->> @__strlit0::text))::text like @__strlit1::text and (n0.properties ->> @__strlit2::text) like @__strlit3::text)) select s0.n0 as n from s0;

-- case: match (s) where not (s)-[]-() return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (not exists (select 1 from edge e0 where (e0.start_id = (s0.n0).id or e0.end_id = (s0.n0).id)));

-- case: match (s) where not (s)-[]->()-[]->() return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (not (with s1 as (select e0.id as e0, s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n1 on n1.id = e0.end_id where (s0.n0).id = e0.start_id), s2 as (select s1.e0 as e0, s1.n0 as n0, s1.n1 as n1 from s1 join edge e1 on (s1.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id != s1.e0) select count(*) > 0 from s2));

-- case: match (s) where ()-[]->()-[]->(s) return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where ((with s1 as (select e0.id as e0, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id), s2 as (select s1.e0 as e0, s1.n0 as n0, s1.n2 as n2 from s1 join edge e1 on (s1.n2).id = e1.start_id join node n0 on (s1.n0).id = e1.end_id where e1.id != s1.e0) select count(*) > 0 from s2));

-- case: match (g:Group) where (:User)-[:MemberOf]->(:Group)-[:MemberOf]->(g) return count(g)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [13]::int2[]) select count(s0.n0)::int8 from s0 where ((with s1 as (select e0.id as e0, s0.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [6]::int2[] and n1.id = e0.start_id join node n2 on n2.kind_ids operator (pg_catalog.@>) array [13]::int2[] and n2.id = e0.end_id where e0.kind_id = any (array [25]::int2[])), s2 as (select s1.e0 as e0, s1.n0 as n0, s1.n2 as n2 from s1 join edge e1 on (s1.n2).id = e1.start_id join node n0 on (s1.n0).id = e1.end_id where e1.kind_id = any (array [25]::int2[]) and e1.id != s1.e0) select count(*) > 0 from s2));

-- case: match (s) where not (s)-[{prop: 'a'}]-({name: 'n3'}) return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n3","__strlit3":"prop","__strlit4":"a"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (not (with s1 as (select s0.n0 as n0 from edge e0 join node n1 on (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit2::text) and (n1.id = e0.end_id or n1.id = e0.start_id) where ((s0.n0).id <> n1.id) and (jsonb_typeof((e0.properties -> @__strlit3::text)) = @__strlit1::text and (e0.properties ->> @__strlit3::text) = @__strlit4::text) and ((s0.n0).id = e0.end_id or (s0.n0).id = e0.start_id)) select count(*) > 0 from s1));

-- case: match (s) where not (s)<-[{prop: 'a'}]-({name: 'n3'}) return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n3","__strlit3":"prop","__strlit4":"a"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (not (with s1 as (select s0.n0 as n0 from edge e0 join node n1 on (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit2::text) and n1.id = e0.start_id where (jsonb_typeof((e0.properties -> @__strlit3::text)) = @__strlit1::text and (e0.properties ->> @__strlit3::text) = @__strlit4::text) and (s0.n0).id = e0.end_id) select count(*) > 0 from s1));

-- case: match (n:NodeKind1) where n.distinguishedname = toUpper('admin') return n
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"string","__strlit2":"admin"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = upper(@__strlit2::text)::text)) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where n.distinguishedname starts with toUpper('admin') return n
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"admin"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_starts_with((n0.properties ->> @__strlit0::text), (upper(@__strlit1::text)::text)::text)::bool) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where n.distinguishedname contains toUpper('admin') return n
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"admin"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_contains((n0.properties ->> @__strlit0::text), (upper(@__strlit1::text)::text)::text)::bool) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where n.distinguishedname ends with toUpper('admin') return n
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"admin"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_ends_with((n0.properties ->> @__strlit0::text), (upper(@__strlit1::text)::text)::text)::bool) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (s) where not (s)-[{prop: 'a'}]->({name: 'n3'}) return s
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n3","__strlit3":"prop","__strlit4":"a"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (not (with s1 as (select s0.n0 as n0 from edge e0 join node n1 on (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit2::text) and n1.id = e0.end_id where (jsonb_typeof((e0.properties -> @__strlit3::text)) = @__strlit1::text and (e0.properties ->> @__strlit3::text) = @__strlit4::text) and (s0.n0).id = e0.start_id) select count(*) > 0 from s1));

-- case: match (s) where not (s)-[]-() return id(s)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (s0.n0).id from s0 where (not exists (select 1 from edge e0 where (e0.start_id = (s0.n0).id or e0.end_id = (s0.n0).id)));

-- case: match (s) where ()--(s) return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (exists (select 1 from edge e0 where (e0.start_id = (s0.n0).id or e0.end_id = (s0.n0).id)));

-- case: match (a), (b) where (a)--(b) return a
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select s1.n0 as a from s1 where (exists (select 1 from edge e0 where ((e0.start_id = (s1.n0).id and e0.end_id = (s1.n1).id) or (e0.start_id = (s1.n1).id and e0.end_id = (s1.n0).id))));

-- case: match (s) where ()--() return s
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as s from s0 where (exists (select 1 from edge e0));

-- case: match (g) where ({name: 'n3'})-[{prop: 'a'}]-(g) return g
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"n3","__strlit3":"prop","__strlit4":"a"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select s0.n0 as g from s0 where ((with s1 as (select s0.n0 as n0 from edge e0 join node n1 on (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit2::text) and (n1.id = e0.end_id or n1.id = e0.start_id) where ((s0.n0).id <> n1.id) and (jsonb_typeof((e0.properties -> @__strlit3::text)) = @__strlit1::text and (e0.properties ->> @__strlit3::text) = @__strlit4::text) and ((s0.n0).id = e0.end_id or (s0.n0).id = e0.start_id)) select count(*) > 0 from s1));

-- case: match (a:NodeKind1), (b:NodeKind2) where (a:NodeKind1)-[]-(b:NodeKind2) return a
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n0 as a from s1 where ((with s2 as (select s1.n0 as n0, s1.n1 as n1 from edge e0 where ((s1.n0).id <> (s1.n1).id) and (((s1.n0).id = e0.start_id and (s1.n1).id = e0.end_id) or ((s1.n1).id = e0.start_id and (s1.n0).id = e0.end_id))) select count(*) > 0 from s2));

-- case: match (x:NodeKind1{name:'foo'}) match (x)-[]-(y:NodeKind2{name:'bar'}) return x
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"foo","__strlit3":"bar"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and (jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)), s1 as (select s0.n0 as n0 from s0 join edge e0 on ((s0.n0).id = e0.end_id or (s0.n0).id = e0.start_id) join node n1 on n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit3::text) and (n1.id = e0.end_id or n1.id = e0.start_id) where ((s0.n0).id <> n1.id)) select s1.n0 as x from s1;

-- case: match (y:NodeKind2{name:'bar'}) match ()-[]-(y) return y
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"bar"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [2]::int2[] and (jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)), s1 as (select s0.n0 as n0 from s0 join edge e0 on ((s0.n0).id = e0.end_id or (s0.n0).id = e0.start_id) join node n1 on (n1.id = e0.end_id or n1.id = e0.start_id) where ((s0.n0).id <> n1.id)) select s1.n0 as y from s1;

-- case: match (x:NodeKind1{name:'foo'}) match (y:NodeKind2{name:'bar'}) match (x)-[]-(y) return x
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"foo","__strlit3":"bar"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[] and (jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text)), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where n1.kind_ids operator (pg_catalog.@>) array [2]::int2[] and (jsonb_typeof((n1.properties -> @__strlit0::text)) = @__strlit1::text and (n1.properties ->> @__strlit0::text) = @__strlit3::text)), s2 as (select s1.n0 as n0, s1.n1 as n1 from s1 join edge e0 on ((s1.n0).id = e0.start_id or (s1.n0).id = e0.end_id) and ((s1.n1).id = e0.end_id or (s1.n1).id = e0.start_id) where ((s1.n0).id <> (s1.n1).id)) select s2.n0 as x from s2;

-- case: match (n) where n.system_tags contains ($param) return n
-- pgsql_params:{"__strlit0":"system_tags","pi0":null}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_contains((n0.properties ->> @__strlit0::text), (@pi0)::text)::bool)) select s0.n0 as n from s0;

-- case: match (n) where not n.system_tags contains ($param) return n
-- pgsql_params:{"__strlit0":"system_tags","__strlit1":"","pi0":null}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not cypher_contains(coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text, (@pi0)::text)::bool)) select s0.n0 as n from s0;

-- case: match (n) where n.system_tags starts with (1) return n
-- pgsql_params:{"__strlit0":"system_tags"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_starts_with((n0.properties ->> @__strlit0::text), (1)::text)::bool)) select s0.n0 as n from s0;

-- case: match (n) where n.system_tags ends with ('text') return n
-- pgsql_params:{"__strlit0":"system_tags","__strlit1":"text"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_ends_with((n0.properties ->> @__strlit0::text), (@__strlit1::text)::text)::bool)) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where toString(n.functionallevel) in ['2008 R2','2012','2008','2003','2003 Interim','2000 Mixed/Native'] return n
-- pgsql_params:{"__strlit0":"functionallevel","__strlit1":"2008 R2","__strlit2":"2012","__strlit3":"2008","__strlit4":"2003","__strlit5":"2003 Interim","__strlit6":"2000 Mixed/Native"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> @__strlit0::text) = any (array [@__strlit1::text, @__strlit2::text, @__strlit3::text, @__strlit4::text, @__strlit5::text, @__strlit6::text]::text[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where toInteger(n.value) in [1, 2, 3, 4] return n
-- pgsql_params:{"__strlit0":"value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::int8 = any (array [1, 2, 3, 4]::int8[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (u:NodeKind1) where u.pwdlastset < (datetime().epochseconds - (365 * 86400)) and not u.pwdlastset IN [-1.0, 0.0] return u limit 100
-- pgsql_params:{"__strlit0":"pwdlastset"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::numeric < (extract(epoch from now()::timestamp with time zone)::numeric - (365 * 86400)) and not ((n0.properties ->> @__strlit0::text))::float8 = any (array [- 1, 0]::float8[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as u from s0 limit 100;

-- case: match (u:NodeKind1) where u.pwdlastset < (datetime().epochmillis - 86400000) and not u.pwdlastset IN [-1.0, 0.0] return u limit 100
-- pgsql_params:{"__strlit0":"pwdlastset"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> @__strlit0::text))::numeric < (extract(epoch from now()::timestamp with time zone)::numeric * 1000 - 86400000) and not ((n0.properties ->> @__strlit0::text))::float8 = any (array [- 1, 0]::float8[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as u from s0 limit 100;

-- case: match (n:NodeKind1) where size(n.array_value) > 0 return n
-- pgsql_params:{"__strlit0":"array_value"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (jsonb_array_length((n0.properties -> @__strlit0::text))::int > 0) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n) where 1 in n.array return n
-- pgsql_params:{"__strlit0":"array"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (1 = any (jsonb_to_text_array((n0.properties -> @__strlit0::text))::int8[]))) select s0.n0 as n from s0;

-- case: match (n) where $p in n.array or $f in n.array return n
-- cypher_params: {"f":"text","p":1}
-- pgsql_params:{"__strlit0":"array","pi0":1,"pi1":"text"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (@pi0::float8 = any (jsonb_to_text_array((n0.properties -> @__strlit0::text))::float8[]) or @pi1::text = any (jsonb_to_text_array((n0.properties -> @__strlit0::text))::text[]))) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where coalesce(n.system_tags, '') contains 'admin_tier_0' return n
-- pgsql_params:{"__strlit0":"system_tags","__strlit1":"","__strlit2":"%admin_tier_0%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text like @__strlit2::text) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where coalesce(n.a, n.b, 1) = 1 return n
-- pgsql_params:{"__strlit0":"a","__strlit1":"b"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (coalesce(((n0.properties ->> @__strlit0::text))::int8, ((n0.properties ->> @__strlit1::text))::int8, 1)::int8 = 1) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where coalesce(n.a, n.b) = 1 return n
-- pgsql_params:{"__strlit0":"a","__strlit1":"b"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (coalesce((n0.properties ->> @__strlit0::text), (n0.properties ->> @__strlit1::text))::int8 = 1) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where 1 = coalesce(n.a, n.b) return n
-- pgsql_params:{"__strlit0":"a","__strlit1":"b"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (1 = coalesce((n0.properties ->> @__strlit0::text), (n0.properties ->> @__strlit1::text))::int8) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (u:NodeKind1) where u.hasspn = true and u.enabled = true and not '-502' ends with u.objectid and not coalesce(u.gmsa, false) = true and not coalesce(u.msa, false) = true return u limit 10
-- pgsql_params:{"__strlit0":"hasspn","__strlit1":"enabled","__strlit2":"-502","__strlit3":"objectid","__strlit4":"","__strlit5":"gmsa","__strlit6":"msa"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text))::jsonb = to_jsonb((true)::bool)::jsonb and ((n0.properties -> @__strlit1::text))::jsonb = to_jsonb((true)::bool)::jsonb and not cypher_ends_with((@__strlit2::text)::text, coalesce((n0.properties ->> @__strlit3::text), @__strlit4::text)::text)::bool and not coalesce(((n0.properties ->> @__strlit5::text))::bool, false)::bool = true and not coalesce(((n0.properties ->> @__strlit6::text))::bool, false)::bool = true) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as u from s0 limit 10;

-- case: match (n:NodeKind1) where coalesce(n.name, '') = coalesce(n.migrated_name, '') return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"","__strlit2":"migrated_name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (coalesce((n0.properties ->> @__strlit0::text), @__strlit1::text)::text = coalesce((n0.properties ->> @__strlit2::text), @__strlit1::text)::text) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where '1' in n.array_prop + ['1', '2'] return n
-- pgsql_params:{"__strlit0":"1","__strlit1":"array_prop","__strlit2":"2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (@__strlit0::text = any (jsonb_to_text_array((n0.properties -> @__strlit1::text))::text[] || array [@__strlit0::text, @__strlit2::text]::text[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) where ['DES-CBC-CRC', 'DES-CBC-MD5', 'RC4-HMAC-MD5'] in n.arrayProperty return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (false) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (u:NodeKind1) where 'DES-CBC-CRC' in u.arrayProperty or 'DES-CBC-MD5' in u.arrayProperty or 'RC4-HMAC-MD5' in u.arrayProperty return u
-- pgsql_params:{"__strlit0":"DES-CBC-CRC","__strlit1":"arrayProperty","__strlit2":"DES-CBC-MD5","__strlit3":"RC4-HMAC-MD5"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (@__strlit0::text = any (jsonb_to_text_array((n0.properties -> @__strlit1::text))::text[]) or @__strlit2::text = any (jsonb_to_text_array((n0.properties -> @__strlit1::text))::text[]) or @__strlit3::text = any (jsonb_to_text_array((n0.properties -> @__strlit1::text))::text[])) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as u from s0;

-- case: match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = 'CN=ADMINSDHOLDER,CN=SYSTEM,' + n.distinguishedname return m
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"CN=ADMINSDHOLDER,CN=SYSTEM,"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = @__strlit1::text || ((s0.n0).properties ->> @__strlit0::text)) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n1 as m from s1;

-- case: match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = n.distinguishedname + 'CN=ADMINSDHOLDER,CN=SYSTEM,' return m
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"CN=ADMINSDHOLDER,CN=SYSTEM,"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = ((s0.n0).properties ->> @__strlit0::text) || @__strlit1::text) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n1 as m from s1;

-- case: match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = n.unknown + m.unknown return m
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"unknown"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = ((s0.n0).properties ->> @__strlit1::text) || (n1.properties ->> @__strlit1::text)) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n1 as m from s1;

-- case: match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = '1' + '2' return m
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"1","__strlit2":"2"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = @__strlit1::text || @__strlit2::text) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s1.n1 as m from s1;

-- case: match (n) where not n.property is not null return n
-- pgsql_params:{"__strlit0":"property","__strlit1":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (n0.properties ? @__strlit0::text and not (n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb))) select s0.n0 as n from s0;

-- case: match (s) where s.prop = [] return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (s) where [] = s.prop return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (s) where not s.prop <> [] return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> @__strlit0::text) != (@__strlit1::text)::jsonb and (n0.properties -> @__strlit0::text) != (@__strlit2::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (s) where not s.prop = [] return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> @__strlit0::text) = (@__strlit1::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (s) where s.prop <> [] return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text) != (@__strlit1::text)::jsonb and (n0.properties -> @__strlit0::text) != (@__strlit2::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (s) where [] <> s.prop return s
-- pgsql_params:{"__strlit0":"prop","__strlit1":"[]","__strlit2":"null"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> @__strlit0::text) != (@__strlit1::text)::jsonb and (n0.properties -> @__strlit0::text) != (@__strlit2::text)::jsonb or (n0.properties -> @__strlit0::text) = (@__strlit2::text)::jsonb and null))) select s0.n0 as s from s0;

-- case: match (n:NodeKind1) optional match (m:NodeKind2) where m.distinguishedname = n.unknown + m.unknown return n, m
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"unknown"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = ((s0.n0).properties ->> @__strlit1::text) || (n1.properties ->> @__strlit1::text)) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]), s2 as (select s0.n0 as n0, s1.n1 as n1 from s0 left outer join s1 on (s0.n0 = s1.n0)) select s2.n0 as n, s2.n1 as m from s2;

-- case: optional match (n:NodeKind1) return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) optional match (m:NodeKind2) where m.distinguishedname = n.unknown + m.unknown optional match (o:NodeKind2) where o.distinguishedname <> n.otherunknown return n, m, o
-- pgsql_params:{"__strlit0":"distinguishedname","__strlit1":"unknown","__strlit2":"otherunknown"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties ->> @__strlit0::text) = ((s0.n0).properties ->> @__strlit1::text) || (n1.properties ->> @__strlit1::text)) and n1.kind_ids operator (pg_catalog.@>) array [2]::int2[]), s2 as (select s0.n0 as n0, s1.n1 as n1 from s0 left outer join s1 on (s0.n0 = s1.n0)), s3 as (select s2.n0 as n0, s2.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s2, node n2 where ((n2.properties -> @__strlit0::text) <> ((s2.n0).properties -> @__strlit2::text)) and n2.kind_ids operator (pg_catalog.@>) array [2]::int2[]), s4 as (select s2.n0 as n0, s2.n1 as n1, s3.n2 as n2 from s2 left outer join s3 on (s2.n1 = s3.n1) and (s2.n0 = s3.n0)) select s4.n0 as n, s4.n1 as m, s4.n2 as o from s4;

-- case: match (n) where n.name = "alpha' || (SELECT inet_server_addr()::text::int) || '" return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"string","__strlit2":"alpha' || (SELECT inet_server_addr()::text::int) || '"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> @__strlit0::text)) = @__strlit1::text and (n0.properties ->> @__strlit0::text) = @__strlit2::text))) select s0.n0 as n from s0;

-- case: match (g:NodeKind2) where not ((g)<-[:EdgeKind1]-(:NodeKind1)) return g
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [2]::int2[]) select s0.n0 as g from s0 where (not ((with s1 as (select s0.n0 as n0 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n1.id = e0.start_id where e0.kind_id = any (array [3]::int2[]) and (s0.n0).id = e0.end_id) select count(*) > 0 from s1)));
