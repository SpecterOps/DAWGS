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

-- case: match (n) set n.other = 1 set n.prop = '1' return n
-- pgsql_params:{"__strlit0":"other","__strlit1":"prop","__strlit2":"1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, 1, @__strlit1::text, @__strlit2::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) set n:NodeKind1:NodeKind2 return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = uniq(sort(n1.kind_ids || array [1, 2]::int2[])::int2[])::int2[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) remove n:NodeKind1:NodeKind2 return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = n1.kind_ids - array [1, 2]::int2[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) set n:NodeKind1 remove n:NodeKind2 return n
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = uniq(sort(n1.kind_ids - array [2]::int2[] || array [1]::int2[])::int2[])::int2[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) set n:NodeKind1 set n.prop = '1' return n
-- pgsql_params:{"__strlit0":"prop","__strlit1":"1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = uniq(sort(n1.kind_ids || array [1]::int2[])::int2[])::int2[], properties = n1.properties || jsonb_build_object(@__strlit0::text, @__strlit1::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) set n:NodeKind1 remove n:NodeKind2 set n.prop = '1' remove n.name return n
-- pgsql_params:{"__strlit0":"name","__strlit1":"prop","__strlit2":"1"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = uniq(sort(n1.kind_ids - array [2]::int2[] || array [1]::int2[])::int2[])::int2[], properties = n1.properties - array [@__strlit0::text]::text[] || jsonb_build_object(@__strlit1::text, @__strlit2::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) remove n:NodeKind1 remove n.prop return n
-- pgsql_params:{"__strlit0":"prop"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set kind_ids = n1.kind_ids - array [1]::int2[], properties = n1.properties - array [@__strlit0::text]::text[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n) where n.name = '1234' set n.is_target = true
-- pgsql_params:{"__strlit0":"string","__strlit1":"1234","__strlit2":"is_target"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'name')) = @__strlit0::text and (n0.properties ->> 'name') = @__strlit1::text))), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select 1;

-- case: match (n) where n.name = '1234' match (e) where e.tag = n.tag_id set e.is_target = true
-- pgsql_params:{"__strlit0":"string","__strlit1":"1234","__strlit2":"is_target"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'name')) = @__strlit0::text and (n0.properties ->> 'name') = @__strlit1::text))), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((n1.properties -> 'tag') = ((s0.n0).properties -> 'tag_id'))), s2 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s1 where (s1.n1).id = n2.id returning s1.n0 as n0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n1) select 1;

-- case: match (n1), (n3) set n1.target = true set n3.target = true return n1, n3
-- pgsql_params:{"__strlit0":"target"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1), s2 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s1 where (s1.n0).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0, s1.n1 as n1), s3 as (update node n3 set properties = n3.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s2 where (s2.n1).id = n3.id returning s2.n0 as n0, (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n1) select s3.n0 as n1, s3.n1 as n3 from s3;

-- case: match ()-[r]->(:NodeKind1) set r.is_special_outbound = true
-- pgsql_params:{"__strlit0":"is_special_outbound"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id), s1 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e0) select 1;

-- case: match (a)-[r]->(:NodeKind1) set a.name = '123', r.is_special_outbound = true
-- pgsql_params:{"__strlit0":"name","__strlit1":"123","__strlit2":"is_special_outbound"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id), s1 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit0::text, @__strlit1::text)::jsonb from s0 where (s0.n0).id = n2.id returning s0.e0 as e0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0), s2 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s1 where (s1.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e0, s1.n0 as n0) select 1;

-- case: match (a)-[r]->(:NodeKind1) set a.name = '123', r.is_special_outbound = true
-- pgsql_params:{"__strlit0":"name","__strlit1":"123","__strlit2":"is_special_outbound"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n1 on n1.kind_ids operator (pg_catalog.@>) array [1]::int2[] and n1.id = e0.end_id join node n0 on n0.id = e0.start_id), s1 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit0::text, @__strlit1::text)::jsonb from s0 where (s0.n0).id = n2.id returning s0.e0 as e0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0), s2 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s1 where (s1.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e0, s1.n0 as n0) select 1;

-- case: match (s) remove s.name
-- pgsql_params:{"__strlit0":"name"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set properties = n1.properties - array [@__strlit0::text]::text[] from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select 1;

-- case: match (s) set s.name = 'n' + id(s) remove s.prop
-- pgsql_params:{"__strlit0":"prop","__strlit1":"name","__strlit2":"n"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (update node n1 set properties = n1.properties - array [@__strlit0::text]::text[] || jsonb_build_object(@__strlit1::text, @__strlit2::text || (s0.n0).id)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select 1;

-- case: match (n) where n.name = 'n3' set n.name = 'RENAMED' return n
-- pgsql_params:{"__strlit0":"string","__strlit1":"n3","__strlit2":"name","__strlit3":"RENAMED"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'name')) = @__strlit0::text and (n0.properties ->> 'name') = @__strlit1::text))), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit2::text, @__strlit3::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select s1.n0 as n from s1;

-- case: match (n), (e) where n.name = 'n1' and e.name = 'n4' set n.name = e.name set e.name = 'RENAMED'
-- pgsql_params:{"__strlit0":"string","__strlit1":"n1","__strlit2":"n4","__strlit3":"name","__strlit4":"RENAMED"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((jsonb_typeof((n0.properties -> 'name')) = @__strlit0::text and (n0.properties ->> 'name') = @__strlit1::text))), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where ((jsonb_typeof((n1.properties -> 'name')) = @__strlit0::text and (n1.properties ->> 'name') = @__strlit2::text))), s2 as (update node n2 set properties = n2.properties || jsonb_build_object(@__strlit3::text, ((s1.n1).properties -> 'name'))::jsonb from s1 where (s1.n0).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0, s1.n1 as n1), s3 as (update node n3 set properties = n3.properties || jsonb_build_object(@__strlit3::text, @__strlit4::text)::jsonb from s2 where (s2.n1).id = n3.id returning s2.n0 as n0, (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n1) select 1;

-- case: match (n)-[r:EdgeKind1]->() where n:NodeKind1 set r.visited = true return r
-- pgsql_params:{"__strlit0":"visited"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on (n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e0, s0.n0 as n0) select s1.e0 as r from s1;

-- case: match (n)-[]->()-[r]->() where n.name = 'n1' set r.visited = true return r.name
-- pgsql_params:{"__strlit0":"string","__strlit1":"n1","__strlit2":"visited"}
with s0 as (select e0.id as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on ((jsonb_typeof((n0.properties -> 'name')) = @__strlit0::text and (n0.properties ->> 'name') = @__strlit1::text)) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.id != s0.e0), s2 as (update edge e2 set properties = e2.properties || jsonb_build_object(@__strlit2::text, true)::jsonb from s1 where (s1.e1).id = e2.id returning s1.e0 as e0, (e2.id, e2.start_id, e2.end_id, e2.kind_id, e2.properties)::edgecomposite as e1, s1.n0 as n0, s1.n1 as n1) select ((s2.e1).properties -> 'name') from s2;

