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

-- case: match (n:NodeKind1) return n.x + n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return n.x - n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"-"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return n.x * n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"*"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return n.x / n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"/"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return n.x % n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"%"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return n.x ^ n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"^"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) return (n.x + n.y) * n.y as value
-- pgsql_params:{"__strlit0":"null","__strlit1":"+","__strlit2":"*"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select cypher_arithmetic(cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit2::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) where n.x + n.y > 9 return n
-- pgsql_params:{"__strlit0":"null","__strlit1":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (cypher_arithmetic(nullif((n0.properties -> E'x'), (@__strlit0::text)::jsonb)::jsonb, nullif((n0.properties -> E'y'), (@__strlit0::text)::jsonb)::jsonb, @__strlit1::text)::jsonb > to_jsonb((9)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) with n.x as x, n.y as y return x + y as value
-- pgsql_params:{"__strlit0":"+"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select ((s1.n0).properties -> E'x') as i0, ((s1.n0).properties -> E'y') as i1 from s1) select cypher_arithmetic((i0)::jsonb, (i1)::jsonb, @__strlit0::text)::jsonb as value from s0;

-- case: match (n:NodeKind1) set n.total = n.x + n.y return n
-- pgsql_params:{"__strlit0":"total","__strlit1":"null","__strlit2":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (update node n1 set properties = n1.properties || jsonb_build_object(@__strlit0::text, cypher_arithmetic(nullif(((s0.n0).properties -> E'x'), (@__strlit1::text)::jsonb)::jsonb, nullif(((s0.n0).properties -> E'y'), (@__strlit1::text)::jsonb)::jsonb, @__strlit2::text)::jsonb)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1), s2 as (select case when (s0.n0).id = (s1.n1).id then s1.n1 else s0.n0 end as n0 from s0 left outer join s1 on (s0.n0).id = (s1.n1).id) select s2.n0 as n from s2;

-- case: match ()-[r:EdgeKind1]->() set r.total = r.x % r.y
-- pgsql_params:{"__strlit0":"total","__strlit1":"null","__strlit2":"%"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s1 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, cypher_arithmetic(nullif(((s0.e0).properties -> E'x'), (@__strlit1::text)::jsonb)::jsonb, nullif(((s0.e0).properties -> E'y'), (@__strlit1::text)::jsonb)::jsonb, @__strlit2::text)::jsonb)::jsonb from s0 where (s0.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s2 as (select case when (s0.e0).id = (s1.e1).id then s1.e1 else s0.e0 end as e0 from s0 left outer join s1 on (s0.e0).id = (s1.e1).id) select 1;

-- case: match (n:NodeKind1) create (m:NodeKind2 {total: n.x + n.y})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"total","__strlit3":"null","__strlit4":"+"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]), s1 as (select s0.n0 as n0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n1_id from s0), s2 as (insert into node (graph_id, id, kind_ids, properties) select 0, s1.n1_id, array [2]::int2[], jsonb_build_object(@__strlit2::text, cypher_arithmetic(nullif(((s1.n0).properties -> E'x'), (@__strlit3::text)::jsonb)::jsonb, nullif(((s1.n0).properties -> E'y'), (@__strlit3::text)::jsonb)::jsonb, @__strlit4::text)::jsonb)::jsonb from s1 returning id as n1_id, (id, kind_ids, properties)::nodecomposite as n1), s3 as (select s1.n0 as n0, s2.n1 as n1 from s1, s2 where s2.n1_id = s1.n1_id) select 1;

