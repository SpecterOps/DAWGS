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

-- case: MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE r IN xs RETURN r
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where (case when cardinality(s0.i0) = 0 then false when e0.id is null or s0.i0 is null then (null)::bool else false end)) select s2.e0 as r from s2;

-- case: MATCH ()-[r]->() WITH collect(r) AS xs MATCH (n) WHERE n IN xs RETURN n
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select array_remove(coalesce(array_agg((e0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select s0.i0 as i0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0, node n2 where (case when cardinality(s0.i0) = 0 then false when n2.id is null or s0.i0 is null then (null)::bool else false end)) select s2.n2 as n from s2;

-- case: MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE NOT r IN xs RETURN r
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where (not case when cardinality(s0.i0) = 0 then false when e0.id is null or s0.i0 is null then (null)::bool else false end)) select s2.e0 as r from s2;

-- case: MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN m
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (n1.id = any (s0.i0))) select s2.n1 as m from s2;

-- case: MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN s
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select array_remove(coalesce(array_agg((e0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.i0 as i0 from s0, edge e1 join node n2 on n2.id = e1.start_id join node n3 on n3.id = e1.end_id where (e1.id = any (s0.i0))) select s2.e1 as s from s2;

-- case: MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN size(xs)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(s1.n0)::nodecomposite[], array []::nodecomposite[])::nodecomposite[], null)::nodecomposite[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select cardinality(s2.i0)::int as "size(xs)" from s2 where (case when s2.i0 is null then (null)::bool else (s2.n1).id = any (array(select (_membership_entity).id from unnest(s2.i0) as _membership_entity)) end);

-- case: MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN size(xs)
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select array_remove(coalesce(array_agg(s1.e0)::edgecomposite[], array []::edgecomposite[])::edgecomposite[], null)::edgecomposite[] as i0 from s1), s2 as (select (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.i0 as i0 from s0, edge e1 join node n2 on n2.id = e1.start_id join node n3 on n3.id = e1.end_id) select cardinality(s2.i0)::int as "size(xs)" from s2 where (case when s2.i0 is null then (null)::bool else (s2.e1).id = any (array(select (_membership_entity).id from unnest(s2.i0) as _membership_entity)) end);

-- case: MATCH (n) WITH collect(n) AS xs WITH xs AS ys MATCH ()-[r]->() WHERE r IN ys RETURN r
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg(s1.n0)::nodecomposite[], array []::nodecomposite[])::nodecomposite[], null)::nodecomposite[] as i0 from s1), s2 as (select s0.i0 as i1 from s0), s3 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s2.i1 as i1 from s2, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where (case when cardinality(s2.i1) = 0 then false when e0.id is null or s2.i1 is null or array_position(s2.i1, null) is not null then (null)::bool else false end)) select s3.e0 as r from s3;

-- case: MATCH (n) WITH collect(n) AS xs OPTIONAL MATCH ()-[r:EdgeKind1]->() RETURN r IN xs
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as materialized (select s0.i0 as i0, row_number() over () as i1 from s0), s3 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s2.i0 as i0, s2.i1 as i1, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s2, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where e0.kind_id = any (array [3]::int2[])), s4 as (select s2.i0 as i0, s3.e0 as e0, s3.n1 as n1, s3.n2 as n2 from s2 left outer join s3 on s2.i1 = s3.i1) select case when cardinality(s4.i0) = 0 then false when (s4.e0).id is null or s4.i0 is null then (null)::bool else false end as "r in xs" from s4;

-- case: MATCH (n) WITH collect(n) AS xs MATCH (m) RETURN id(m) IN xs
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1) select case when cardinality(s2.i0) = 0 then false when (s2.n1).id is null or s2.i0 is null then (null)::bool else false end as "id(m) in xs" from s2;

-- case: MATCH (n) WITH collect(id(n)) AS xs MATCH (m) WHERE m IN xs RETURN m
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((s1.n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select s0.i0 as i0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (case when cardinality(s0.i0) = 0 then false when n1.id is null or s0.i0 is null or array_position(s0.i0, null) is not null then (null)::bool else false end)) select s2.n1 as m from s2;

-- case: MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE r IN xs SET r.hit = true RETURN r
-- pgsql_params:{"__strlit0":"hit"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where (case when cardinality(s0.i0) = 0 then false when e0.id is null or s0.i0 is null then (null)::bool else false end)), s3 as (update edge e1 set properties = e1.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s2 where (s2.e0).id = e1.id returning (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1), s4 as (select case when (s2.e0).id = (s3.e1).id then s3.e1 else s2.e0 end as e0, s2.i0 as i0 from s2 left outer join s3 on (s2.e0).id = (s3.e1).id) select s4.e0 as r from s4;

-- case: MATCH ()-[r]->() WITH collect(r) AS xs MATCH (n) WHERE n IN xs SET n.hit = true
-- pgsql_params:{"__strlit0":"hit"}
with s0 as (with s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select array_remove(coalesce(array_agg((e0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select s0.i0 as i0, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0, node n2 where (case when cardinality(s0.i0) = 0 then false when n2.id is null or s0.i0 is null then (null)::bool else false end)), s3 as (update node n3 set properties = n3.properties || jsonb_build_object(@__strlit0::text, true)::jsonb from s2 where (s2.n2).id = n3.id returning (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n3), s4 as (select s2.i0 as i0, case when (s2.n2).id = (s3.n3).id then s3.n3 else s2.n2 end as n2 from s2 left outer join s3 on (s2.n2).id = (s3.n3).id) select 1;

-- case: MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE NOT r IN xs CREATE (:NodeKind1 {hit: true})
-- pgsql_params:{"__strlit0":"node","__strlit1":"id","__strlit2":"hit"}
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select array_remove(coalesce(array_agg((n0).id)::int8[], array []::int8[])::int8[], null)::int8[] as i0 from s1), s2 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.i0 as i0 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where (not case when cardinality(s0.i0) = 0 then false when e0.id is null or s0.i0 is null then (null)::bool else false end)), s3 as (select s2.e0 as e0, s2.i0 as i0, nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))::int8 as n3_id from s2), s4 as (insert into node (graph_id, id, kind_ids, properties) select 0, s3.n3_id, array [1]::int2[], jsonb_build_object(@__strlit2::text, true)::jsonb from s3 returning id as n3_id, (id, kind_ids, properties)::nodecomposite as n3), s5 as (select s3.e0 as e0, s3.i0 as i0, s4.n3 as n3 from s3, s4 where s4.n3_id = s3.n3_id) select 1;

-- case: MATCH (n) RETURN head(collect(n)) IN collect(n)
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0) select (array_remove(coalesce(array_agg(s0.n0)::nodecomposite[], array []::nodecomposite[])::nodecomposite[], null)::nodecomposite[])[1] = any (array_remove(coalesce(array_agg(s0.n0)::nodecomposite[], array []::nodecomposite[])::nodecomposite[], null)::nodecomposite[]) as "head(collect(n)) in collect(n)" from s0;

-- case: MATCH (n)-[r]->() RETURN head(collect(n)) IN collect(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select case when cardinality(array_remove(coalesce(array_agg(s0.e0)::edgecomposite[], array []::edgecomposite[])::edgecomposite[], null)::edgecomposite[]) = 0 then false when (array_remove(coalesce(array_agg(s0.n0)::nodecomposite[], array []::nodecomposite[])::nodecomposite[], null)::nodecomposite[])[1] is null or array_remove(coalesce(array_agg(s0.e0)::edgecomposite[], array []::edgecomposite[])::edgecomposite[], null)::edgecomposite[] is null or array_position(array_remove(coalesce(array_agg(s0.e0)::edgecomposite[], array []::edgecomposite[])::edgecomposite[], null)::edgecomposite[], null) is not null then (null)::bool else false end as "head(collect(n)) in collect(r)" from s0;
