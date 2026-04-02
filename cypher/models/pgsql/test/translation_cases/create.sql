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

-- case: create (n:NodeKind1 {name: 'Bob'}) return n
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'Bob')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select s0.n0 as n from s0;

-- case: create (n:NodeKind1)
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select 1;

-- case: create (n)
with s0 as (insert into node (kind_ids, properties) values (array []::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select 1;

-- case: create (n:NodeKind1:NodeKind2 {name: 'Bob', value: 1}) return n
with s0 as (insert into node (kind_ids, properties) values (array [1, 2]::int2[], jsonb_build_object('name', 'Bob', 'value', 1)::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select s0.n0 as n from s0;

-- case: create (n) return n
with s0 as (insert into node (kind_ids, properties) values (array []::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select s0.n0 as n from s0;

-- case: create (n:NodeKind1 {name: 'Alice', value: 42}) return n
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'Alice', 'value', 42)::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0) select s0.n0 as n from s0;

-- case: match (n:NodeKind1) with n create (m:NodeKind2) return m
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1), s2 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1) select s2.n1 as m from s2;

-- case: match (n:NodeKind1) with n create (m:NodeKind2 {name: 'Bob'}) return m
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1), s2 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object('name', 'Bob')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1) select s2.n1 as m from s2;

-- case: match (n:NodeKind1) with n create (m:NodeKind2)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1), s2 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1) select 1;

-- case: create (a:NodeKind1)-[:EdgeKind1]->(b:NodeKind2)
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object()::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select 1;

-- case: create (a:NodeKind1)-[r:EdgeKind1]->(b:NodeKind2) return r
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object()::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select s2.e0 as r from s2;

-- case: create (a:NodeKind1)-[:EdgeKind1 {name: 'rel'}]->(b:NodeKind2)
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object('name', 'rel')::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select 1;

-- case: create (a:NodeKind1)<-[:EdgeKind1]-(b:NodeKind2)
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s1.n1).id, (s0.n0).id, 3, jsonb_build_object()::jsonb from s1, s0 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select 1;

-- case: match (a:NodeKind1) with a create (a)-[:EdgeKind1]->(b:NodeKind2)
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1), s2 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s3 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s2.n1).id, 3, jsonb_build_object()::jsonb from s0, s2 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select 1;

-- case: match (a:NodeKind1) with a create (a)-[r:EdgeKind1]->(b:NodeKind2) return r
with s0 as (with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where n0.kind_ids operator (pg_catalog.@>) array [1]::int2[]) select s1.n0 as n0 from s1), s2 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object()::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s3 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s2.n1).id, 3, jsonb_build_object()::jsonb from s0, s2 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select s3.e0 as r from s3;

-- case: create (a:NodeKind1 {name: 'abc'})-[:EdgeKind1 {prop: 123}]->(:NodeKind2 {name: 'test'}) return a
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'abc')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object('name', 'test')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object('prop', 123)::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select s0.n0 as a from s0;

-- case: create (:NodeKind1 {name: 'abc'})-[:EdgeKind1 {prop: 123}]->(c:NodeKind2 {name: 'test'}) return c
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'abc')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object('name', 'test')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object('prop', 123)::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select s1.n1 as c from s1;

-- case: create (:NodeKind1 {name: 'abc'})-[:EdgeKind1 {prop: 123}]->(c:NodeKind2 {name: 'test'})<-[:EdgeKind2]-(:NodeKind1 {name: 'other'}) return c
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'abc')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object('name', 'test')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'other')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n2), s3 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object('prop', 123)::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0), s4 as (insert into edge (start_id, end_id, kind_id, properties) select (s2.n2).id, (s1.n1).id, 4, jsonb_build_object()::jsonb from s2, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e1) select s1.n1 as c from s1;

-- case: create p = (:NodeKind1 {name: 'abc'})-[:EdgeKind1 {prop: 123}]->(:NodeKind2 {name: 'test'}) return p
with s0 as (insert into node (kind_ids, properties) values (array [1]::int2[], jsonb_build_object('name', 'abc')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n0), s1 as (insert into node (kind_ids, properties) values (array [2]::int2[], jsonb_build_object('name', 'test')::jsonb) returning (id, kind_ids, properties)::nodecomposite as n1), s2 as (insert into edge (start_id, end_id, kind_id, properties) select (s0.n0).id, (s1.n1).id, 3, jsonb_build_object('prop', 123)::jsonb from s0, s1 returning (id, start_id, end_id, kind_id, properties)::edgecomposite as e0) select edges_to_path(variadic array [(s2.e0).id]::int8[])::pathcomposite as p from s0, s2, s1;
