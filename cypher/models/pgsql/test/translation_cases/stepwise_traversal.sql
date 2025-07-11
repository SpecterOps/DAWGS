-- case: match ()-[r]->() return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id) select s0.e0 as r from s0;

-- case: match ()-[r]->() where type(r) = 'EdgeKind1' return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.kind_id = 3)) select s0.e0 as r from s0;

-- case: match ()-[r]->() where 'EdgeKind1' = type(r) return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (3 = e0.kind_id)) select s0.e0 as r from s0;

-- case: match (n), ()-[r]->() return n, r
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0), s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id) select s1.n0 as n, s1.e0 as r from s1;

-- case: match ()-[r]->(), ()-[e]->() return r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2, (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n3 from s0, edge e1 join node n2 on n2.id = e1.start_id join node n3 on n3.id = e1.end_id) select s1.e0 as r, s1.e1 as e from s1;

-- case: match p = (:NodeKind1)-[:EdgeKind1|EdgeKind2]->(c:NodeKind2) where '123' in c.prop2 or '243' in c.prop2 or size(c.prop2) = 0 return p limit 10
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where ('123' = any (jsonb_to_text_array((n1.properties -> 'prop2'))::text[]) or '243' = any (jsonb_to_text_array((n1.properties -> 'prop2'))::text[]) or jsonb_array_length((n1.properties -> 'prop2'))::int = 0) and n1.kind_ids operator (pg_catalog.&&) array [2]::int2[] and e0.kind_id = any (array [3, 4]::int2[]) and n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]) select edges_to_path(variadic array [(s0.e0).id]::int8[])::pathcomposite as p from s0 limit 10;

-- case: match ()-[r:EdgeKind1]->() return count(r) as the_count
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3]::int2[])) select count(s0.e0)::int8 as the_count from s0;

-- case: match ()-[r:EdgeKind1]->({name: "123"}) return count(r) as the_count
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.properties ->> 'name') = '123' and e0.kind_id = any (array [3]::int2[])) select count(s0.e0)::int8 as the_count from s0;

-- case: match (s)-[r]->(e) where id(e) = $a and not (id(s) = $b) and (r:EdgeKind1 or r:EdgeKind2) and not (s.objectid ends with $c or e.objectid ends with $d) return distinct id(s), id(r), id(e)
-- cypher_params: {"a":1,"b":2,"c":"123","d":"456"}
-- pgsql_params:{"pi0":1,"pi1":2,"pi2":"123","pi3":"456"}
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (not ((n0.properties ->> 'objectid') like '%' || @pi2::text or (n1.properties ->> 'objectid') like '%' || @pi3::text) and n1.id = @pi0::float8) and ((e0.kind_id = any (array [3]::int2[]) or e0.kind_id = any (array [4]::int2[]))) and (not (n0.id = @pi1::float8))) select (s0.n0).id, (s0.e0).id, (s0.n1).id from s0;

-- case: match (s)-[r]->(e) where s.name = '123' and e:NodeKind1 and not r.property return s, r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.kind_ids operator (pg_catalog.&&) array [1]::int2[]) and (not ((e0.properties ->> 'property'))::bool) and ((n0.properties ->> 'name') = '123')) select s0.n0 as s, s0.e0 as r, s0.n1 as e from s0;

-- case: match ()-[r]->() where r.value = 42 return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties ->> 'value'))::int8 = 42)) select s0.e0 as r from s0;

-- case: match ()-[r]->() where r.bool_prop return r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (((e0.properties ->> 'bool_prop'))::bool)) select s0.e0 as r from s0;

-- case: match (n)-[r]->() where n.name = '123' return n, r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where ((n0.properties ->> 'name') = '123')) select s0.n0 as n, s0.e0 as r from s0;

-- case: match (n:NodeKind1)-[r]->() where n.name = '123' or n.name = '321' or n.name = '222' or n.name = '333' return n, r
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where ((n0.properties ->> 'name') = '123' or (n0.properties ->> 'name') = '321' or (n0.properties ->> 'name') = '222' or (n0.properties ->> 'name') = '333') and n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]) select s0.n0 as n, s0.e0 as r from s0;

-- case: match (s)-[r]->(e) where s.name = '123' and e.name = '321' return s, r, e
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where ((n1.properties ->> 'name') = '321') and ((n0.properties ->> 'name') = '123')) select s0.n0 as s, s0.e0 as r, s0.n1 as e from s0;

-- case: match (f), (s)-[r]->(e) where not f.bool_field and s.name = '123' and e.name = '321' return f, s, r, e
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties ->> 'bool_field'))::bool)), s1 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0, edge e0 join node n1 on n1.id = e0.start_id join node n2 on n2.id = e0.end_id where ((n2.properties ->> 'name') = '321') and ((n1.properties ->> 'name') = '123')) select s1.n0 as f, s1.n1 as s, s1.e0 as r, s1.n2 as e from s1;

-- case: match ()-[e0]->(n)<-[e1]-() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.end_id join node n2 on n2.id = e1.start_id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match ()-[e0]->(n)-[e1]->() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match ()<-[e0]-(n)<-[e1]-() return e0, n, e1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.end_id join node n1 on n1.id = e0.start_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.end_id join node n2 on n2.id = e1.start_id) select s1.e0 as e0, s1.n1 as n, s1.e1 as e1 from s1;

-- case: match (s)<-[r:EdgeKind1|EdgeKind2]-(e) return s.name, e.name
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.end_id join node n1 on n1.id = e0.start_id where e0.kind_id = any (array [3, 4]::int2[])) select ((s0.n0).properties -> 'name'), ((s0.n1).properties -> 'name') from s0;

-- case: match (s)-[:EdgeKind1|EdgeKind2]->(e)-[:EdgeKind1]->() return s.name as s_name, e.name as e_name
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [3, 4]::int2[])), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where e1.kind_id = any (array [3]::int2[])) select ((s1.n0).properties -> 'name') as s_name, ((s1.n1).properties -> 'name') as e_name from s1;

-- case: match (s:NodeKind1)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2) return s.name, e.name
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where n1.kind_ids operator (pg_catalog.&&) array [2]::int2[] and e0.kind_id = any (array [3, 4]::int2[]) and n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]) select ((s0.n0).properties -> 'name'), ((s0.n1).properties -> 'name') from s0;

-- case: match (s)-[r:EdgeKind1]->() where (s)-[r {prop: 'a'}]->() return s
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.properties ->> 'prop') = 'a' and e0.kind_id = any (array [3]::int2[])) select s0.n0 as s from s0 where ((with s1 as (select s0.e0 as e0, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e0 on (s0.n0).id = (s0.e0).start_id join node n2 on n2.id = (s0.e0).end_id) select count(*) > 0 from s1));

-- case: match (s)-[r:EdgeKind1]->(e) where not (s.system_tags contains 'admin_tier_0') and id(e) = 1 return id(s), labels(s), id(r), type(r)
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.id = 1) and e0.kind_id = any (array [3]::int2[]) and (not (coalesce((n0.properties ->> 'system_tags'), '')::text like '%admin\_tier\_0%'))) select (s0.n0).id, (s0.n0).kind_ids, (s0.e0).id, (s0.e0).kind_id from s0;

-- case: match (s)-[r]->(e) where s:NodeKind1 and toLower(s.name) starts with 'test' and r:EdgeKind1 and id(e) in [1, 2] return r limit 1
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.id = any (array [1, 2]::int8[])) and (e0.kind_id = any (array [3]::int2[])) and (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] and lower((n0.properties ->> 'name'))::text like 'test%')) select s0.e0 as r from s0 limit 1;

-- case: match (n1)-[]->(n2) where n1 <> n2 return n2
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n0.id <> n1.id)) select s0.n1 as n2 from s0;

-- case: match (n1)-[]->(n2) where n2 <> n1 return n2
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (n1.id <> n0.id)) select s0.n1 as n2 from s0;

-- case: match ()-[r]->()-[e]->(n) where r <> e return n
with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id), s1 as (select s0.e0 as e0, (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1, s0.n0 as n0, s0.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2 from s0 join edge e1 on (s0.n1).id = e1.start_id join node n2 on n2.id = e1.end_id where ((s0.e0).id <> e1.id)) select s1.n2 as n from s1;

