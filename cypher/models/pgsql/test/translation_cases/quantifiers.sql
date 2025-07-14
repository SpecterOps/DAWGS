-- Tests the ANY quantifier
-- TODO: Maybe consider making the test framework add new kinds instead of failing so that you don't have to extend the harness for new tests
-- case: MATCH (n:Base) WHERE n.usedeskeyonly OR ANY(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') RETURN n LIMIT 100
with s1 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0

            -- Note: Consider the use of the exists(...) function-like wrapper. PGSQL will optimize downstream materialization if used and may make the resulting comparison much faster
            where (((n0.properties ->> 'usedeskeyonly'))::bool or ((select count(*)::int
                                                                    from unnest(jsonb_to_text_array((n0.properties -> 'supportedencryptiontypes'))) as i0
                                                                    where (i0 like '%DES%')) >= 1)::bool)
              and n0.kind_ids operator (pg_catalog.&&) array [5]::int2[])
select s1.n0 as n
from s1
limit 100;
