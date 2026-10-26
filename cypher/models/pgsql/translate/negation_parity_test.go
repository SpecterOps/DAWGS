package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// Repeated NOT tokens previously collapsed into a single negation regardless of
// parity, so `NOT NOT n.k = 1` filtered the rows the positive predicate keeps.
// Every shape here must translate with the NOT count's parity intact in both
// optimizer modes: an even count cancels and an odd count negates exactly once.
func TestNegationParityTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, statement string
			}{
				{
					"double negation in where",
					`MATCH (n:MemberOf) WHERE NOT NOT n.k = 1 RETURN n.k AS x`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'k') as x from s0;`,
				},
				{
					"triple negation in where",
					`MATCH (n:MemberOf) WHERE NOT NOT NOT n.k = 1 RETURN n.k AS x`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'k') as x from s0;`,
				},
				{
					"explicit parenthesized double negation",
					`MATCH (n:MemberOf) WHERE NOT (NOT n.k = 1) RETURN n.k AS x`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'k') as x from s0;`,
				},
				{
					"single negation control",
					`MATCH (n:MemberOf) WHERE NOT n.k = 1 RETURN n.k AS x`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (not ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'k') as x from s0;`,
				},
				{
					"double negation within a conjunction",
					`MATCH (n:MemberOf) WHERE n.j = 2 AND NOT NOT n.k = 1 RETURN n.k AS x`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'j'))::jsonb = to_jsonb((2)::int8)::jsonb and ((n0.properties -> E'k'))::jsonb = to_jsonb((1)::int8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'k') as x from s0;`,
				},
				{
					"double negated string predicate loses the negation rewrite",
					`MATCH (n:MemberOf) WHERE NOT NOT n.name STARTS WITH 'a' RETURN n.name`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((n0.properties ->> E'name') like @__strlit0::text) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;`,
				},
				{
					"double negated null predicate keeps the property rewrite",
					`MATCH (n:MemberOf) WHERE NOT NOT n.name IS NULL RETURN n.name`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where ((not n0.properties ? E'name' or (n0.properties -> E'name') = (@__strlit0::text)::jsonb)) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;`,
				},
				{
					"double negation in projection",
					`RETURN NOT NOT true AS value`,
					`select true as value;`,
				},
				{
					"double negation in with where continuation",
					`UNWIND [1,2,3] AS x WITH x WHERE NOT NOT x > 1 RETURN x ORDER BY x`,
					`with s0 as (select i0 as i0 from unnest(array [1, 2, 3]::int8[]) as i0 where (i0 > 1)) select s0.i0 as x from s0 order by s0.i0;`,
				},
				{
					"double negated projected predicate",
					`UNWIND [1,2,3] AS x RETURN x, NOT NOT x > 1 AS again ORDER BY x`,
					`select i0 as x, i0 > 1 as again from unnest(array [1, 2, 3]::int8[]) as i0 order by x;`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err, "%s must translate without an error", tc.query)

					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Equal(t, tc.statement, formatted.Statement)
				})
			}
		})
	}
}
