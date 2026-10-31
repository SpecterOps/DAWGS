package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// Integral floating-point literals previously rendered through
// strconv.FormatFloat('f', -1), so `3.0` degraded to the integer literal `3`
// and PostgreSQL performed integer arithmetic: `RETURN 3.0 / 2.0` returned 1
// and `toString(1.0)` returned "1". Every shape here must keep a fractional
// suffix in both optimizer modes so the literal remains a PostgreSQL numeric
// literal and float arithmetic never silently becomes integer arithmetic.
func TestFloatLiteralTypeTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, statement string
			}{
				{
					"float division stays float arithmetic",
					`RETURN 3.0 / 2.0 AS a`,
					`select 3.0 / 2.0 as a;`,
				},
				{
					"integral float projection keeps its type",
					`RETURN 3.0 AS a`,
					`select 3.0 as a;`,
				},
				{
					"exponent literal keeps float type in decimal form",
					`RETURN 1e3 AS b`,
					`select 1000.0 as b;`,
				},
				{
					"toString of an integral float keeps fractional text",
					`RETURN toString(1.0) AS s`,
					`select (1.0)::text as s;`,
				},
				{
					"float list literal keeps element types",
					`UNWIND [1.0, 2.5] AS x RETURN x`,
					`select i0 as x from unnest(array [1.0, 2.5]::float8[]) as i0;`,
				},
				{
					"float membership list keeps element types",
					`MATCH (n:MemberOf) WHERE n.score IN [-1.0, 0.0] RETURN n.name`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties ->> E'score'))::float8 = any (array [- 1.0, 0.0]::float8[])) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;`,
				},
				{
					"property equality against an integral float",
					`MATCH (n:MemberOf) WHERE n.score = 2.0 RETURN n.name`,
					`with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (((n0.properties -> E'score'))::jsonb = to_jsonb((2.0)::float8)::jsonb) and n0.kind_ids operator (pg_catalog.@>) array [10]::int2[]) select ((s0.n0).properties -> E'name') as "n.name" from s0;`,
				},
				{
					"integer division control stays integer arithmetic",
					`RETURN 3 / 2 AS a`,
					`select 3 / 2 as a;`,
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
