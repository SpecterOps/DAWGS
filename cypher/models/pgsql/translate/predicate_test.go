package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPatternPredicateDoesNotLeakIsolatedFrameIntoPathProjection(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("Domain"))
	kindMapper.Put(graph.StringKind("CrossForestTrust"))
	kindMapper.Put(graph.StringKind("SpoofSIDHistory"))
	kindMapper.Put(graph.StringKind("AbuseTGTDelegation"))

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p=(n:Domain)-[:CrossForestTrust|SpoofSIDHistory|AbuseTGTDelegation]-(m:Domain)
WHERE (n)-[:SpoofSIDHistory|AbuseTGTDelegation]-(m)
RETURN p`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "as p from s0 where")
	require.Contains(t, formatted, "with s1 as")
	require.NotContains(t, formatted, "as p from s1 where")
}

func translatePredicateQuery(t *testing.T, cypherQuery string, parameters map[string]any) string {
	t.Helper()

	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("NodeKind1"))

	query, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, parameters, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	return formatted
}

func TestDynamicStringPredicatesUseHelperFunctions(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		query      string
		parameters map[string]any
		function   string
	}{
		{
			name:       "contains parameter",
			query:      `MATCH (n:NodeKind1) WHERE n.name CONTAINS $needle RETURN n`,
			parameters: map[string]any{"needle": "needle"},
			function:   "cypher_contains",
		},
		{
			name:       "starts with parameter",
			query:      `MATCH (n:NodeKind1) WHERE n.name STARTS WITH $prefix RETURN n`,
			parameters: map[string]any{"prefix": "prefix"},
			function:   "cypher_starts_with",
		},
		{
			name:       "ends with parameter",
			query:      `MATCH (n:NodeKind1) WHERE n.name ENDS WITH $suffix RETURN n`,
			parameters: map[string]any{"suffix": "suffix"},
			function:   "cypher_ends_with",
		},
		{
			name:     "contains property",
			query:    `MATCH (n:NodeKind1) WHERE n.name CONTAINS n.other RETURN n`,
			function: "cypher_contains",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			formatted := translatePredicateQuery(t, testCase.query, testCase.parameters)

			require.Contains(t, formatted, testCase.function+"(")
			require.NotContains(t, formatted, "replace(")
		})
	}
}

func TestLiteralStringPredicatesKeepLikePatterns(t *testing.T) {
	formatted := translatePredicateQuery(t, `MATCH (n:NodeKind1) WHERE n.name CONTAINS 'needle' RETURN n`, nil)

	require.Contains(t, formatted, " like ")
	require.Contains(t, formatted, "'%needle%'")
	require.NotContains(t, formatted, "cypher_contains(")
	require.Equal(t, 1, strings.Count(formatted, " like "))
}

func TestNegatedDynamicStringPredicatesCoalescePropertyLookups(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		query      string
		parameters map[string]any
		expected   string
	}{
		{
			name:       "contains parameter",
			query:      `MATCH (n:NodeKind1) WHERE not n.name CONTAINS $needle RETURN n`,
			parameters: map[string]any{"needle": "needle"},
			expected:   "not cypher_contains(coalesce((n0.properties ->> 'name'), '')::text, (@pi0::text)::text)::bool",
		},
		{
			name:     "contains property",
			query:    `MATCH (n:NodeKind1) WHERE not n.name CONTAINS n.other RETURN n`,
			expected: "not cypher_contains(coalesce((n0.properties ->> 'name'), '')::text, coalesce((n0.properties ->> 'other'), '')::text)::bool",
		},
		{
			name:       "starts with parameter",
			query:      `MATCH (n:NodeKind1) WHERE not n.name STARTS WITH $prefix RETURN n`,
			parameters: map[string]any{"prefix": "prefix"},
			expected:   "not cypher_starts_with(coalesce((n0.properties ->> 'name'), '')::text, (@pi0::text)::text)::bool",
		},
		{
			name:       "ends with parameter",
			query:      `MATCH (n:NodeKind1) WHERE not n.name ENDS WITH $suffix RETURN n`,
			parameters: map[string]any{"suffix": "suffix"},
			expected:   "not cypher_ends_with(coalesce((n0.properties ->> 'name'), '')::text, (@pi0::text)::text)::bool",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			formatted := translatePredicateQuery(t, testCase.query, testCase.parameters)

			require.Contains(t, formatted, testCase.expected)
		})
	}
}
