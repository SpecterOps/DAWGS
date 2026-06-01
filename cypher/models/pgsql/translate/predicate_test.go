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
	require.Contains(t, formatted, "exists (select 1 from edge")
	require.Contains(t, formatted, "kind_id = any (array [3, 4]::int2[])")
	require.Contains(t, formatted, "start_id = (s0.n0).id")
	require.Contains(t, formatted, "end_id = (s0.n1).id")
	require.NotContains(t, formatted, "with s1 as")
}

func TestOptimizedPatternPredicatesContinueAfterFirstPlacement(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("Domain"))
	kindMapper.Put(graph.StringKind("SpoofSIDHistory"))
	kindMapper.Put(graph.StringKind("AbuseTGTDelegation"))

	query, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:Domain), (m:Domain)
		WHERE (n)-[:SpoofSIDHistory]-(m)
		AND (n)-[:AbuseTGTDelegation]-(m)
		RETURN n
	`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "array [2]::int2[]")
	require.Contains(t, formatted, "array [3]::int2[]")
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

func TestExclusiveDisjunctionTranslates(t *testing.T) {
	formatted := translatePredicateQuery(t, `MATCH (n:NodeKind1) WHERE true XOR false RETURN n`, nil)

	require.Contains(t, formatted, "true != false")
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
	for _, testCase := range []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "contains",
			query:    `MATCH (n:NodeKind1) WHERE n.name CONTAINS 'needle' RETURN n`,
			expected: "((n0.properties ->> 'name') like '%needle%')",
		},
		{
			name:     "starts with",
			query:    `MATCH (n:NodeKind1) WHERE n.name STARTS WITH 'prefix' RETURN n`,
			expected: "((n0.properties ->> 'name') like 'prefix%')",
		},
		{
			name:     "ends with",
			query:    `MATCH (n:NodeKind1) WHERE n.name ENDS WITH 'suffix' RETURN n`,
			expected: "((n0.properties ->> 'name') like '%suffix')",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			formatted := translatePredicateQuery(t, testCase.query, nil)

			require.Contains(t, formatted, testCase.expected)
			require.Contains(t, formatted, " like ")
			require.NotContains(t, formatted, "cypher_contains(")
			require.NotContains(t, formatted, "cypher_starts_with(")
			require.NotContains(t, formatted, "cypher_ends_with(")
			require.NotContains(t, formatted, "coalesce(")
			require.Equal(t, 1, strings.Count(formatted, " like "))
		})
	}
}

func TestStringPropertyEqualityKeepsBTreeIndexableTextLookup(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		query      string
		parameters map[string]any
		expected   string
	}{
		{
			name:       "untyped parameter equality",
			query:      `MATCH (n) WHERE n.objectid = $objectid RETURN n`,
			parameters: map[string]any{"objectid": "S-1-5-21-1"},
			expected:   "jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text",
		},
		{
			name:       "typed parameter equality",
			query:      `MATCH (n:NodeKind1) WHERE n.objectid = $objectid RETURN n`,
			parameters: map[string]any{"objectid": "S-1-5-21-1"},
			expected:   "jsonb_typeof((n0.properties -> 'objectid')) = 'string' and (n0.properties ->> 'objectid') = @pi0::text",
		},
		{
			name:     "inline property map equality",
			query:    `MATCH (n:NodeKind1 {name: 'indexed-name'}) RETURN n`,
			expected: "jsonb_typeof((n0.properties -> 'name')) = 'string' and (n0.properties ->> 'name') = 'indexed-name'",
		},
		{
			name:     "reversed literal equality",
			query:    `MATCH (n) WHERE 'S-1-5-21-1' = n.objectid RETURN n`,
			expected: "jsonb_typeof((n0.properties -> 'objectid')) = 'string' and 'S-1-5-21-1' = (n0.properties ->> 'objectid')",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			formatted := translatePredicateQuery(t, testCase.query, testCase.parameters)
			normalized := strings.Join(strings.Fields(formatted), " ")

			require.Contains(t, normalized, testCase.expected)
			require.NotContains(t, normalized, "coalesce(")
			require.NotContains(t, normalized, "lower(")
			require.NotContains(t, normalized, "to_jsonb(")
			require.NotContains(t, normalized, "->> 'objectid')::")
			require.NotContains(t, normalized, "->> 'name')::")
		})
	}
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

func TestSelfReferentialPatternDoesNotPanic(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	for _, testCase := range []struct {
		name  string
		query string
	}{
		{
			name:  "self-referential directed with path binding",
			query: `match p = (u)-[]->(u) return p`,
		},
		{
			name:  "self-referential directed without path binding",
			query: `match (u)-[]->(u) return u`,
		},
		{
			name:  "self-referential undirected with path binding",
			query: `match p = (u)-[]-(u) return p`,
		},
		{
			name:  "self-referential expansion",
			query: `match p = (u)-[*1..3]->(u) return p`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			require.NoError(t, err)

			translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
			require.NoError(t, err)

			_, err = Translated(translation)
			require.NoError(t, err)
		})
	}
}
