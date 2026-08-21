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

	return formatted.Statement
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

// TestTwoHopUndirectedPatternPredicateWithUnboundDirectionlessRoot validates that a
// two-hop undirected WHERE predicate whose root step has no outer-bound endpoints is
// correctly translated.  Specifically it checks the behaviour of
// previousFrameTraversalSource: because the root step carries OmitPreviousFrameSource,
// the outer MATCH frame (s0) must NOT be comma-joined into the root CTE's FROM clause
// (which would re-scan the outer CTE and break per-row correlation).  Instead, s0's
// projected column is still carried through the CTE chain as a correlated reference, and
// the terminal set (s2) is sourced from the root CTE (s1).
func TestTwoHopUndirectedPatternPredicateWithUnboundDirectionlessRoot(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("Domain"))             // kind ID 1
	kindMapper.Put(graph.StringKind("SpoofSIDHistory"))    // kind ID 2
	kindMapper.Put(graph.StringKind("AbuseTGTDelegation")) // kind ID 3

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n:Domain)
WHERE (a:Domain)-[:SpoofSIDHistory]-(b:Domain)-[:AbuseTGTDelegation]-(c:Domain)
RETURN n`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	// Extract the individual CTE bodies so each assertion is scoped to the CTE it
	// describes, rather than matching anywhere in the flattened query string.
	s1Body := extractCTEBody(t, formatted.Statement, "s1")
	s2Body := extractCTEBody(t, formatted.Statement, "s2")

	// The predicate root CTE (s1) must NOT have the outer MATCH frame (s0) as a
	// comma-joined FROM source.  OmitPreviousFrameSource suppresses it so the subquery
	// does not re-scan s0 for every outer row.
	require.NotContains(t, s1Body, "from s0, edge")

	// Even without a comma-joined s0, the outer row's column (s0.n0) is projected
	// through s1 as a correlated reference, keeping the predicate tied to its enclosing
	// row without re-scanning the outer CTE.
	require.Contains(t, s1Body, "s0.n0 as n0")

	// The first hop's edge-kind filter (SpoofSIDHistory) belongs to the root CTE (s1).
	require.Contains(t, s1Body, "array [2]::int2[]")

	// The terminal CTE (s2) is built by sourcing rows from the root CTE (s1), forming
	// the correlated terminal set of the two-hop traversal.
	require.Contains(t, s2Body, "from s1")

	// The second hop's edge-kind filter (AbuseTGTDelegation) belongs to the terminal CTE (s2).
	require.Contains(t, s2Body, "array [3]::int2[]")

	// The first hop's filter must not leak into the terminal CTE, nor the second hop's
	// filter into the root CTE.
	require.NotContains(t, s1Body, "array [3]::int2[]")
	require.NotContains(t, s2Body, "array [2]::int2[]")

	// The existence check reads from the terminal set.
	require.Contains(t, formatted, "count(*) > 0 from s2")
}

// extractCTEBody returns the parenthesised body of the named common table
// expression (e.g. "s1") from the formatted query.  It locates "<name> as ("
// and returns the content up to its balanced closing parenthesis, allowing
// assertions to be scoped to a single CTE.
func extractCTEBody(t *testing.T, formatted, name string) string {
	t.Helper()

	marker := name + " as ("
	start := strings.Index(formatted, marker)
	require.NotEqualf(t, -1, start, "CTE %q not found in query", name)

	open := start + len(marker)
	depth := 1

	for i := open; i < len(formatted); i++ {
		switch formatted[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return formatted[open:i]
			}
		}
	}

	require.Failf(t, "unbalanced parentheses", "CTE %q body has no closing parenthesis", name)
	return ""
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
