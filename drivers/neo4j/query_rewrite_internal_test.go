package neo4j

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewritePatternPropertyParameters(t *testing.T) {
	params := map[string]any{
		"props": map[string]any{
			"name":  "beta",
			"score": 2,
		},
	}

	query, rewrittenParams, err := rewritePatternPropertyParameters(
		"match (n:TemplateNodeKind1 $props) return n.name",
		params,
	)
	require.NoError(t, err)
	require.NotEqual(t, "match (n:TemplateNodeKind1 $props) return n.name", query)
	require.Contains(t, query, "match (n:TemplateNodeKind1 {")
	require.Contains(t, query, "name: $__dawgs_pattern_property_")
	require.Contains(t, query, "score: $__dawgs_pattern_property_")
	require.Contains(t, query, "return n.name")
	require.Equal(t, params["props"], rewrittenParams["props"])
	require.Len(t, rewrittenParams, 3)
}

func TestRewritePatternPropertyParameters_EmptyMap(t *testing.T) {
	query, rewrittenParams, err := rewritePatternPropertyParameters(
		"match (n:TemplateNodeKind1 $props) return n.name",
		map[string]any{"props": map[string]any{}},
	)
	require.NoError(t, err)
	require.Equal(t, "match (n:TemplateNodeKind1) return n.name", query)
	require.Empty(t, rewrittenParams)
}

func TestRewritePatternPropertyParameters_LeavesRawUnsupportedQueryAlone(t *testing.T) {
	query, rewrittenParams, err := rewritePatternPropertyParameters(
		"match (n) return n[$prop]",
		map[string]any{"prop": "name"},
	)
	require.NoError(t, err)
	require.Equal(t, "match (n) return n[$prop]", query)
	require.Equal(t, map[string]any{"prop": "name"}, rewrittenParams)
}

func TestQueryMayNeedRewrite(t *testing.T) {
	testCases := []struct {
		name  string
		query string
		want  bool
	}{{
		name:  "plain query",
		query: "match (n) return n",
		want:  false,
	}, {
		name:  "pattern property parameter candidate",
		query: "match (n:TemplateNodeKind1 $props) return n",
		want:  true,
	}, {
		name:  "datetime function candidate",
		query: "match (n) where n.lastseen >= datetime() return n",
		want:  true,
	}, {
		name:  "temporal function is case insensitive and allows whitespace",
		query: "match (n) where DateTime \n () <= n.lastseen return n",
		want:  true,
	}, {
		name:  "property names containing date are not temporal function calls",
		query: "match (n) where n.updated_at is not null return n",
		want:  false,
	}, {
		name:  "identifier suffix is not temporal function call",
		query: "match (n) where candidate() return n",
		want:  false,
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, queryMayNeedRewrite(testCase.query))
		})
	}
}

func TestRewriteQuery_WrapsTemporalPropertyComparison(t *testing.T) {
	query, rewrittenParams, err := rewriteQuery(
		"MATCH p=(:Computer)-[r:HasSession]->(:User) WHERE r.lastseen >= datetime() - duration('P3D') RETURN p LIMIT 100",
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, rewrittenParams)
	require.Contains(t, query, "where datetime(r.lastseen) >= datetime() - duration('P3D')")
	require.Contains(t, query, "return p limit 100")
}

func TestRewriteQuery_WrapsTemporalPropertyComparisonRightOperand(t *testing.T) {
	query, rewrittenParams, err := rewriteQuery(
		"match (n) where date() <= n.created_at return n",
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, rewrittenParams)
	require.Contains(t, query, "where date() <= date(n.created_at)")
}

func TestRewriteQuery_DoesNotWrapTemporalComponentLookup(t *testing.T) {
	input := "match (n) where datetime().epochseconds >= n.pwdlastset return n"
	query, rewrittenParams, err := rewriteQuery(input, nil)
	require.NoError(t, err)
	require.Equal(t, input, query)
	require.Nil(t, rewrittenParams)
}

func TestRewriteQuery_RewritesPatternPropertiesAndTemporalComparisons(t *testing.T) {
	params := map[string]any{
		"props": map[string]any{
			"name": "beta",
		},
	}

	query, rewrittenParams, err := rewriteQuery(
		"match (n:TemplateNodeKind1 $props) where n.lastseen >= datetime() return n.name",
		params,
	)
	require.NoError(t, err)
	require.Contains(t, query, "match (n:TemplateNodeKind1 {")
	require.Contains(t, query, "name: $__dawgs_pattern_property_")
	require.Contains(t, query, "where datetime(n.lastseen) >= datetime()")
	require.Equal(t, params["props"], rewrittenParams["props"])
	require.Len(t, rewrittenParams, 2)
}

// Rewritten queries previously degraded integral float literals to integers,
// so `3.0 / 2.0` reached Neo4j as integer division `3 / 2`. The rewrite must
// preserve floating-point literal spellings in the re-emitted query text.
func TestRewriteQuery_PreservesIntegralFloatLiterals(t *testing.T) {
	query, rewrittenParams, err := rewriteQuery(
		"match (n) where date() <= n.created_at return 3.0 / 2.0 as a, [1.0, 2.5] as c, 1e3 as b",
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, rewrittenParams)
	require.Contains(t, query, "return 3.0 / 2.0 as a")
	require.Contains(t, query, "[1.0, 2.5] as c")
	require.Contains(t, query, "1000.0 as b")
}

// Unsupported subscripts must keep the original text for Neo4j rather than
// allowing a different rewrite to emit the parser's former lossy AST.
func TestRewritePreservesListIndexing(t *testing.T) {
	for _, source := range []string{
		"MATCH (n $props) RETURN n.values[0] AS value",
		"MATCH (n) WHERE datetime(n.created) < $threshold RETURN n.values[-1] AS value",
		"WITH $values AS xs RETURN xs[$idx] AS value",
	} {
		t.Run(source, func(t *testing.T) {
			params := map[string]any{"props": map[string]any{"name": "test"}, "values": []int64{1, 2}, "idx": int64(0), "threshold": "2026-01-01"}
			rewritten, rebound, err := rewriteQuery(source, params)
			require.NoError(t, err)
			require.Equal(t, source, rewritten)
			require.Equal(t, params, rebound)
		})
	}
}

func TestRewritePreservesListSlicing(t *testing.T) {
	for _, source := range []string{
		"MATCH (n $props) RETURN n.values[0..2] AS value",
		"MATCH (n) WHERE datetime(n.created) < $threshold RETURN n.values[-1..] AS value",
		"WITH $values AS xs RETURN xs[$idx..] AS value",
	} {
		t.Run(source, func(t *testing.T) {
			params := map[string]any{"props": map[string]any{"name": "test"}, "values": []int64{1, 2}, "idx": int64(0), "threshold": "2026-01-01"}
			rewritten, rebound, err := rewriteQuery(source, params)
			require.NoError(t, err)
			require.Equal(t, source, rewritten)
			require.Equal(t, params, rebound)
		})
	}
}

func TestRewriteQueryPreservesStandaloneCall(t *testing.T) {
	for _, source := range []string{"CALL db.labels()", "CALL db.labels", "CALL db.labels() YIELD *", "CALL example.write($value)", "CALL db.labels() YIELD label WHERE label = $value"} {
		t.Run(source, func(t *testing.T) {
			params := map[string]any{"value": "example"}
			rewritten, rebound, err := rewriteQuery(source, params)
			require.NoError(t, err)
			require.Equal(t, source, rewritten)
			require.Equal(t, params, rebound)
		})
	}
}

func TestRewriteQueryPreservesShortestPathExpressions(t *testing.T) {
	for _, source := range []string{
		"MATCH (a), (b) RETURN shortestPath((a)-[:R*]->(b)) AS p",
		"MATCH (a), (b) RETURN allShortestPaths((a)-[:R*]->(b)) AS p",
		"MATCH (a), (b) SET a.value = $value RETURN shortestPath((a)-[:R*]->(b)) AS p",
	} {
		t.Run(source, func(t *testing.T) {
			params := map[string]any{"value": int64(1)}
			rewritten, rebound, err := rewriteQuery(source, params)
			require.NoError(t, err)
			require.Equal(t, source, rewritten)
			require.Equal(t, params, rebound)
		})
	}
}
