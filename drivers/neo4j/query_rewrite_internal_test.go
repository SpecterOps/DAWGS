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
