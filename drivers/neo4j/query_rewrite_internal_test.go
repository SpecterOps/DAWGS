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
