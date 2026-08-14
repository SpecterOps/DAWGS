package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// TestTargetGraphUsesConcreteRelationsInOuterAndHarnessSQL verifies graph partitioning in both the outer query and shortest-path harness.
func TestTargetGraphUsesConcreteRelationsInOuterAndHarnessSQL(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..]->(e:Domain))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
		LIMIT 1
	`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1),
		"end_id":   int64(2),
	}, 42)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "node_42")
	require.Contains(t, formatted, "generate_subscripts(s1.path, 1)")
	require.Contains(t, formatted, "join edge_42")
	require.NotRegexp(t, `(?i)(from|join) (node|edge)(?:\s|;)`, formatted)

	var fragments []string
	for _, value := range translation.Parameters {
		if fragment, ok := value.(string); ok && strings.Contains(fragment, "pg_temp.bsp_") {
			fragments = append(fragments, fragment)
		}
	}
	require.Empty(t, fragments, "contained inline execution should not emit workspace harness fragments")
	for _, fragment := range fragments {
		require.Contains(t, fragment, "edge_42", fmt.Sprintf("unscoped harness fragment: %s", fragment))
		require.NotRegexp(t, `(?i)(from|join) (node|edge)(?:\s|;)`, fragment)
	}
}

// TestFixedSuffixTargetGraphUsesOnlyConcreteRelations verifies that suffix-seeded translation never falls back to unpartitioned graph tables.
func TestFixedSuffixTargetGraphUsesOnlyConcreteRelations(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), optimizerFixedSuffixQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, 42)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "node_42")
	require.Contains(t, formatted, "edge_42")
	require.NotRegexp(t, `(?i)(from|join) (node|edge)(?:\s|;)`, formatted)
	require.Equal(t, 2, strings.Count(formatted, "ordered_edge_ids_to_path(42,"))
}
