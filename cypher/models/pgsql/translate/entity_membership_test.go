package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestEntityMembershipTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct{ query, fragment string }{
				{"MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE r IN xs RETURN r", "else false end"},
				{"MATCH ()-[r]->() WITH collect(r) AS xs MATCH (n) WHERE n IN xs RETURN n", "else false end"},
				{"MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN m", "n1.id = any (s0.i0)"},
				{"MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN s", "e1.id = any (s0.i0)"},
				{"MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN size(xs)", "_membership_entity"},
				{"MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN size(xs)", "_membership_entity"},
				{"MATCH (n) WITH collect(n) AS xs WITH xs AS ys MATCH ()-[r]->() WHERE r IN ys RETURN r", "else false end"},
				{"MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE id(m) IN xs RETURN m", "else false end"},
				{"MATCH (n) WITH collect(id(n)) AS xs MATCH (m) WHERE m IN xs RETURN m", "else false end"},
				{"MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() RETURN r IN xs", "else false end"},
				{"MATCH (n) WITH collect(n) AS xs RETURN head(xs) IN xs", "_membership_entity"},
				{"MATCH (n) RETURN head(collect(n)) IN collect(n)", "array_agg("},
				{"MATCH (n)-[r]->() RETURN head(collect(n)) IN collect(r)", "else false end"},
			} {
				t.Run(tc.query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, tc.fragment)
				})
			}
		})
	}
}

func TestPrepareCollectIDExpressionRetainsEntityType(t *testing.T) {
	scope := NewScope()
	for _, tc := range []struct{ bindingType, want pgsql.DataType }{
		{pgsql.NodeComposite, pgsql.NodeComposite}, {pgsql.EdgeComposite, pgsql.EdgeComposite},
		{pgsql.ExpansionRootNode, pgsql.NodeComposite}, {pgsql.ExpansionTerminalNode, pgsql.NodeComposite},
		// An expansion relationship variable is a list, not a single relationship.
		{pgsql.ExpansionEdge, pgsql.UnsetDataType}, {pgsql.Int8, pgsql.UnsetDataType},
	} {
		t.Run(string(tc.bindingType), func(t *testing.T) {
			binding, err := scope.DefineNew(tc.bindingType)
			require.NoError(t, err)
			_, entityType, ok := prepareCollectIDExpression(scope, binding.Identifier)
			require.Equal(t, tc.want.IsKnown(), ok)
			require.Equal(t, tc.want, entityType)
		})
	}
	for _, expression := range []pgsql.Expression{pgsql.Identifier("unbound"), pgsql.NewLiteral(1, pgsql.Int8)} {
		_, _, ok := prepareCollectIDExpression(scope, expression)
		require.False(t, ok)
	}
}

func TestCollectedEntityTypeSurvivesScopeCopies(t *testing.T) {
	for _, entityType := range []pgsql.DataType{pgsql.NodeComposite, pgsql.EdgeComposite} {
		binding := &BoundIdentifier{DataType: pgsql.Int8Array, collectedEntityType: entityType}
		require.Equal(t, entityType, binding.Copy().collectedEntityType)
	}
}
