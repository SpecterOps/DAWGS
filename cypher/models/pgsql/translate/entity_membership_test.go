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
			for _, tc := range []struct {
				// query is the Cypher statement exercised by this case.
				query string
				// fragment is the SQL substring required in the translation.
				fragment string
			}{
				{query: "MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() WHERE r IN xs RETURN r", fragment: "else false end"},
				{query: "MATCH ()-[r]->() WITH collect(r) AS xs MATCH (n) WHERE n IN xs RETURN n", fragment: "else false end"},
				{query: "MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN m", fragment: "n1.id = any (s0.i0)"},
				{query: "MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN s", fragment: "e1.id = any (s0.i0)"},
				{query: "MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE m IN xs RETURN size(xs)", fragment: "_membership_entity"},
				{query: "MATCH ()-[r]->() WITH collect(r) AS xs MATCH ()-[s]->() WHERE s IN xs RETURN size(xs)", fragment: "_membership_entity"},
				{query: "MATCH (n) WITH collect(n) AS xs WITH xs AS ys MATCH ()-[r]->() WHERE r IN ys RETURN r", fragment: "else false end"},
				{query: "MATCH (n) WITH collect(n) AS xs MATCH (m) WHERE id(m) IN xs RETURN m", fragment: "else false end"},
				{query: "MATCH (n) WITH collect(id(n)) AS xs MATCH (m) WHERE m IN xs RETURN m", fragment: "else false end"},
				{query: "MATCH (n) WITH collect(n) AS xs MATCH ()-[r]->() RETURN r IN xs", fragment: "else false end"},
				{query: "MATCH (n) WITH collect(n) AS xs RETURN head(xs) IN xs", fragment: "_membership_entity"},
				{query: "MATCH (n) RETURN head(collect(n)) IN collect(n)", fragment: "array_agg("},
				{query: "MATCH (n)-[r]->() RETURN head(collect(n)) IN collect(r)", fragment: "else false end"},
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
	for _, tc := range []struct {
		// bindingType is the tracked type of the membership operand.
		bindingType pgsql.DataType
		// want is the expected entity type, or UnsetDataType when membership is incompatible.
		want pgsql.DataType
	}{
		{bindingType: pgsql.NodeComposite, want: pgsql.NodeComposite}, {bindingType: pgsql.EdgeComposite, want: pgsql.EdgeComposite},
		{bindingType: pgsql.ExpansionRootNode, want: pgsql.NodeComposite}, {bindingType: pgsql.ExpansionTerminalNode, want: pgsql.NodeComposite},
		// An expansion relationship variable is a list, not a single relationship.
		{bindingType: pgsql.ExpansionEdge, want: pgsql.UnsetDataType}, {bindingType: pgsql.Int8, want: pgsql.UnsetDataType},
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
