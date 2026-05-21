package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func referencedIdentifiersForQuery(t *testing.T, cypherQuery string) *pgsql.IdentifierSet {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)
	require.NotNil(t, regularQuery.SingleQuery)
	require.NotNil(t, regularQuery.SingleQuery.SinglePartQuery)

	referencedIdentifiers, err := collectReferencedIdentifiers(regularQuery.SingleQuery.SinglePartQuery)
	require.NoError(t, err)

	return referencedIdentifiers
}

func TestCollectReferencedIdentifiersIgnoresPatternDeclarations(t *testing.T) {
	referencedIdentifiers := referencedIdentifiersForQuery(t, "match p = ()-[r1]->()-[r2]->(e) return e")

	require.False(t, referencedIdentifiers.Contains("p"))
	require.False(t, referencedIdentifiers.Contains("r1"))
	require.False(t, referencedIdentifiers.Contains("r2"))
	require.True(t, referencedIdentifiers.Contains("e"))
}

func TestCollectReferencedIdentifiersIncludesUsedPathBinding(t *testing.T) {
	referencedIdentifiers := referencedIdentifiersForQuery(t, "match p = ()-[r]->(e) return p")

	require.True(t, referencedIdentifiers.Contains("p"))
	require.False(t, referencedIdentifiers.Contains("r"))
	require.False(t, referencedIdentifiers.Contains("e"))
}

func TestCollectReferencedIdentifiersIncludesPatternPredicateReferences(t *testing.T) {
	referencedIdentifiers := referencedIdentifiersForQuery(t, "match (s)-[r]->() where (s)-[r {prop: 'a'}]->() return s")

	require.True(t, referencedIdentifiers.Contains("s"))
	require.True(t, referencedIdentifiers.Contains("r"))
}

func TestCollectReferencedIdentifiersIncludesRepeatedMatchPatternDeclarations(t *testing.T) {
	referencedIdentifiers := referencedIdentifiersForQuery(t, "match (a)-->(b) match (b)-->(c) return c")

	require.False(t, referencedIdentifiers.Contains("a"))
	require.True(t, referencedIdentifiers.Contains("b"))
	require.True(t, referencedIdentifiers.Contains("c"))
}

func TestQueryPartReferencesBindingForGreedyProjection(t *testing.T) {
	queryPart := NewQueryPart(1, 0)
	queryPart.SetReferencedIdentifiers(pgsql.AsIdentifierSet(pgsql.Identifier(cypher.TokenLiteralAsterisk)))

	binding := &BoundIdentifier{
		Alias: models.OptionalValue(pgsql.Identifier("r")),
	}

	require.True(t, queryPart.ReferencesBinding(binding))
}
