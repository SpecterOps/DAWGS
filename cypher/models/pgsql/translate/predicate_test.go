package translate

import (
	"context"
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
