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

type tracingKindMapper struct {
	*pgutil.InMemoryKindMapper
	lookups, assertions graph.Kinds
}

func (s *tracingKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	s.lookups = append(s.lookups, kinds...)
	return s.InMemoryKindMapper.MapKinds(ctx, kinds)
}

func (s *tracingKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	s.assertions = append(s.assertions, kinds...)
	return s.InMemoryKindMapper.AssertKinds(ctx, kinds)
}

func TestQuotedKindsResolveSemanticNames(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, source := range []string{
			"MATCH (n:Base) RETURN count(n)", "MATCH (n:`Base`) RETURN count(n)",
			"MATCH (n:`Base`:`has space`) RETURN n", "MATCH (n:```Base```) RETURN n",
			"MATCH ()-[r:`Base`|`has``tick`]->() RETURN r",
			"MATCH (n) WHERE NOT n:`Base` RETURN n", "MATCH (n) WHERE n:`has space` RETURN n",
			"MATCH (n:`Base`) SET n:`has space` RETURN n", "MATCH (n:`Base`) REMOVE n:`has``tick` RETURN n",
			"CREATE (n:`Base`)-[:`has``tick`]->(m:`has space`) RETURN n",
		} {
			t.Run(string(mode)+"/"+source, func(t *testing.T) {
				mapper := &tracingKindMapper{InMemoryKindMapper: pgutil.NewInMemoryKindMapper()}
				for _, raw := range []string{"Base", "has space", "has`tick", "`Base`"} {
					mapper.Put(graph.StringKind(raw))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.False(t, result.HasMissingKinds)
				require.Len(t, mapper.KindToID, 4)
				if strings.HasPrefix(source, "CREATE") {
					require.ElementsMatch(t, graph.Kinds{graph.StringKind("Base"), graph.StringKind("has space"), graph.StringKind("has`tick")}, mapper.assertions)
				} else {
					require.NotEmpty(t, mapper.lookups)
					require.Empty(t, mapper.assertions)
				}
				_, err = Translated(result)
				require.NoError(t, err)
			})
		}
	}
}

func TestQuotedAndBareKindsUseSameIDs(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, source := range []string{
			"MATCH (n:Base) RETURN count(n)",
			"MATCH (n) WHERE NOT n:Base RETURN n",
			"MATCH ()-[r:Base|Other]->() RETURN r",
			"MATCH (n:Base) SET n:Other RETURN n",
			"MATCH (n:Base) REMOVE n:Other RETURN n",
			"CREATE (n:Base)-[:Other]->(m:Base) RETURN n",
		} {
			t.Run(string(mode)+"/"+source, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, raw := range []string{"Base", "Other", "`Base`", "`Other`"} {
					mapper.Put(graph.StringKind(raw))
				}
				var statements []string
				for _, query := range []string{source, strings.NewReplacer(":Base", ":`Base`", ":Other", ":`Other`", "|Other", "|`Other`").Replace(source)} {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					require.False(t, result.HasMissingKinds)
					sql, err := Translated(result)
					require.NoError(t, err)
					statements = append(statements, sql.Statement)
				}
				require.Equal(t, statements[0], statements[1])
				require.Len(t, mapper.KindToID, 4)
			})
		}
	}
}
