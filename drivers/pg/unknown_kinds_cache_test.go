package pg

import (
	"context"
	"maps"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// A successful empty match must be recompiled after kind registration, through
// both the compiler cache and the connection-local cache, including shortest paths.
func TestUnknownKindsBypassTranslationCaches(t *testing.T) {
	for _, query := range []string{"MATCH (n:NewKind) RETURN count(n)", "MATCH p = shortestPath((n:NewKind)-[:EdgeKind*1..2]->(t)) RETURN p"} {
		t.Run(query, func(t *testing.T) {
			mapper := pgutil.NewInMemoryKindMapper()
			mapper.Put(graph.StringKind("EdgeKind"))
			parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
			require.NoError(t, err)
			registered := false
			builds := 0
			build := func() (translate.Result, string, error) {
				builds++
				result, err := translate.Translate(context.Background(), parsed, mapper, nil, 0)
				if err != nil {
					return result, "", err
				}
				require.Equal(t, !registered, result.HasMissingKinds)
				sql, err := translate.Translated(result)
				maps.Copy(result.Parameters, sql.Parameters)
				result.LiteralParameters = sql.LiteralParameters
				return result, sql.Statement, err
			}
			_, connectionCache, _ := newTestCache(t, 4)
			compilerCache := newTranslationCache(4)
			defer compilerCache.Close()
			for _, registration := range []bool{false, true} {
				registered = registration
				if registered {
					mapper.Put(graph.StringKind("NewKind"))
				}
				for i := 0; i < 2; i++ {
					_, _, err = connectionCache.TranslateWithPolicy(query, 0, nil, "", build)
					require.NoError(t, err)
					_, _, err = compilerCache.GetOrBuild(compilerCache.Key(query, 0, nil), nil, func() (string, translationCacheBuildResult, error) {
						result, sql, err := build()
						return sql, translationCacheBuildResult{parameters: result.Parameters, parameterSources: result.ParameterSources, literalParameters: result.LiteralParameters, hasMissingKinds: result.HasMissingKinds}, err
					})
					require.NoError(t, err)
				}
				if !registered {
					require.Equal(t, 4, builds)
				} else {
					if strings.Contains(query, "shortestPath") {
						// Existing generated SQL parameters already bypass caching for this shape.
						require.Equal(t, 8, builds)
					} else {
						require.Equal(t, 6, builds)
					}
				}
			}
		})
	}
}
