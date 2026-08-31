package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
)

func benchmarkTranslationBuild(query string, parameters map[string]any) func() (string, translationCacheBuildResult, error) {
	return func() (string, translationCacheBuildResult, error) {
		parsedQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
		if err != nil {
			return "", translationCacheBuildResult{}, err
		}

		kindMapper := pgutil.NewInMemoryKindMapper()
		kindMapper.Put(graph.StringKind("NodeKind1"))
		translation, parameterSources, err := translate.TranslateWithOptionsAndParameterSources(context.Background(), parsedQuery, kindMapper, parameters, translate.DefaultGraphID, translate.DefaultOptions())
		if err != nil {
			return "", translationCacheBuildResult{}, err
		}

		sql, err := translate.Translated(translation)
		return sql, translationCacheBuildResult{
			parameters:       translation.Parameters,
			parameterSources: parameterSources,
		}, err
	}
}

func BenchmarkTranslationCacheUncached(b *testing.B) {
	build := benchmarkTranslationBuild(`MATCH (n:NodeKind1) WHERE n.name = $name RETURN n`, map[string]any{"name": "first"})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := build(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslationCacheColdPopulation(b *testing.B) {
	parameters := map[string]any{"name": "first"}
	build := benchmarkTranslationBuild(`MATCH (n:NodeKind1) WHERE n.name = $name RETURN n`, parameters)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		translationCache := newTranslationCache(1)
		if _, _, err := translationCache.GetOrBuild(translationCache.Key(`MATCH (n:NodeKind1) WHERE n.name = $name RETURN n`, 1, parameters), parameters, build); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslationCacheParameterizedHit(b *testing.B) {
	parameters := map[string]any{"name": "first"}
	query := `MATCH (n:NodeKind1) WHERE n.name = $name RETURN n`
	translationCache := newTranslationCache(1)
	key := translationCache.Key(query, 1, parameters)
	build := benchmarkTranslationBuild(query, parameters)
	if _, _, err := translationCache.GetOrBuild(key, parameters, build); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		parameters["name"] = "next"
		if _, _, err := translationCache.GetOrBuild(translationCache.Key(query, 1, parameters), parameters, build); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslationCacheParameterlessHit(b *testing.B) {
	query := `MATCH (n:NodeKind1) RETURN n`
	translationCache := newTranslationCache(1)
	key := translationCache.Key(query, 1, nil)
	build := benchmarkTranslationBuild(query, nil)
	if _, _, err := translationCache.GetOrBuild(key, nil, build); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := translationCache.GetOrBuild(key, nil, build); err != nil {
			b.Fatal(err)
		}
	}
}
