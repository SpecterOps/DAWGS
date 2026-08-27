package pg

import (
	"context"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

func (s *SchemaManager) compileText(ctx context.Context, source string, parameters map[string]any, graphID int32) (string, map[string]any, error) {
	return s.compile(ctx, strings.TrimSpace(source), parameters, graphID, func() (*cypher.RegularQuery, error) {
		return frontend.ParseCypher(frontend.NewContext(), source)
	})
}

func (s *SchemaManager) compile(ctx context.Context, source string, parameters map[string]any, graphID int32, parse func() (*cypher.RegularQuery, error)) (string, map[string]any, error) {
	translationCache := s.translationCacheProvider.TranslationCache()

	build := func() (string, translationCacheBuildResult, error) {
		if regularQuery, err := parse(); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if translated, parameterSources, err := translate.TranslateWithOptionsAndParameterSources(ctx, regularQuery, s, parameters, graphID, translate.DefaultOptions()); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if sqlQuery, err := translate.Translated(translated); err != nil {
			return "", translationCacheBuildResult{}, err
		} else {
			return sqlQuery, translationCacheBuildResult{
				parameters:       translated.Parameters,
				parameterSources: parameterSources,
			}, nil
		}
	}

	key := translationCache.Key(source, graphID, parameters)
	return translationCache.GetOrBuildContext(ctx, key, parameters, build)
}
