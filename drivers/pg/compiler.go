package pg

import (
	"context"
	"maps"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	cypherFormat "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/query"
)

const builderParameterPrefix = "__dawgs_builder_p"

var builderCommentNewlines = strings.NewReplacer("\r\n", "\n-- ", "\r", "\n-- ", "\n", "\n-- ")

// preparedRegularQuery holds a canonical view of a builder query. The source
// AST remains untouched on cache hits; cold compilation creates its own copy.
// Parameter values remain request-local and are never retained by the cache.
type preparedRegularQuery struct {
	query         *cypher.RegularQuery
	source        string
	commentSource string
	parameters    map[string]any
	identity      builderCacheIdentity
}

func prepareRegularQuery(regularQuery *cypher.RegularQuery) (preparedRegularQuery, error) {
	namer := query.NewParameterNamerWithPrefix(builderParameterPrefix)
	if err := walk.Cypher(regularQuery, namer); err != nil {
		return preparedRegularQuery{}, err
	} else if source, err := cypherFormat.RegularQueryWithParameterSequence(regularQuery, false, namer.Symbols); err != nil {
		return preparedRegularQuery{}, err
	} else if commentSource, err := cypherFormat.RegularQueryWithParameterSequence(regularQuery, true, namer.Symbols); err != nil {
		return preparedRegularQuery{}, err
	} else {
		return preparedRegularQuery{
			query:         regularQuery,
			source:        strings.TrimSpace(source),
			commentSource: strings.TrimSpace(commentSource),
			parameters:    namer.Parameters,
			identity:      builderIdentity(regularQuery),
		}, nil
	}
}

func (s preparedRegularQuery) translationQuery() (*cypher.RegularQuery, error) {
	owned := cypher.Copy(s.query)
	rewriter := query.NewParameterRewriterWithPrefix(builderParameterPrefix)
	if err := walk.Cypher(owned, rewriter); err != nil {
		return nil, err
	}

	return owned, nil
}

// Both PostgreSQL Cypher entry points share cacheability rules, with separate
// identities because formatted builder text cannot represent every AST detail.
func (s *SchemaManager) compileText(ctx context.Context, source string, parameters map[string]any, graphID int32) (string, map[string]any, error) {
	return s.compile(ctx, strings.TrimSpace(source), parameters, graphID, nil, func() (*cypher.RegularQuery, error) {
		return frontend.ParseCypher(frontend.NewContext(), source)
	})
}

func (s *SchemaManager) compileRegularQuery(ctx context.Context, prepared preparedRegularQuery, graphID int32) (string, map[string]any, error) {
	return s.compile(ctx, prepared.source, prepared.parameters, graphID, &prepared.identity, func() (*cypher.RegularQuery, error) {
		return prepared.translationQuery()
	})
}

func (s *SchemaManager) compile(ctx context.Context, source string, parameters map[string]any, graphID int32, identity *builderCacheIdentity, parse func() (*cypher.RegularQuery, error)) (string, map[string]any, error) {
	var (
		translationCache   = s.compilationCacheProvider.TranslationCache()
		optimized          = OptimizedTranslationEnabled()
		translationOptions = translate.Options{
			OptimizerMode: translate.OptimizerDisabled,
		}
	)
	if optimized {
		translationOptions.OptimizerMode = translate.OptimizerEnabled
	}

	build := func() (string, translationCacheBuildResult, error) {
		if regularQuery, err := parse(); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if translated, parameterSources, err := translate.TranslateWithOptionsAndParameterSources(ctx, regularQuery, s, parameters, graphID, translationOptions); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if sqlQuery, err := translate.Translated(translated); err != nil {
			return "", translationCacheBuildResult{}, err
		} else {
			maps.Copy(translated.Parameters, sqlQuery.Parameters)
			return sqlQuery.Statement, translationCacheBuildResult{
				hasMissingKinds:   translated.HasMissingKinds,
				parameters:        translated.Parameters,
				parameterSources:  parameterSources,
				literalParameters: sqlQuery.LiteralParameters,
			}, nil
		}
	}

	if !optimized {
		return translationCache.BuildUnoptimized(build)
	}
	if identity != nil && !identity.valid {
		translationCache.bypasses.Add(1)
		return translationCache.buildUncached(build)
	}

	key := translationCache.Key(source, graphID, parameters)
	if identity != nil {
		key.builder = true
		key.builderDigest = identity.digest
	}
	return translationCache.GetOrBuildContext(ctx, key, parameters, build)
}

func commentRegularQuery(source, sql string) string {
	return "-- " + builderCommentNewlines.Replace(source) + "\n" + sql
}
