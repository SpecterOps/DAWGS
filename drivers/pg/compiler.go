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

const (
	// builderParameterPrefix namespaces generated builder bindings without retaining caller values.
	builderParameterPrefix = "__dawgs_builder_p"

	// compactBuilderCachePolicySuffix separates snapshot-backed SQL from legacy builder entries.
	compactBuilderCachePolicySuffix = ":snapshot-owned-asp-v2"
)

// compiledBuilderQuery keeps a rendered builder statement and its execution bindings together.
type compiledBuilderQuery struct {
	// statement is the PostgreSQL statement generated for this compilation mode.
	statement string

	// bindings contains request-local values for the generated statement parameters.
	bindings map[string]any
}

// builderCommentNewlines keeps rendered Cypher comments confined to SQL comment lines.
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
	return s.compileWithMapper(ctx, source, parameters, graphID, identity, parse, s)
}

// compileWithMapper preserves legacy compilation for callers without compact snapshot ownership.
func (s *SchemaManager) compileWithMapper(ctx context.Context, source string, parameters map[string]any, graphID int32, identity *builderCacheIdentity, parse func() (*cypher.RegularQuery, error), mapper KindMapper) (string, map[string]any, error) {
	if compiled, err := s.compileWithMapperAndCompactSearch(ctx, source, parameters, graphID, identity, parse, mapper, false); err != nil {
		return "", nil, err
	} else {
		return compiled.statement, compiled.bindings, nil
	}
}

// compileWithMapperAndCompactSearch admits V2 only when the executing builder
// has independently established its stable snapshot and query budget lifetime.
func (s *SchemaManager) compileWithMapperAndCompactSearch(ctx context.Context, source string, parameters map[string]any, graphID int32, identity *builderCacheIdentity, parse func() (*cypher.RegularQuery, error), mapper KindMapper, compactSearch bool) (compiledBuilderQuery, error) {
	var (
		statement string
		bindings  map[string]any
		err       error

		translationCache   = s.compilationCacheProvider.TranslationCache()
		optimized          = OptimizedTranslationEnabled()
		translationOptions = translate.Options{
			OptimizerMode:                   translate.OptimizerDisabled,
			DisableBudgetedAllShortestPaths: !compactSearch,
		}
	)
	if optimized {
		translationOptions.OptimizerMode = translate.OptimizerEnabled
	}

	build := func() (string, translationCacheBuildResult, error) {
		if regularQuery, err := parse(); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if translated, parameterSources, err := translate.TranslateWithOptionsAndParameterSources(ctx, regularQuery, mapper, parameters, graphID, translationOptions); err != nil {
			return "", translationCacheBuildResult{}, err
		} else if sqlQuery, err := translate.Translated(translated); err != nil {
			return "", translationCacheBuildResult{}, err
		} else {
			maps.Copy(translated.Parameters, sqlQuery.Parameters)
			if local, ok := mapper.(*transactionKindMapper); ok && local.bypassTranslations() {
				translated.HasMissingKinds = true
			}
			return sqlQuery.Statement, translationCacheBuildResult{
				hasMissingKinds:   translated.HasMissingKinds,
				parameters:        translated.Parameters,
				parameterSources:  parameterSources,
				literalParameters: sqlQuery.LiteralParameters,
			}, nil
		}
	}

	if local, ok := mapper.(*transactionKindMapper); ok && local.bypassTranslations() {
		statement, bindings, err = translationCache.buildUncached(build)
	} else if !optimized {
		statement, bindings, err = translationCache.BuildUnoptimized(build)
	} else if identity != nil && !identity.valid {
		translationCache.bypasses.Add(1)
		statement, bindings, err = translationCache.buildUncached(build)
	} else {
		key := translationCache.Key(source, graphID, parameters)
		if compactSearch {
			key.policy += compactBuilderCachePolicySuffix
		}
		if identity != nil {
			key.builder = true
			key.builderDigest = identity.digest
		}

		statement, bindings, err = translationCache.GetOrBuildContext(ctx, key, parameters, build)
	}
	if err != nil {
		return compiledBuilderQuery{}, err
	}

	return compiledBuilderQuery{
		statement: statement,
		bindings:  bindings,
	}, nil
}

func commentRegularQuery(source, sql string) string {
	return "-- " + builderCommentNewlines.Replace(source) + "\n" + sql
}
