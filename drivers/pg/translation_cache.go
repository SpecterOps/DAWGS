package pg

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs/cache"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

const (
	translationCacheCapacity  = 256
	translationCacheKeyFormat = 3
	maxCachedCypherBytes      = 64 * 1024
	translationCachePolicy    = "compiler-v4:optimized"
)

type translationCacheKey struct {
	format           uint8
	queryDigest      [sha256.Size]byte
	querySize        int
	graphID          int32
	parameterTypes   string
	policy           string
	schemaGeneration uint64
}

type translationCacheBuildResult struct {
	parameters       map[string]any
	parameterSources map[string]string
}

type translationCacheEntry struct {
	sql              string
	parameterSources map[string]string
}

type translationCacheCall struct {
	done     chan struct{}
	doneOnce sync.Once
	reusable bool
}

func (s *translationCacheCall) finish(reusable bool) {
	s.doneOnce.Do(func() {
		s.reusable = reusable
		close(s.done)
	})
}

// TranslationCacheStats contains aggregate counters only. It intentionally
// exposes no source text, SQL, parameter names, values, or connection data.
type TranslationCacheStats struct {
	Hits                    int64
	Misses                  int64
	Coalesced               int64
	Bypasses                int64
	Insertions              int64
	Evictions               int64
	BindingFailures         int64
	BuildFailures           int64
	UnoptimizedCompilations int64
	Size                    int64
	Capacity                int
	Generation              uint64
}

type translationCache struct {
	capacity int
	policy   string
	entries  cache.Cache[translationCacheKey, translationCacheEntry]

	lock    sync.Mutex
	pending map[translationCacheKey]*translationCacheCall
	closed  bool

	schemaGeneration        atomic.Uint64
	hits                    atomic.Int64
	misses                  atomic.Int64
	coalesced               atomic.Int64
	bypasses                atomic.Int64
	insertions              atomic.Int64
	evictions               atomic.Int64
	bindingFailures         atomic.Int64
	buildFailures           atomic.Int64
	unoptimizedCompilations atomic.Int64
}

type translationCacheProvider interface {
	TranslationCache() *translationCache
}

type sharedTranslationCacheProvider struct {
	cache *translationCache
}

func (s sharedTranslationCacheProvider) TranslationCache() *translationCache {
	return s.cache
}

func newTranslationCache(capacity int) *translationCache {
	return newTranslationCacheWithPolicy(capacity, translationCachePolicy)
}

func newTranslationCacheWithPolicy(capacity int, policy string) *translationCache {
	if capacity < 0 {
		capacity = 0
	}
	if policy == "" {
		policy = translationCachePolicy
	}

	cacheInstance := &translationCache{
		capacity: capacity,
		policy:   policy,
		pending:  map[translationCacheKey]*translationCacheCall{},
	}
	if capacity > 0 {
		cacheInstance.entries = cache.NewSieve[translationCacheKey, translationCacheEntry](capacity)
	}

	return cacheInstance
}

func (s *translationCache) Key(query string, graphID int32, parameters map[string]any) translationCacheKey {
	query = strings.TrimSpace(query)

	return translationCacheKey{
		format:           translationCacheKeyFormat,
		queryDigest:      sha256.Sum256([]byte(query)),
		querySize:        len(query),
		graphID:          graphID,
		parameterTypes:   parameterTypeShape(parameters),
		policy:           s.policy,
		schemaGeneration: s.schemaGeneration.Load(),
	}
}

func parameterTypeShape(parameters map[string]any) string {
	if len(parameters) == 0 {
		return ""
	}

	names := make([]string, 0, len(parameters))
	for name := range parameters {
		names = append(names, name)
	}
	sort.Strings(names)

	shape := strings.Builder{}
	for _, name := range names {
		dataType, err := pgsql.ValueToDataType(parameters[name])
		if err != nil {
			dataType = pgsql.DataType("unsupported:" + stableTypeName(parameters[name]))
		}

		fmt.Fprintf(&shape, "%d:%s=%s;", len(name), name, dataType)
	}

	return shape.String()
}

func stableTypeName(value any) string {
	if value == nil {
		return "nil"
	}

	return reflect.TypeOf(value).String()
}

func (s *translationCache) GetOrBuild(key translationCacheKey, parameters map[string]any, build func() (string, translationCacheBuildResult, error)) (string, map[string]any, error) {
	return s.GetOrBuildContext(context.Background(), key, parameters, build)
}

func (s *translationCache) GetOrBuildContext(ctx context.Context, key translationCacheKey, parameters map[string]any, build func() (string, translationCacheBuildResult, error)) (string, map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if key.querySize > maxCachedCypherBytes || s.capacity == 0 {
		s.bypasses.Add(1)
		return s.buildUncached(build)
	}

	for {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}

		s.lock.Lock()
		if s.closed {
			s.lock.Unlock()
			s.bypasses.Add(1)
			return s.buildUncached(build)
		}
		if s.entries != nil {
			if entry, found := s.entries.Get(key); found {
				s.lock.Unlock()
				bindings, err := bindCachedParameters(entry.parameterSources, parameters)
				if err != nil {
					s.bindingFailures.Add(1)
					return "", nil, err
				}
				s.hits.Add(1)
				return entry.sql, bindings, nil
			}
		}

		if pending := s.pending[key]; pending != nil {
			s.lock.Unlock()
			select {
			case <-pending.done:
				s.coalesced.Add(1)
				if !pending.reusable {
					s.bypasses.Add(1)
					return s.buildUncached(build)
				}
				continue

			case <-ctx.Done():
				return "", nil, ctx.Err()
			}
		}

		pending := &translationCacheCall{
			done: make(chan struct{}),
		}
		s.pending[key] = pending
		s.misses.Add(1)
		s.lock.Unlock()

		sql, result, err, panicked := callTranslationBuild(build)
		entry, cacheable := cacheableTranslation(sql, result, parameters, err)

		s.lock.Lock()
		currentGeneration := s.schemaGeneration.Load()
		generationCurrent := key.schemaGeneration == currentGeneration
		closed := s.closed
		reusable := cacheable && generationCurrent && !closed && panicked == nil
		if reusable {
			if s.entries.Stats().Size() >= int64(s.capacity) {
				s.evictions.Add(1)
			}
			s.entries.Put(cloneTranslationCacheKey(key), entry)
			s.insertions.Add(1)
		}
		if s.pending[key] == pending {
			delete(s.pending, key)
		}
		pending.finish(reusable)
		s.lock.Unlock()

		if panicked != nil {
			panic(panicked)
		}
		if err != nil {
			s.buildFailures.Add(1)
			return "", nil, err
		}
		if !generationCurrent && !closed {
			key.schemaGeneration = currentGeneration
			continue
		}
		if closed || !cacheable {
			s.bypasses.Add(1)
			return sql, result.parameters, nil
		}

		return sql, result.parameters, nil
	}
}

func callTranslationBuild(build func() (string, translationCacheBuildResult, error)) (sql string, result translationCacheBuildResult, err error, panicked any) {
	defer func() {
		panicked = recover()
	}()

	sql, result, err = build()
	return sql, result, err, nil
}

func (s *translationCache) buildUncached(build func() (string, translationCacheBuildResult, error)) (string, map[string]any, error) {
	sql, result, err, panicked := callTranslationBuild(build)
	if panicked != nil {
		panic(panicked)
	}
	if err != nil {
		s.buildFailures.Add(1)
		return "", nil, err
	}

	return sql, result.parameters, nil
}

func (s *translationCache) BuildUnoptimized(build func() (string, translationCacheBuildResult, error)) (string, map[string]any, error) {
	s.unoptimizedCompilations.Add(1)
	s.bypasses.Add(1)
	return s.buildUncached(build)
}

func cloneTranslationCacheKey(key translationCacheKey) translationCacheKey {
	key.parameterTypes = strings.Clone(key.parameterTypes)
	key.policy = strings.Clone(key.policy)
	return key
}

func cacheableTranslation(sql string, result translationCacheBuildResult, parameters map[string]any, err error) (translationCacheEntry, bool) {
	if err != nil {
		return translationCacheEntry{}, false
	}
	if len(result.parameters) != len(result.parameterSources) {
		return translationCacheEntry{}, false
	}

	sources := make(map[string]string, len(result.parameterSources))
	for generated := range result.parameters {
		source, found := result.parameterSources[generated]
		if !found {
			return translationCacheEntry{}, false
		}
		if source == "" {
			return translationCacheEntry{}, false
		}
		if _, found := parameters[source]; !found {
			return translationCacheEntry{}, false
		}
		sources[strings.Clone(generated)] = strings.Clone(source)
	}

	return translationCacheEntry{
		sql:              strings.Clone(sql),
		parameterSources: sources,
	}, true
}

func bindCachedParameters(sources map[string]string, parameters map[string]any) (map[string]any, error) {
	if len(sources) == 0 {
		return nil, nil
	}

	bindings := make(map[string]any, len(sources))
	for generated, source := range sources {
		value, found := parameters[source]
		if !found {
			return nil, fmt.Errorf("missing value for Cypher parameter %q", source)
		}
		negotiated, err := pgsql.NegotiateValue(value)
		if err != nil {
			return nil, err
		}
		bindings[generated] = negotiated
	}

	return bindings, nil
}

func (s *translationCache) Invalidate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.schemaGeneration.Add(1)
	if !s.closed && s.capacity > 0 {
		s.entries = cache.NewSieve[translationCacheKey, translationCacheEntry](s.capacity)
	}
}

func (s *translationCache) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.closed = true
	s.entries = nil
	for key, pending := range s.pending {
		delete(s.pending, key)
		pending.finish(false)
	}
}

func (s *translationCache) Stats() TranslationCacheStats {
	s.lock.Lock()
	defer s.lock.Unlock()

	var size int64
	if s.entries != nil {
		size = s.entries.Stats().Size()
	}

	return TranslationCacheStats{
		Hits:                    s.hits.Load(),
		Misses:                  s.misses.Load(),
		Coalesced:               s.coalesced.Load(),
		Bypasses:                s.bypasses.Load(),
		Insertions:              s.insertions.Load(),
		Evictions:               s.evictions.Load(),
		BindingFailures:         s.bindingFailures.Load(),
		BuildFailures:           s.buildFailures.Load(),
		UnoptimizedCompilations: s.unoptimizedCompilations.Load(),
		Size:                    size,
		Capacity:                s.capacity,
		Generation:              s.schemaGeneration.Load(),
	}
}
