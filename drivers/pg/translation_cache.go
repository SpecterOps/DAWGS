package pg

import (
	"container/list"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	model "github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

// defaultCypherTranslationCacheEntries is the maximum number of translated SQL
// entries retained when no cache capacity is configured.
const defaultCypherTranslationCacheEntries = 256

// cypherTranslationCacheKey identifies SQL that can be reused for one query, graph, and parameter type shape.
type cypherTranslationCacheKey struct {
	// query is normalized Cypher text cloned on a cache miss.
	query string

	// graphID scopes generated SQL to the selected graph.
	graphID int32

	// parameterType captures sorted parameter names and negotiated PostgreSQL types.
	parameterType string
}

// cypherTranslationCacheValue stores generated SQL and the source mapping needed to bind fresh parameter values.
type cypherTranslationCacheValue struct {
	// key is the immutable identity used by the LRU index.
	key cypherTranslationCacheKey

	// sql is the rendered PostgreSQL statement reused by cache hits.
	sql string

	// parameterSources maps generated SQL parameters back to caller-supplied Cypher parameter names.
	parameterSources map[string]string
}

// bind negotiates current caller values for every generated parameter recorded by the cached translation.
func (s cypherTranslationCacheValue) bind(parameters map[string]any) (map[string]any, error) {
	bound := make(map[string]any, len(s.parameterSources))
	for identifier, source := range s.parameterSources {
		value, found := parameters[source]
		if !found {
			return nil, fmt.Errorf("cached translation requires missing parameter source %q", source)
		}
		negotiated, err := model.NegotiateValue(value)
		if err != nil {
			return nil, fmt.Errorf("negotiate cached parameter %s: %w", source, err)
		}
		bound[identifier] = negotiated
	}
	return bound, nil
}

// cypherTranslationCall publishes one in-flight build result to callers waiting on the same cache key.
type cypherTranslationCall struct {
	// done closes after value, err, and cacheable have been published.
	done chan struct{}

	// value is the translation produced by the build owner.
	value cypherTranslationCacheValue

	// err is the build failure shared with waiting callers.
	err error

	// cacheable reports whether waiters may safely rebind and reuse value.
	cacheable bool
}

// cypherTranslationCache is a bounded LRU of reusable SQL translations with single-flight miss coalescing.
type cypherTranslationCache struct {
	// lock protects completed entries, pending calls, closure state, and counters.
	lock sync.Mutex

	// capacity is the maximum number of completed translations retained.
	capacity int

	// entries indexes completed translations by their reusable input shape.
	entries map[cypherTranslationCacheKey]*list.Element

	// lru orders completed translations from most to least recently used.
	lru *list.List

	// pending coalesces concurrent builds for the same translation key.
	pending map[cypherTranslationCacheKey]*cypherTranslationCall

	// closed prevents completed or future builds from being retained.
	closed bool

	// stats accumulates cache activity for this instance.
	stats TranslationCacheStats
}

// TranslationCacheStats is a query-text-free snapshot of translation cache activity and occupancy.
type TranslationCacheStats struct {
	// Hits counts translations served from completed cache entries.
	Hits uint64 `json:"hits"`

	// Misses counts builds owned by callers that established pending entries.
	Misses uint64 `json:"misses"`

	// Bypasses counts builds that could not be retained or safely shared.
	Bypasses uint64 `json:"bypasses"`

	// Evictions counts least-recently-used translations removed at capacity.
	Evictions uint64 `json:"evictions"`

	// CoalescedMisses counts callers that waited for an existing build of the same key.
	CoalescedMisses uint64 `json:"coalesced_misses"`

	// Entries is the number of completed translations retained when the snapshot was taken.
	Entries int `json:"entries"`

	// Pending is the number of in-flight translation builds when the snapshot was taken.
	Pending int `json:"pending"`
}

// newCypherTranslationCache initializes an empty LRU translation cache with the requested capacity.
func newCypherTranslationCache(capacity int) *cypherTranslationCache {
	return &cypherTranslationCache{
		capacity: capacity,
		entries:  make(map[cypherTranslationCacheKey]*list.Element, capacity),
		lru:      list.New(),
		pending:  map[cypherTranslationCacheKey]*cypherTranslationCall{},
	}
}

// translationParameterTypeKey encodes sorted parameter names and negotiated data types into an unambiguous cache-key component.
func translationParameterTypeKey(parameters map[string]any) string {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var key strings.Builder
	for _, name := range keys {
		value := parameters[name]
		var typeName string
		if value == nil {
			typeName = "null"
		} else if dataType, err := model.ValueToDataType(value); err == nil {
			typeName = dataType.String()
		} else {
			// Translation will report the same unsupported value error. Retaining
			// its Go type here prevents unrelated invalid shapes from coalescing.
			typeName = fmt.Sprintf("invalid:%T", value)
		}

		key.WriteString(strconv.Itoa(len(name)))
		key.WriteByte(':')
		key.WriteString(name)
		key.WriteString(strconv.Itoa(len(typeName)))
		key.WriteByte(':')
		key.WriteString(typeName)
	}
	return key.String()
}

// cacheableTranslation reports whether every translated parameter can be rebound from a current caller parameter.
func cacheableTranslation(result translate.Result, parameters map[string]any) bool {
	if len(result.Parameters) != len(result.ParameterSources) {
		return false
	}
	for identifier := range result.Parameters {
		source, found := result.ParameterSources[identifier]
		if !found || source == "" {
			return false
		}
		if _, found := parameters[source]; !found {
			return false
		}
	}
	return true
}

// cloneSources copies parameter-source metadata so cached values do not alias translator-owned maps.
func cloneSources(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// Translate returns reusable SQL with values rebound from parameters, building or coalescing a translation on a miss.
func (s *cypherTranslationCache) Translate(query string, graphID int32, parameters map[string]any, build func() (translate.Result, string, error)) (string, map[string]any, error) {
	trimmed := strings.TrimSpace(query)
	if s == nil || s.capacity <= 0 || len(query) > maxCachedCypherQueryBytes {
		if result, sql, err := build(); err != nil {
			return "", nil, err
		} else {
			return sql, result.Parameters, nil
		}
	}
	key := cypherTranslationCacheKey{
		query:         trimmed,
		graphID:       graphID,
		parameterType: translationParameterTypeKey(parameters),
	}

	s.lock.Lock()
	if s.closed {
		s.stats.Bypasses++
		s.lock.Unlock()
		if result, sql, err := build(); err != nil {
			return "", nil, err
		} else {
			return sql, result.Parameters, nil
		}
	}
	if element, found := s.entries[key]; found {
		s.stats.Hits++
		s.lru.MoveToFront(element)
		value := element.Value.(cypherTranslationCacheValue)
		s.lock.Unlock()
		if bound, err := value.bind(parameters); err != nil {
			return "", nil, err
		} else {
			return value.sql, bound, nil
		}
	}
	if call, found := s.pending[key]; found {
		s.stats.CoalescedMisses++
		s.lock.Unlock()
		<-call.done
		if call.err != nil {
			return "", nil, call.err
		}
		if !call.cacheable {
			if result, sql, err := build(); err != nil {
				return "", nil, err
			} else {
				return sql, result.Parameters, nil
			}
		}
		if bound, err := call.value.bind(parameters); err != nil {
			return "", nil, err
		} else {
			return call.value.sql, bound, nil
		}
	}

	key.query = strings.Clone(key.query)
	s.stats.Misses++
	call := &cypherTranslationCall{
		done: make(chan struct{}),
	}
	s.pending[key] = call
	s.lock.Unlock()

	result, sql, err := build()
	value := cypherTranslationCacheValue{
		key:              key,
		sql:              sql,
		parameterSources: cloneSources(result.ParameterSources),
	}
	cacheable := err == nil && cacheableTranslation(result, parameters)

	s.lock.Lock()
	call.value, call.err, call.cacheable = value, err, cacheable
	if cacheable && !s.closed {
		element := s.lru.PushFront(value)
		s.entries[key] = element
		if s.lru.Len() > s.capacity {
			evicted := s.lru.Back()
			s.lru.Remove(evicted)
			delete(s.entries, evicted.Value.(cypherTranslationCacheValue).key)
			s.stats.Evictions++
		}
	} else if err == nil {
		s.stats.Bypasses++
	}
	delete(s.pending, key)
	close(call.done)
	s.lock.Unlock()

	if err != nil {
		return "", nil, err
	}

	return sql, result.Parameters, nil
}

// Stats returns a consistent snapshot of counters and current cache occupancy.
func (s *cypherTranslationCache) Stats() TranslationCacheStats {
	if s == nil {
		return TranslationCacheStats{}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	stats := s.stats
	stats.Entries = len(s.entries)
	stats.Pending = len(s.pending)
	return stats
}

// Close releases retained translations and prevents future builds from repopulating the cache.
func (s *cypherTranslationCache) Close() {
	if s == nil {
		return
	}
	s.lock.Lock()
	s.closed = true
	s.entries = nil
	s.lru.Init()
	s.lock.Unlock()
}
