package pg

import (
	"container/list"
	"fmt"
	"sort"
	"strings"
	"sync"

	model "github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

const defaultCypherTranslationCacheEntries = 256

type cypherTranslationCacheKey struct {
	query         string
	graphID       int32
	parameterType string
}

type cypherTranslationCacheValue struct {
	key              cypherTranslationCacheKey
	sql              string
	defaults         map[string]any
	parameterSources map[string]string
}

func (s cypherTranslationCacheValue) bind(parameters map[string]any) (map[string]any, error) {
	bound := make(map[string]any, len(s.defaults))
	for identifier, value := range s.defaults {
		bound[identifier] = value
	}
	for identifier, source := range s.parameterSources {
		value, found := parameters[source]
		if !found {
			continue
		}
		negotiated, err := model.NegotiateValue(value)
		if err != nil {
			return nil, fmt.Errorf("negotiate cached parameter %s: %w", source, err)
		}
		bound[identifier] = negotiated
	}
	return bound, nil
}

type cypherTranslationCall struct {
	done      chan struct{}
	value     cypherTranslationCacheValue
	err       error
	cacheable bool
}

type cypherTranslationCache struct {
	lock     sync.Mutex
	capacity int
	entries  map[cypherTranslationCacheKey]*list.Element
	lru      *list.List
	pending  map[cypherTranslationCacheKey]*cypherTranslationCall
	closed   bool
	stats    TranslationCacheStats
}

type TranslationCacheStats struct {
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Bypasses        uint64 `json:"bypasses"`
	Evictions       uint64 `json:"evictions"`
	CoalescedMisses uint64 `json:"coalesced_misses"`
	Entries         int    `json:"entries"`
	Pending         int    `json:"pending"`
}

func newCypherTranslationCache(capacity int) *cypherTranslationCache {
	return &cypherTranslationCache{
		capacity: capacity,
		entries:  make(map[cypherTranslationCacheKey]*list.Element, capacity),
		lru:      list.New(),
		pending:  map[cypherTranslationCacheKey]*cypherTranslationCall{},
	}
}

func translationParameterTypeKey(parameters map[string]any) string {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var key strings.Builder
	for _, name := range keys {
		key.WriteString(name)
		key.WriteByte('=')
		value := parameters[name]
		if value == nil {
			key.WriteString("null")
		} else if dataType, err := model.ValueToDataType(value); err == nil {
			key.WriteString(dataType.String())
		} else {
			// Translation will report the same unsupported value error. Retaining
			// its Go type here prevents unrelated invalid shapes from coalescing.
			key.WriteString(fmt.Sprintf("invalid:%T", value))
		}
		key.WriteByte(';')
	}
	return key.String()
}

func cacheableTranslation(result translate.Result) bool {
	for identifier := range result.Parameters {
		if _, found := result.ParameterSources[identifier]; !found {
			return false
		}
	}
	return true
}

func cloneValues(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneSources(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (s *cypherTranslationCache) Translate(query string, graphID int32, parameters map[string]any, build func() (translate.Result, string, error)) (string, map[string]any, error) {
	trimmed := strings.TrimSpace(query)
	if s == nil || s.capacity <= 0 || len(query) > maxCachedCypherQueryBytes {
		result, sql, err := build()
		return sql, result.Parameters, err
	}
	key := cypherTranslationCacheKey{query: trimmed, graphID: graphID, parameterType: translationParameterTypeKey(parameters)}

	s.lock.Lock()
	if s.closed {
		s.stats.Bypasses++
		s.lock.Unlock()
		result, sql, err := build()
		return sql, result.Parameters, err
	}
	if element, found := s.entries[key]; found {
		s.stats.Hits++
		s.lru.MoveToFront(element)
		value := element.Value.(cypherTranslationCacheValue)
		s.lock.Unlock()
		bound, err := value.bind(parameters)
		return value.sql, bound, err
	}
	if call, found := s.pending[key]; found {
		s.stats.CoalescedMisses++
		s.lock.Unlock()
		<-call.done
		if call.err != nil {
			return "", nil, call.err
		}
		if !call.cacheable {
			result, sql, err := build()
			return sql, result.Parameters, err
		}
		bound, err := call.value.bind(parameters)
		return call.value.sql, bound, err
	}

	key.query = strings.Clone(key.query)
	s.stats.Misses++
	call := &cypherTranslationCall{done: make(chan struct{})}
	s.pending[key] = call
	s.lock.Unlock()

	result, sql, err := build()
	value := cypherTranslationCacheValue{
		key: key, sql: sql, defaults: cloneValues(result.Parameters), parameterSources: cloneSources(result.ParameterSources),
	}
	cacheable := err == nil && cacheableTranslation(result)

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

	return sql, result.Parameters, err
}

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
