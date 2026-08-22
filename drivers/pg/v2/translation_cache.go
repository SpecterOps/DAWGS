package v2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	dawgscache "github.com/specterops/dawgs/cache"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
)

// translationEntry contains only immutable SQL and parameter-source metadata.
type translationEntry struct {
	// sql is the immutable PostgreSQL statement reused by cache hits.
	sql string

	// parameterSources maps generated parameters to caller-provided Cypher values.
	parameterSources map[string]string
}

// bind negotiates current caller values for the immutable cached parameter mapping.
func (s translationEntry) bind(parameters map[string]any) (map[string]any, error) {
	// pgx.NamedArgs accepts nil for statements with no placeholders. Returning
	// nil avoids allocating an empty map for the common parameter-free cached
	// shortest-path shape while preserving its execution semantics.
	if len(s.parameterSources) == 0 {
		return nil, nil
	}
	bound := make(map[string]any, len(s.parameterSources))
	for identifier, source := range s.parameterSources {
		value, found := parameters[source]
		if !found {
			return nil, fmt.Errorf("cached translation requires missing parameter source %q", source)
		}

		negotiated, err := pgsql.NegotiateValue(value)
		if err != nil {
			return nil, fmt.Errorf("negotiate cached parameter %s: %w", source, err)
		}
		bound[identifier] = negotiated
	}
	return bound, nil
}

// connectionTranslationCache wraps SIEVE so cache closure and counters remain
// coherent with provider lifecycle operations.
type connectionTranslationCache struct {
	// lock serializes cache lifecycle, statistics, and SIEVE operations.
	lock sync.Mutex

	// capacity bounds translations retained by this physical connection.
	capacity int

	// shared provides the pool-wide tier for immutable shortest-path templates.
	shared *sharedTemplateCache

	// generation returns the current schema-sensitive cache generation.
	generation func() uint64

	// sieve stores reusable translations for this physical connection.
	sieve dawgscache.Cache[translationKey, translationEntry]

	// closed prevents subsequent cache publication after connection retirement.
	closed bool

	// stats records query-text-free local cache activity.
	stats TranslationCacheStats
}

// The CypherTranslationCache assertion ensures the local cache remains compatible with pg transactions.
var _ pg.CypherTranslationCache = (*connectionTranslationCache)(nil)

// newConnectionTranslationCache initializes one physical connection's bounded translation cache.
func newConnectionTranslationCache(capacity int, generation func() uint64, shared *sharedTemplateCache) *connectionTranslationCache {
	cache := &connectionTranslationCache{
		capacity:   capacity,
		generation: generation,
		shared:     shared,
		stats: TranslationCacheStats{
			Capacity: capacity,
		},
	}
	if capacity > 0 {
		cache.sieve = dawgscache.NewSieve[translationKey, translationEntry](capacity)
	}
	return cache
}

// TranslateWithPolicy returns a cached immutable translation with fresh
// bindings, or runs build without retention when this cache is unavailable.
func (s *connectionTranslationCache) TranslateWithPolicy(query string, graphID int32, parameters map[string]any, policyIdentity string, build func() (translate.Result, string, error)) (string, map[string]any, error) {
	schemaGeneration := uint64(0)
	if s.generation != nil {
		schemaGeneration = s.generation()
	}
	key := newTranslationKey(query, graphID, parameters, policyIdentity, schemaGeneration)

	s.lock.Lock()
	if s.closed || s.capacity == 0 || len(query) > pg.MaxCachedCypherQueryBytes {
		s.stats.Misses++
		s.stats.Bypasses++
		s.lock.Unlock()
		return buildUncached(build)
	}
	if entry, found := s.sieve.Get(key); found {
		s.stats.Hits++
		s.lock.Unlock()
		bound, err := entry.bind(parameters)
		if err != nil {
			s.lock.Lock()
			s.stats.BindingFailures++
			s.lock.Unlock()
			return "", nil, err
		}
		return entry.sql, bound, nil
	}
	s.stats.Misses++
	s.lock.Unlock()
	if isShortestPathQuery(query) {
		if entry, found := s.shared.get(key); found {
			bound, bindErr := entry.bind(parameters)
			if bindErr != nil {
				s.lock.Lock()
				s.stats.BindingFailures++
				s.lock.Unlock()
				return "", nil, bindErr
			}
			s.putL1(key, entry)
			return entry.sql, bound, nil
		}
	}

	result, sql, err := build()
	if err != nil {
		return "", nil, err
	}
	if !cacheableTranslation(result, parameters) {
		s.lock.Lock()
		s.stats.Bypasses++
		s.lock.Unlock()
		return sql, result.Parameters, nil
	}

	entry := translationEntry{
		sql:              strings.Clone(sql),
		parameterSources: cloneParameterSources(result.ParameterSources),
	}
	key.query = strings.Clone(key.query)

	if isShortestPathQuery(query) {
		s.shared.put(key, entry)
	}
	s.putL1(key, entry)

	return sql, result.Parameters, nil
}

// putL1 retains a local translation unless the connection has closed or already has it.
func (s *connectionTranslationCache) putL1(key translationKey, entry translationEntry) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		s.stats.Bypasses++
		return
	}
	if _, exists := s.sieve.Get(key); !exists {
		if s.sieve.Stats().Size() >= int64(s.capacity) {
			s.stats.Evictions++
		}
		s.sieve.Put(key, entry)
		s.stats.Insertions++
	}
}

// isShortestPathQuery reports whether Cypher source selects the shortest-path template tier.
func isShortestPathQuery(query string) bool {
	return strings.Contains(strings.ToLower(query), "shortestpath")
}

// buildUncached executes a translation builder and returns its immediate SQL and bindings.
func buildUncached(build func() (translate.Result, string, error)) (string, map[string]any, error) {
	result, sql, err := build()
	if err != nil {
		return "", nil, err
	}
	return sql, result.Parameters, nil
}

// cacheableTranslation reports whether every generated parameter can bind fresh caller input.
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

// cloneParameterSources detaches cached source metadata from translator-owned storage.
func cloneParameterSources(parameterSources map[string]string) map[string]string {
	cloned := make(map[string]string, len(parameterSources))
	for identifier, source := range parameterSources {
		cloned[strings.Clone(identifier)] = strings.Clone(source)
	}
	return cloned
}

// statsSnapshot returns a consistent local cache snapshot with current occupancy.
func (s *connectionTranslationCache) statsSnapshot() TranslationCacheStats {
	s.lock.Lock()
	defer s.lock.Unlock()
	stats := s.stats
	if s.sieve != nil {
		stats.Entries = int(s.sieve.Stats().Size())
	}
	return stats
}

// close drops retained entries and prevents an already acquired cache handle
// from publishing a translation after its physical connection has closed.
func (s *connectionTranslationCache) close() TranslationCacheStats {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return s.stats
	}

	s.closed = true
	final := s.stats
	if s.sieve != nil {
		final.Entries = int(s.sieve.Stats().Size())
		s.sieve = nil
	}
	s.stats.Entries = 0
	return final
}

// connectionState is registered only while its physical connection is live.
// It intentionally does not retain the physical connection identity.
type connectionState struct {
	// id is the opaque provider-assigned diagnostic identifier.
	id uint64

	// cache owns reusable translations while the physical connection remains live.
	cache *connectionTranslationCache

	// workspaceReadyGeneration records the schema generation with initialized workspaces.
	workspaceReadyGeneration uint64

	// workspace records setup and reuse activity for the physical connection.
	workspace TraversalWorkspaceStats

	// preparedStatements contains hashes of selected warmed statements.
	preparedStatements map[[sha256.Size]byte]struct{}

	// prepared records warm-up outcomes for the physical connection.
	prepared PreparedStatementStats
}

// connectionCacheProvider maps physical connection identities to their cache
// state. Map keys are process-local implementation details and are never
// exposed through diagnostics or cache keys.
type connectionCacheProvider struct {
	// lock serializes physical connection state and aggregate diagnostics.
	lock sync.RWMutex

	// capacity bounds translations retained on each physical connection.
	capacity int

	// minConnections reports the configured pgx pool lower connection bound.
	minConnections int32

	// maxConnections reports the configured pgx pool upper connection bound.
	maxConnections int32

	// generation invalidates schema-sensitive connection state.
	generation uint64

	// nextID allocates opaque identifiers for newly registered connections.
	nextID uint64

	// closed prevents registration and publication after provider teardown.
	closed bool

	// states maps live physical connections to their provider-owned state.
	states map[*pgx.Conn]*connectionState

	// retiredConnections counts connections removed from states.
	retiredConnections uint64

	// retiredStats accumulates translation-cache activity from retired connections.
	retiredStats TranslationCacheStats

	// retiredPrepared accumulates warm-up activity from retired connections.
	retiredPrepared PreparedStatementStats

	// sqlGeneration accumulates query-text-free SQL generation timing.
	sqlGeneration SQLGenerationStats

	// strategySelection accumulates production traversal selection telemetry.
	strategySelection StrategySelectionStats

	// routeDecisions accumulates topology route-decision telemetry.
	routeDecisions TraversalRouteDecisionStats

	// shapeCapacity bounds retained query-digest structural classifications.
	shapeCapacity int

	// shapeCache maps query digests to immutable structural classifications.
	shapeCache map[[sha256.Size]byte]pg.TraversalShape

	// shapeOrder preserves insertion order for bounded shape-cache eviction.
	shapeOrder [][sha256.Size]byte

	// shapeStats records query-text-free shape-cache activity.
	shapeStats TraversalShapeCacheStats

	// sharedTemplates holds immutable shortest-path templates common to the pool.
	sharedTemplates *sharedTemplateCache
}

// The CypherTranslationCacheProvider assertion preserves pg transaction cache selection.
var _ pg.CypherTranslationCacheProvider = (*connectionCacheProvider)(nil)

// The StableSnapshotTraversalWorkspaceProvider assertion preserves workspace setup support.
var _ pg.StableSnapshotTraversalWorkspaceProvider = (*connectionCacheProvider)(nil)

// The LazyStableSnapshotTraversalWorkspaceProvider assertion preserves lazy workspace setup support.
var _ pg.LazyStableSnapshotTraversalWorkspaceProvider = (*connectionCacheProvider)(nil)

// The SQLGenerationProfileCollector assertion preserves SQL timing telemetry collection.
var _ pg.SQLGenerationProfileCollector = (*connectionCacheProvider)(nil)

// The TraversalStrategySelectionCollector assertion preserves routing telemetry collection.
var _ pg.TraversalStrategySelectionCollector = (*connectionCacheProvider)(nil)

// The TraversalShapeCacheProvider assertion preserves bounded structural classification caching.
var _ pg.TraversalShapeCacheProvider = (*connectionCacheProvider)(nil)

// The TraversalRouteDecisionCollector assertion preserves topology decision telemetry collection.
var _ pg.TraversalRouteDecisionCollector = (*connectionCacheProvider)(nil)

// TraversalShapeFor caches a bounded classifier result by a query digest. It
// never retains source text, values, parsed ASTs, or database state.
func (s *connectionCacheProvider) TraversalShapeFor(query string, classify func() (pg.TraversalShape, error)) (pg.TraversalShape, error) {
	if classify == nil {
		return pg.TraversalShape{}, fmt.Errorf("traversal shape classifier is required")
	}
	if s == nil || len(query) > pg.MaxCachedCypherQueryBytes {
		return classify()
	}
	identity := sha256.Sum256([]byte(strings.TrimSpace(query)))
	s.lock.Lock()
	if shape, found := s.shapeCache[identity]; found {
		s.shapeStats.Hits++
		s.lock.Unlock()
		return shape, nil
	}
	s.shapeStats.Misses++
	s.lock.Unlock()

	shape, err := classify()
	if err != nil {
		return pg.TraversalShape{}, err
	}
	if s.shapeCapacity == 0 {
		return shape, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return shape, nil
	}
	if cached, found := s.shapeCache[identity]; found {
		return cached, nil
	}
	if len(s.shapeOrder) == s.shapeCapacity {
		delete(s.shapeCache, s.shapeOrder[0])
		s.shapeOrder = s.shapeOrder[1:]
	}
	s.shapeCache[identity] = shape
	s.shapeOrder = append(s.shapeOrder, identity)
	s.shapeStats.Entries = len(s.shapeCache)
	return shape, nil
}

// RecordTraversalStrategySelection records an observation-only routing
// outcome. No per-query, SQL, graph, parameter, or decision data is retained.
func (s *connectionCacheProvider) RecordTraversalStrategySelection(selection pg.TraversalStrategySelection) {
	if s == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if selection.Reason == "shape_unavailable" {
		s.strategySelection.ShapeUnavailable++
	}
	if selection.Mode == "exact_query_canary" {
		s.strategySelection.ExactQueryCanary++
	} else if selection.Mode == "structural_authorized" {
		s.strategySelection.StructuralAuthorized++
	} else if selection.Mode == "topology_selected" {
		s.strategySelection.TopologySelected++
	} else if selection.Mode == "structural_shadow" {
		s.strategySelection.StructuralShadow++
	} else {
		s.strategySelection.Incumbent++
	}
}

// RecordTraversalRouteDecision records a query-text-free topology routing outcome.
func (s *connectionCacheProvider) RecordTraversalRouteDecision(decision pg.TraversalRouteDecision) {
	if s == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	switch decision.Reason {
	case "topology_route_disabled":
		s.routeDecisions.Disabled++
	case "topology_synopsis_unavailable":
		s.routeDecisions.SynopsisUnavailable++
	case "topology_route_shadow_miss":
		s.routeDecisions.ShadowMiss++
	case "topology_route_shadow_hit":
		s.routeDecisions.ShadowHit++
	case "topology_route_candidate_hit":
		s.routeDecisions.CandidateHit++
	case "topology_route_first_use_candidate":
		s.routeDecisions.FirstUseCandidate++
	case "topology_estimate_rejected":
		s.routeDecisions.EstimateRejected++
	case "topology_route_capacity":
		s.routeDecisions.Capacity++
	case "topology_route_parameters_unverifiable":
		s.routeDecisions.ParametersInvalid++
	}
}

// RecordSQLGenerationProfile retains query-text-free timing totals for the
// v2 architecture. A profile is recorded after pgx has returned a row stream,
// not after its rows are consumed.
func (s *connectionCacheProvider) RecordSQLGenerationProfile(profile pg.SQLGenerationProfile) {
	if s == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if profile.QueryClass == "shortest_path" {
		s.sqlGeneration.ShortestPath.add(profile)
	} else {
		s.sqlGeneration.Other.add(profile)
	}
}

// DeferStableSnapshotTraversalWorkspaces keeps V2 ordinary repeatable-read
// transactions free of temporary shortest-path workspace initialization.
func (s *connectionCacheProvider) DeferStableSnapshotTraversalWorkspaces() bool {
	return true
}

// newConnectionCacheProvider initializes cache state shared by one v2 pool.
func newConnectionCacheProvider(config Config) (*connectionCacheProvider, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	poolConfig := config.resolvedPoolConfig()
	return &connectionCacheProvider{
		capacity:        config.TranslationCacheEntries,
		shapeCapacity:   config.TranslationCacheEntries,
		shapeCache:      map[[sha256.Size]byte]pg.TraversalShape{},
		shapeStats:      TraversalShapeCacheStats{Capacity: config.TranslationCacheEntries},
		sharedTemplates: newSharedTemplateCache(config.SharedShortestPathTemplateEntries),
		minConnections:  poolConfig.MinConnections,
		maxConnections:  poolConfig.MaxConnections,
		generation:      1,
		states:          map[*pgx.Conn]*connectionState{},
	}, nil
}

// CacheForConnection returns the cache owned by conn's physical connection.
// A connection that was not registered or was already removed bypasses
// retention through the v1 transaction seam.
func (s *connectionCacheProvider) CacheForConnection(conn *pgx.Conn) pg.CypherTranslationCache {
	if s == nil || conn == nil {
		return nil
	}
	s.lock.RLock()
	state := s.states[conn]
	s.lock.RUnlock()
	if state == nil {
		return nil
	}
	return state.cache
}

// EnsureStableSnapshotTraversalWorkspaces initializes a leased connection's
// reusable traversal workspace at most once per schema generation. The setup
// is never marked ready until PostgreSQL accepts it successfully.
func (s *connectionCacheProvider) EnsureStableSnapshotTraversalWorkspaces(ctx context.Context, conn *pgxpool.Conn) error {
	if conn == nil {
		return fmt.Errorf("PostgreSQL connection is required for traversal workspace setup")
	}
	return s.ensureWorkspaceForConnection(conn.Conn(), func() error {
		return pg.EnsureStableSnapshotTraversalWorkspaces(ctx, conn)
	})
}

// ensureWorkspaceForConnection initializes a connection workspace once per schema generation.
func (s *connectionCacheProvider) ensureWorkspaceForConnection(conn *pgx.Conn, initialize func() error) error {
	if initialize == nil {
		return fmt.Errorf("traversal workspace initializer is required")
	}
	if s == nil || conn == nil {
		return initialize()
	}

	s.lock.Lock()
	state := s.states[conn]
	generation := s.generation
	if state != nil && !s.closed && state.workspaceReadyGeneration == generation {
		state.workspace.Reuses++
		s.lock.Unlock()
		return nil
	}
	s.lock.Unlock()

	err := initialize()

	s.lock.Lock()
	defer s.lock.Unlock()
	if state != nil && s.states[conn] == state {
		if err != nil {
			state.workspace.Failures++
			return err
		}
		state.workspace.Initializations++
		if !s.closed && s.generation == generation {
			state.workspaceReadyGeneration = generation
			state.workspace.Ready = true
		}
	}
	return err
}

// registerConnection allocates state only after the pool's earlier
// AfterConnect initialization has completed successfully.
func (s *connectionCacheProvider) registerConnection(conn *pgx.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	if _, exists := s.states[conn]; exists {
		return
	}
	s.nextID++
	s.states[conn] = &connectionState{
		id: s.nextID,
		cache: newConnectionTranslationCache(s.capacity, func() uint64 {
			s.lock.RLock()
			defer s.lock.RUnlock()
			return s.generation
		}, s.sharedTemplates),
		preparedStatements: map[[sha256.Size]byte]struct{}{},
	}
}

// preparedStatementWarmup binds normalized SQL to its stable prepared-statement identity.
type preparedStatementWarmup struct {
	// identity is the SHA-256 digest used for deduplication and pgx naming.
	identity [sha256.Size]byte

	// sql is the normalized statement text prepared on physical connections.
	sql string
}

// normalizePreparedStatementWarmups validates, deduplicates, and clones selected SQL.
func normalizePreparedStatementWarmups(statements []string) ([]preparedStatementWarmup, error) {
	warmups := make([]preparedStatementWarmup, 0, len(statements))
	seen := map[[sha256.Size]byte]struct{}{}
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			return nil, fmt.Errorf("prepared statement SQL must not be empty")
		}
		if len(statement) > pg.MaxCachedCypherQueryBytes {
			return nil, fmt.Errorf("prepared statement SQL exceeds %d bytes", pg.MaxCachedCypherQueryBytes)
		}
		identity := sha256.Sum256([]byte(statement))
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		warmups = append(warmups, preparedStatementWarmup{identity: identity, sql: strings.Clone(statement)})
	}
	return warmups, nil
}

// pgxStatementCacheName derives the name pgx uses for a warmed statement.
func pgxStatementCacheName(identity [sha256.Size]byte) string {
	return "stmtcache_" + hex.EncodeToString(identity[:24])
}

// warmStatementsForConnection prepares SQL using pgx's CacheStatement naming
// convention. pgx then adopts the already-prepared server statement on its
// first regular CacheStatement execution instead of preparing it twice.
func (s *connectionCacheProvider) warmStatementsForConnection(conn *pgx.Conn, statements []preparedStatementWarmup, prepare func(string, string) error) error {
	if len(statements) == 0 {
		return nil
	}
	if prepare == nil {
		return fmt.Errorf("prepared statement initializer is required")
	}
	if s == nil || conn == nil {
		return fmt.Errorf("registered PostgreSQL connection is required for statement warm-up")
	}

	var errs []error
	for _, statement := range statements {
		s.lock.Lock()
		state := s.states[conn]
		if state == nil || s.closed {
			s.lock.Unlock()
			return fmt.Errorf("PostgreSQL connection is not registered for statement warm-up")
		}
		if _, prepared := state.preparedStatements[statement.identity]; prepared {
			state.prepared.Reuses++
			s.lock.Unlock()
			continue
		}
		state.prepared.Attempts++
		s.lock.Unlock()

		if err := prepare(pgxStatementCacheName(statement.identity), statement.sql); err != nil {
			s.lock.Lock()
			if s.states[conn] == state {
				state.prepared.Failures++
			}
			s.lock.Unlock()
			errs = append(errs, err)
			continue
		}

		s.lock.Lock()
		if s.states[conn] == state && !s.closed {
			state.preparedStatements[statement.identity] = struct{}{}
			state.prepared.Prepared++
		}
		s.lock.Unlock()
	}
	return errors.Join(errs...)
}

// removeConnection unregisters and closes state. It is idempotent so pool
// shutdown and failed initialization paths cannot retain connection state.
func (s *connectionCacheProvider) removeConnection(conn *pgx.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.lock.Lock()
	state := s.states[conn]
	if state != nil {
		delete(s.states, conn)
	}
	s.lock.Unlock()
	if state == nil {
		return
	}

	stats := state.cache.close()
	s.lock.Lock()
	s.retiredConnections++
	stats.Entries = 0
	stats.Capacity = 0
	s.retiredStats.add(stats)
	state.prepared.Entries = 0
	s.retiredPrepared.add(state.prepared)
	s.lock.Unlock()
}

// advanceSchemaGeneration invalidates schema-sensitive shape and workspace state.
func (s *connectionCacheProvider) advanceSchemaGeneration() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.generation++
	s.shapeCache = map[[sha256.Size]byte]pg.TraversalShape{}
	s.shapeOrder = nil
	s.shapeStats.Entries = 0
	for _, state := range s.states {
		state.workspace.Ready = false
	}
	return s.generation
}

// close retires all provider state after the pool lifecycle has ended.
func (s *connectionCacheProvider) close() {
	if s == nil {
		return
	}
	s.lock.Lock()
	if s.closed {
		s.lock.Unlock()
		return
	}
	s.closed = true
	s.shapeCache = nil
	s.shapeOrder = nil
	s.shapeStats.Entries = 0
	states := make([]*connectionState, 0, len(s.states))
	for _, state := range s.states {
		states = append(states, state)
	}
	s.states = nil
	s.lock.Unlock()

	for _, state := range states {
		stats := state.cache.close()
		s.lock.Lock()
		s.retiredConnections++
		stats.Entries = 0
		stats.Capacity = 0
		s.retiredStats.add(stats)
		state.prepared.Entries = 0
		s.retiredPrepared.add(state.prepared)
		s.lock.Unlock()
	}
}

// stats returns a query-text-free snapshot spanning live and retired connections.
func (s *connectionCacheProvider) stats() Stats {
	if s == nil {
		return Stats{}
	}
	s.lock.RLock()
	stats := Stats{
		SchemaGeneration:            s.generation,
		CapacityPerConnection:       s.capacity,
		MinConnections:              s.minConnections,
		MaxConnections:              s.maxConnections,
		LiveConnections:             len(s.states),
		RetiredConnections:          s.retiredConnections,
		Aggregate:                   s.retiredStats,
		PreparedStatements:          s.retiredPrepared,
		SQLGeneration:               s.sqlGeneration,
		StrategySelection:           s.strategySelection,
		TraversalShapeCache:         s.shapeStats,
		TraversalRouteDecision:      s.routeDecisions,
		SharedShortestPathTemplates: s.sharedTemplates.snapshot(),
		Connections:                 make([]ConnectionCacheStats, 0, len(s.states)),
	}
	states := make([]*connectionState, 0, len(s.states))
	workspaceStats := make([]TraversalWorkspaceStats, 0, len(s.states))
	preparedStats := make([]PreparedStatementStats, 0, len(s.states))
	for _, state := range s.states {
		states = append(states, state)
		workspaceStats = append(workspaceStats, state.workspace)
		prepared := state.prepared
		prepared.Entries = len(state.preparedStatements)
		preparedStats = append(preparedStats, prepared)
		stats.TraversalWorkspace.add(state.workspace)
		stats.PreparedStatements.add(prepared)
	}
	s.lock.RUnlock()

	for index, state := range states {
		connectionStats := state.cache.statsSnapshot()
		stats.Connections = append(stats.Connections, ConnectionCacheStats{
			ID:                 state.id,
			Translation:        connectionStats,
			TraversalWorkspace: workspaceStats[index],
			PreparedStatements: preparedStats[index],
		})
		stats.Aggregate.add(connectionStats)
	}
	stats.Aggregate.Capacity = stats.CapacityPerConnection * stats.LiveConnections
	return stats
}
