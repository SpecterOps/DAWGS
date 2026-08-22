package pg

import "time"

// TranslationCacheStats is a query-text-free snapshot of one connection's
// translation cache activity and occupancy.
type TranslationCacheStats struct {
	// Hits counts translations served from the connection-local cache.
	Hits uint64 `json:"hits"`

	// Misses counts translations not found in the connection-local cache.
	Misses uint64 `json:"misses"`

	// Bypasses counts translations intentionally built without retention.
	Bypasses uint64 `json:"bypasses"`

	// Insertions counts reusable translations published to the local cache.
	Insertions uint64 `json:"insertions"`

	// Evictions counts cached translations displaced at capacity.
	Evictions uint64 `json:"evictions"`

	// BindingFailures counts cache hits that cannot bind current caller values.
	BindingFailures uint64 `json:"binding_failures"`

	// Entries is the local cache occupancy at snapshot time.
	Entries int `json:"entries"`

	// Capacity is the maximum number of retained local translations.
	Capacity int `json:"capacity"`
}

// add accumulates another connection cache snapshot into s.
func (s *TranslationCacheStats) add(other TranslationCacheStats) {
	s.Hits += other.Hits
	s.Misses += other.Misses
	s.Bypasses += other.Bypasses
	s.Insertions += other.Insertions
	s.Evictions += other.Evictions
	s.BindingFailures += other.BindingFailures
	s.Entries += other.Entries
}

// TraversalWorkspaceStats describes setup activity for session-local
// stable-snapshot traversal workspaces without exposing backend identity.
type TraversalWorkspaceStats struct {
	// Initializations counts successful workspace setup operations.
	Initializations uint64 `json:"initializations"`

	// Reuses counts requests served by a workspace ready in the current generation.
	Reuses uint64 `json:"reuses"`

	// Failures counts workspace setup attempts rejected by PostgreSQL.
	Failures uint64 `json:"failures"`

	// Ready reports whether the workspace is ready for the current schema generation.
	Ready bool `json:"ready"`
}

// add accumulates setup counts while intentionally excluding connection-local readiness.
func (s *TraversalWorkspaceStats) add(other TraversalWorkspaceStats) {
	s.Initializations += other.Initializations
	s.Reuses += other.Reuses
	s.Failures += other.Failures
}

// PreparedStatementStats describes opt-in statement warm-up activity. Entries
// counts only SHA-256 statement identities, never SQL text or parameter data.
type PreparedStatementStats struct {
	// Attempts counts requests to prepare selected statements.
	Attempts uint64 `json:"attempts"`

	// Prepared counts statements successfully prepared on a connection.
	Prepared uint64 `json:"prepared"`

	// Reuses counts requests satisfied by an already prepared statement.
	Reuses uint64 `json:"reuses"`

	// Failures counts prepare requests rejected by PostgreSQL.
	Failures uint64 `json:"failures"`

	// Entries is the number of statement identities retained for the connection.
	Entries int `json:"entries"`
}

// StrategySelectionStats records query-text-free production-routing
// observations. These counters describe selection only; they do not claim an
// emitted candidate executed.
type StrategySelectionStats struct {
	// Incumbent counts selections that leave the stable executor active.
	Incumbent uint64 `json:"incumbent"`

	// ExactQueryCanary counts candidates selected by exact-query authorization.
	ExactQueryCanary uint64 `json:"exact_query_canary"`

	// StructuralShadow counts structural matches observed without authorization.
	StructuralShadow uint64 `json:"structural_shadow"`

	// StructuralAuthorized counts candidates selected by structural authorization.
	StructuralAuthorized uint64 `json:"structural_authorized"`

	// TopologySelected counts candidates selected by transaction-local topology routing.
	TopologySelected uint64 `json:"topology_selected"`

	// ShapeUnavailable counts queries outside the structural classifier scope.
	ShapeUnavailable uint64 `json:"shape_unavailable"`
}

// TraversalShapeCacheStats describes V2's bounded, query-text-free structural
// classification cache. Entries retain only a digest and immutable shape.
type TraversalShapeCacheStats struct {
	// Hits counts classifications served from the bounded shape cache.
	Hits uint64 `json:"hits"`

	// Misses counts classifications computed without a retained entry.
	Misses uint64 `json:"misses"`

	// Entries is the number of retained query-digest classifications.
	Entries int `json:"entries"`

	// Capacity bounds retained classifications.
	Capacity int `json:"capacity"`
}

// TraversalRouteDecisionStats aggregates topology-routing shadow states
// without retaining transaction tokens, graph IDs, or caller values.
type TraversalRouteDecisionStats struct {
	// Disabled counts queries ineligible for topology routing.
	Disabled uint64 `json:"disabled"`

	// SynopsisUnavailable counts queries without a current graph synopsis.
	SynopsisUnavailable uint64 `json:"synopsis_unavailable"`

	// ShadowMiss counts first observations that populate route-decision shadow state.
	ShadowMiss uint64 `json:"shadow_miss"`

	// ShadowHit counts repeated shadow observations that remain on the incumbent.
	ShadowHit uint64 `json:"shadow_hit"`

	// CandidateHit counts repeated observations that select the candidate route.
	CandidateHit uint64 `json:"candidate_hit"`

	// FirstUseCandidate counts first-use protocol selections of the candidate route.
	FirstUseCandidate uint64 `json:"first_use_candidate"`

	// EstimateRejected counts candidates rejected by the synopsis estimate.
	EstimateRejected uint64 `json:"estimate_rejected"`

	// Capacity counts decisions rejected by bounded transaction-local state.
	Capacity uint64 `json:"capacity"`

	// ParametersInvalid counts queries whose parameters cannot be fingerprinted.
	ParametersInvalid uint64 `json:"parameters_invalid"`
}

// add accumulates another connection's prepared-statement statistics.
func (s *PreparedStatementStats) add(other PreparedStatementStats) {
	s.Attempts += other.Attempts
	s.Prepared += other.Prepared
	s.Reuses += other.Reuses
	s.Failures += other.Failures
	s.Entries += other.Entries
}

// ConnectionCacheStats describes one currently live connection cache. ID is
// an opaque diagnostic identifier; it is not a backend PID or pointer value.
type ConnectionCacheStats struct {
	// ID is an opaque provider-assigned identifier for the live connection.
	ID uint64 `json:"id"`

	// Translation reports cache activity scoped to this physical connection.
	Translation TranslationCacheStats `json:"translation"`

	// TraversalWorkspace reports setup state scoped to this physical connection.
	TraversalWorkspace TraversalWorkspaceStats `json:"traversal_workspace"`

	// PreparedStatements reports selected statement warm-up for this connection.
	PreparedStatements PreparedStatementStats `json:"prepared_statements"`
}

// Stats is a query-text-free provider snapshot. Aggregate combines live and
// retired connection counters; its Capacity is the current theoretical bound
// across live connections, not a global retained-entry limit.
type Stats struct {
	// SchemaGeneration partitions state invalidated by schema-sensitive changes.
	SchemaGeneration uint64 `json:"schema_generation"`

	// CapacityPerConnection bounds retained translations for one live connection.
	CapacityPerConnection int `json:"capacity_per_connection"`

	// MinConnections is the configured pgx pool lower connection bound.
	MinConnections int32 `json:"min_connections"`

	// MaxConnections is the configured pgx pool upper connection bound.
	MaxConnections int32 `json:"max_connections"`

	// LiveConnections counts physical connections currently registered by the provider.
	LiveConnections int `json:"live_connections"`

	// RetiredConnections counts connections whose cache state has been closed.
	RetiredConnections uint64 `json:"retired_connections"`

	// Aggregate combines active and retired translation-cache statistics.
	Aggregate TranslationCacheStats `json:"aggregate"`

	// TraversalWorkspace aggregates reusable workspace setup activity.
	TraversalWorkspace TraversalWorkspaceStats `json:"traversal_workspace"`

	// PreparedStatements aggregates selected statement warm-up activity.
	PreparedStatements PreparedStatementStats `json:"prepared_statements"`

	// Connections reports cache state for each currently live physical connection.
	Connections []ConnectionCacheStats `json:"connections"`

	// SQLGeneration aggregates query-text-free SQL generation timings.
	SQLGeneration SQLGenerationStats `json:"sql_generation"`

	// StrategySelection aggregates production routing observations.
	StrategySelection StrategySelectionStats `json:"strategy_selection"`

	// TraversalShapeCache reports bounded structural classifier cache activity.
	TraversalShapeCache TraversalShapeCacheStats `json:"traversal_shape_cache"`

	// TraversalRouteDecision aggregates topology route-decision outcomes.
	TraversalRouteDecision TraversalRouteDecisionStats `json:"traversal_route_decision"`

	// SharedShortestPathTemplates reports the pool-wide immutable template cache.
	SharedShortestPathTemplates SharedTemplateStats `json:"shared_shortest_path_templates"`
}

// SharedTemplateStats reports the bounded V2-wide immutable shortest-path
// template tier. It excludes query text and caller values.
type SharedTemplateStats struct {
	// Hits counts templates served from the shared shortest-path tier.
	Hits uint64 `json:"hits"`

	// Misses counts shared-tier lookups without a retained template.
	Misses uint64 `json:"misses"`

	// Insertions counts templates added to the shared tier.
	Insertions uint64 `json:"insertions"`

	// Evictions counts templates displaced from the shared tier at capacity.
	Evictions uint64 `json:"evictions"`

	// Entries is the shared-tier occupancy at snapshot time.
	Entries int `json:"entries"`

	// Capacity bounds retained templates in the shared tier.
	Capacity int `json:"capacity"`
}

// SQLGenerationTiming contains aggregate V2 SQL-generation durations. Count
// is separate so a zero-cost stage remains observable.
type SQLGenerationTiming struct {
	// Count records SQL generation profiles represented by this aggregate.
	Count uint64 `json:"count"`

	// Parse accumulates Cypher parsing time.
	Parse time.Duration `json:"parse"`

	// Graph accumulates graph-resolution time.
	Graph time.Duration `json:"graph"`

	// Policy accumulates traversal-policy selection time.
	Policy time.Duration `json:"policy"`

	// Cache accumulates translation-cache lookup time.
	Cache time.Duration `json:"cache"`

	// Translate accumulates Cypher-to-SQL lowering time.
	Translate time.Duration `json:"translate"`

	// Format accumulates SQL rendering time.
	Format time.Duration `json:"format"`

	// Dispatch accumulates pgx query-dispatch time.
	Dispatch time.Duration `json:"dispatch"`
}

// add accumulates one query-text-free SQL-generation profile.
func (s *SQLGenerationTiming) add(profile SQLGenerationProfile) {
	s.Count++
	s.Parse += profile.Parse
	s.Graph += profile.Graph
	s.Policy += profile.Policy
	s.Cache += profile.Cache
	s.Translate += profile.Translate
	s.Format += profile.Format
	s.Dispatch += profile.Dispatch
}

// SQLGenerationStats separates shortest-path timing from other graph work.
type SQLGenerationStats struct {
	// ShortestPath aggregates generation timings for shortest-path queries.
	ShortestPath SQLGenerationTiming `json:"shortest_path"`

	// Other aggregates generation timings for all other queries.
	Other SQLGenerationTiming `json:"other"`
}
