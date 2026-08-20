package v2

import (
	"time"

	"github.com/specterops/dawgs/drivers/pg"
)

// TranslationCacheStats is a query-text-free snapshot of one connection's
// translation cache activity and occupancy.
type TranslationCacheStats struct {
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Bypasses        uint64 `json:"bypasses"`
	Insertions      uint64 `json:"insertions"`
	Evictions       uint64 `json:"evictions"`
	BindingFailures uint64 `json:"binding_failures"`
	Entries         int    `json:"entries"`
	Capacity        int    `json:"capacity"`
}

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
	Initializations uint64 `json:"initializations"`
	Reuses          uint64 `json:"reuses"`
	Failures        uint64 `json:"failures"`
	Ready           bool   `json:"ready"`
}

func (s *TraversalWorkspaceStats) add(other TraversalWorkspaceStats) {
	s.Initializations += other.Initializations
	s.Reuses += other.Reuses
	s.Failures += other.Failures
}

// PreparedStatementStats describes opt-in statement warm-up activity. Entries
// counts only SHA-256 statement identities, never SQL text or parameter data.
type PreparedStatementStats struct {
	Attempts uint64 `json:"attempts"`
	Prepared uint64 `json:"prepared"`
	Reuses   uint64 `json:"reuses"`
	Failures uint64 `json:"failures"`
	Entries  int    `json:"entries"`
}

// StrategySelectionStats records query-text-free production-routing
// observations. These counters describe selection only; they do not claim an
// emitted candidate executed.
type StrategySelectionStats struct {
	Incumbent            uint64 `json:"incumbent"`
	ExactQueryCanary     uint64 `json:"exact_query_canary"`
	StructuralShadow     uint64 `json:"structural_shadow"`
	StructuralAuthorized uint64 `json:"structural_authorized"`
	TopologySelected     uint64 `json:"topology_selected"`
	ShapeUnavailable     uint64 `json:"shape_unavailable"`
}

// TraversalShapeCacheStats describes V2's bounded, query-text-free structural
// classification cache. Entries retain only a digest and immutable shape.
type TraversalShapeCacheStats struct {
	Hits     uint64 `json:"hits"`
	Misses   uint64 `json:"misses"`
	Entries  int    `json:"entries"`
	Capacity int    `json:"capacity"`
}

// TraversalRouteDecisionStats aggregates topology-routing shadow states
// without retaining transaction tokens, graph IDs, or caller values.
type TraversalRouteDecisionStats struct {
	Disabled            uint64 `json:"disabled"`
	SynopsisUnavailable uint64 `json:"synopsis_unavailable"`
	ShadowMiss          uint64 `json:"shadow_miss"`
	ShadowHit           uint64 `json:"shadow_hit"`
	CandidateHit        uint64 `json:"candidate_hit"`
	Capacity            uint64 `json:"capacity"`
	ParametersInvalid   uint64 `json:"parameters_invalid"`
}

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
	ID                 uint64                  `json:"id"`
	Translation        TranslationCacheStats   `json:"translation"`
	TraversalWorkspace TraversalWorkspaceStats `json:"traversal_workspace"`
	PreparedStatements PreparedStatementStats  `json:"prepared_statements"`
}

// Stats is a query-text-free provider snapshot. Aggregate combines live and
// retired connection counters; its Capacity is the current theoretical bound
// across live connections, not a global retained-entry limit.
type Stats struct {
	SchemaGeneration            uint64                      `json:"schema_generation"`
	CapacityPerConnection       int                         `json:"capacity_per_connection"`
	MinConnections              int32                       `json:"min_connections"`
	MaxConnections              int32                       `json:"max_connections"`
	LiveConnections             int                         `json:"live_connections"`
	RetiredConnections          uint64                      `json:"retired_connections"`
	Aggregate                   TranslationCacheStats       `json:"aggregate"`
	TraversalWorkspace          TraversalWorkspaceStats     `json:"traversal_workspace"`
	PreparedStatements          PreparedStatementStats      `json:"prepared_statements"`
	Connections                 []ConnectionCacheStats      `json:"connections"`
	SQLGeneration               SQLGenerationStats          `json:"sql_generation"`
	StrategySelection           StrategySelectionStats      `json:"strategy_selection"`
	TraversalShapeCache         TraversalShapeCacheStats    `json:"traversal_shape_cache"`
	TraversalRouteDecision      TraversalRouteDecisionStats `json:"traversal_route_decision"`
	SharedShortestPathTemplates SharedTemplateStats         `json:"shared_shortest_path_templates"`
}

// SharedTemplateStats reports the bounded V2-wide immutable shortest-path
// template tier. It excludes query text and caller values.
type SharedTemplateStats struct {
	Hits       uint64 `json:"hits"`
	Misses     uint64 `json:"misses"`
	Insertions uint64 `json:"insertions"`
	Evictions  uint64 `json:"evictions"`
	Entries    int    `json:"entries"`
	Capacity   int    `json:"capacity"`
}

// SQLGenerationTiming contains aggregate V2 SQL-generation durations. Count
// is separate so a zero-cost stage remains observable.
type SQLGenerationTiming struct {
	Count     uint64        `json:"count"`
	Parse     time.Duration `json:"parse"`
	Graph     time.Duration `json:"graph"`
	Policy    time.Duration `json:"policy"`
	Cache     time.Duration `json:"cache"`
	Translate time.Duration `json:"translate"`
	Format    time.Duration `json:"format"`
	Dispatch  time.Duration `json:"dispatch"`
}

func (s *SQLGenerationTiming) add(profile pg.SQLGenerationProfile) {
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
	ShortestPath SQLGenerationTiming `json:"shortest_path"`
	Other        SQLGenerationTiming `json:"other"`
}
