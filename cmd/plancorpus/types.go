package main

import (
	"encoding/json"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/testutil"
)

// planRecordSchemaVersion reserves the stable protocol value used to recognize plan record schema version across artifacts and executions.
const planRecordSchemaVersion = 2

// PlanRecord captures a query plan together with workload, fixture, and environment identity.
type PlanRecord struct {
	// SchemaVersion identifies the serialized plan-record schema revision.
	SchemaVersion int `json:"schema_version"`
	// Metadata captures build and baseline metadata.
	Metadata testutil.BaselineMetadata `json:"metadata"`
	// Driver identifies the database driver that produced the plan or summary.
	Driver string `json:"driver"`
	// Source identifies the source corpus file.
	Source string `json:"source"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset,omitempty"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// WorkloadSHA256 identifies the backend-independent source workload.
	WorkloadSHA256 string `json:"workload_sha256"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params map[string]any `json:"params,omitempty"`
	// SQL contains the rendered SQL statement.
	SQL string `json:"sql,omitempty"`
	// PGPlan contains the normalized PostgreSQL text plan.
	PGPlan []string `json:"pg_plan,omitempty"`
	// PGPlanFingerprint identifies the normalized PostgreSQL plan without retaining another copy.
	PGPlanFingerprint string `json:"pg_plan_fingerprint,omitempty"`
	// PGOperators lists normalized PostgreSQL operators found in the captured plan.
	PGOperators []string `json:"pg_operators,omitempty"`
	// Neo4jPlan contains the normalized Neo4j operator tree.
	Neo4jPlan *Neo4jPlanNode `json:"neo4j_plan,omitempty"`
	// Neo4jPlanFingerprint identifies the normalized Neo4j plan tree.
	Neo4jPlanFingerprint string `json:"neo4j_plan_fingerprint,omitempty"`
	// Neo4jOperators lists normalized Neo4j operators found in the captured plan.
	Neo4jOperators []string `json:"neo4j_operators,omitempty"`
	// PlannedLowerings lists SQL lowering opportunities identified before optimization.
	PlannedLowerings []string `json:"planned_lowerings,omitempty"`
	// AppliedLowerings lists SQL lowerings actually applied during translation.
	AppliedLowerings []string `json:"applied_lowerings,omitempty"`
	// SkippedLowerings lists identified SQL lowerings not applied.
	SkippedLowerings []translate.SkippedLowering `json:"skipped_lowerings,omitempty"`
	// Optimization captures translation optimization and lowering decisions.
	Optimization *translate.OptimizationSummary `json:"optimization,omitempty"`
	// Error supplies the error input to the PlanRecord contract.
	Error string `json:"error,omitempty"`
}

// Neo4jPlanNode models the recursive operator tree returned by Neo4j EXPLAIN.
type Neo4jPlanNode struct {
	// Operator identifies the backend plan operator at this node.
	Operator string `json:"operator"`
	// Arguments maps backend plan argument names to stable string representations.
	Arguments map[string]string `json:"arguments,omitempty"`
	// Identifiers lists variables or identifiers referenced by the Neo4j plan node.
	Identifiers []string `json:"identifiers,omitempty"`
	// Children contains child Neo4j plan operators in backend order.
	Children []Neo4jPlanNode `json:"children,omitempty"`
	// EstimatedRows records planner cardinality when exposed by the server.
	EstimatedRows *float64 `json:"estimated_rows,omitempty"`
	// ActualRows records profiled output cardinality when this is an executed read plan.
	ActualRows *int64 `json:"actual_rows,omitempty"`
	// DBHits records profiled store accesses when exposed by the server.
	DBHits *int64 `json:"db_hits,omitempty"`
	// PageCacheHits records profiled page-cache hits when exposed by the server.
	PageCacheHits *int64 `json:"page_cache_hits,omitempty"`
	// PageCacheMisses records profiled page-cache misses when exposed by the server.
	PageCacheMisses *int64 `json:"page_cache_misses,omitempty"`
	// TimeNS records profiled operator time in nanoseconds when exposed by the server.
	TimeNS *int64 `json:"time_ns,omitempty"`
}

// PlanDeltaReport contains backend-paired semantic plan comparisons without
// treating backend-specific operator counters as interchangeable.
type PlanDeltaReport struct {
	// Version identifies the serialized plan-delta schema revision.
	Version int `json:"version"`
	// Records contains complete and explicitly incomplete backend pairs.
	Records []PlanDeltaRecord `json:"records"`
	// RankedFindings prioritizes semantic disagreements and qualification cases.
	RankedFindings []PlanDeltaFinding `json:"ranked_findings,omitempty"`
}

// PlanDeltaFinding ranks one cross-backend semantic observation for review.
type PlanDeltaFinding struct {
	// Rank is the one-based position after stable severity ordering.
	Rank int `json:"rank"`
	// Category identifies the semantic disagreement being ranked.
	Category string `json:"category"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset,omitempty"`
	// Source identifies the corpus declaration.
	Source string `json:"source"`
	// Name identifies the workload case.
	Name string `json:"name"`
	// PairSHA256 identifies the exact paired record.
	PairSHA256 string `json:"pair_sha256"`
	// Score is a category-local descending severity score.
	Score float64 `json:"score"`
	// Summary is a compact stable explanation of the finding.
	Summary string `json:"summary"`
}

// PlanDeltaRecord compares one source workload across PostgreSQL and Neo4j.
type PlanDeltaRecord struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset,omitempty"`
	// Source identifies the source corpus declaration.
	Source string `json:"source"`
	// Name identifies the case within its source.
	Name string `json:"name"`
	// WorkloadSHA256 identifies the backend-independent source workload.
	WorkloadSHA256 string `json:"workload_sha256"`
	// SourceRevision identifies the DAWGS source used for capture.
	SourceRevision string `json:"source_revision,omitempty"`
	// PairSHA256 binds workload, source revision, and both backend plan fingerprints.
	PairSHA256 string `json:"pair_sha256"`
	// Postgres supplies the postgres input to the PlanDeltaRecord contract.
	Postgres *SemanticPlan `json:"postgres,omitempty"`
	// Neo4j supplies the neo4j input to the PlanDeltaRecord contract.
	Neo4j *SemanticPlan `json:"neo4j,omitempty"`
	// Complete reports whether both backend plans were captured successfully.
	Complete bool `json:"complete"`
	// IncompleteReason explains a missing or failed backend side.
	IncompleteReason string `json:"incomplete_reason,omitempty"`
	// OppositeStartingSides reports a material starting-side disagreement.
	OppositeStartingSides bool `json:"opposite_starting_sides,omitempty"`
	// OppositePhysicalDirections reports a physical adjacency disagreement.
	OppositePhysicalDirections bool `json:"opposite_physical_directions,omitempty"`
	// Neo4jReorderedPattern reports that Neo4j started from the opposite logical endpoint.
	Neo4jReorderedPattern bool `json:"neo4j_reordered_pattern,omitempty"`
	// ChosenSideDidLessObservedWork reports whether Neo4j's first leaf had no more profiled work than the alternative leaf.
	ChosenSideDidLessObservedWork *bool `json:"chosen_side_did_less_observed_work,omitempty"`
	// SeedEstimateQError reports symmetric disagreement between backend seed estimates.
	SeedEstimateQError *float64 `json:"seed_estimate_q_error,omitempty"`
	// TraversalEstimateQError reports symmetric disagreement between backend traversal estimates.
	TraversalEstimateQError *float64 `json:"traversal_estimate_q_error,omitempty"`
	// OutputEstimateQError reports symmetric disagreement between backend output estimates.
	OutputEstimateQError *float64 `json:"output_estimate_q_error,omitempty"`
	// PredicatePlacementMoved reports a backend disagreement in predicate-bearing stages.
	PredicatePlacementMoved bool `json:"predicate_placement_moved,omitempty"`
	// HydrationEstimateQError reports symmetric disagreement in identifiable hydration work.
	HydrationEstimateQError *float64 `json:"hydration_estimate_q_error,omitempty"`
}

// SemanticPlan normalizes one backend plan into comparable traversal stages.
type SemanticPlan struct {
	// Driver identifies the backend that produced this plan.
	Driver string `json:"driver"`
	// PlanFingerprint identifies the complete normalized backend plan.
	PlanFingerprint string `json:"plan_fingerprint"`
	// StartingAccess describes the first observed leaf access.
	StartingAccess string `json:"starting_access,omitempty"`
	// TerminalAccess describes the opposite endpoint access when identifiable.
	TerminalAccess string `json:"terminal_access,omitempty"`
	// LogicalDirection describes the query's directed traversal orientation.
	LogicalDirection string `json:"logical_direction,omitempty"`
	// PhysicalDirection identifies start_id or end_id adjacency use.
	PhysicalDirection string `json:"physical_direction,omitempty"`
	// PredicatePlacement lists stages carrying predicates or filters.
	PredicatePlacement []string `json:"predicate_placement,omitempty"`
	// EndpointBinding reports whether both endpoints are available before traversal.
	EndpointBinding string `json:"endpoint_binding,omitempty"`
	// OperatorFamily classifies ordinary expansion, SP, ASP, or fixed-hop work.
	OperatorFamily string `json:"operator_family,omitempty"`
	// EstimatedSeeds records a comparable seed estimate when exposed.
	EstimatedSeeds *float64 `json:"estimated_seeds,omitempty"`
	// EstimatedTraversal records a comparable traversal estimate when exposed.
	EstimatedTraversal *float64 `json:"estimated_traversal,omitempty"`
	// EstimatedOutput records a comparable output estimate when exposed.
	EstimatedOutput *float64 `json:"estimated_output,omitempty"`
	// EstimatedHydration records rows at an identifiable hydration/materialization stage.
	EstimatedHydration *float64 `json:"estimated_hydration,omitempty"`
	// ActualOutput records profiled output rows when exposed.
	ActualOutput *int64 `json:"actual_output,omitempty"`
	// ObservedSeedWork records actual rows or store hits at the selected seed leaf when exposed.
	ObservedSeedWork *int64 `json:"observed_seed_work,omitempty"`
	// ObservedAlternativeSeedWork supplies the observed alternative seed work input to the SemanticPlan contract.
	ObservedAlternativeSeedWork *int64 `json:"observed_alternative_seed_work,omitempty"`
	// ObservedTraversalWork records profiled traversal DB hits when exposed.
	ObservedTraversalWork *int64 `json:"observed_traversal_work,omitempty"`
	// ObservedHydrationRows records profiled hydration rows when exposed.
	ObservedHydrationRows *int64 `json:"observed_hydration_rows,omitempty"`
	// OutputQError records estimate error when both estimate and actual output exist.
	OutputQError *float64 `json:"output_q_error,omitempty"`
	// PlannedIdentity identifies the planned identity.
	PlannedIdentity string `json:"planned_identity,omitempty"`
	// EmittedIdentity identifies the emitted identity.
	EmittedIdentity string `json:"emitted_identity,omitempty"`
	// PlannedCandidates lists the complete typed candidate set.
	PlannedCandidates []string `json:"planned_candidates,omitempty"`
	// EmittedCandidates lists the arms present in translated SQL.
	EmittedCandidates []string `json:"emitted_candidates,omitempty"`
	// FallbackIdentity identifies the fallback identity.
	FallbackIdentity string `json:"fallback_identity,omitempty"`
	// FallbackReason records static qualification failure or guarded fallback intent.
	FallbackReason string `json:"fallback_reason,omitempty"`
	// SelectorVersion identifies the policy that produced the plan.
	SelectorVersion string `json:"selector_version,omitempty"`
	// ProbeCaps records bounded runtime evidence limits declared by the plan.
	ProbeCaps map[string]int64 `json:"probe_caps,omitempty"`
	// RuntimeIdentityKnown is false for PlanCorpus because execution telemetry is GraphBench authority.
	RuntimeIdentityKnown bool `json:"runtime_identity_known"`
	// InternalTraversalWork marks backend work that profiling cannot expose.
	InternalTraversalWork string `json:"internal_traversal_work,omitempty"`
	// Error retains a capture failure without dropping the pair.
	Error string `json:"error,omitempty"`
	// RawOptimization retains typed translation diagnostics for PostgreSQL.
	RawOptimization *translate.OptimizationSummary `json:"raw_optimization,omitempty"`
	// PlanJSON optionally retains a stable semantic projection for downstream tools.
	PlanJSON json.RawMessage `json:"plan_json,omitempty"`
}

// CorpusQuery defines one corpus query and the fixture parameters needed to execute it.
type CorpusQuery struct {
	// Source identifies the source corpus file.
	Source string
	// Dataset identifies the fixture dataset.
	Dataset string
	// Name identifies the case or record within its dataset.
	Name string
	// Cypher contains the Cypher statement under test.
	Cypher string
	// Params supplies literal query parameters.
	Params map[string]any
}
