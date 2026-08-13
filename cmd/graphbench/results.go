// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/testutil"
)

const (
	// StatusOK marks a benchmark case whose execution and expectations succeeded.
	StatusOK = "ok"

	// StatusRowMismatch marks a benchmark case whose observed row count differed from its expectation.
	StatusRowMismatch = "row_mismatch"

	// StatusError marks a benchmark case that failed during execution.
	StatusError = "error"

	// StatusNotImplemented marks a benchmark case unsupported by the selected backend.
	StatusNotImplemented = "not_implemented"
)

// DurationStats summarizes warmup policy, measured latency samples, quantiles, and sample sufficiency.
type DurationStats struct {
	// Iterations records the number of iterations.
	Iterations int `json:"iterations"`
	// WarmupIterations records the number of warmup iterations.
	WarmupIterations int `json:"warmup_iterations"`
	// Median supplies the median input to the DurationStats contract.
	Median time.Duration `json:"median"`
	// P95 supplies the p95 input to the DurationStats contract.
	P95 time.Duration `json:"p95"`
	// P99 supplies the p99 input to the DurationStats contract.
	P99 time.Duration `json:"p99"`
	// P99Gated reports whether the sample count is sufficient to enforce the P99 noise threshold.
	P99Gated bool `json:"p99_gated"`
	// Max supplies the max input to the DurationStats contract.
	Max time.Duration `json:"max"`
	// Samples contains the individual measurements.
	Samples []LatencySample `json:"samples,omitempty"`
}

// RuntimeReceiptEvent records one ordered executor transition observed during
// a measured traversal invocation. Multiple events preserve nested fallback
// chains such as I1 -> S4 -> S3 without reducing them to the terminal arm.
type RuntimeReceiptEvent struct {
	// InvocationID binds this event to the session-local timed invocation that emitted it.
	InvocationID string `json:"invocation_id,omitempty"`
	// Ordinal supplies the ordinal input to the RuntimeReceiptEvent contract.
	Ordinal int `json:"ordinal"`
	// RuntimeIdentity identifies the runtime identity.
	RuntimeIdentity string `json:"runtime_identity"`
	// RuntimeBranch supplies the runtime branch input to the RuntimeReceiptEvent contract.
	RuntimeBranch string `json:"runtime_branch"`
	// FallbackExecuted indicates whether fallback executed applies.
	FallbackExecuted bool `json:"fallback_executed"`
}

// LatencySample records one labeled duration and its measurement order.
type LatencySample struct {
	// Round identifies the measurement round.
	Round int `json:"round"`
	// Block identifies the measurement block used to control carryover effects.
	Block int `json:"block,omitempty"`
	// Arm identifies the measurement arm that produced the sample.
	Arm string `json:"arm,omitempty"`
	// ArmOrder supplies the arm order input to the LatencySample contract.
	ArmOrder int `json:"arm_order,omitempty"`
	// RunUUID links the sample to its resumable benchmark run series.
	RunUUID string `json:"run_uuid,omitempty"`
	// Iteration identifies the measured iteration within its worker or round.
	Iteration int `json:"iteration"`
	// Case identifies the workload whose iteration produced the sample.
	Case string `json:"case"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Backend identifies the execution backend.
	Backend ExecutionMode `json:"backend"`
	// ConnectionID identifies the connection id.
	ConnectionID string `json:"connection_id,omitempty"`
	// Classification supplies the classification input to the LatencySample contract.
	Classification string `json:"classification"`
	// Duration records elapsed time for this observation.
	Duration time.Duration `json:"duration"`
	// RequestedIdentity identifies the requested identity.
	RequestedIdentity string `json:"requested_identity,omitempty"`
	// RuntimeIdentity identifies the runtime identity.
	RuntimeIdentity string `json:"runtime_identity,omitempty"`
	// RuntimeBranch supplies the runtime branch input to the LatencySample contract.
	RuntimeBranch string `json:"runtime_branch,omitempty"`
	// FallbackExecuted records whether the candidate delegated to its exact incumbent.
	FallbackExecuted *bool `json:"fallback_executed,omitempty"`
	// RuntimeAttestation identifies the boundary that supplied runtime identity.
	RuntimeAttestation string `json:"runtime_attestation,omitempty"`
	// RuntimeInvocationID uniquely identifies the session-local timed invocation.
	RuntimeInvocationID string `json:"runtime_invocation_id,omitempty"`
	// RuntimeReceiptEvents preserves the complete ordered runtime branch chain
	// for this exact measured invocation.
	RuntimeReceiptEvents []RuntimeReceiptEvent `json:"runtime_receipt_events,omitempty"`
}

// ConcurrencySample records one concurrent worker iteration and its connection and latency stages.
type ConcurrencySample struct {
	// Worker identifies the concurrent worker that produced the sample.
	Worker int `json:"worker"`
	// Iteration identifies the measured iteration within its worker or round.
	Iteration int `json:"iteration"`
	// ConnectionID identifies the connection id.
	ConnectionID string `json:"connection_id"`
	// Classification supplies the classification input to the ConcurrencySample contract.
	Classification string `json:"classification"`
	// PoolWait records latency spent acquiring a database connection from the pool.
	PoolWait time.Duration `json:"pool_wait"`
	// Transaction records latency spent beginning and configuring the transaction.
	Transaction time.Duration `json:"transaction_setup"`
	// ExecuteDrain records latency spent executing and draining all rows.
	ExecuteDrain time.Duration `json:"execute_decode_drain"`
	// Total supplies the total input to the ConcurrencySample contract.
	Total time.Duration `json:"total"`
}

// ConcurrencyBlock summarizes all samples and connection usage for one concurrency level.
type ConcurrencyBlock struct {
	// Concurrency supplies the concurrency input to the ConcurrencyBlock contract.
	Concurrency int `json:"concurrency"`
	// PoolSize sets the database connection-pool size.
	PoolSize int `json:"pool_size"`
	// Operations records successful query operations completed by a concurrency block.
	Operations int `json:"operations"`
	// Wall records end-to-end wall time for a concurrency block.
	Wall time.Duration `json:"wall"`
	// QPS reports completed query iterations per second.
	QPS float64 `json:"qps"`
	// Samples contains the individual measurements.
	Samples []ConcurrencySample `json:"samples"`
}

// PostgresReferenceResult records one independent PostgreSQL reference arm's identity, plan, observations, and timings.
type PostgresReferenceResult struct {
	// SchemaVersion identifies the PostgreSQL reference-result schema revision.
	SchemaVersion int `json:"schema_version"`
	// Name identifies the independently measured reference arm.
	Name string `json:"name"`
	// LegacyName retains a compatibility alias for the reference arm.
	LegacyName string `json:"legacy_name,omitempty"`
	// Architecture identifies the executor architecture.
	Architecture string `json:"architecture"`
	// ImplementationID provides a versioned identity for the measured reference algorithm and materializer.
	ImplementationID string `json:"implementation_id"`
	// StateShape describes recursive state retained by the reference implementation.
	StateShape string `json:"state_shape"`
	// ObservationShape describes normalized values returned by the reference boundary.
	ObservationShape string `json:"observation_shape"`
	// SemanticValidation identifies the exact observation contract enforced for the reference.
	SemanticValidation string `json:"semantic_validation"`
	// Boundary identifies the measured execution boundary.
	Boundary string `json:"boundary"`
	// TimingBoundary describes which reference stages contribute to latency samples.
	TimingBoundary string `json:"timing_boundary"`
	// FullComparator indicates that the reference returns the complete public observation.
	FullComparator bool `json:"full_comparator"`
	// MeasurementOrder supplies the measurement order input to the PostgresReferenceResult contract.
	MeasurementOrder int `json:"measurement_order,omitempty"`
	// AAAliasOf identifies the reference arm reused for an explicit A/A comparison.
	AAAliasOf string `json:"aa_alias_of,omitempty"`
	// SQL contains the rendered SQL statement.
	SQL string `json:"sql"`
	// SQLFingerprint identifies normalized SQL without retaining the statement text.
	SQLFingerprint string `json:"sql_fingerprint"`
	// RowCount records the number of row count.
	RowCount int64 `json:"row_count"`
	// ObservedRows contains stable serialized observations used for correctness comparison.
	ObservedRows []string `json:"observed_rows,omitempty"`
	// Stats contains latency statistics for the enclosing result or reference.
	Stats DurationStats `json:"stats"`
	// PostgresPlan contains normalized PostgreSQL text-plan lines.
	PostgresPlan []string `json:"postgres_plan,omitempty"`
	// PostgresPlanJSON contains structured PostgreSQL EXPLAIN evidence.
	PostgresPlanJSON json.RawMessage `json:"postgres_plan_json,omitempty"`
	// PostgresMetrics contains normalized PostgreSQL plan resource metrics.
	PostgresMetrics *PostgresPlanMetrics `json:"postgres_metrics,omitempty"`
	// TraversalTelemetry contains lightweight execution identity and optional untimed diagnostic counters.
	TraversalTelemetry *TraversalExecutionTelemetry `json:"traversal_execution_telemetry,omitempty"`
	// traversalTelemetryParameters retains invocation parameters only until all
	// timed samples finish and the optional replay is attached.
	traversalTelemetryParameters map[string]any
}

// CompileSample breaks one Cypher compilation into parse, translate, and render stages.
type CompileSample struct {
	// Iteration identifies the measured iteration within its worker or round.
	Iteration int `json:"iteration"`
	// Parse records Cypher parse latency.
	Parse time.Duration `json:"parse"`
	// Optimize records query optimization latency.
	Optimize time.Duration `json:"optimize"`
	// TranslateIncludingOptimize records combined translation and optimization latency.
	TranslateIncludingOptimize time.Duration `json:"translate_including_optimize"`
	// Render records SQL rendering latency after translation.
	Render time.Duration `json:"render"`
	// Total supplies the total input to the CompileSample contract.
	Total time.Duration `json:"total"`
	// Allocations records allocation count while measuring the client-side stage.
	Allocations uint64 `json:"allocations"`
	// AllocatedBytes records bytes allocated while measuring the client-side stage.
	AllocatedBytes uint64 `json:"allocated_bytes"`
}

// ClientWaterfall summarizes compile and raw-request samples at the client boundary.
type ClientWaterfall struct {
	// IntervalsOverlap warns that nested compilation stages cannot be summed as exclusive costs.
	IntervalsOverlap bool `json:"intervals_overlap"`
	// Notes contains human-readable caveats attached to the artifact or case.
	Notes string `json:"notes"`
	// Samples contains the individual measurements.
	Samples []CompileSample `json:"samples"`
}

// BoundarySample breaks one raw PostgreSQL request into client-side latency stages.
type BoundarySample struct {
	// Iteration identifies the measured iteration within its worker or round.
	Iteration int `json:"iteration"`
	// PoolWait records latency spent acquiring a PostgreSQL connection from the pool.
	PoolWait time.Duration `json:"pool_wait"`
	// Transaction records latency spent beginning and configuring the transaction.
	Transaction time.Duration `json:"transaction_setup"`
	// BindPrepare records PostgreSQL bind and statement-prepare latency.
	BindPrepare time.Duration `json:"bind_prepare"`
	// FirstRow records latency until the first result row becomes available.
	FirstRow time.Duration `json:"first_row"`
	// AllRowsDecode records client time to decode the complete result set.
	AllRowsDecode time.Duration `json:"all_rows_decode"`
	// DrainClose records latency spent draining remaining rows and closing the iterator.
	DrainClose time.Duration `json:"drain_close"`
	// Total supplies the total input to the BoundarySample contract.
	Total time.Duration `json:"total"`
	// Rows records the number of rows.
	Rows int64 `json:"rows"`
	// Allocations records allocation count while measuring the client-side stage.
	Allocations uint64 `json:"allocations"`
	// AllocatedBytes records bytes allocated while measuring the client-side stage.
	AllocatedBytes uint64 `json:"allocated_bytes"`
}

// PostgresBoundaryWaterfall summarizes PostgreSQL planning, execution, and client overhead samples.
type PostgresBoundaryWaterfall struct {
	// Boundary identifies the measured execution boundary.
	Boundary string `json:"boundary"`
	// SQLFingerprint identifies normalized SQL without retaining the statement text.
	SQLFingerprint string `json:"sql_fingerprint"`
	// WarmupIterations records the number of warmup iterations.
	WarmupIterations int `json:"warmup_iterations"`
	// MeasurementOrder supplies the measurement order input to the PostgresBoundaryWaterfall contract.
	MeasurementOrder int `json:"measurement_order,omitempty"`
	// Samples contains the individual measurements.
	Samples []BoundarySample `json:"samples"`
}

// PostgresPlanMetrics aggregates structural, cardinality, timing, and buffer evidence from a PostgreSQL plan.
type PostgresPlanMetrics struct {
	// PlanningMS records PostgreSQL planning time in milliseconds.
	PlanningMS *float64 `json:"planning_ms,omitempty"`
	// ExecutionMS records PostgreSQL execution time in milliseconds.
	ExecutionMS *float64 `json:"execution_ms,omitempty"`
	// Buffers contains shared, local, and temporary buffer activity attributed to the plan.
	Buffers Buffers `json:"buffers,omitempty"`
	// TempFiles records temporary files created by the backend session.
	TempFiles int64 `json:"temp_files,omitempty"`
	// TempBytes records temporary bytes written by the backend session.
	TempBytes int64 `json:"temp_bytes,omitempty"`
	// WALRecords records write-ahead-log records attributed to the plan node.
	WALRecords int64 `json:"wal_records,omitempty"`
	// WALBytes records write-ahead-log bytes attributed to the plan node.
	WALBytes int64 `json:"wal_bytes,omitempty"`
	// RootRows records rows emitted by root selection in the PostgreSQL plan.
	RootRows int64 `json:"root_rows,omitempty"`
	// RecursiveRows records rows emitted by recursive traversal state.
	RecursiveRows int64 `json:"recursive_rows,omitempty"`
	// RecursiveLoops records loops performed by recursive plan nodes.
	RecursiveLoops int64 `json:"recursive_loops,omitempty"`
	// FrontierRows records rows retained in the active traversal frontier.
	FrontierRows int64 `json:"frontier_rows,omitempty"`
	// WitnessRows records rows retained for shortest-path witness reconstruction.
	WitnessRows int64 `json:"witness_rows,omitempty"`
	// MeetingRows records bidirectional search rows where frontiers meet.
	MeetingRows int64 `json:"meeting_rows,omitempty"`
	// HydrationRows records rows processed while hydrating paths from ID trails.
	HydrationRows int64 `json:"hydration_rows,omitempty"`
	// ForwardEdgeProbes records relationship probes performed by forward search.
	ForwardEdgeProbes int64 `json:"forward_edge_probes,omitempty"`
	// ReverseEdgeProbes records relationship probes performed by reverse search.
	ReverseEdgeProbes int64 `json:"reverse_edge_probes,omitempty"`
	// RootLookupLoops records repeated plan loops used to locate traversal roots.
	RootLookupLoops int64 `json:"root_lookup_loops,omitempty"`
	// BoundaryLookupLoops records loops used to resolve traversal boundaries.
	BoundaryLookupLoops int64 `json:"boundary_lookup_loops,omitempty"`
	// HydrationLoops records loops performed while hydrating search results.
	HydrationLoops int64 `json:"hydration_loops,omitempty"`
	// EndpointProbeRows records rows examined by endpoint preflight probing.
	EndpointProbeRows int64 `json:"endpoint_probe_rows,omitempty"`
	// ReverseStateProbeRows records reverse-search state rows examined by probing.
	ReverseStateProbeRows int64 `json:"reverse_state_probe_rows,omitempty"`
	// EndpointGuardOverflow reports whether endpoint-seeded search exceeded its configured guard.
	EndpointGuardOverflow bool `json:"endpoint_guard_overflow,omitempty"`
	// StateGuardOverflow reports whether recursive state exceeded its configured guard.
	StateGuardOverflow bool `json:"state_guard_overflow,omitempty"`
	// ExpansionFallbackExecuted reports whether guarded expansion switched to its exact fallback executor.
	ExpansionFallbackExecuted bool `json:"expansion_fallback_executed,omitempty"`
	// PlanNodes lists normalized PostgreSQL plan-node metrics in traversal order.
	PlanNodes []PostgresPlanNodeMetric `json:"plan_nodes,omitempty"`
	// Provenance maps derived metric names to the plan evidence used to compute them.
	Provenance map[string]string `json:"provenance,omitempty"`
}

// PostgresPlanNodeMetric captures one PostgreSQL plan node's identity, counters, and buffers.
type PostgresPlanNodeMetric struct {
	// PlanNodeID identifies this node within the normalized pre-order plan tree.
	PlanNodeID int64 `json:"plan_node_id,omitempty"`
	// ParentPlanNodeID identifies the direct parent node; the root has no parent.
	ParentPlanNodeID int64 `json:"parent_plan_node_id,omitempty"`
	// NodeType identifies the PostgreSQL plan node type.
	NodeType string `json:"node_type"`
	// ParentRelationship identifies the relationship by which this plan node is attached to its parent.
	ParentRelationship string `json:"parent_relationship,omitempty"`
	// CTEName names the recursive common-table expression referenced by the plan node.
	CTEName string `json:"cte_name,omitempty"`
	// RelationName identifies the PostgreSQL relation scanned by the plan node.
	RelationName string `json:"relation_name,omitempty"`
	// Alias contains the display alias assigned to the plan node.
	Alias string `json:"alias,omitempty"`
	// IndexName names the PostgreSQL index scanned by the plan node.
	IndexName string `json:"index_name,omitempty"`
	// FunctionName identifies a SQL function invoked by a Function Scan without exposing its internal work.
	FunctionName string `json:"function_name,omitempty"`
	// SubplanName names an initplan, subplan, or CTE body used for stable branch attribution.
	SubplanName string `json:"subplan_name,omitempty"`
	// PlanRows records the number of plan rows.
	PlanRows int64 `json:"plan_rows,omitempty"`
	// PlanWidth supplies the plan width input to the PostgresPlanNodeMetric contract.
	PlanWidth int64 `json:"plan_width,omitempty"`
	// ActualRows records rows actually emitted by the plan node.
	ActualRows int64 `json:"actual_rows,omitempty"`
	// ActualLoops records how many times the PostgreSQL plan node executed.
	ActualLoops int64 `json:"actual_loops,omitempty"`
	// RowsRemovedByFilter records rows PostgreSQL reports as rejected by this node's filter.
	RowsRemovedByFilter int64 `json:"rows_removed_by_filter,omitempty"`
	// ActualTotalMS records total observed time for the PostgreSQL plan node.
	ActualTotalMS float64 `json:"actual_total_ms,omitempty"`
	// Buffers contains shared, local, and temporary buffer activity attributed to the plan.
	Buffers Buffers `json:"buffers,omitempty"`
	// Provenance identifies the plan evidence from which this node metric was measured.
	Provenance string `json:"provenance"`
}

// Buffers contains PostgreSQL buffer activity split by storage class and operation.
type Buffers struct {
	// SharedHit records shared PostgreSQL buffer cache hits.
	SharedHit int64 `json:"shared_hit,omitempty"`
	// SharedRead records shared PostgreSQL buffers read by the plan.
	SharedRead int64 `json:"shared_read,omitempty"`
	// SharedDirtied records shared PostgreSQL buffers dirtied by the plan.
	SharedDirtied int64 `json:"shared_dirtied,omitempty"`
	// SharedWritten records shared PostgreSQL buffers written by the plan.
	SharedWritten int64 `json:"shared_written,omitempty"`
	// LocalHit records local PostgreSQL buffer cache hits.
	LocalHit int64 `json:"local_hit,omitempty"`
	// LocalRead records local PostgreSQL buffers read by the plan.
	LocalRead int64 `json:"local_read,omitempty"`
	// LocalDirtied records local PostgreSQL buffers dirtied by the plan.
	LocalDirtied int64 `json:"local_dirtied,omitempty"`
	// LocalWritten records local PostgreSQL buffers written by the plan.
	LocalWritten int64 `json:"local_written,omitempty"`
	// TempRead records temporary PostgreSQL buffers read by the plan.
	TempRead int64 `json:"temp_read,omitempty"`
	// TempWritten records temporary PostgreSQL buffers written by the plan.
	TempWritten int64 `json:"temp_written,omitempty"`
}

// CaseResult records one workload execution with provenance, observations, plan evidence, and latency samples.
type CaseResult struct {
	// Metadata captures build and baseline metadata.
	Metadata testutil.BaselineMetadata `json:"metadata"`
	// Environment captures the environment in which the measurement ran.
	Environment *RunEnvironment `json:"environment,omitempty"`
	// PostgresEnvironment captures PostgreSQL settings required for comparability.
	PostgresEnvironment *PostgresEnvironment `json:"postgres_environment,omitempty"`
	// Fixture captures the fixture identity and cardinality contract.
	Fixture *FixtureMetadata `json:"fixture,omitempty"`
	// Source identifies the source corpus file.
	Source string `json:"source"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// WorkloadSHA256 binds the result to the case declaration and execution mode.
	WorkloadSHA256 string `json:"workload_sha256"`
	// Category groups cases by workload category.
	Category string `json:"category"`
	// Shape describes the workload shape used for selection and comparison.
	Shape WorkloadShape `json:"shape"`
	// ExecutionMode identifies the backend execution mode that produced the case result.
	ExecutionMode ExecutionMode `json:"execution_mode"`
	// Status supplies the status input to the CaseResult contract.
	Status string `json:"status"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params map[string]any `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// ExpectedRowCount sets the required result-row count when known.
	ExpectedRowCount *int64 `json:"expected_row_count,omitempty"`
	// ObservedRows contains stable serialized observations used for correctness comparison.
	ObservedRows []string `json:"observed_rows,omitempty"`
	// RowCount records the number of row count.
	RowCount int64 `json:"row_count,omitempty"`
	// MatchedCount records entities selected by the measured mutation.
	MatchedCount *int64 `json:"matched_count,omitempty"`
	// AffectedCount records entities actually changed by the measured mutation.
	AffectedCount *int64 `json:"affected_count,omitempty"`
	// PostState contains the observed results of post-write validation queries.
	PostState []StateQueryResult `json:"post_state,omitempty"`
	// Stats contains latency statistics for the enclosing result or reference.
	Stats DurationStats `json:"stats,omitempty"`
	// Concurrency contains opt-in worker-count measurements for this case.
	Concurrency []ConcurrencyBlock `json:"concurrency,omitempty"`
	// PostgresReferences contains independent PostgreSQL reference results for the case.
	PostgresReferences []PostgresReferenceResult `json:"postgres_references,omitempty"`
	// ClientWaterfall contains Cypher compilation and client-boundary timing samples.
	ClientWaterfall *ClientWaterfall `json:"client_waterfall,omitempty"`
	// RawPGXWaterfall contains raw PGX boundary timings used for PostgreSQL cost attribution.
	RawPGXWaterfall *PostgresBoundaryWaterfall `json:"raw_pgx_waterfall,omitempty"`
	// RawPGXRoundTrip records legacy aggregate raw-PGX round-trip latency.
	RawPGXRoundTrip *PostgresBoundaryWaterfall `json:"raw_pgx_round_trip,omitempty"`
	// SQL contains the rendered SQL statement.
	SQL string `json:"sql,omitempty"`
	// SQLFingerprint identifies normalized SQL without retaining the statement text.
	SQLFingerprint string `json:"sql_fingerprint,omitempty"`
	// PostgresPlan contains normalized PostgreSQL text-plan lines.
	PostgresPlan []string `json:"postgres_plan,omitempty"`
	// PostgresPlanJSON contains structured PostgreSQL EXPLAIN evidence.
	PostgresPlanJSON json.RawMessage `json:"postgres_plan_json,omitempty"`
	// PostgresMetrics contains normalized PostgreSQL plan resource metrics.
	PostgresMetrics *PostgresPlanMetrics `json:"postgres_metrics,omitempty"`
	// TraversalTelemetry contains lightweight execution identity and optional untimed diagnostic counters.
	TraversalTelemetry *TraversalExecutionTelemetry `json:"traversal_execution_telemetry,omitempty"`
	// Neo4jPlan contains the normalized Neo4j operator tree.
	Neo4jPlan *Neo4jPlanNode `json:"neo4j_plan,omitempty"`
	// Neo4jOperators lists normalized Neo4j operators found in the captured plan.
	Neo4jOperators []string `json:"neo4j_operators,omitempty"`
	// Optimization captures translation optimization and lowering decisions.
	Optimization *translate.OptimizationSummary `json:"optimization,omitempty"`
	// ParseCache reports parse-cache hit and miss statistics for the case.
	ParseCache *pg.ParseCacheStats `json:"parse_cache,omitempty"`
	// Baseline contains the latency comparison with a matching baseline record.
	Baseline *BaselineComparison `json:"baseline,omitempty"`
	// FallbackReason explains why execution used a fallback architecture.
	FallbackReason string `json:"fallback_reason,omitempty"`
	// ExistingGraph selects read-only execution against a pre-existing graph.
	ExistingGraph *ExistingGraphRun `json:"existing_graph,omitempty"`
	// Error supplies the error input to the CaseResult contract.
	Error string `json:"error,omitempty"`
	// StableObservation reports whether ObservedRows contains a backend-independent normalized result.
	StableObservation bool `json:"observation_captured,omitempty"`
}

// StateQueryResult records a post-write validation query's row count and optional scalar value.
type StateQueryResult struct {
	// Name labels the post-write state assertion that produced this result.
	Name string `json:"name"`
	// RowCount records the number of row count.
	RowCount int64 `json:"row_count"`
	// ScalarInt contains the observed scalar value when the state query expects one.
	ScalarInt *int64 `json:"scalar_int,omitempty"`
}

// BaselineComparison compares current median latency with a previously recorded baseline.
type BaselineComparison struct {
	// BaselineMedian supplies the baseline median input to the BaselineComparison contract.
	BaselineMedian time.Duration `json:"baseline_median"`
	// CurrentMedian supplies the current median input to the BaselineComparison contract.
	CurrentMedian time.Duration `json:"current_median"`
	// Change records current latency relative to the selected baseline.
	Change time.Duration `json:"change"`
	// Ratio reports the candidate-to-baseline latency ratio.
	Ratio float64 `json:"ratio"`
}

// validateBackendObservations checks row counts and stable observations across successful backend results.
func validateBackendObservations(records []CaseResult) error {
	// observationKey identifies one dataset, case, backend, and round during observation validation.
	type observationKey struct {
		// dataset names the fixture shared by observations compared across backends.
		dataset string
		// name identifies the workload case compared across backends.
		name string
	}

	postgres := map[observationKey][]string{}
	for _, record := range records {
		if record.ExecutionMode == ModePostgresSQL && record.Status == StatusOK && record.StableObservation && record.ObservedRows != nil {
			postgres[observationKey{
				dataset: record.Dataset,
				name:    record.Name,
			}] = record.ObservedRows
		}
	}

	for _, record := range records {
		if record.ExecutionMode != ModeNeo4j || record.Status != StatusOK || !record.StableObservation || record.ObservedRows == nil {
			continue
		}
		key := observationKey{
			dataset: record.Dataset,
			name:    record.Name,
		}
		if expected, found := postgres[key]; found && !slices.Equal(expected, record.ObservedRows) {
			return fmt.Errorf("backend observations differ for %s/%s: postgres=%v neo4j=%v", record.Dataset, record.Name, expected, record.ObservedRows)
		}
	}

	return nil
}

// newCaseResult initializes workload identity, expectations, observation policy, and successful status for one case.
func newCaseResult(testCase ScaleCase, mode ExecutionMode, params map[string]any) CaseResult {
	return CaseResult{
		Source:           testCase.Source,
		Dataset:          testCase.Dataset,
		Name:             testCase.Name,
		WorkloadSHA256:   scaleCaseWorkloadIdentity(testCase, mode),
		Category:         testCase.Category,
		Shape:            testCase.Shape,
		ExecutionMode:    mode,
		Status:           StatusOK,
		Cypher:           testCase.Cypher,
		Params:           params,
		NodeParams:       testCase.NodeParams,
		NodeListParams:   testCase.NodeListParams,
		ExpectedRowCount: testCase.Expected.RowCount,
		StableObservation: testCase.Expected.ResultKind == "id_rows" ||
			testCase.Expected.ResultKind == "scalar" ||
			(testCase.Expected.ResultKind == "path_set" && (len(testCase.Expected.PathRows) > 0 ||
				testCase.Expected.RowCount != nil && *testCase.Expected.RowCount == 0)),
	}
}

// scaleCaseWorkloadIdentity hashes the logical workload fields that must match across artifacts.
func scaleCaseWorkloadIdentity(testCase ScaleCase, mode ExecutionMode) string {
	payload := struct {
		// Version identifies the serialized schema revision.
		Version int `json:"version"`
		// Source identifies the source corpus file.
		Source string `json:"source"`
		// Backend identifies the execution backend.
		Backend ExecutionMode `json:"backend"`
		// Case contains the complete workload declaration included in the identity digest.
		Case ScaleCase `json:"case"`
	}{
		Version: 1,
		Source:  testCase.Source,
		Backend: mode,
		Case:    testCase,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// attachFixtureMetadata adds fixture metadata to the owning artifact.
func attachFixtureMetadata(record *CaseResult, fixture FixtureMetadata) {
	if record == nil {
		return
	}
	record.Fixture = &fixture
	payload := struct {
		// Version identifies the serialized schema revision.
		Version int `json:"version"`
		// LogicalWorkloadSHA256 identifies query semantics independently of runtime measurements.
		LogicalWorkloadSHA256 string `json:"logical_workload_sha256"`
		// Dataset identifies the fixture dataset.
		Dataset string `json:"dataset"`
		// Checksum binds fixture identity to its canonical logical contents.
		Checksum string `json:"checksum"`
		// NodeCount records logical fixture nodes declared or loaded.
		NodeCount int `json:"node_count"`
		// EdgeCount records logical fixture relationships declared or loaded.
		EdgeCount int `json:"edge_count"`
		// PhysicalNodeCount records physical node rows present in the backend fixture.
		PhysicalNodeCount int64 `json:"physical_node_count,omitempty"`
		// PhysicalEdgeCount records physical relationship rows present in the backend fixture.
		PhysicalEdgeCount int64 `json:"physical_edge_count,omitempty"`
		// Configuration captures the generator parameters that define the fixture shape.
		Configuration string `json:"configuration,omitempty"`
		// Shortest contains expectations derived from a generated shortest-path fixture.
		Shortest *ShortestFixtureExpectations `json:"shortest,omitempty"`
		// FixedSuffixExpansion contains expectations derived from a fixed-suffix expansion fixture.
		FixedSuffixExpansion *FixedSuffixExpansionFixtureExpectations `json:"fixed_suffix_expansion,omitempty"`
		// EndpointSeededExpansion contains expectations derived from an endpoint-seeded expansion fixture.
		EndpointSeededExpansion *EndpointSeededExpansionFixtureExpectations `json:"endpoint_seeded_expansion,omitempty"`
	}{
		Version:                 1,
		LogicalWorkloadSHA256:   record.WorkloadSHA256,
		Dataset:                 fixture.Dataset,
		Checksum:                fixture.Checksum,
		NodeCount:               fixture.NodeCount,
		EdgeCount:               fixture.EdgeCount,
		PhysicalNodeCount:       fixture.PhysicalNodeCount,
		PhysicalEdgeCount:       fixture.PhysicalEdgeCount,
		Configuration:           fixture.Configuration,
		Shortest:                fixture.Shortest,
		FixedSuffixExpansion:    fixture.FixedSuffixExpansion,
		EndpointSeededExpansion: fixture.EndpointSeededExpansion,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		record.WorkloadSHA256 = ""
		return
	}
	digest := sha256.Sum256(raw)
	record.WorkloadSHA256 = hex.EncodeToString(digest[:])
}

// computeDurationStats validates measured durations and derives median, tail, maximum, and labeled sample data.
func computeDurationStats(durations []time.Duration) (DurationStats, error) {
	if len(durations) == 0 {
		return DurationStats{}, fmt.Errorf("duration stats require at least one duration")
	}

	sortedDurations := append([]time.Duration(nil), durations...)
	sort.Slice(sortedDurations, func(i, j int) bool {
		return sortedDurations[i] < sortedDurations[j]
	})

	n := len(sortedDurations)
	p95Index := (95*n+99)/100 - 1
	p99Index := (99*n+99)/100 - 1
	return DurationStats{
		Iterations: n,
		Median:     sortedDurations[n/2],
		P95:        sortedDurations[p95Index],
		P99:        sortedDurations[p99Index],
		P99Gated:   n >= 10_000,
		Max:        sortedDurations[n-1],
		Samples: func() []LatencySample {
			samples := make([]LatencySample, len(durations))
			for idx, duration := range durations {
				samples[idx] = LatencySample{
					Round:          1,
					Iteration:      idx + 1,
					Classification: "warm",
					Duration:       duration,
				}
			}
			return samples
		}(),
	}, nil
}

// labelLatencySamples attaches backend, dataset, and case identity to every latency sample in stats.
func labelLatencySamples(stats *DurationStats, mode ExecutionMode, testCase ScaleCase) {
	for idx := range stats.Samples {
		stats.Samples[idx].Backend = mode
		stats.Samples[idx].Case = testCase.Name
		stats.Samples[idx].Dataset = testCase.Dataset
	}
}

// setSampleRound assigns a measurement round to every latency sample in stats.
func setSampleRound(stats *DurationStats, round int) {
	for idx := range stats.Samples {
		stats.Samples[idx].Round = round
	}
}

// setSampleRunMetadata copies run, arm, block, and round identity onto every latency sample in stats.
func setSampleRunMetadata(stats *DurationStats, environment RunEnvironment) {
	for idx := range stats.Samples {
		stats.Samples[idx].Round = environment.Round
		stats.Samples[idx].Block = environment.Block
		stats.Samples[idx].Arm = environment.Arm
		stats.Samples[idx].ArmOrder = environment.ArmOrder
		stats.Samples[idx].RunUUID = environment.RunUUID
	}
}

// setSampleTraversalRuntimeMetadata binds every timed sample to the singular
// invocation-local replay outcome obtained for the same case, parameters, SQL,
// and physical session. This supports diagnostics but deliberately does not
// claim per-timed-invocation attribution; promotion gates require the stronger
// "timed_invocation" attestation.
func setSampleTraversalRuntimeMetadata(stats *DurationStats, telemetry *TraversalExecutionTelemetry) {
	if stats == nil || telemetry == nil {
		return
	}
	for idx := range stats.Samples {
		if stats.Samples[idx].RuntimeAttestation == "timed_invocation" {
			continue
		}
		stats.Samples[idx].RequestedIdentity = telemetry.Summary.RequestedIdentity
		stats.Samples[idx].RuntimeIdentity = telemetry.Summary.RuntimeIdentity
		stats.Samples[idx].RuntimeBranch = telemetry.Summary.RuntimeBranch
		stats.Samples[idx].FallbackExecuted = telemetry.Summary.FallbackExecuted
		stats.Samples[idx].RuntimeAttestation = "same_case_invocation_local_replay"
	}
}

// setCaseRunMetadata assigns case run metadata across the supplied records.
func setCaseRunMetadata(record *CaseResult, metadata testutil.BaselineMetadata, environment RunEnvironment) {
	if record == nil {
		return
	}
	record.Metadata = metadata
	record.Environment = &environment
	setSampleRunMetadata(&record.Stats, environment)
	for idx := range record.PostgresReferences {
		setSampleRunMetadata(&record.PostgresReferences[idx].Stats, environment)
	}
}

// applyRowExpectation marks a successful result as mismatched when its row count violates the declared expectation.
func applyRowExpectation(result *CaseResult) {
	if result.ExpectedRowCount != nil && result.RowCount != *result.ExpectedRowCount {
		result.Status = StatusRowMismatch
		result.Error = fmt.Sprintf("expected %d rows, got %d", *result.ExpectedRowCount, result.RowCount)
	}
}

// writeJSONLFile writes records to standard output or replaces the requested JSON Lines artifact.
func writeJSONLFile(path string, records []CaseResult) (err error) {
	if path == "" {
		return writeJSONL(os.Stdout, records)
	}

	if err := ensureOutputDir(path); err != nil {
		return err
	}

	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	return writeJSONL(output, records)
}

// appendJSONLFile validates compatibility with existing records before appending new JSON Lines entries.
func appendJSONLFile(path string, records []CaseResult) (err error) {
	if path == "" {
		return errors.New("append JSONL path must not be empty")
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}

	if existing, readErr := readJSONLFile(path); readErr == nil {
		if err := validateJSONLAppend(existing, records); err != nil {
			return err
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}

	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	return writeJSONL(output, records)
}

// validateJSONLAppend ensures appended records share run identity and do not duplicate case rounds.
func validateJSONLAppend(existing, appended []CaseResult) error {
	if len(existing) == 0 || len(appended) == 0 {
		return nil
	}

	left, right := existing[0].Environment, appended[0].Environment
	if left == nil || right == nil {
		return errors.New("append JSONL requires run environment metadata")
	}
	if left.RunUUID != right.RunUUID || left.Arm != right.Arm || left.BinarySHA256 != right.BinarySHA256 || left.DirtyDiffSHA256 != right.DirtyDiffSHA256 {
		return fmt.Errorf("append JSONL run identity mismatch: existing run=%q arm=%q binary=%q diff=%q, appended run=%q arm=%q binary=%q diff=%q",
			left.RunUUID, left.Arm, left.BinarySHA256, left.DirtyDiffSHA256,
			right.RunUUID, right.Arm, right.BinarySHA256, right.DirtyDiffSHA256)
	}

	// recordKey identifies one run, dataset, case, mode, and round during append validation.
	type recordKey struct {
		// dataset names the fixture component of the append-deduplication key.
		dataset string
		// name identifies the workload case within its dataset.
		name string
		// mode separates records for different execution backends within the same round.
		mode ExecutionMode
		// round identifies the measurement round used to balance execution order.
		round int
	}
	seen := make(map[recordKey]struct{}, len(existing))
	for _, record := range existing {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		seen[recordKey{
			dataset: record.Dataset,
			name:    record.Name,
			mode:    record.ExecutionMode,
			round:   round,
		}] = struct{}{}
	}
	for _, record := range appended {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		key := recordKey{
			dataset: record.Dataset,
			name:    record.Name,
			mode:    record.ExecutionMode,
			round:   round,
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("append JSONL duplicate record for %s/%s/%s round %d", key.dataset, key.name, key.mode, key.round)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// writeJSONL encodes each case result as one JSON Lines record in input order.
func writeJSONL(w io.Writer, records []CaseResult) error {
	encoder := json.NewEncoder(w)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}

	return nil
}

// readJSONLFile reads JSON Lines file and propagates I/O or decoding failures.
func readJSONLFile(path string) ([]CaseResult, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	var (
		decoder = json.NewDecoder(input)
		records []CaseResult
	)

	for {
		var record CaseResult
		if err := decoder.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}
		normalizeHistoricalReferences(&record)

		records = append(records, record)
	}

	return records, nil
}

// normalizeHistoricalReferences canonicalizes historical references for stable comparison.
func normalizeHistoricalReferences(record *CaseResult) {
	for idx := range record.PostgresReferences {
		reference := &record.PostgresReferences[idx]
		if reference.SchemaVersion != 0 {
			continue
		}
		reference.SchemaVersion = 1
		switch reference.Name {
		case "complete_reference_s1_array_cte":
			reference.LegacyName = reference.Name
			reference.Name = "s3_unidirectional_trail_cte"
			reference.Architecture = "SP-S3-U-NE"
			reference.ImplementationID = "inline_recursive_cte_unidirectional_v1"
		case "candidate_s2_bidirectional_cte":
			reference.LegacyName = reference.Name
			reference.Name = "s3_bidirectional_trail_cte"
			reference.Architecture = "SP-S3-B"
			reference.ImplementationID = "inline_recursive_cte_bidirectional_trails_v1"
		}
		if reference.StateShape == "" {
			reference.StateShape = "legacy_unspecified"
		}
		if reference.ObservationShape == "" {
			reference.ObservationShape = reference.Boundary
		}
		if reference.SemanticValidation == "" {
			reference.SemanticValidation = "legacy_row_count_only"
			if !reference.FullComparator {
				reference.SemanticValidation = "row_count_stability"
			}
		}
	}
}

// ensureOutputDir creates the parent directory needed for an output file.
func ensureOutputDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}

	return os.MkdirAll(dir, 0o755)
}

// applyBaseline attaches median latency deltas and ratios from matching baseline records.
func applyBaseline(path string, records []CaseResult) error {
	baseline, err := readJSONLFile(path)
	if err != nil {
		return err
	}

	byKey := make(map[string]CaseResult, len(baseline))
	for _, record := range baseline {
		byKey[resultKey(record.Dataset, record.Name, record.ExecutionMode)] = record
	}

	for idx := range records {
		record := &records[idx]
		previous, found := byKey[resultKey(record.Dataset, record.Name, record.ExecutionMode)]
		if !found || previous.Stats.Iterations == 0 || record.Stats.Iterations == 0 || previous.Stats.Median == 0 {
			continue
		}

		record.Baseline = &BaselineComparison{
			BaselineMedian: previous.Stats.Median,
			CurrentMedian:  record.Stats.Median,
			Change:         record.Stats.Median - previous.Stats.Median,
			Ratio:          float64(record.Stats.Median) / float64(previous.Stats.Median),
		}
	}

	return nil
}

// resultKey joins result identity fields into the append-validation key.
func resultKey(dataset, name string, mode ExecutionMode) string {
	return dataset + "\x00" + name + "\x00" + string(mode)
}
