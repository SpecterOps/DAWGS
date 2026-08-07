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
	StatusOK             = "ok"
	StatusRowMismatch    = "row_mismatch"
	StatusError          = "error"
	StatusNotImplemented = "not_implemented"
)

type DurationStats struct {
	Iterations       int             `json:"iterations"`
	WarmupIterations int             `json:"warmup_iterations"`
	Median           time.Duration   `json:"median"`
	P95              time.Duration   `json:"p95"`
	P99              time.Duration   `json:"p99"`
	P99Gated         bool            `json:"p99_gated"`
	Max              time.Duration   `json:"max"`
	Samples          []LatencySample `json:"samples,omitempty"`
}

type LatencySample struct {
	Round          int           `json:"round"`
	Block          int           `json:"block,omitempty"`
	Arm            string        `json:"arm,omitempty"`
	ArmOrder       int           `json:"arm_order,omitempty"`
	RunUUID        string        `json:"run_uuid,omitempty"`
	Iteration      int           `json:"iteration"`
	Case           string        `json:"case"`
	Dataset        string        `json:"dataset"`
	Backend        ExecutionMode `json:"backend"`
	ConnectionID   string        `json:"connection_id,omitempty"`
	Classification string        `json:"classification"`
	Duration       time.Duration `json:"duration"`
}

type ConcurrencySample struct {
	Worker         int           `json:"worker"`
	Iteration      int           `json:"iteration"`
	ConnectionID   string        `json:"connection_id"`
	Classification string        `json:"classification"`
	PoolWait       time.Duration `json:"pool_wait"`
	Transaction    time.Duration `json:"transaction_setup"`
	ExecuteDrain   time.Duration `json:"execute_decode_drain"`
	Total          time.Duration `json:"total"`
}

type ConcurrencyBlock struct {
	Concurrency int                 `json:"concurrency"`
	PoolSize    int                 `json:"pool_size"`
	Operations  int                 `json:"operations"`
	Wall        time.Duration       `json:"wall"`
	QPS         float64             `json:"qps"`
	Samples     []ConcurrencySample `json:"samples"`
}

type PostgresReferenceResult struct {
	SchemaVersion      int                  `json:"schema_version"`
	Name               string               `json:"name"`
	LegacyName         string               `json:"legacy_name,omitempty"`
	Architecture       string               `json:"architecture"`
	ImplementationID   string               `json:"implementation_id"`
	StateShape         string               `json:"state_shape"`
	ObservationShape   string               `json:"observation_shape"`
	SemanticValidation string               `json:"semantic_validation"`
	Boundary           string               `json:"boundary"`
	TimingBoundary     string               `json:"timing_boundary"`
	FullComparator     bool                 `json:"full_comparator"`
	MeasurementOrder   int                  `json:"measurement_order,omitempty"`
	AAAliasOf          string               `json:"aa_alias_of,omitempty"`
	SQL                string               `json:"sql"`
	SQLFingerprint     string               `json:"sql_fingerprint"`
	RowCount           int64                `json:"row_count"`
	ObservedRows       []string             `json:"observed_rows,omitempty"`
	Stats              DurationStats        `json:"stats"`
	PostgresPlan       []string             `json:"postgres_plan,omitempty"`
	PostgresPlanJSON   json.RawMessage      `json:"postgres_plan_json,omitempty"`
	PostgresMetrics    *PostgresPlanMetrics `json:"postgres_metrics,omitempty"`
}

type CompileSample struct {
	Iteration                  int           `json:"iteration"`
	Parse                      time.Duration `json:"parse"`
	Optimize                   time.Duration `json:"optimize"`
	TranslateIncludingOptimize time.Duration `json:"translate_including_optimize"`
	Render                     time.Duration `json:"render"`
	Total                      time.Duration `json:"total"`
	Allocations                uint64        `json:"allocations"`
	AllocatedBytes             uint64        `json:"allocated_bytes"`
}

type ClientWaterfall struct {
	IntervalsOverlap bool            `json:"intervals_overlap"`
	Notes            string          `json:"notes"`
	Samples          []CompileSample `json:"samples"`
}

type BoundarySample struct {
	Iteration      int           `json:"iteration"`
	PoolWait       time.Duration `json:"pool_wait"`
	Transaction    time.Duration `json:"transaction_setup"`
	BindPrepare    time.Duration `json:"bind_prepare"`
	FirstRow       time.Duration `json:"first_row"`
	AllRowsDecode  time.Duration `json:"all_rows_decode"`
	DrainClose     time.Duration `json:"drain_close"`
	Total          time.Duration `json:"total"`
	Rows           int64         `json:"rows"`
	Allocations    uint64        `json:"allocations"`
	AllocatedBytes uint64        `json:"allocated_bytes"`
}

type PostgresBoundaryWaterfall struct {
	Boundary         string           `json:"boundary"`
	SQLFingerprint   string           `json:"sql_fingerprint"`
	WarmupIterations int              `json:"warmup_iterations"`
	MeasurementOrder int              `json:"measurement_order,omitempty"`
	Samples          []BoundarySample `json:"samples"`
}

type PostgresPlanMetrics struct {
	PlanningMS          *float64                 `json:"planning_ms,omitempty"`
	ExecutionMS         *float64                 `json:"execution_ms,omitempty"`
	Buffers             Buffers                  `json:"buffers,omitempty"`
	TempFiles           int64                    `json:"temp_files,omitempty"`
	TempBytes           int64                    `json:"temp_bytes,omitempty"`
	WALRecords          int64                    `json:"wal_records,omitempty"`
	WALBytes            int64                    `json:"wal_bytes,omitempty"`
	RootRows            int64                    `json:"root_rows,omitempty"`
	RecursiveRows       int64                    `json:"recursive_rows,omitempty"`
	RecursiveLoops      int64                    `json:"recursive_loops,omitempty"`
	FrontierRows        int64                    `json:"frontier_rows,omitempty"`
	WitnessRows         int64                    `json:"witness_rows,omitempty"`
	MeetingRows         int64                    `json:"meeting_rows,omitempty"`
	HydrationRows       int64                    `json:"hydration_rows,omitempty"`
	ForwardEdgeProbes   int64                    `json:"forward_edge_probes,omitempty"`
	ReverseEdgeProbes   int64                    `json:"reverse_edge_probes,omitempty"`
	RootLookupLoops     int64                    `json:"root_lookup_loops,omitempty"`
	BoundaryLookupLoops int64                    `json:"boundary_lookup_loops,omitempty"`
	HydrationLoops      int64                    `json:"hydration_loops,omitempty"`
	PlanNodes           []PostgresPlanNodeMetric `json:"plan_nodes,omitempty"`
	Provenance          map[string]string        `json:"provenance,omitempty"`
}

type PostgresPlanNodeMetric struct {
	NodeType           string  `json:"node_type"`
	ParentRelationship string  `json:"parent_relationship,omitempty"`
	CTEName            string  `json:"cte_name,omitempty"`
	RelationName       string  `json:"relation_name,omitempty"`
	Alias              string  `json:"alias,omitempty"`
	IndexName          string  `json:"index_name,omitempty"`
	PlanRows           int64   `json:"plan_rows,omitempty"`
	PlanWidth          int64   `json:"plan_width,omitempty"`
	ActualRows         int64   `json:"actual_rows,omitempty"`
	ActualLoops        int64   `json:"actual_loops,omitempty"`
	ActualTotalMS      float64 `json:"actual_total_ms,omitempty"`
	Buffers            Buffers `json:"buffers,omitempty"`
	Provenance         string  `json:"provenance"`
}

type Buffers struct {
	SharedHit     int64 `json:"shared_hit,omitempty"`
	SharedRead    int64 `json:"shared_read,omitempty"`
	SharedDirtied int64 `json:"shared_dirtied,omitempty"`
	SharedWritten int64 `json:"shared_written,omitempty"`
	LocalHit      int64 `json:"local_hit,omitempty"`
	LocalRead     int64 `json:"local_read,omitempty"`
	LocalDirtied  int64 `json:"local_dirtied,omitempty"`
	LocalWritten  int64 `json:"local_written,omitempty"`
	TempRead      int64 `json:"temp_read,omitempty"`
	TempWritten   int64 `json:"temp_written,omitempty"`
}

type CaseResult struct {
	Metadata            testutil.BaselineMetadata      `json:"metadata"`
	Environment         *RunEnvironment                `json:"environment,omitempty"`
	PostgresEnvironment *PostgresEnvironment           `json:"postgres_environment,omitempty"`
	Fixture             *FixtureMetadata               `json:"fixture,omitempty"`
	Source              string                         `json:"source"`
	Dataset             string                         `json:"dataset"`
	Name                string                         `json:"name"`
	Category            string                         `json:"category"`
	Shape               WorkloadShape                  `json:"shape"`
	ExecutionMode       ExecutionMode                  `json:"execution_mode"`
	Status              string                         `json:"status"`
	Cypher              string                         `json:"cypher"`
	Params              map[string]any                 `json:"params,omitempty"`
	NodeParams          map[string]string              `json:"node_params,omitempty"`
	NodeListParams      map[string][]string            `json:"node_list_params,omitempty"`
	ExpectedRowCount    *int64                         `json:"expected_row_count,omitempty"`
	ObservedRows        []string                       `json:"observed_rows,omitempty"`
	RowCount            int64                          `json:"row_count,omitempty"`
	MatchedCount        *int64                         `json:"matched_count,omitempty"`
	AffectedCount       *int64                         `json:"affected_count,omitempty"`
	PostState           []StateQueryResult             `json:"post_state,omitempty"`
	Stats               DurationStats                  `json:"stats,omitempty"`
	Concurrency         []ConcurrencyBlock             `json:"concurrency,omitempty"`
	PostgresReferences  []PostgresReferenceResult      `json:"postgres_references,omitempty"`
	ClientWaterfall     *ClientWaterfall               `json:"client_waterfall,omitempty"`
	RawPGXWaterfall     *PostgresBoundaryWaterfall     `json:"raw_pgx_waterfall,omitempty"`
	RawPGXRoundTrip     *PostgresBoundaryWaterfall     `json:"raw_pgx_round_trip,omitempty"`
	SQL                 string                         `json:"sql,omitempty"`
	SQLFingerprint      string                         `json:"sql_fingerprint,omitempty"`
	PostgresPlan        []string                       `json:"postgres_plan,omitempty"`
	PostgresPlanJSON    json.RawMessage                `json:"postgres_plan_json,omitempty"`
	PostgresMetrics     *PostgresPlanMetrics           `json:"postgres_metrics,omitempty"`
	Neo4jPlan           *Neo4jPlanNode                 `json:"neo4j_plan,omitempty"`
	Neo4jOperators      []string                       `json:"neo4j_operators,omitempty"`
	Optimization        *translate.OptimizationSummary `json:"optimization,omitempty"`
	ParseCache          *pg.ParseCacheStats            `json:"parse_cache,omitempty"`
	Baseline            *BaselineComparison            `json:"baseline,omitempty"`
	FallbackReason      string                         `json:"fallback_reason,omitempty"`
	ExistingGraph       *ExistingGraphRun              `json:"existing_graph,omitempty"`
	Error               string                         `json:"error,omitempty"`
	StableObservation   bool                           `json:"-"`
}

type StateQueryResult struct {
	Name      string `json:"name"`
	RowCount  int64  `json:"row_count"`
	ScalarInt *int64 `json:"scalar_int,omitempty"`
}

type BaselineComparison struct {
	BaselineMedian time.Duration `json:"baseline_median"`
	CurrentMedian  time.Duration `json:"current_median"`
	Change         time.Duration `json:"change"`
	Ratio          float64       `json:"ratio"`
}

func validateBackendObservations(records []CaseResult) error {
	type observationKey struct {
		dataset string
		name    string
	}

	postgres := map[observationKey][]string{}
	for _, record := range records {
		if record.ExecutionMode == ModePostgresSQL && record.Status == StatusOK && record.StableObservation && record.ObservedRows != nil {
			postgres[observationKey{dataset: record.Dataset, name: record.Name}] = record.ObservedRows
		}
	}

	for _, record := range records {
		if record.ExecutionMode != ModeNeo4j || record.Status != StatusOK || !record.StableObservation || record.ObservedRows == nil {
			continue
		}
		key := observationKey{dataset: record.Dataset, name: record.Name}
		if expected, found := postgres[key]; found && !slices.Equal(expected, record.ObservedRows) {
			return fmt.Errorf("backend observations differ for %s/%s: postgres=%v neo4j=%v", record.Dataset, record.Name, expected, record.ObservedRows)
		}
	}

	return nil
}

func newCaseResult(testCase ScaleCase, mode ExecutionMode, params map[string]any) CaseResult {
	return CaseResult{
		Source:           testCase.Source,
		Dataset:          testCase.Dataset,
		Name:             testCase.Name,
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
			(testCase.Expected.ResultKind == "path_set" && len(testCase.Expected.PathRows) > 0),
	}
}

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

func labelLatencySamples(stats *DurationStats, mode ExecutionMode, testCase ScaleCase) {
	for idx := range stats.Samples {
		stats.Samples[idx].Backend = mode
		stats.Samples[idx].Case = testCase.Name
		stats.Samples[idx].Dataset = testCase.Dataset
	}
}

func setSampleRound(stats *DurationStats, round int) {
	for idx := range stats.Samples {
		stats.Samples[idx].Round = round
	}
}

func setSampleRunMetadata(stats *DurationStats, environment RunEnvironment) {
	for idx := range stats.Samples {
		stats.Samples[idx].Round = environment.Round
		stats.Samples[idx].Block = environment.Block
		stats.Samples[idx].Arm = environment.Arm
		stats.Samples[idx].ArmOrder = environment.ArmOrder
		stats.Samples[idx].RunUUID = environment.RunUUID
	}
}

func applyRowExpectation(result *CaseResult) {
	if result.ExpectedRowCount != nil && result.RowCount != *result.ExpectedRowCount {
		result.Status = StatusRowMismatch
		result.Error = fmt.Sprintf("expected %d rows, got %d", *result.ExpectedRowCount, result.RowCount)
	}
}

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

	type recordKey struct {
		dataset string
		name    string
		mode    ExecutionMode
		round   int
	}
	seen := make(map[recordKey]struct{}, len(existing))
	for _, record := range existing {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		seen[recordKey{dataset: record.Dataset, name: record.Name, mode: record.ExecutionMode, round: round}] = struct{}{}
	}
	for _, record := range appended {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		key := recordKey{dataset: record.Dataset, name: record.Name, mode: record.ExecutionMode, round: round}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("append JSONL duplicate record for %s/%s/%s round %d", key.dataset, key.name, key.mode, key.round)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func writeJSONL(w io.Writer, records []CaseResult) error {
	encoder := json.NewEncoder(w)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}

	return nil
}

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

func ensureOutputDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}

	return os.MkdirAll(dir, 0o755)
}

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

func resultKey(dataset, name string, mode ExecutionMode) string {
	return dataset + "\x00" + name + "\x00" + string(mode)
}
