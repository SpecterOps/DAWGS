// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/specterops/dawgs/drivers/pg"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
	"github.com/specterops/dawgs/util/size"
)

const (
	p5AdjacencyFeasibilitySchema   = "p5-adjacency-materialization-feasibility-v1"
	p5AdjacencyFeasibilityProtocol = "benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v1.json"
	p5WarmupIterations             = 1
	p5TimedIterations              = 5
	p5Blocks                       = 4
)

var p5AdjacencyGraphSequence int64

// P5AdjacencyFeasibilityReport is the intentionally non-promotional physical
// evidence artifact for the P5 shadow relation. Its layout separates writes,
// WAL, storage, and raw lookup observations so it cannot be mistaken for a
// Cypher performance qualification.
type P5AdjacencyFeasibilityReport struct {
	Schema              string                       `json:"schema"`
	Status              string                       `json:"status"`
	ProtocolSHA256      string                       `json:"protocol_sha256"`
	SourceArchiveSHA256 string                       `json:"source_archive_sha256"`
	Environment         RunEnvironment               `json:"environment"`
	Postgres            PostgresEnvironment          `json:"postgres"`
	Conditions          []P5AdjacencyConditionResult `json:"conditions"`
	Calibrations        []P5AdjacencyCalibration     `json:"committed_calibrations"`
	Cancellation        P5AdjacencyCancellation      `json:"cancellation_and_pool_reuse"`
	Passed              bool                         `json:"passed"`
	NoBudgetDecision    bool                         `json:"no_budget_decision"`
	NoCypherReadPath    bool                         `json:"no_cypher_read_path"`
}

// P5AdjacencyConditionResult records one counterbalanced fixture condition.
type P5AdjacencyConditionResult struct {
	Block         int                               `json:"block"`
	Condition     string                            `json:"condition"`
	Targets       int                               `json:"targets"`
	GraphID       int32                             `json:"graph_id"`
	SetupWAL      int64                             `json:"setup_wal_bytes"`
	BaseStorage   P5AdjacencyRelationSize           `json:"base_edge_storage"`
	ShadowStorage *P5AdjacencyRelationSize          `json:"shadow_storage,omitempty"`
	Operations    []P5AdjacencyOperationMeasurement `json:"operations"`
	ReadProbes    []P5AdjacencyReadProbe            `json:"read_probes"`
}

// P5AdjacencyRelationSize reports heap and index bytes for one physical relation.
type P5AdjacencyRelationSize struct {
	Relation   string `json:"relation"`
	HeapBytes  int64  `json:"heap_bytes"`
	IndexBytes int64  `json:"index_bytes"`
	TotalBytes int64  `json:"total_bytes"`
}

// P5AdjacencyOperationMeasurement records one rollback-only timing series.
type P5AdjacencyOperationMeasurement struct {
	Operation string                         `json:"operation"`
	Warmup    P5AdjacencyLatencySample       `json:"warmup"`
	Samples   []P5AdjacencyLatencySample     `json:"samples"`
	Median    time.Duration                  `json:"median"`
	P95       time.Duration                  `json:"p95"`
	Observed  P5AdjacencyMutationObservation `json:"observed"`
}

// P5AdjacencyLatencySample supplies one rollback-only physical write timing.
type P5AdjacencyLatencySample struct {
	Iteration int           `json:"iteration"`
	Duration  time.Duration `json:"duration"`
}

// P5AdjacencyMutationObservation records state transition evidence outside
// the timed statement itself.
type P5AdjacencyMutationObservation struct {
	AffectedRows            int64 `json:"affected_rows"`
	BaseEdgesBefore         int64 `json:"base_edges_before"`
	BaseEdgesAfter          int64 `json:"base_edges_after"`
	ShadowRowsBefore        int64 `json:"shadow_rows_before,omitempty"`
	ShadowRowsAfter         int64 `json:"shadow_rows_after,omitempty"`
	MaintenanceRowsChanged  int64 `json:"maintenance_rows_changed,omitempty"`
	PropertyRowsUnchanged   bool  `json:"property_rows_unchanged"`
	RollbackRestoredFixture bool  `json:"rollback_restored_fixture"`
}

// P5AdjacencyReadProbe preserves a raw SQL lookup, its result cardinality,
// elapsed time, and a PostgreSQL plan containing buffer observations.
type P5AdjacencyReadProbe struct {
	Relation          string          `json:"relation"`
	Duration          time.Duration   `json:"duration"`
	ResultCardinality int64           `json:"result_cardinality"`
	Plan              json.RawMessage `json:"plan"`
}

// P5AdjacencyCalibration records one committed setup or mutation calibration.
type P5AdjacencyCalibration struct {
	Condition         string                         `json:"condition"`
	Targets           int                            `json:"targets"`
	Operation         string                         `json:"operation"`
	GraphID           int32                          `json:"graph_id"`
	SetupWAL          int64                          `json:"setup_wal_lsn_delta_bytes"`
	MutationWALLSN    int64                          `json:"mutation_wal_lsn_delta_bytes"`
	StatementWALBytes int64                          `json:"statement_wal_bytes"`
	WALQuiescent      bool                           `json:"wal_quiescent"`
	Duration          time.Duration                  `json:"duration"`
	Observed          P5AdjacencyMutationObservation `json:"observed"`
}

// P5AdjacencyCancellation records recovery after a cancelled write. pgx may
// close the cancelled connection, so the replay PID is recorded explicitly.
type P5AdjacencyCancellation struct {
	Ran                 bool   `json:"ran"`
	CancelledBackendPID uint32 `json:"cancelled_backend_pid"`
	ReplayBackendPID    uint32 `json:"replay_backend_pid"`
	PoolReuseSucceeded  bool   `json:"pool_reuse_succeeded"`
	RollbackObserved    bool   `json:"rollback_observed"`
}

type p5AdjacencyGraph struct {
	db      graph.Database
	pool    *pgxpool.Pool
	graphID int32
}

type p5AdjacencyFixture struct {
	graphID       int32
	rootID        int64
	targetIDs     []int64
	deleteEdgeIDs []int64
	updateEdgeIDs []int64
	deleteKindID  int16
	updateKindID  int16
	createKindID  int16
	nodes         int64
	edges         int64
}

type p5AdjacencyRowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// runP5AdjacencyFeasibilityCapture performs the frozen physical study. It is
// deliberately separate from the normal corpus runner and does not load,
// translate, or execute any Cypher query.
func runP5AdjacencyFeasibilityCapture(ctx context.Context, cfg config, connection string, args []string) (_ P5AdjacencyFeasibilityReport, err error) {
	if err := databaseguard.ValidateEnvironment(connection); err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}
	if cfg.PoolSize != 1 {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("P5 adjacency feasibility capture requires pool-size 1")
	}
	if current := workingTreeSHA256(); current != cleanWorkingTreeSHA256() {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("P5 adjacency feasibility capture requires a clean source tree")
	}
	protocolSHA, err := fileSHA256(p5AdjacencyFeasibilityProtocol)
	if err != nil {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("checksum P5 protocol: %w", err)
	}
	sourceArchive, err := sourceArchiveSHA256()
	if err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}

	startedAt := time.Now().UTC()
	environment := resolveRunEnvironment(cfg, args, SelectionManifest{}, startedAt, startedAt)
	environment.Selection = nil
	environment.Protocol = p5AdjacencyFeasibilitySchema
	environment.WarmupIterations = p5WarmupIterations
	environment.PoolSize = 1

	control, err := openP5AdjacencyGraph(ctx, connection)
	if err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}
	defer func() {
		cleanupErr := dropP5AdjacencyShadow(ctx, control.db)
		closeErr := control.db.Close(ctx)
		if err == nil && cleanupErr != nil {
			err = fmt.Errorf("remove P5 adjacency shadow after capture: %w", cleanupErr)
		}
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close P5 adjacency control graph: %w", closeErr)
		}
	}()
	if err := dropP5AdjacencyShadow(ctx, control.db); err != nil {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("reset P5 adjacency shadow: %w", err)
	}
	if err := cleanupP5AdjacencyOwnedGraphs(ctx, control); err != nil {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("reset abandoned P5 adjacency graphs: %w", err)
	}
	if err := disableP5AdjacencyAutovacuum(ctx, control.pool, false); err != nil {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("disable autovacuum for P5 WAL attribution: %w", err)
	}
	defer func() {
		if restoreErr := restoreP5AdjacencyAutovacuum(ctx, control.pool); err == nil && restoreErr != nil {
			err = fmt.Errorf("restore autovacuum after P5 capture: %w", restoreErr)
		}
	}()

	postgres, err := captureP5PostgresEnvironment(ctx, control.pool)
	if err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}
	report := P5AdjacencyFeasibilityReport{
		Schema:              p5AdjacencyFeasibilitySchema,
		Status:              "physical_feasibility_capture_no_budget_decision",
		ProtocolSHA256:      protocolSHA,
		SourceArchiveSHA256: sourceArchive,
		Environment:         environment,
		Postgres:            postgres,
		NoBudgetDecision:    true,
		NoCypherReadPath:    true,
	}

	conditions := []string{"base", "shadow"}
	for block := 1; block <= p5Blocks; block++ {
		ordered := append([]string(nil), conditions...)
		if block%2 == 0 {
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		for _, condition := range ordered {
			shadow := condition == "shadow"
			if !shadow {
				if err := dropP5AdjacencyShadow(ctx, control.db); err != nil {
					return P5AdjacencyFeasibilityReport{}, fmt.Errorf("prepare base block %d: %w", block, err)
				}
			}
			for _, targets := range []int{1, 1_000, 2_000} {
				result, cancellation, runErr := runP5AdjacencyTimedCondition(ctx, connection, block, condition, targets, report.Cancellation.Ran)
				if runErr != nil {
					return P5AdjacencyFeasibilityReport{}, runErr
				}
				report.Conditions = append(report.Conditions, result)
				if cancellation.Ran {
					report.Cancellation = cancellation
				}
			}
		}
	}

	for _, condition := range conditions {
		shadow := condition == "shadow"
		if !shadow {
			if err := dropP5AdjacencyShadow(ctx, control.db); err != nil {
				return P5AdjacencyFeasibilityReport{}, fmt.Errorf("prepare base calibrations: %w", err)
			}
		}
		for _, targets := range []int{1, 1_000, 2_000} {
			for _, operation := range p5AdjacencyOperations() {
				calibration, runErr := runP5AdjacencyCalibration(ctx, connection, condition, targets, operation)
				if runErr != nil {
					return P5AdjacencyFeasibilityReport{}, runErr
				}
				report.Calibrations = append(report.Calibrations, calibration)
			}
		}
	}
	if !report.Cancellation.Ran || !report.Cancellation.PoolReuseSucceeded {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("P5 cancellation and pool reuse proof did not complete")
	}
	if err := dropP5AdjacencyShadow(ctx, control.db); err != nil {
		return P5AdjacencyFeasibilityReport{}, fmt.Errorf("remove P5 adjacency shadow before artifact write: %w", err)
	}
	if err := deleteP5AdjacencyGraph(ctx, control, false); err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}

	report.Environment.EndedAt = time.Now().UTC()
	report.Passed = true
	if err := writeP5AdjacencyFeasibilityReport(cfg.P5AdjacencyFeasibilityOutput, report); err != nil {
		return P5AdjacencyFeasibilityReport{}, err
	}
	return report, nil
}

func p5AdjacencyOperations() []string {
	return []string{
		"batch_relationship_create",
		"relationship_upsert_conflict_merge",
		"relationship_property_only_update",
		"batched_relationship_delete",
		"batched_node_delete_cascade",
		"graph_clear_reload",
		"graph_drop",
	}
}

func runP5AdjacencyTimedCondition(ctx context.Context, connection string, block int, condition string, targets int, cancellationComplete bool) (P5AdjacencyConditionResult, P5AdjacencyCancellation, error) {
	shadow := condition == "shadow"
	graphState, err := openP5AdjacencyGraph(ctx, connection)
	if err != nil {
		return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
	}
	defer graphState.db.Close(ctx)
	fixture, setupWAL, err := setupP5AdjacencyFixture(ctx, graphState, targets, shadow)
	if err != nil {
		return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
	}
	baseStorage, err := p5AdjacencyRelationSize(ctx, graphState.pool, fmt.Sprintf("edge_%d", fixture.graphID))
	if err != nil {
		return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
	}
	result := P5AdjacencyConditionResult{
		Block:       block,
		Condition:   condition,
		Targets:     targets,
		GraphID:     fixture.graphID,
		SetupWAL:    setupWAL,
		BaseStorage: baseStorage,
	}
	if shadow {
		shadowStorage, err := p5AdjacencyRelationSize(ctx, graphState.pool, fmt.Sprintf("p5_adjacency_v1_%d", fixture.graphID))
		if err != nil {
			return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
		}
		result.ShadowStorage = &shadowStorage
	}

	for _, operation := range p5AdjacencyOperations() {
		measurement, err := measureP5AdjacencyOperation(ctx, graphState, fixture, shadow, operation)
		if err != nil {
			return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
		}
		result.Operations = append(result.Operations, measurement)
	}
	baseProbe, err := p5AdjacencyReadProbe(ctx, graphState, fixture, false)
	if err != nil {
		return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
	}
	result.ReadProbes = append(result.ReadProbes, baseProbe)
	if shadow {
		shadowProbe, err := p5AdjacencyReadProbe(ctx, graphState, fixture, true)
		if err != nil {
			return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
		}
		result.ReadProbes = append(result.ReadProbes, shadowProbe)
	}

	cancellation := P5AdjacencyCancellation{}
	if shadow && !cancellationComplete {
		cancellation, err = p5AdjacencyCancellationAndReuse(ctx, graphState, fixture)
		if err != nil {
			return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
		}
	}
	if err := deleteP5AdjacencyGraph(ctx, graphState, shadow); err != nil {
		return P5AdjacencyConditionResult{}, P5AdjacencyCancellation{}, err
	}
	return result, cancellation, nil
}

func runP5AdjacencyCalibration(ctx context.Context, connection, condition string, targets int, operation string) (P5AdjacencyCalibration, error) {
	shadow := condition == "shadow"
	graphState, err := openP5AdjacencyGraph(ctx, connection)
	if err != nil {
		return P5AdjacencyCalibration{}, err
	}
	defer graphState.db.Close(ctx)
	fixture, setupWAL, err := setupP5AdjacencyFixture(ctx, graphState, targets, shadow)
	if err != nil {
		return P5AdjacencyCalibration{}, err
	}
	if err := waitForP5WALQuiescence(ctx, graphState.pool); err != nil {
		return P5AdjacencyCalibration{}, err
	}
	beforeWAL, err := p5AdjacencyWALPosition(ctx, graphState.pool)
	if err != nil {
		return P5AdjacencyCalibration{}, err
	}
	tx, err := graphState.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return P5AdjacencyCalibration{}, err
	}
	observation, duration, statementWAL, runErr := runP5AdjacencyMutationWithWAL(ctx, tx, fixture, shadow, operation)
	if runErr == nil {
		runErr = tx.Commit(ctx)
	} else {
		_ = tx.Rollback(ctx)
	}
	if runErr != nil {
		return P5AdjacencyCalibration{}, fmt.Errorf("committed %s calibration: %w", operation, runErr)
	}
	if err := waitForP5WALQuiescence(ctx, graphState.pool); err != nil {
		return P5AdjacencyCalibration{}, err
	}
	afterWAL, err := p5AdjacencyWALPosition(ctx, graphState.pool)
	if err != nil {
		return P5AdjacencyCalibration{}, err
	}
	if afterWAL-beforeWAL < statementWAL {
		return P5AdjacencyCalibration{}, fmt.Errorf("%s statement WAL bytes exceed its LSN delta", operation)
	}
	if shadow {
		if err := assertP5AdjacencyExact(ctx, graphState.pool, fixture.graphID); err != nil {
			return P5AdjacencyCalibration{}, err
		}
	} else if err := assertP5AdjacencyAbsent(ctx, graphState.pool); err != nil {
		return P5AdjacencyCalibration{}, err
	}
	if err := deleteP5AdjacencyGraph(ctx, graphState, shadow); err != nil {
		return P5AdjacencyCalibration{}, err
	}
	return P5AdjacencyCalibration{
		Condition:         condition,
		Targets:           targets,
		Operation:         operation,
		GraphID:           fixture.graphID,
		SetupWAL:          setupWAL,
		MutationWALLSN:    afterWAL - beforeWAL,
		StatementWALBytes: statementWAL,
		WALQuiescent:      true,
		Duration:          duration,
		Observed:          observation,
	}, nil
}

func measureP5AdjacencyOperation(ctx context.Context, graphState *p5AdjacencyGraph, fixture p5AdjacencyFixture, shadow bool, operation string) (P5AdjacencyOperationMeasurement, error) {
	measurement := P5AdjacencyOperationMeasurement{Operation: operation}
	var expected P5AdjacencyMutationObservation
	for iteration := 0; iteration <= p5TimedIterations; iteration++ {
		tx, err := graphState.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return P5AdjacencyOperationMeasurement{}, err
		}
		observation, duration, runErr := runP5AdjacencyMutationTimed(ctx, tx, fixture, shadow, operation)
		rollbackErr := tx.Rollback(ctx)
		if runErr != nil {
			return P5AdjacencyOperationMeasurement{}, runErr
		}
		if rollbackErr != nil {
			return P5AdjacencyOperationMeasurement{}, rollbackErr
		}
		if shadow {
			if err := assertP5AdjacencyExact(ctx, graphState.pool, fixture.graphID); err != nil {
				return P5AdjacencyOperationMeasurement{}, fmt.Errorf("verify rollback %s: %w", operation, err)
			}
		} else if err := assertP5AdjacencyAbsent(ctx, graphState.pool); err != nil {
			return P5AdjacencyOperationMeasurement{}, err
		}
		observation.RollbackRestoredFixture = true
		if iteration == 0 {
			expected = observation
			measurement.Warmup = P5AdjacencyLatencySample{Iteration: iteration, Duration: duration}
			continue
		}
		if !p5AdjacencyObservationEqual(expected, observation) {
			return P5AdjacencyOperationMeasurement{}, fmt.Errorf("%s iteration %d changed state observations", operation, iteration)
		}
		measurement.Samples = append(measurement.Samples, P5AdjacencyLatencySample{Iteration: iteration, Duration: duration})
	}
	measurement.Observed = expected
	durations := make([]time.Duration, 0, len(measurement.Samples))
	for _, sample := range measurement.Samples {
		durations = append(durations, sample.Duration)
	}
	measurement.Median, measurement.P95 = p5AdjacencyQuantiles(durations)
	return measurement, nil
}

func p5AdjacencyObservationEqual(left, right P5AdjacencyMutationObservation) bool {
	return left.AffectedRows == right.AffectedRows &&
		left.BaseEdgesBefore == right.BaseEdgesBefore &&
		left.BaseEdgesAfter == right.BaseEdgesAfter &&
		left.ShadowRowsBefore == right.ShadowRowsBefore &&
		left.ShadowRowsAfter == right.ShadowRowsAfter &&
		left.MaintenanceRowsChanged == right.MaintenanceRowsChanged &&
		left.PropertyRowsUnchanged == right.PropertyRowsUnchanged
}

func p5AdjacencyQuantiles(durations []time.Duration) (time.Duration, time.Duration) {
	values := append([]time.Duration(nil), durations...)
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	median := values[len(values)/2]
	p95 := values[int(math.Ceil(float64(len(values))*0.95))-1]
	return median, p95
}

type p5AdjacencyMutationExecution struct {
	observation       P5AdjacencyMutationObservation
	duration          time.Duration
	statementWALBytes int64
}

func runP5AdjacencyMutationTimed(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture, shadow bool, operation string) (P5AdjacencyMutationObservation, time.Duration, error) {
	execution, err := runP5AdjacencyMutationInternal(ctx, tx, fixture, shadow, operation, false)
	return execution.observation, execution.duration, err
}

func runP5AdjacencyMutationWithWAL(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture, shadow bool, operation string) (P5AdjacencyMutationObservation, time.Duration, int64, error) {
	execution, err := runP5AdjacencyMutationInternal(ctx, tx, fixture, shadow, operation, true)
	return execution.observation, execution.duration, execution.statementWALBytes, err
}

func runP5AdjacencyMutation(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture, shadow bool, operation string) (P5AdjacencyMutationObservation, error) {
	execution, err := runP5AdjacencyMutationInternal(ctx, tx, fixture, shadow, operation, false)
	return execution.observation, err
}

func runP5AdjacencyMutationInternal(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture, shadow bool, operation string, collectStatementWAL bool) (p5AdjacencyMutationExecution, error) {
	if shadow {
		if err := assertP5AdjacencyExact(ctx, tx, fixture.graphID); err != nil {
			return p5AdjacencyMutationExecution{}, err
		}
	} else if err := assertP5AdjacencyAbsent(ctx, tx); err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	observation := P5AdjacencyMutationObservation{}
	if err := tx.QueryRow(ctx, `select count(*) from edge where graph_id = $1`, fixture.graphID).Scan(&observation.BaseEdgesBefore); err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	if shadow {
		if err := tx.QueryRow(ctx, `select count(*) from public.p5_adjacency_v1 where graph_id = $1`, fixture.graphID).Scan(&observation.ShadowRowsBefore); err != nil {
			return p5AdjacencyMutationExecution{}, err
		}
	}
	identityBefore := ""
	if shadow && (operation == "relationship_upsert_conflict_merge" || operation == "relationship_property_only_update") {
		var err error
		identityBefore, err = p5AdjacencyShadowIdentity(ctx, tx, fixture)
		if err != nil {
			return p5AdjacencyMutationExecution{}, err
		}
	}

	statement, arguments, err := p5AdjacencyMutationStatement(fixture, operation)
	if err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	start := time.Now()
	var (
		affected          int64
		statementWALBytes int64
	)
	if collectStatementWAL {
		var plan []byte
		err = tx.QueryRow(ctx, "explain (analyze, wal, format json) "+statement, arguments...).Scan(&plan)
		if err == nil {
			statementWALBytes, err = p5AdjacencyStatementWALBytes(plan)
		}
		if err == nil {
			affected, err = p5AdjacencyExpectedAffected(fixture, operation)
		}
	} else {
		var result pgconn.CommandTag
		result, err = tx.Exec(ctx, statement, arguments...)
		affected = result.RowsAffected()
	}
	duration := time.Since(start)
	if err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	observation.AffectedRows = affected
	if err := tx.QueryRow(ctx, `select count(*) from edge where graph_id = $1`, fixture.graphID).Scan(&observation.BaseEdgesAfter); err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	if err := assertP5AdjacencyMutationPostState(ctx, tx, fixture, operation, observation); err != nil {
		return p5AdjacencyMutationExecution{}, err
	}
	if shadow {
		if err := tx.QueryRow(ctx, `select count(*) from public.p5_adjacency_v1 where graph_id = $1`, fixture.graphID).Scan(&observation.ShadowRowsAfter); err != nil {
			return p5AdjacencyMutationExecution{}, err
		}
		observation.MaintenanceRowsChanged = absInt64(observation.ShadowRowsAfter - observation.ShadowRowsBefore)
		if err := assertP5AdjacencyExact(ctx, tx, fixture.graphID); err != nil {
			return p5AdjacencyMutationExecution{}, err
		}
		if identityBefore != "" {
			identityAfter, err := p5AdjacencyShadowIdentity(ctx, tx, fixture)
			if err != nil {
				return p5AdjacencyMutationExecution{}, err
			}
			observation.PropertyRowsUnchanged = identityBefore == identityAfter
			if !observation.PropertyRowsUnchanged {
				return p5AdjacencyMutationExecution{}, fmt.Errorf("%s rewrote P5 shadow rows", operation)
			}
		}
	}
	return p5AdjacencyMutationExecution{observation: observation, duration: duration, statementWALBytes: statementWALBytes}, nil
}

func p5AdjacencyMutationStatement(fixture p5AdjacencyFixture, operation string) (string, []any, error) {
	switch operation {
	case "batch_relationship_create":
		return `
			insert into edge(graph_id, start_id, end_id, kind_id, properties)
			select $1, $2, target_id, $3, '{"p5_create":true}'::jsonb
			from unnest($4::bigint[]) as targets(target_id)`,
			[]any{fixture.graphID, fixture.rootID, fixture.createKindID, fixture.targetIDs}, nil
	case "relationship_upsert_conflict_merge":
		return `
			insert into edge(graph_id, start_id, end_id, kind_id, properties)
			select $1, $2, target_id, $3, '{"p5_upsert":true}'::jsonb
			from unnest($4::bigint[]) as targets(target_id)
			on conflict (start_id, end_id, kind_id, graph_id) do update
			  set properties = edge.properties || excluded.properties`,
			[]any{fixture.graphID, fixture.rootID, fixture.updateKindID, fixture.targetIDs}, nil
	case "relationship_property_only_update":
		return `update edge set properties = properties || '{"p5_property_only":true}'::jsonb where graph_id = $1 and id = any($2::bigint[])`,
			[]any{fixture.graphID, fixture.updateEdgeIDs}, nil
	case "batched_relationship_delete":
		return `delete from edge where graph_id = $1 and id = any($2::bigint[])`, []any{fixture.graphID, fixture.deleteEdgeIDs}, nil
	case "batched_node_delete_cascade":
		return `delete from node where graph_id = $1 and id = any($2::bigint[])`, []any{fixture.graphID, fixture.targetIDs}, nil
	case "graph_clear_reload":
		return `delete from node where graph_id = $1`, []any{fixture.graphID}, nil
	case "graph_drop":
		return `delete from graph where id = $1`, []any{fixture.graphID}, nil
	default:
		return "", nil, fmt.Errorf("unknown P5 mutation %q", operation)
	}
}

func p5AdjacencyExpectedAffected(fixture p5AdjacencyFixture, operation string) (int64, error) {
	switch operation {
	case "batch_relationship_create", "relationship_upsert_conflict_merge", "relationship_property_only_update", "batched_relationship_delete", "batched_node_delete_cascade":
		return int64(len(fixture.targetIDs)), nil
	case "graph_clear_reload":
		return fixture.nodes, nil
	case "graph_drop":
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown P5 mutation %q", operation)
	}
}

func p5AdjacencyStatementWALBytes(raw []byte) (int64, error) {
	var explain []struct {
		Plan map[string]json.RawMessage `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &explain); err != nil {
		return 0, fmt.Errorf("decode statement WAL plan: %w", err)
	}
	if len(explain) != 1 {
		return 0, fmt.Errorf("statement WAL plan must contain exactly one root")
	}
	value, found := explain[0].Plan["WAL Bytes"]
	if !found {
		return 0, fmt.Errorf("statement WAL plan is missing WAL Bytes")
	}
	var walBytes int64
	if err := json.Unmarshal(value, &walBytes); err != nil {
		return 0, fmt.Errorf("decode statement WAL bytes: %w", err)
	}
	return walBytes, nil
}

func assertP5AdjacencyMutationPostState(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture, operation string, observation P5AdjacencyMutationObservation) error {
	expectedAffected := int64(len(fixture.targetIDs))
	expectedEdges := fixture.edges
	switch operation {
	case "batch_relationship_create":
		expectedEdges += expectedAffected
	case "relationship_upsert_conflict_merge", "relationship_property_only_update":
		// These mutate properties only and retain the base-edge cardinality.
	case "batched_relationship_delete":
		expectedEdges -= expectedAffected
	case "batched_node_delete_cascade":
		expectedEdges = 2
	case "graph_clear_reload":
		expectedAffected = fixture.nodes
		expectedEdges = 0
	case "graph_drop":
		expectedAffected = 1
		expectedEdges = 0
	default:
		return fmt.Errorf("unknown P5 mutation %q", operation)
	}
	if observation.AffectedRows != expectedAffected {
		return fmt.Errorf("%s affected %d rows, expected %d", operation, observation.AffectedRows, expectedAffected)
	}
	if observation.BaseEdgesAfter != expectedEdges {
		return fmt.Errorf("%s left %d base edges, expected %d", operation, observation.BaseEdgesAfter, expectedEdges)
	}
	if operation != "batched_node_delete_cascade" && operation != "graph_clear_reload" && operation != "graph_drop" {
		return nil
	}
	var nodes int64
	if err := tx.QueryRow(ctx, `select count(*) from node where graph_id = $1`, fixture.graphID).Scan(&nodes); err != nil {
		return err
	}
	expectedNodes := int64(2)
	if operation == "graph_clear_reload" || operation == "graph_drop" {
		expectedNodes = 0
	}
	if nodes != expectedNodes {
		return fmt.Errorf("%s left %d nodes, expected %d", operation, nodes, expectedNodes)
	}
	return nil
}

func p5AdjacencyCreateRelationships(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `
		insert into edge(graph_id, start_id, end_id, kind_id, properties)
		select $1, $2, target_id, $3, '{"p5_create":true}'::jsonb
		from unnest($4::bigint[]) as targets(target_id)`,
		fixture.graphID, fixture.rootID, fixture.createKindID, fixture.targetIDs,
	)
	return result.RowsAffected(), err
}

func p5AdjacencyUpsertRelationships(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `
		insert into edge(graph_id, start_id, end_id, kind_id, properties)
		select $1, $2, target_id, $3, '{"p5_upsert":true}'::jsonb
		from unnest($4::bigint[]) as targets(target_id)
		on conflict (start_id, end_id, kind_id, graph_id) do update
		  set properties = edge.properties || excluded.properties`,
		fixture.graphID, fixture.rootID, fixture.updateKindID, fixture.targetIDs,
	)
	return result.RowsAffected(), err
}

func p5AdjacencyUpdateRelationshipProperties(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx,
		`update edge set properties = properties || '{"p5_property_only":true}'::jsonb where graph_id = $1 and id = any($2::bigint[])`,
		fixture.graphID, fixture.updateEdgeIDs,
	)
	return result.RowsAffected(), err
}

func p5AdjacencyDeleteRelationships(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `delete from edge where graph_id = $1 and id = any($2::bigint[])`, fixture.graphID, fixture.deleteEdgeIDs)
	return result.RowsAffected(), err
}

func p5AdjacencyDeleteNodes(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `delete from node where graph_id = $1 and id = any($2::bigint[])`, fixture.graphID, fixture.targetIDs)
	return result.RowsAffected(), err
}

func p5AdjacencyClearGraph(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `delete from node where graph_id = $1`, fixture.graphID)
	return result.RowsAffected(), err
}

func p5AdjacencyDropGraph(ctx context.Context, tx pgx.Tx, fixture p5AdjacencyFixture) (int64, error) {
	result, err := tx.Exec(ctx, `delete from graph where id = $1`, fixture.graphID)
	return result.RowsAffected(), err
}

func setupP5AdjacencyFixture(ctx context.Context, graphState *p5AdjacencyGraph, targets int, shadow bool) (p5AdjacencyFixture, int64, error) {
	if shadow {
		if err := installP5AdjacencyShadow(ctx, graphState.db); err != nil {
			return p5AdjacencyFixture{}, 0, err
		}
		if err := disableP5AdjacencyAutovacuum(ctx, graphState.pool, true); err != nil {
			return p5AdjacencyFixture{}, 0, err
		}
	}
	if err := waitForP5WALQuiescence(ctx, graphState.pool); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	beforeWAL, err := p5AdjacencyWALPosition(ctx, graphState.pool)
	if err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	fixtureGraph := testutil.NewDirectWriteScaleFixture(targets)
	idMap, err := opengraph.WriteGraph(ctx, graphState.db, fixtureGraph)
	if err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	if err := waitForP5WALQuiescence(ctx, graphState.pool); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	afterWAL, err := p5AdjacencyWALPosition(ctx, graphState.pool)
	if err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	fixture := p5AdjacencyFixture{
		graphID:   graphState.graphID,
		rootID:    idMap["write-root"].Int64(),
		targetIDs: make([]int64, 0, targets),
	}
	for _, name := range testutil.FixtureNames("write-target", targets) {
		fixture.targetIDs = append(fixture.targetIDs, idMap[name].Int64())
	}
	for name, destination := range map[string]*int16{
		"WriteDeleteRelationship": &fixture.deleteKindID,
		"WriteUpdateRelationship": &fixture.updateKindID,
		"WriteSurvivor":           &fixture.createKindID,
	} {
		if err := graphState.pool.QueryRow(ctx, `select id from kind where name = $1`, name).Scan(destination); err != nil {
			return p5AdjacencyFixture{}, 0, err
		}
	}
	if err := graphState.pool.QueryRow(ctx, `select coalesce(array_agg(id order by id), '{}'::bigint[]) from edge where graph_id = $1 and kind_id = $2 and properties ->> 'deletebatch' = 'true'`, fixture.graphID, fixture.deleteKindID).Scan(&fixture.deleteEdgeIDs); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	if err := graphState.pool.QueryRow(ctx, `select coalesce(array_agg(id order by id), '{}'::bigint[]) from edge where graph_id = $1 and kind_id = $2 and start_id = $3 and end_id = any($4::bigint[])`, fixture.graphID, fixture.updateKindID, fixture.rootID, fixture.targetIDs).Scan(&fixture.updateEdgeIDs); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	if int64(len(fixture.deleteEdgeIDs)) != int64(targets) || int64(len(fixture.updateEdgeIDs)) != int64(targets) {
		return p5AdjacencyFixture{}, 0, fmt.Errorf("direct-write fixture %d did not produce the required mutation targets", targets)
	}
	if err := graphState.pool.QueryRow(ctx, `select count(*) from node where graph_id = $1`, fixture.graphID).Scan(&fixture.nodes); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	if err := graphState.pool.QueryRow(ctx, `select count(*) from edge where graph_id = $1`, fixture.graphID).Scan(&fixture.edges); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	if shadow {
		if err := assertP5AdjacencyExact(ctx, graphState.pool, fixture.graphID); err != nil {
			return p5AdjacencyFixture{}, 0, err
		}
	} else if err := assertP5AdjacencyAbsent(ctx, graphState.pool); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	statements := fmt.Sprintf("vacuum (analyze) node_%d, edge_%d", fixture.graphID, fixture.graphID)
	if shadow {
		statements += fmt.Sprintf(", p5_adjacency_v1_%d", fixture.graphID)
	}
	if _, err := graphState.pool.Exec(ctx, statements); err != nil {
		return p5AdjacencyFixture{}, 0, err
	}
	return fixture, afterWAL - beforeWAL, nil
}

func openP5AdjacencyGraph(ctx context.Context, connection string) (*p5AdjacencyGraph, error) {
	poolConfig, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, err
	}
	poolConfig.MinConns = 1
	poolConfig.MaxConns = 1
	poolConfig.AfterConnect = pg.AfterPooledConnectionEstablished
	poolConfig.AfterRelease = pg.AfterPooledConnectionRelease
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connection,
		Pool:                  pool,
	})
	if err != nil {
		pool.Close()
		return nil, err
	}
	fixture := testutil.NewDirectWriteScaleFixture(1)
	nodeKinds, edgeKinds := fixture.Kinds()
	graphSchema := graph.Graph{
		Name:  fmt.Sprintf("p5_adjacency_%d", atomic.AddInt64(&p5AdjacencyGraphSequence, 1)),
		Nodes: nodeKinds,
		Edges: edgeKinds,
	}
	if err := db.AssertSchema(ctx, graph.Schema{Graphs: []graph.Graph{graphSchema}, DefaultGraph: graphSchema}); err != nil {
		_ = db.Close(ctx)
		return nil, err
	}
	driver, ok := db.(*pg.Driver)
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("expected PostgreSQL graph driver, got %T", db)
	}
	defaultGraph, ok := driver.DefaultGraph()
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("P5 adjacency graph was not selected")
	}
	return &p5AdjacencyGraph{db: db, pool: pool, graphID: defaultGraph.ID}, nil
}

func installP5AdjacencyShadow(ctx context.Context, db graph.Database) error {
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return pgquery.On(tx).InstallP5AdjacencyShadow()
	}, pg.OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol))
}

func dropP5AdjacencyShadow(ctx context.Context, db graph.Database) error {
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return pgquery.On(tx).DropP5AdjacencyShadow()
	}, pg.OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol))
}

func deleteP5AdjacencyGraph(ctx context.Context, graphState *p5AdjacencyGraph, shadow bool) error {
	if _, err := graphState.pool.Exec(ctx, `delete from graph where id = $1`, graphState.graphID); err != nil {
		return err
	}
	if shadow {
		var rows int64
		if err := graphState.pool.QueryRow(ctx, `select count(*) from public.p5_adjacency_v1 where graph_id = $1`, graphState.graphID).Scan(&rows); err != nil {
			return err
		}
		if rows != 0 {
			return fmt.Errorf("graph %d left %d committed P5 adjacency rows", graphState.graphID, rows)
		}
	}
	return nil
}

// cleanupP5AdjacencyOwnedGraphs removes only prior runner fixtures that may
// remain if a process is interrupted before its normal graph-drop cleanup.
// The current control graph remains available for P5 schema setup and removal.
func cleanupP5AdjacencyOwnedGraphs(ctx context.Context, control *p5AdjacencyGraph) error {
	_, err := control.pool.Exec(ctx, `delete from graph where name like 'p5_adjacency_%' and id <> $1`, control.graphID)
	return err
}

func assertP5AdjacencyAbsent(ctx context.Context, queryer p5AdjacencyRowQueryer) error {
	var absent bool
	if err := queryer.QueryRow(ctx, `select to_regclass('public.p5_adjacency_v1') is null`).Scan(&absent); err != nil {
		return err
	}
	if !absent {
		return fmt.Errorf("P5 adjacency relation is present in a base condition")
	}
	return nil
}

func assertP5AdjacencyExact(ctx context.Context, queryer p5AdjacencyRowQueryer, graphID int32) error {
	var baseEdges, shadowRows, mismatches int64
	if err := queryer.QueryRow(ctx, `select count(*) from edge where graph_id = $1`, graphID).Scan(&baseEdges); err != nil {
		return err
	}
	if err := queryer.QueryRow(ctx, `select count(*) from public.p5_adjacency_v1 where graph_id = $1`, graphID).Scan(&shadowRows); err != nil {
		return err
	}
	if shadowRows != baseEdges*2 {
		return fmt.Errorf("graph %d has %d base edges but %d P5 rows", graphID, baseEdges, shadowRows)
	}
	if err := queryer.QueryRow(ctx, `
		select
		  (select count(*) from edge e where e.graph_id = $1 and (
		    not exists (select 1 from public.p5_adjacency_v1 a where a.graph_id = e.graph_id and a.edge_id = e.id and a.direction = 1 and a.anchor_id = e.start_id and a.neighbor_id = e.end_id and a.kind_id = e.kind_id)
		    or not exists (select 1 from public.p5_adjacency_v1 a where a.graph_id = e.graph_id and a.edge_id = e.id and a.direction = -1 and a.anchor_id = e.end_id and a.neighbor_id = e.start_id and a.kind_id = e.kind_id)
		  ))
		  +
		  (select count(*) from public.p5_adjacency_v1 a where a.graph_id = $1 and not exists (
		    select 1 from edge e where e.graph_id = a.graph_id and e.id = a.edge_id and e.kind_id = a.kind_id and (
		      (a.direction = 1 and a.anchor_id = e.start_id and a.neighbor_id = e.end_id)
		      or (a.direction = -1 and a.anchor_id = e.end_id and a.neighbor_id = e.start_id)
		    )
		  ))`, graphID).Scan(&mismatches); err != nil {
		return err
	}
	if mismatches != 0 {
		return fmt.Errorf("graph %d has %d P5 base/shadow mismatches", graphID, mismatches)
	}
	return nil
}

func p5AdjacencyShadowIdentity(ctx context.Context, queryer p5AdjacencyRowQueryer, fixture p5AdjacencyFixture) (string, error) {
	var identity string
	err := queryer.QueryRow(ctx, `
		select coalesce(string_agg(edge_id::text || ':' || direction::text || ':' || ctid::text, ',' order by edge_id, direction), '')
		from public.p5_adjacency_v1
		where graph_id = $1 and edge_id = any($2::bigint[])`, fixture.graphID, fixture.updateEdgeIDs).Scan(&identity)
	return identity, err
}

func p5AdjacencyReadProbe(ctx context.Context, graphState *p5AdjacencyGraph, fixture p5AdjacencyFixture, shadow bool) (P5AdjacencyReadProbe, error) {
	if shadow {
		if err := assertP5AdjacencyExact(ctx, graphState.pool, fixture.graphID); err != nil {
			return P5AdjacencyReadProbe{}, err
		}
	}
	statement := `select id, end_id, kind_id from edge where graph_id = $1 and start_id = $2 and kind_id = $3 order by id`
	relation := "edge"
	if shadow {
		statement = `select edge_id, neighbor_id, kind_id from public.p5_adjacency_v1 where graph_id = $1 and direction = 1 and anchor_id = $2 and kind_id = $3 order by edge_id`
		relation = "p5_adjacency_v1"
	}
	args := []any{fixture.graphID, fixture.rootID, fixture.updateKindID}
	start := time.Now()
	rows, err := graphState.pool.Query(ctx, statement, args...)
	if err != nil {
		return P5AdjacencyReadProbe{}, err
	}
	var cardinality int64
	for rows.Next() {
		var first, second int64
		var kind int16
		if err := rows.Scan(&first, &second, &kind); err != nil {
			rows.Close()
			return P5AdjacencyReadProbe{}, err
		}
		cardinality++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return P5AdjacencyReadProbe{}, err
	}
	rows.Close()
	if cardinality != int64(len(fixture.targetIDs)) {
		return P5AdjacencyReadProbe{}, fmt.Errorf("%s read probe returned %d rows, expected %d", relation, cardinality, len(fixture.targetIDs))
	}
	var plan []byte
	if err := graphState.pool.QueryRow(ctx, "explain (analyze, buffers, format json) "+statement, args...).Scan(&plan); err != nil {
		return P5AdjacencyReadProbe{}, err
	}
	if !json.Valid(plan) {
		return P5AdjacencyReadProbe{}, fmt.Errorf("%s read probe returned invalid plan JSON", relation)
	}
	return P5AdjacencyReadProbe{Relation: relation, Duration: time.Since(start), ResultCardinality: cardinality, Plan: plan}, nil
}

func p5AdjacencyRelationSize(ctx context.Context, pool *pgxpool.Pool, relation string) (P5AdjacencyRelationSize, error) {
	size := P5AdjacencyRelationSize{Relation: relation}
	err := pool.QueryRow(ctx, `
		select coalesce(pg_relation_size(to_regclass($1)), 0),
		       coalesce(pg_indexes_size(to_regclass($1)), 0),
		       coalesce(pg_total_relation_size(to_regclass($1)), 0)`, relation).Scan(&size.HeapBytes, &size.IndexBytes, &size.TotalBytes)
	return size, err
}

func p5AdjacencyWALPosition(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var position int64
	err := pool.QueryRow(ctx, `select pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`).Scan(&position)
	return position, err
}

// disableP5AdjacencyAutovacuum prevents disposable fixture cleanup from
// changing the global LSN during the setup calibration. Statement-level WAL
// remains the authoritative mutation value, while the LSN deltas are retained
// as a quiescent cross-check.
func disableP5AdjacencyAutovacuum(ctx context.Context, pool *pgxpool.Pool, shadow bool) error {
	relations := []string{"node", "edge"}
	if shadow {
		relations = append(relations, "public.p5_adjacency_v1")
	}
	for _, relation := range relations {
		if _, err := pool.Exec(ctx, "alter table "+relation+" set (autovacuum_enabled = false)"); err != nil {
			return err
		}
	}
	return nil
}

// restoreP5AdjacencyAutovacuum restores the database-wide parent settings
// after the disposable capture has finished. The shadow relation may already
// have been removed, so only core relations are reset here.
func restoreP5AdjacencyAutovacuum(ctx context.Context, pool *pgxpool.Pool) error {
	for _, relation := range []string{"node", "edge"} {
		if _, err := pool.Exec(ctx, "alter table "+relation+" reset (autovacuum_enabled)"); err != nil {
			return err
		}
	}
	return nil
}

func waitForP5WALQuiescence(ctx context.Context, pool *pgxpool.Pool) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		var activeAutovacuum int
		if err := pool.QueryRow(ctx, `
			select count(*)
			from pg_stat_activity
			where datname = current_database()
			  and backend_type = 'autovacuum worker'
			  and state <> 'idle'`).Scan(&activeAutovacuum); err != nil {
			return err
		}
		if activeAutovacuum == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("autovacuum remained active during P5 WAL calibration")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func captureP5PostgresEnvironment(ctx context.Context, pool *pgxpool.Pool) (PostgresEnvironment, error) {
	var environment PostgresEnvironment
	err := pool.QueryRow(ctx, `
		select version(), current_database(), current_setting('plan_cache_mode'), current_setting('transaction_isolation'),
		       current_setting('work_mem'), current_setting('temp_file_limit'), (select count(*) from graph),
		       pg_postmaster_start_time(), (select oid::int8 from pg_database where datname = current_database()), current_setting('autovacuum')`).Scan(
		&environment.Version,
		&environment.Database,
		&environment.PlanCacheMode,
		&environment.TransactionIsolation,
		&environment.WorkMem,
		&environment.TempFileLimit,
		&environment.GraphPartitionCount,
		&environment.PostmasterStartedAt,
		&environment.DatabaseOID,
		&environment.Autovacuum,
	)
	return environment, err
}

func p5AdjacencyCancellationAndReuse(ctx context.Context, graphState *p5AdjacencyGraph, fixture p5AdjacencyFixture) (P5AdjacencyCancellation, error) {
	proof := P5AdjacencyCancellation{Ran: true}
	if err := graphState.pool.QueryRow(ctx, `select pg_backend_pid()`).Scan(&proof.CancelledBackendPID); err != nil {
		return P5AdjacencyCancellation{}, err
	}
	tx, err := graphState.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return P5AdjacencyCancellation{}, err
	}
	cancelledContext, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(20*time.Millisecond, cancel)
	_, executeErr := tx.Exec(cancelledContext, `
		with delayed as materialized (select pg_sleep(1))
		insert into edge(graph_id, start_id, end_id, kind_id, properties)
		select $1, $2, $3, $4, '{"p5_cancel":true}'::jsonb from delayed`,
		fixture.graphID, fixture.rootID, fixture.targetIDs[0], fixture.createKindID,
	)
	timer.Stop()
	cancel()
	rollbackErr := tx.Rollback(ctx)
	proof.RollbackObserved = rollbackErr == nil || executeErr != nil
	if executeErr == nil {
		return P5AdjacencyCancellation{}, fmt.Errorf("P5 cancellation statement committed unexpectedly")
	}
	if err := graphState.pool.QueryRow(ctx, `select pg_backend_pid()`).Scan(&proof.ReplayBackendPID); err != nil {
		return P5AdjacencyCancellation{}, fmt.Errorf("reacquire pool connection after cancellation: %w", err)
	}
	proof.PoolReuseSucceeded = true
	if err := assertP5AdjacencyExact(ctx, graphState.pool, fixture.graphID); err != nil {
		return P5AdjacencyCancellation{}, err
	}
	return proof, nil
}

func writeP5AdjacencyFeasibilityReport(path string, report P5AdjacencyFeasibilityReport) error {
	if path == "" {
		return fmt.Errorf("P5 adjacency feasibility output path must not be empty")
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable P5 adjacency feasibility artifact: %w", err)
	}
	defer output.Close()
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
