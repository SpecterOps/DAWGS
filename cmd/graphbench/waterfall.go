// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

// postgresBoundarySession is the small shared execution surface of a dedicated
// pgx connection and a pooled pgx connection.
type postgresBoundarySession interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// postgresBoundaryPIDReader provides a physical PostgreSQL backend identity.
type postgresBoundaryPIDReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// measureCompileWaterfall times Cypher parse, translate, and SQL rendering separately.
func measureCompileWaterfall(
	ctx context.Context,
	cypherQuery string,
	params map[string]any,
	kindMapper pgsql.KindMapper,
	graphID int32,
	iterations int,
	toolOptions translate.ToolOptions,
) (ClientWaterfall, error) {
	waterfall := ClientWaterfall{
		IntervalsOverlap: true,
		Notes:            "translate_including_optimize repeats optimization internally; parse, optimize, translate, and render must not be summed as an additive client attribution",
		Samples:          make([]CompileSample, 0, iterations),
	}
	for iteration := 1; iteration <= iterations; iteration++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		totalStart := time.Now()

		parseStart := time.Now()
		query, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
		if err != nil {
			return ClientWaterfall{}, fmt.Errorf("parse: %w", err)
		}
		parseDuration := time.Since(parseStart)

		optimizeStart := time.Now()
		if _, err := optimize.Optimize(query); err != nil {
			return ClientWaterfall{}, fmt.Errorf("optimize: %w", err)
		}
		optimizeDuration := time.Since(optimizeStart)

		translateStart := time.Now()
		var translation translate.Result
		if !hasForcedToolOptions(toolOptions) {
			translation, err = translate.Translate(ctx, query, kindMapper, params, graphID)
		} else {
			translation, err = translate.TranslateForTool(ctx, query, kindMapper, params, graphID, toolOptions)
		}
		if err != nil {
			return ClientWaterfall{}, fmt.Errorf("translate: %w", err)
		}
		translateDuration := time.Since(translateStart)

		renderStart := time.Now()
		if _, err := translate.Translated(translation); err != nil {
			return ClientWaterfall{}, fmt.Errorf("render: %w", err)
		}
		renderDuration := time.Since(renderStart)
		totalDuration := time.Since(totalStart)
		runtime.ReadMemStats(&after)

		waterfall.Samples = append(waterfall.Samples, CompileSample{
			Iteration:                  iteration,
			Parse:                      parseDuration,
			Optimize:                   optimizeDuration,
			TranslateIncludingOptimize: translateDuration,
			Render:                     renderDuration,
			Total:                      totalDuration,
			Allocations:                after.Mallocs - before.Mallocs,
			AllocatedBytes:             after.TotalAlloc - before.TotalAlloc,
		})
	}
	return waterfall, nil
}

// measureRawPGXWaterfall times PostgreSQL bind, first row, drain, and close stages separately.
func measureRawPGXWaterfall(ctx context.Context, pool *pgxpool.Pool, sqlQuery string, params map[string]any, warmupIterations, iterations int, isolation ...pgx.TxIsoLevel) (PostgresBoundaryWaterfall, error) {
	if warmupIterations < 0 || iterations < 1 {
		return PostgresBoundaryWaterfall{}, fmt.Errorf("invalid raw pgx warmup/iteration counts")
	}
	run := func(iteration int, retain bool) (BoundarySample, error) {
		totalStart := time.Now()
		acquireStart := time.Now()
		connection, err := pool.Acquire(ctx)
		if err != nil {
			return BoundarySample{}, err
		}
		defer connection.Release()
		return measureRawPGXOnSession(ctx, connection, sqlQuery, params, iteration, totalStart, time.Since(acquireStart), retain, false, isolation...)
	}
	for idx := 0; idx < warmupIterations; idx++ {
		if _, err := run(-(idx + 1), false); err != nil {
			return PostgresBoundaryWaterfall{}, err
		}
	}
	result := PostgresBoundaryWaterfall{
		Boundary:         "identical translated SQL through raw pgx pool/transaction/decode/drain",
		SQLFingerprint:   sqlFingerprint(sqlQuery),
		WarmupIterations: warmupIterations,
		Samples:          make([]BoundarySample, 0, iterations),
	}
	var expectedRows int64 = -1
	for iteration := 1; iteration <= iterations; iteration++ {
		sample, err := run(iteration, true)
		if err != nil {
			return PostgresBoundaryWaterfall{}, err
		}
		if expectedRows < 0 {
			expectedRows = sample.Rows
		}
		if sample.Rows != expectedRows {
			return PostgresBoundaryWaterfall{}, fmt.Errorf("raw pgx row count changed from %d to %d", expectedRows, sample.Rows)
		}
		result.Samples = append(result.Samples, sample)
	}
	return result, nil
}

// measureRawPGXOnSession executes one exact translated statement through an
// already-selected PostgreSQL session. Workspace collection happens after the
// result is drained and is excluded from all timing intervals.
func measureRawPGXOnSession(
	ctx context.Context,
	session postgresBoundarySession,
	sqlQuery string,
	params map[string]any,
	iteration int,
	totalStart time.Time,
	poolWait time.Duration,
	retainAllocations bool,
	captureWorkspace bool,
	isolation ...pgx.TxIsoLevel,
) (BoundarySample, error) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	transactionStart := time.Now()
	// DAWGS read queries may invoke session-local workspace DDL/DML. The raw
	// boundary therefore uses a rollback-only read-write transaction, preserving
	// the exact translated SQL without committing benchmark side effects.
	txOptions := pgx.TxOptions{AccessMode: pgx.ReadWrite}
	if len(isolation) > 0 {
		txOptions.IsoLevel = isolation[0]
	}
	tx, err := session.BeginTx(ctx, txOptions)
	if err != nil {
		return BoundarySample{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	transactionDuration := time.Since(transactionStart)

	bindStart := time.Now()
	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}}
	if len(params) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(params))
	}
	rows, err := tx.Query(ctx, sqlQuery, queryArgs...)
	if err != nil {
		return BoundarySample{}, err
	}
	bindDuration := time.Since(bindStart)
	firstRowStart := time.Now()
	var rowCount int64
	if rows.Next() {
		rowCount++
		if _, err := rows.Values(); err != nil {
			rows.Close()
			return BoundarySample{}, err
		}
	}
	firstRowDuration := time.Since(firstRowStart)
	allRowsStart := time.Now()
	for rows.Next() {
		rowCount++
		if _, err := rows.Values(); err != nil {
			rows.Close()
			return BoundarySample{}, err
		}
	}
	allRowsDuration := time.Since(allRowsStart)

	drainStart := time.Now()
	rows.Close()
	if err := rows.Err(); err != nil {
		return BoundarySample{}, err
	}
	drainDuration := time.Since(drainStart)
	var workspaceBytes *int64
	if captureWorkspace {
		workspaceStart := time.Now()
		workspace, err := measurePostgresTemporaryWorkspace(ctx, tx)
		if err != nil {
			return BoundarySample{}, err
		}
		workspaceBytes = &workspace
		// The observation query is intentionally outside the raw execution
		// intervals; only cleanup remains part of the direct request boundary.
		workspaceDuration := time.Since(workspaceStart)
		totalStart = totalStart.Add(workspaceDuration)
	}
	rollbackStart := time.Now()
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		return BoundarySample{}, err
	}
	drainDuration += time.Since(rollbackStart)
	runtime.ReadMemStats(&after)
	sample := BoundarySample{
		Iteration:      iteration,
		PoolWait:       poolWait,
		Transaction:    transactionDuration,
		BindPrepare:    bindDuration,
		FirstRow:       firstRowDuration,
		AllRowsDecode:  allRowsDuration,
		DrainClose:     drainDuration,
		Total:          time.Since(totalStart),
		Rows:           rowCount,
		WorkspaceBytes: workspaceBytes,
	}
	if retainAllocations {
		sample.Allocations = after.Mallocs - before.Mallocs
		sample.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	}
	return sample, nil
}

// measurePostgresTemporaryWorkspace reports all non-diagnostic temporary
// relations visible to the exact raw query transaction. Runtime attestation
// and telemetry tables are measurement scaffolding, so they are excluded from
// the component's performance-workspace budget.
func measurePostgresTemporaryWorkspace(ctx context.Context, tx pgx.Tx) (int64, error) {
	var workspace int64
	if err := tx.QueryRow(ctx, `
		select coalesce(sum(pg_total_relation_size(c.oid)), 0)::int8
		from pg_class c
		where c.relnamespace = pg_my_temp_schema()
		  and c.relname <> 'traversal_runtime_attestation_v1'
		  and c.relname not like '%telemetry%'
		  and not exists (
		    select 1
		    from pg_index i
		    join pg_class indexed on indexed.oid = i.indrelid
		    where i.indexrelid = c.oid
		      and indexed.relnamespace = pg_my_temp_schema()
		      and (indexed.relname = 'traversal_runtime_attestation_v1' or indexed.relname like '%telemetry%')
		  )
	`).Scan(&workspace); err != nil {
		return 0, fmt.Errorf("measure temporary workspace high-water: %w", err)
	}
	return workspace, nil
}

// postgresBackendPID reads one physical PostgreSQL connection identity outside
// the timing boundary so closure records can prove pool release/reacquisition.
func postgresBackendPID(ctx context.Context, connection postgresBoundaryPIDReader) (string, error) {
	var backendPID int64
	if err := connection.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID); err != nil {
		return "", fmt.Errorf("read PostgreSQL backend PID: %w", err)
	}
	return fmt.Sprintf("%d", backendPID), nil
}

// measurePostgresBoundaryClosure captures explicit fresh-session, prepared-hit,
// and release/reacquisition strata without changing the SQL under measurement.
func measurePostgresBoundaryClosure(ctx context.Context, pool *pgxpool.Pool, sqlQuery string, params map[string]any, iterations int, sessionCeilingBytes, poolCeilingBytes int64, isolation ...pgx.TxIsoLevel) (PostgresBoundaryClosure, error) {
	if iterations < 1 {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure iterations must be positive")
	}
	if sessionCeilingBytes <= 0 || poolCeilingBytes <= 0 {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure workspace ceilings must be positive")
	}
	freshConfig := pool.Config().ConnConfig.Copy()
	fresh, err := pgx.ConnectConfig(ctx, freshConfig)
	if err != nil {
		return PostgresBoundaryClosure{}, fmt.Errorf("open fresh PostgreSQL closure session: %w", err)
	}
	defer fresh.Close(ctx)

	closure := PostgresBoundaryClosure{
		Boundary:                   "identical translated SQL through fresh and size-one raw pgx sessions/transactions/decode/drain",
		SQLFingerprint:             sqlFingerprint(sqlQuery),
		SameSessionPreparedHits:    make([]BoundarySample, 0, iterations),
		PoolReacquiredPreparedHits: make([]BoundarySample, 0, iterations),
	}
	freshPID, err := postgresBackendPID(ctx, fresh)
	if err != nil {
		return PostgresBoundaryClosure{}, err
	}
	closure.FreshSessionPreparedMiss, err = measureRawPGXOnSession(ctx, fresh, sqlQuery, params, 1, time.Now(), 0, true, true, isolation...)
	if err != nil {
		return PostgresBoundaryClosure{}, fmt.Errorf("fresh-session prepared miss: %w", err)
	}
	closure.FreshSessionPreparedMiss.ConnectionID = freshPID
	for iteration := 1; iteration <= iterations; iteration++ {
		sample, err := measureRawPGXOnSession(ctx, fresh, sqlQuery, params, iteration, time.Now(), 0, true, true, isolation...)
		if err != nil {
			return PostgresBoundaryClosure{}, fmt.Errorf("same-session prepared hit %d: %w", iteration, err)
		}
		sample.ConnectionID = freshPID
		closure.SameSessionPreparedHits = append(closure.SameSessionPreparedHits, sample)
	}

	acquireStart := time.Now()
	pooled, err := pool.Acquire(ctx)
	if err != nil {
		return PostgresBoundaryClosure{}, fmt.Errorf("acquire pooled prepared miss session: %w", err)
	}
	poolWait := time.Since(acquireStart)
	pooledPID, err := postgresBackendPID(ctx, pooled)
	if err == nil {
		closure.PoolPreparedMiss, err = measureRawPGXOnSession(ctx, pooled, sqlQuery, params, 1, acquireStart, poolWait, true, true, isolation...)
	}
	pooled.Release()
	if err != nil {
		return PostgresBoundaryClosure{}, fmt.Errorf("pooled prepared miss: %w", err)
	}
	closure.PoolPreparedMiss.ConnectionID = pooledPID

	for iteration := 1; iteration <= iterations; iteration++ {
		acquireStart := time.Now()
		pooled, err := pool.Acquire(ctx)
		if err != nil {
			return PostgresBoundaryClosure{}, fmt.Errorf("reacquire pooled prepared-hit session %d: %w", iteration, err)
		}
		poolWait := time.Since(acquireStart)
		currentPID, pidErr := postgresBackendPID(ctx, pooled)
		if pidErr == nil && currentPID != pooledPID {
			pidErr = fmt.Errorf("pooled backend changed after release: %s -> %s", pooledPID, currentPID)
		}
		var sample BoundarySample
		if pidErr == nil {
			sample, pidErr = measureRawPGXOnSession(ctx, pooled, sqlQuery, params, iteration, acquireStart, poolWait, true, true, isolation...)
		}
		pooled.Release()
		if pidErr != nil {
			return PostgresBoundaryClosure{}, fmt.Errorf("pooled prepared hit %d: %w", iteration, pidErr)
		}
		sample.ConnectionID = currentPID
		closure.PoolReacquiredPreparedHits = append(closure.PoolReacquiredPreparedHits, sample)
	}

	closure, err = finalizePostgresBoundaryClosure(closure, sessionCeilingBytes, poolCeilingBytes)
	if err != nil {
		return PostgresBoundaryClosure{}, err
	}
	return closure, nil
}

// finalizePostgresBoundaryClosure fails closed on an incomplete stratum,
// changing pooled connection, absent workspace observation, or ceiling breach.
func finalizePostgresBoundaryClosure(closure PostgresBoundaryClosure, sessionCeilingBytes, poolCeilingBytes int64) (PostgresBoundaryClosure, error) {
	if closure.SQLFingerprint == "" || closure.FreshSessionPreparedMiss.WorkspaceBytes == nil ||
		len(closure.SameSessionPreparedHits) == 0 || closure.PoolPreparedMiss.WorkspaceBytes == nil ||
		len(closure.PoolReacquiredPreparedHits) == 0 {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure lacks a complete fresh, same-session, or pooled prepared-state stratum")
	}
	if closure.PoolPreparedMiss.ConnectionID == "" {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure pooled prepared miss lacks a backend identity")
	}
	all := []BoundarySample{closure.FreshSessionPreparedMiss, closure.PoolPreparedMiss}
	all = append(all, closure.SameSessionPreparedHits...)
	all = append(all, closure.PoolReacquiredPreparedHits...)
	for _, sample := range all {
		if sample.WorkspaceBytes == nil || sample.Rows < 0 || sample.Total <= 0 || sample.ConnectionID == "" {
			return PostgresBoundaryClosure{}, fmt.Errorf("closure contains an incomplete boundary sample")
		}
		if *sample.WorkspaceBytes > closure.Workspace.PerQueryPeakBytes {
			closure.Workspace.PerQueryPeakBytes = *sample.WorkspaceBytes
		}
	}
	for _, sample := range append([]BoundarySample{closure.FreshSessionPreparedMiss}, closure.SameSessionPreparedHits...) {
		if *sample.WorkspaceBytes > closure.Workspace.FreshSessionPeakBytes {
			closure.Workspace.FreshSessionPeakBytes = *sample.WorkspaceBytes
		}
	}
	for _, sample := range append([]BoundarySample{closure.PoolPreparedMiss}, closure.PoolReacquiredPreparedHits...) {
		if sample.ConnectionID != closure.PoolPreparedMiss.ConnectionID {
			return PostgresBoundaryClosure{}, fmt.Errorf("closure pooled prepared-hit backend identity differs from prepared miss")
		}
		if *sample.WorkspaceBytes > closure.Workspace.SessionPeakBytes {
			closure.Workspace.SessionPeakBytes = *sample.WorkspaceBytes
		}
	}
	closure.Workspace.PoolPeakBytes = closure.Workspace.SessionPeakBytes
	if closure.Workspace.SessionPeakBytes > sessionCeilingBytes {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure session workspace high-water %d exceeds ceiling %d", closure.Workspace.SessionPeakBytes, sessionCeilingBytes)
	}
	if closure.Workspace.PoolPeakBytes > poolCeilingBytes {
		return PostgresBoundaryClosure{}, fmt.Errorf("closure pool workspace high-water %d exceeds ceiling %d", closure.Workspace.PoolPeakBytes, poolCeilingBytes)
	}
	return closure, nil
}

// postgresBoundaryClosureSamples returns every raw SQL observation contained in
// a closure, preserving its explicit prepared-state strata in their declared
// order for exact-result validation.
func postgresBoundaryClosureSamples(closure PostgresBoundaryClosure) []BoundarySample {
	samples := make([]BoundarySample, 0, 2+len(closure.SameSessionPreparedHits)+len(closure.PoolReacquiredPreparedHits))
	samples = append(samples, closure.FreshSessionPreparedMiss)
	samples = append(samples, closure.SameSessionPreparedHits...)
	samples = append(samples, closure.PoolPreparedMiss)
	samples = append(samples, closure.PoolReacquiredPreparedHits...)
	return samples
}
