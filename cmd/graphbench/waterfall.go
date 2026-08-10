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

func measureRawPGXWaterfall(ctx context.Context, pool *pgxpool.Pool, sqlQuery string, params map[string]any, warmupIterations, iterations int) (PostgresBoundaryWaterfall, error) {
	if warmupIterations < 0 || iterations < 1 {
		return PostgresBoundaryWaterfall{}, fmt.Errorf("invalid raw pgx warmup/iteration counts")
	}
	run := func(iteration int, retain bool) (BoundarySample, error) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		totalStart := time.Now()
		acquireStart := time.Now()
		connection, err := pool.Acquire(ctx)
		if err != nil {
			return BoundarySample{}, err
		}
		defer connection.Release()

		poolWait := time.Since(acquireStart)
		transactionStart := time.Now()
		// DAWGS read queries may invoke the incumbent shortest-path workspace,
		// whose SQL performs session-local DDL/DML. Use a rollback-only
		// read-write transaction so the raw boundary can execute the identical
		// translated SQL without committing state.
		tx, err := connection.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadWrite})
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
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			return BoundarySample{}, err
		}
		drainDuration := time.Since(drainStart)
		runtime.ReadMemStats(&after)
		sample := BoundarySample{
			Iteration:     iteration,
			PoolWait:      poolWait,
			Transaction:   transactionDuration,
			BindPrepare:   bindDuration,
			FirstRow:      firstRowDuration,
			AllRowsDecode: allRowsDuration,
			DrainClose:    drainDuration,
			Total:         time.Since(totalStart),
			Rows:          rowCount,
		}
		if retain {
			sample.Allocations = after.Mallocs - before.Mallocs
			sample.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
		}
		return sample, nil
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
