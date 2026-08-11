// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// measurePostgresConcurrency runs requested concurrency levels and records latency and connection reuse.
func measurePostgresConcurrency(
	ctx context.Context,
	pool *pgxpool.Pool,
	sqlQuery string,
	parameters map[string]any,
	poolSize int,
	levels []int,
	iterations int,
) ([]ConcurrencyBlock, error) {
	blocks := make([]ConcurrencyBlock, 0, len(levels))
	for _, concurrency := range levels {
		block, err := measurePostgresConcurrencyBlock(ctx, pool, sqlQuery, parameters, poolSize, concurrency, iterations)
		if err != nil {
			return nil, fmt.Errorf("concurrency %d: %w", concurrency, err)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// measurePostgresConcurrencyBlock coordinates workers for one concurrency level and aggregates their samples.
func measurePostgresConcurrencyBlock(
	ctx context.Context,
	pool *pgxpool.Pool,
	sqlQuery string,
	parameters map[string]any,
	poolSize, concurrency, iterations int,
) (ConcurrencyBlock, error) {
	var (
		startBarrier = make(chan struct{})
		wg           sync.WaitGroup
		mutex        sync.Mutex
		samples      = make([]ConcurrencySample, 0, concurrency*iterations)
		errorsSeen   []error
		seenPID      = map[uint32]struct{}{}
	)
	blockStart := time.Now()
	for worker := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startBarrier
			for iteration := range iterations {
				sample, pid, err := measurePostgresConcurrentIteration(ctx, pool, sqlQuery, parameters, worker+1, iteration+1)
				mutex.Lock()
				if err != nil {
					errorsSeen = append(errorsSeen, err)
					mutex.Unlock()
					return
				}
				if _, found := seenPID[pid]; found {
					sample.Classification = "warm-session"
				} else {
					seenPID[pid] = struct{}{}
					sample.Classification = "cold-session"
				}
				samples = append(samples, sample)
				mutex.Unlock()
			}
		}()
	}
	close(startBarrier)
	wg.Wait()
	wall := time.Since(blockStart)
	if len(errorsSeen) > 0 {
		return ConcurrencyBlock{}, errorsSeen[0]
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Worker != samples[j].Worker {
			return samples[i].Worker < samples[j].Worker
		}
		return samples[i].Iteration < samples[j].Iteration
	})
	return ConcurrencyBlock{
		Concurrency: concurrency,
		PoolSize:    poolSize,
		Operations:  len(samples),
		Wall:        wall,
		QPS:         float64(len(samples)) / wall.Seconds(),
		Samples:     samples,
	}, nil
}

// measurePostgresConcurrentIteration executes one timed query in a transaction and records its backend process ID.
func measurePostgresConcurrentIteration(
	ctx context.Context,
	pool *pgxpool.Pool,
	sqlQuery string,
	parameters map[string]any,
	worker, iteration int,
) (ConcurrencySample, uint32, error) {
	totalStart := time.Now()
	acquireStart := time.Now()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return ConcurrencySample{}, 0, err
	}
	defer conn.Release()

	poolWait := time.Since(acquireStart)
	pid := conn.Conn().PgConn().PID()

	txStart := time.Now()
	// DAWGS read queries may create and reset session-local workspace tables.
	// Keep the transaction read-write, matching drivers/pg ReadTransaction,
	// while rolling it back after the measurement.
	tx, err := conn.BeginTx(ctx, postgresConcurrencyTxOptions())
	if err != nil {
		return ConcurrencySample{}, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	transactionDuration := time.Since(txStart)

	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}}
	if len(parameters) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(parameters))
	}
	executeStart := time.Now()
	rows, err := tx.Query(ctx, sqlQuery, queryArgs...)
	if err != nil {
		return ConcurrencySample{}, 0, err
	}
	for rows.Next() {
		if _, err := rows.Values(); err != nil {
			rows.Close()
			return ConcurrencySample{}, 0, err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ConcurrencySample{}, 0, err
	}
	executeDuration := time.Since(executeStart)
	if err := tx.Rollback(ctx); err != nil {
		return ConcurrencySample{}, 0, err
	}

	return ConcurrencySample{
		Worker:       worker,
		Iteration:    iteration,
		ConnectionID: strconv.FormatUint(uint64(pid), 10),
		PoolWait:     poolWait,
		Transaction:  transactionDuration,
		ExecuteDrain: executeDuration,
		Total:        time.Since(totalStart),
	}, pid, nil
}

// postgresConcurrencyTxOptions returns transaction options that preserve session-local workspace maintenance.
func postgresConcurrencyTxOptions() pgx.TxOptions {
	return pgx.TxOptions{AccessMode: pgx.ReadWrite}
}
