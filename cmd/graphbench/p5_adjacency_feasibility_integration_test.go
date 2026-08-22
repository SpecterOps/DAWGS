//go:build manual_integration && integration

// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/stretchr/testify/require"
)

func TestP5AdjacencyTaggedPGXStatementVisibleToPGStatStatements(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	graphState, err := openP5AdjacencyGraph(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, deleteP5AdjacencyGraph(ctx, graphState, false))
		require.NoError(t, graphState.db.Close(ctx))
	})
	_, release, err := prepareP5AdjacencyWALAttribution(ctx, graphState.pool)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, release())
	})

	tag := p5AdjacencyStatementWALTag("pgx_visibility_probe")
	tx, err := graphState.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	before, err := p5AdjacencyStatementWALStats(ctx, tx, tag)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "with "+tag+" as (select 1) insert into graph(name) values ($1)", pgx.QueryExecModeSimpleProtocol, tag)
	require.NoError(t, err)
	after, err := p5AdjacencyStatementWALStats(ctx, tx, tag)
	require.NoError(t, err)
	stats, err := p5AdjacencyStatementWALDelta(before, after)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))
	require.Equal(t, int64(1), stats.Calls)
	require.Positive(t, stats.Bytes)
}

func TestP5AdjacencyCalibrationAttributesTaggedStatementWAL(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	resetP5AdjacencyTestState(t, ctx, connection)
	t.Cleanup(func() {
		resetP5AdjacencyTestState(t, ctx, connection)
	})
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	attribution, release, err := prepareP5AdjacencyWALAttribution(ctx, pool)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, release())
	})
	require.Equal(t, "pg_stat_statements", attribution.Source)

	calibration, err := runP5AdjacencyCalibration(ctx, connection, "base", 1, "batch_relationship_create")
	require.NoError(t, err)
	require.Equal(t, int64(1), calibration.StatementWAL.Calls)
	require.Positive(t, calibration.StatementWAL.Records)
	require.Positive(t, calibration.StatementWAL.Bytes)
	require.GreaterOrEqual(t, calibration.MutationWALLSN, calibration.StatementWAL.Bytes)

	control, err := openP5AdjacencyGraph(ctx, connection)
	require.NoError(t, err)
	require.NoError(t, installP5AdjacencyShadow(ctx, control.db))
	shadowCalibration, err := runP5AdjacencyCalibration(ctx, connection, "shadow", 1, "batch_relationship_create")
	require.NoError(t, err)
	require.Equal(t, int64(1), shadowCalibration.StatementWAL.Calls)
	require.Positive(t, shadowCalibration.StatementWAL.Records)
	require.Greater(t, shadowCalibration.StatementWAL.Bytes, calibration.StatementWAL.Bytes)
	require.NoError(t, dropP5AdjacencyShadow(ctx, control.db))
	require.NoError(t, deleteP5AdjacencyGraph(ctx, control, false))
	require.NoError(t, control.db.Close(ctx))
}

func resetP5AdjacencyTestState(t *testing.T, ctx context.Context, connection string) {
	t.Helper()
	control, err := openP5AdjacencyGraph(ctx, connection)
	require.NoError(t, err)
	require.NoError(t, dropP5AdjacencyShadow(ctx, control.db))
	require.NoError(t, cleanupP5AdjacencyOwnedGraphs(ctx, control))
	require.NoError(t, deleteP5AdjacencyGraph(ctx, control, false))
	require.NoError(t, control.db.Close(ctx))
}
