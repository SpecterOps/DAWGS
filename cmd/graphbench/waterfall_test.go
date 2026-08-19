// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

// TestMeasureCompileWaterfallMarksOverlappingIntervals verifies that compile phase timings are labeled non-additive and each requested sample records elapsed time and allocations.
func TestMeasureCompileWaterfallMarksOverlappingIntervals(t *testing.T) {
	waterfall, err := measureCompileWaterfall(context.Background(), "MATCH (n) RETURN id(n)", nil, pgutil.NewInMemoryKindMapper(), 1, 2, translate.ToolOptions{})

	require.NoError(t, err)
	require.True(t, waterfall.IntervalsOverlap)
	require.Contains(t, waterfall.Notes, "must not be summed")
	require.Len(t, waterfall.Samples, 2)
	for _, sample := range waterfall.Samples {
		require.Positive(t, sample.Total)
		require.Positive(t, sample.Allocations)
	}
}

// TestFinalizePostgresBoundaryClosureCompletesWorkspaceHighWater verifies the
// closure derives per-query, fresh-session, size-one session, and pool maxima
// only from complete prepared-state strata.
func TestFinalizePostgresBoundaryClosureCompletesWorkspaceHighWater(t *testing.T) {
	workspace := func(bytes int64) *int64 { return &bytes }
	observation, err := stableObservationSHA256([]string{"[1]"})
	require.NoError(t, err)
	sample := func(connection string, bytes int64) BoundarySample {
		return BoundarySample{
			Total: time.Millisecond, Rows: 1, ConnectionID: connection, WorkspaceBytes: workspace(bytes), ObservationSHA256: observation,
		}
	}
	closure, err := finalizePostgresBoundaryClosure(PostgresBoundaryClosure{
		SQLFingerprint:             "sql",
		FreshSessionPreparedMiss:   sample("fresh", 4),
		SameSessionPreparedHits:    []BoundarySample{sample("fresh", 7), sample("fresh", 5)},
		PoolPreparedMiss:           sample("pool", 3),
		PoolReacquiredPreparedHits: []BoundarySample{sample("pool", 9), sample("pool", 8)},
	}, 10, 10)

	require.NoError(t, err)
	require.Equal(t, int64(9), closure.Workspace.PerQueryPeakBytes)
	require.Equal(t, int64(7), closure.Workspace.FreshSessionPeakBytes)
	require.Equal(t, int64(9), closure.Workspace.SessionPeakBytes)
	require.Equal(t, int64(9), closure.Workspace.PoolPeakBytes)
	require.Len(t, postgresBoundaryClosureSamples(closure), 6)
}

// TestFinalizePostgresBoundaryClosureFailsClosed verifies absent workspace
// observations, pool identity drift, and budget overage cannot produce closure
// evidence.
func TestFinalizePostgresBoundaryClosureFailsClosed(t *testing.T) {
	workspace := int64(1)
	observation, err := stableObservationSHA256([]string{"[1]"})
	require.NoError(t, err)
	base := PostgresBoundaryClosure{
		SQLFingerprint:           "sql",
		FreshSessionPreparedMiss: BoundarySample{Total: time.Millisecond, ConnectionID: "fresh", WorkspaceBytes: &workspace, ObservationSHA256: observation},
		SameSessionPreparedHits:  []BoundarySample{{Total: time.Millisecond, ConnectionID: "fresh", WorkspaceBytes: &workspace, ObservationSHA256: observation}},
		PoolPreparedMiss:         BoundarySample{Total: time.Millisecond, ConnectionID: "pool", WorkspaceBytes: &workspace, ObservationSHA256: observation},
		PoolReacquiredPreparedHits: []BoundarySample{{
			Total: time.Millisecond, ConnectionID: "pool", WorkspaceBytes: &workspace, ObservationSHA256: observation,
		}},
	}

	_, err = finalizePostgresBoundaryClosure(base, 0, 1)
	require.ErrorContains(t, err, "exceeds ceiling")

	changedConnection := base
	changedConnection.PoolReacquiredPreparedHits[0].ConnectionID = "other"
	_, err = finalizePostgresBoundaryClosure(changedConnection, 1, 1)
	require.ErrorContains(t, err, "backend identity differs")

	missingWorkspace := base
	missingWorkspace.SameSessionPreparedHits[0].WorkspaceBytes = nil
	_, err = finalizePostgresBoundaryClosure(missingWorkspace, 1, 1)
	require.ErrorContains(t, err, "incomplete boundary sample")
	base.SameSessionPreparedHits[0].WorkspaceBytes = &workspace

	missingObservation := base
	missingObservation.SameSessionPreparedHits[0].ObservationSHA256 = ""
	_, err = finalizePostgresBoundaryClosure(missingObservation, 1, 1)
	require.ErrorContains(t, err, "incomplete boundary sample")
	base.SameSessionPreparedHits[0].ObservationSHA256 = observation

	mismatchedObservation := base
	mismatchedObservation.SameSessionPreparedHits[0].ObservationSHA256 = "different"
	_, err = finalizePostgresBoundaryClosure(mismatchedObservation, 1, 1)
	require.ErrorContains(t, err, "raw observations differ")
}

func TestPostgresBoundaryObservationNormalizerUsesStablePublicRows(t *testing.T) {
	normalizer := postgresBoundaryObservationNormalizer{}
	row, err := normalizer.normalize([]any{int64(42)}, nil)
	require.NoError(t, err)
	require.Equal(t, "[42]", row)

	jsonRow, err := normalizer.normalize([]any{[]byte(`{"value":42}`)}, []pgconn.FieldDescription{{DataTypeOID: pgtype.JSONBOID}})
	require.NoError(t, err)
	require.Equal(t, `[{"value":42}]`, jsonRow)
}
