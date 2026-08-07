// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

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
