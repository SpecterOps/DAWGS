// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package optimize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShortestPathI2QualifiedCapsFreezeProductionContract freezes the shared
// planner and production-authorization cap values.
func TestShortestPathI2QualifiedCapsFreezeProductionContract(t *testing.T) {
	require.Equal(t, int64(100_000), ShortestPathI2QualifiedStateLimit)
	require.Equal(t, int64(100_000), ShortestPathI2QualifiedFrontierLimit)
	require.Equal(t, ShortestPathI2QualifiedStateLimit, defaultShortestPathStateLimit)
	require.Equal(t, ShortestPathI2QualifiedFrontierLimit, defaultShortestPathFrontierLimit)
}
