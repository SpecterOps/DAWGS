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
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg"
	pgv2 "github.com/specterops/dawgs/drivers/pg/v2"
	"github.com/stretchr/testify/require"
)

// TestPostgresBenchmarkDriverModes verifies that v2 is an explicit benchmark
// mode while retaining PostgreSQL-only explain support.
func TestPostgresBenchmarkDriverModes(t *testing.T) {
	require.True(t, isPostgresBenchmarkDriver(pg.DriverName))
	require.True(t, isPostgresBenchmarkDriver(pgV2BenchmarkDriver))
	require.False(t, isPostgresBenchmarkDriver("neo4j"))
}

func TestBenchmarkV2ConfigValidatesAndConvertsPoolLimits(t *testing.T) {
	config, err := benchmarkV2Config(32, 16, 0, 4)
	require.NoError(t, err)
	require.Equal(t, 32, config.TranslationCacheEntries)
	require.Equal(t, 16, config.SharedShortestPathTemplateEntries)
	require.Equal(t, &pgv2.PoolConfig{MinConnections: 0, MaxConnections: 4}, config.Pool)

	for _, arguments := range [][4]int{{-1, 0, 0, 1}, {1, -1, 0, 1}, {1, 0, -1, 1}, {1, 0, 1, 0}, {1, 0, 2, 1}} {
		_, err := benchmarkV2Config(arguments[0], arguments[1], arguments[2], arguments[3])
		require.Error(t, err)
	}
}

func TestBenchmarkShortestPathExecutorAllowlist(t *testing.T) {
	require.True(t, benchmarkShortestPathExecutor(optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance))
	require.True(t, benchmarkShortestPathExecutor(optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness))
	require.True(t, benchmarkShortestPathExecutor(optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG))
	require.True(t, benchmarkShortestPathExecutor(optimize.ShortestPathExecutorASPI1DAG))
	require.False(t, benchmarkShortestPathExecutor(optimize.ShortestPathExecutorIncumbentWorkspace))
	require.False(t, benchmarkShortestPathExecutor("unknown"))
}
