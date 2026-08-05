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
	"time"

	"github.com/stretchr/testify/require"
)

func TestComputeDurationStatsRejectsEmptyDurations(t *testing.T) {
	_, err := computeDurationStats(nil)

	require.ErrorContains(t, err, "at least one duration")
}

func TestComputeDurationStatsCopiesAndSortsDurations(t *testing.T) {
	durations := []time.Duration{
		30 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
	}

	stats, err := computeDurationStats(durations)

	require.NoError(t, err)
	require.Equal(t, 3, stats.Iterations)
	require.Equal(t, 20*time.Millisecond, stats.Median)
	require.Equal(t, 30*time.Millisecond, stats.P95)
	require.Equal(t, 30*time.Millisecond, stats.Max)
	require.Equal(t, 30*time.Millisecond, durations[0])
	require.Equal(t, 10*time.Millisecond, durations[1])
	require.Equal(t, 20*time.Millisecond, durations[2])
	require.Equal(t, []LatencySample{
		{Round: 1, Iteration: 1, Classification: "warm", Duration: 30 * time.Millisecond},
		{Round: 1, Iteration: 2, Classification: "warm", Duration: 10 * time.Millisecond},
		{Round: 1, Iteration: 3, Classification: "warm", Duration: 20 * time.Millisecond},
	}, stats.Samples)

	labelLatencySamples(&stats, ModePostgresSQL, ScaleCase{Name: "case", Dataset: "fixture"})
	require.Equal(t, ModePostgresSQL, stats.Samples[0].Backend)
	require.Equal(t, "case", stats.Samples[0].Case)
	require.Equal(t, "fixture", stats.Samples[0].Dataset)

	setSampleRound(&stats, 7)
	require.Equal(t, 7, stats.Samples[0].Round)
	require.Equal(t, 7, stats.Samples[2].Round)
}

func TestComputeDurationStatsUsesNearestRankP95(t *testing.T) {
	durations := make([]time.Duration, 20)
	for idx := range durations {
		durations[idx] = time.Duration(idx+1) * time.Millisecond
	}

	stats, err := computeDurationStats(durations)

	require.NoError(t, err)
	require.Equal(t, 19*time.Millisecond, stats.P95)
	require.Equal(t, 20*time.Millisecond, stats.Max)
}

func TestCheckStateExpectationChecksRowsAndScalar(t *testing.T) {
	rowCount := int64(1)
	scalar := int64(3)

	require.NoError(t, checkStateExpectation(
		StateQueryResult{RowCount: 1, ScalarInt: &scalar},
		ExpectedResult{RowCount: &rowCount, ScalarInt: &scalar},
	))

	wrong := int64(4)
	require.ErrorContains(t, checkStateExpectation(
		StateQueryResult{RowCount: 1, ScalarInt: &scalar},
		ExpectedResult{ScalarInt: &wrong},
	), "expected scalar integer 4")
}

func TestValidateBackendObservationsPreservesDuplicateStableRows(t *testing.T) {
	records := []CaseResult{
		{Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK, StableObservation: true, ObservedRows: []string{`["a"]`, `["a"]`}},
		{Dataset: "fixture", Name: "case", ExecutionMode: ModeNeo4j, Status: StatusOK, StableObservation: true, ObservedRows: []string{`["a"]`, `["a"]`}},
	}
	require.NoError(t, validateBackendObservations(records))

	records[1].ObservedRows = []string{`["a"]`}
	require.ErrorContains(t, validateBackendObservations(records), "backend observations differ")
}
