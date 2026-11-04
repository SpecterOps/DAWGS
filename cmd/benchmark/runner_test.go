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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunScenarioSamplesWarmsEachWorker verifies that every worker receives
// its own untimed warm-up and contributes the configured number of samples.
func TestRunScenarioSamplesWarmsEachWorker(t *testing.T) {
	var calls atomic.Int64
	measurement, samples, err := runScenarioSamples(3, 2, 4, func() (Measurement, error) {
		calls.Add(1)
		return Measurement{RowCount: 7}, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), measurement.RowCount)
	require.Len(t, samples, 12)
	require.Equal(t, int64(20), calls.Load())
}

// TestRunScenarioSamplesSupportsColdMeasurements verifies that zero warm-ups
// keep the first timed query in the timing distribution.
func TestRunScenarioSamplesSupportsColdMeasurements(t *testing.T) {
	var calls atomic.Int64
	measurement, samples, err := runScenarioSamples(2, 0, 3, func() (Measurement, error) {
		return Measurement{RowCount: calls.Add(1)}, nil
	})
	require.NoError(t, err)
	require.NotZero(t, measurement.RowCount)
	require.Len(t, samples, 6)
	require.Equal(t, int64(6), calls.Load())
}
