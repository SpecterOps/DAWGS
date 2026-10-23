// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPI2PowerSimulationV2FrozenMatrixTerminatesInadequateDesign(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_distance_v2.json")
	protocol, digest, err := loadSPI2ProtocolV2(path)
	require.NoError(t, err)

	report, err := buildSPI2PowerSimulationReportV2(protocol, digest)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Len(t, report.Scenarios, 11)
	expected := map[string][2]int{
		"aa_identity": {0, 272905}, "aa_upper_margin": {0, 273129}, "aa_lower_margin": {0, 273008},
		"target_power": {9588, 78059}, "target_boundary": {8, 78033}, "control_power": {10246, 78052},
		"control_boundary": {0, 78028}, "aa_order_odd_high": {0, 273035}, "aa_order_even_high": {0, 272934},
		"aa_order_upper_margin": {0, 272969}, "aa_order_lower_margin": {0, 273010},
	}
	failures := 0
	for _, scenario := range report.Scenarios {
		require.Equal(t, 20_000, scenario.Runs)
		require.Equal(t, expected[scenario.Name][0], scenario.SuccessfulDecisions)
		require.Equal(t, expected[scenario.Name][1], scenario.CoveredIntervals)
		if !scenario.Passed {
			failures++
		}
	}
	require.Greater(t, failures, 0)
}

func TestSPI2WilsonIntervalV2KnownVector(t *testing.T) {
	interval := spI2WilsonIntervalV2(18_000, 20_000)
	require.InDelta(t, 0.9, interval.Estimate, 1e-12)
	require.InDelta(t, 0.895765, interval.Lower, 0.000001)
	require.InDelta(t, 0.904081, interval.Upper, 0.000001)
}
