// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPI2SuccessorPowerStudyV3TerminatesFrozenDesign(t *testing.T) {
	studyPath := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_successor_power_study_v3.json")
	study, digest, err := loadSPI2SuccessorPowerStudyV3(studyPath)
	require.NoError(t, err)

	report, err := buildSPI2SuccessorPowerStudyReportV3(
		study,
		digest,
		filepath.Join("..", "..", ".coverage", "sp-i2-distance-v1-3865cbc", "discovery-s4.jsonl"),
		filepath.Join("..", "..", ".coverage", "sp-i2-distance-v1-3865cbc", "discovery-i2.jsonl"),
	)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Len(t, report.Scenarios, 11)
	require.InDelta(t, 0.22360679774997896, report.CalibrationScale, 1e-15)
	require.InDelta(t, 0.005805, report.LogStandardErrors.Pooled, 1e-6)
	require.InDelta(t, 0.008209, report.LogStandardErrors.OrderStratum, 1e-6)
	expected := map[string]int{
		"aa_identity": 19_861, "aa_upper_margin": 0, "aa_lower_margin": 0,
		"target_power": 20_000, "target_boundary": 6, "control_power": 20_000,
		"control_boundary": 0, "aa_order_odd_high": 2_937, "aa_order_even_high": 3_043,
		"aa_order_upper_margin": 0, "aa_order_lower_margin": 0,
	}
	for _, scenario := range report.Scenarios {
		require.Equal(t, expected[scenario.Name], scenario.SuccessfulDecisions, scenario.Name)
	}
}

func TestSPI2SuccessorPowerStudyV3TerminalTombstone(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_successor_power_study_v3_rejection.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var tombstone struct {
		Schema      string `json:"schema"`
		Generation  string `json:"generation"`
		ProtocolSHA string `json:"protocol_sha256"`
		Terminal    bool   `json:"terminal"`
		Implemented bool   `json:"candidate_implemented"`
		Corpus      bool   `json:"corpus_created"`
		Timed       bool   `json:"database_timing_started"`
		Holdout     bool   `json:"holdout_opened"`
		FailedGates []struct {
			Scenario string  `json:"scenario"`
			Observed float64 `json:"observed"`
			Required float64 `json:"required"`
		} `json:"failed_gates"`
	}
	require.NoError(t, json.Unmarshal(raw, &tombstone))
	require.Equal(t, "sp-i2-successor-power-study-rejection-v3", tombstone.Schema)
	require.Equal(t, "sp-i2-distance-v3-power-study", tombstone.Generation)
	require.Equal(t, "e11090bbbe73cc36dfae2af97e26b6e1fc4d42590fc6fd331b2204c7a9e04f31", tombstone.ProtocolSHA)
	require.True(t, tombstone.Terminal)
	require.False(t, tombstone.Implemented || tombstone.Corpus || tombstone.Timed || tombstone.Holdout)
	require.Len(t, tombstone.FailedGates, 2)
	require.Equal(t, []string{"aa_order_odd_high", "aa_order_even_high"}, []string{tombstone.FailedGates[0].Scenario, tombstone.FailedGates[1].Scenario})
	require.InDelta(t, 0.14201232557116983, tombstone.FailedGates[0].Observed, 1e-15)
	require.InDelta(t, 0.14723913101703448, tombstone.FailedGates[1].Observed, 1e-15)
	require.Equal(t, 0.90, tombstone.FailedGates[0].Required)
	require.Equal(t, 0.90, tombstone.FailedGates[1].Required)
}
