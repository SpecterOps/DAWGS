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
