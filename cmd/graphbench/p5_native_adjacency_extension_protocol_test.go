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

func TestP5NativeAdjacencyExtensionFeasibilityProtocolFreezesNonCandidateBoundary(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "p5_native_adjacency_extension_feasibility_v1.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var protocol struct {
		Schema      string `json:"schema"`
		Generation  string `json:"generation"`
		Status      string `json:"status"`
		Predecessor struct {
			Generation  string `json:"generation"`
			Disposition string `json:"disposition"`
		} `json:"predecessor"`
		Architecture struct {
			Identity         string   `json:"identity"`
			ExtensionName    string   `json:"extension_name"`
			SourceStorage    string   `json:"source_storage"`
			DiagnosticOutput []string `json:"diagnostic_output"`
			SQLAttributes    []string `json:"sql_attributes"`
			ForbiddenScope   []string `json:"forbidden_scope"`
		} `json:"architecture"`
		DeliveryMatrix struct {
			ServerMajors []int `json:"server_majors"`
			Inventory    struct {
				LiveCaptureServer              string `json:"live_capture_server"`
				AvailableLocalPGXS             string `json:"available_local_pgxs"`
				MatchedPG17BuildInterfaceReady bool   `json:"matched_pg17_build_interface_available"`
			} `json:"planning_inventory"`
		} `json:"delivery_matrix"`
		ProbeContract struct {
			RowCap                        int `json:"row_cap"`
			NativeIndexScansPerInvocation int `json:"native_index_scans_per_invocation"`
		} `json:"probe_contract"`
		Fixtures struct {
			ReadFanoutSizes              []int `json:"read_fanout_sizes"`
			NewGeneratedCorpusPermitted  bool  `json:"new_generated_corpus_permitted"`
			ProtectedCorpusAccessAllowed bool  `json:"protected_corpus_access_permitted"`
		} `json:"fixtures"`
		CaptureDesign struct {
			Blocks          int     `json:"blocks"`
			ConfidenceLevel float64 `json:"confidence_level"`
			CapOverrides    bool    `json:"cap_overrides_permitted"`
		} `json:"capture_design"`
		ResourceBudgets struct {
			PersistentGraphRelations int     `json:"persistent_graph_relations"`
			PersistentGraphIndexes   int     `json:"persistent_graph_indexes"`
			MutationHooks            int     `json:"mutation_triggers_or_hooks"`
			BackgroundWorkers        int     `json:"background_workers"`
			SharedMemoryBytes        int     `json:"shared_memory_bytes"`
			GraphStorageBytesDelta   int     `json:"graph_storage_bytes_delta"`
			ReadWALBytes             int     `json:"read_wal_bytes"`
			ReadTempBytes            int     `json:"read_temp_bytes"`
			MutationWALRatioUpper    float64 `json:"mutation_statement_wal_ratio_upper"`
		} `json:"resource_budgets"`
		RawReadGates struct {
			P50RatioUpper           float64 `json:"all_normal_and_envelope_cases_p50_ratio_upper"`
			P95RatioUpper           float64 `json:"all_normal_and_envelope_cases_p95_ratio_upper"`
			HighDegreeMinimumFanout int     `json:"high_degree_target_minimum_fanout"`
			HighDegreeP50RatioUpper float64 `json:"high_degree_target_p50_ratio_upper"`
			CandidateEvidence       bool    `json:"timings_are_candidate_or_promotion_evidence"`
		} `json:"raw_read_gates"`
		NextAuthorization struct {
			PassAuthorizes       []string `json:"pass_authorizes"`
			PassDoesNotAuthorize []string `json:"pass_does_not_authorize"`
		} `json:"next_authorization"`
	}
	require.NoError(t, json.Unmarshal(raw, &protocol))

	require.Equal(t, "p5-native-adjacency-extension-feasibility-v1", protocol.Schema)
	require.Equal(t, protocol.Schema, protocol.Generation)
	require.Equal(t, "prospective_frozen_non_candidate", protocol.Status)
	require.Equal(t, "p5-adjacency-materialization-feasibility-v2", protocol.Predecessor.Generation)
	require.Equal(t, "benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v2_disposition.json", protocol.Predecessor.Disposition)
	require.Equal(t, "P5-NATIVE-ADJACENCY-SCAN-V1", protocol.Architecture.Identity)
	require.Equal(t, "dawgs_p5_native_adjacency_v1", protocol.Architecture.ExtensionName)
	require.Contains(t, protocol.Architecture.SourceStorage, "existing graph partitions")
	require.Equal(t, []string{"edge_ids", "next_node_ids", "kind_ids", "scanned_index_tuples", "heap_fetches", "returned_rows", "overflow", "complete"}, protocol.Architecture.DiagnosticOutput)
	require.Equal(t, []string{"STABLE", "STRICT", "SECURITY INVOKER", "PARALLEL RESTRICTED"}, protocol.Architecture.SQLAttributes)
	require.Contains(t, protocol.Architecture.ForbiddenScope, "a shortest-path, all-shortest-path, or ordinary-expansion executor")

	require.Equal(t, []int{17, 18}, protocol.DeliveryMatrix.ServerMajors)
	require.Equal(t, "17.10", protocol.DeliveryMatrix.Inventory.LiveCaptureServer)
	require.Equal(t, "18.4", protocol.DeliveryMatrix.Inventory.AvailableLocalPGXS)
	require.False(t, protocol.DeliveryMatrix.Inventory.MatchedPG17BuildInterfaceReady)
	require.Equal(t, 4096, protocol.ProbeContract.RowCap)
	require.Equal(t, 1, protocol.ProbeContract.NativeIndexScansPerInvocation)
	require.Equal(t, []int{32, 33, 128, 512, 513, 1000, 4096, 4097}, protocol.Fixtures.ReadFanoutSizes)
	require.False(t, protocol.Fixtures.NewGeneratedCorpusPermitted)
	require.False(t, protocol.Fixtures.ProtectedCorpusAccessAllowed)

	require.Equal(t, 8, protocol.CaptureDesign.Blocks)
	require.Equal(t, 0.975, protocol.CaptureDesign.ConfidenceLevel)
	require.False(t, protocol.CaptureDesign.CapOverrides)
	require.Zero(t, protocol.ResourceBudgets.PersistentGraphRelations)
	require.Zero(t, protocol.ResourceBudgets.PersistentGraphIndexes)
	require.Zero(t, protocol.ResourceBudgets.MutationHooks)
	require.Zero(t, protocol.ResourceBudgets.BackgroundWorkers)
	require.Zero(t, protocol.ResourceBudgets.SharedMemoryBytes)
	require.Zero(t, protocol.ResourceBudgets.GraphStorageBytesDelta)
	require.Zero(t, protocol.ResourceBudgets.ReadWALBytes)
	require.Zero(t, protocol.ResourceBudgets.ReadTempBytes)
	require.Equal(t, 1.0, protocol.ResourceBudgets.MutationWALRatioUpper)

	require.Equal(t, 1.10, protocol.RawReadGates.P50RatioUpper)
	require.Equal(t, 1.20, protocol.RawReadGates.P95RatioUpper)
	require.Equal(t, 1000, protocol.RawReadGates.HighDegreeMinimumFanout)
	require.Equal(t, 0.95, protocol.RawReadGates.HighDegreeP50RatioUpper)
	require.False(t, protocol.RawReadGates.CandidateEvidence)
	require.Len(t, protocol.NextAuthorization.PassAuthorizes, 2)
	require.NotEmpty(t, protocol.NextAuthorization.PassDoesNotAuthorize)
}
