// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSuffixRouteCacheFeasibilityV1FreezesTransactionOnlySafety verifies the
// pre-implementation contract cannot silently widen cache ownership or permit
// writes before a dedicated feasibility implementation is reviewed.
func TestSuffixRouteCacheFeasibilityV1FreezesTransactionOnlySafety(t *testing.T) {
	contents, err := os.ReadFile("../../benchmark/testdata/scale/protocols/suffix_route_cache_feasibility_v1.json")
	require.NoError(t, err)

	var protocol struct {
		Schema            string `json:"schema"`
		Status            string `json:"status"`
		ProductionDefault string `json:"production_default"`
		Scope             struct {
			Backend     string `json:"backend"`
			Transaction string `json:"transaction"`
			Location    string `json:"cache_location"`
		} `json:"scope"`
		CacheKey struct {
			Required  []string `json:"required_components"`
			Snapshot  string   `json:"snapshot_rule"`
			OnMissing string   `json:"missing_or_unverifiable_component"`
		} `json:"cache_key"`
		Ownership struct {
			Allowed     string `json:"allowed_transactions"`
			Write       string `json:"write_or_savepoint_boundary"`
			Commit      string `json:"commit"`
			Rollback    string `json:"rollback_or_cancellation"`
			Retry       string `json:"retry"`
			PoolRelease string `json:"pool_reacquisition"`
		} `json:"ownership_and_invalidation"`
		MissHit struct {
			Miss string `json:"miss"`
			Hit  string `json:"hit"`
		} `json:"miss_and_hit_contract"`
		Resources struct {
			Entries    int64  `json:"maximum_entries_per_transaction"`
			TotalBytes int64  `json:"maximum_total_bytes_per_transaction"`
			EntryBytes int64  `json:"maximum_entry_bytes"`
			WAL        string `json:"read_path_wal"`
			State      string `json:"persistent_state"`
		} `json:"resource_and_write_boundary"`
		Acceptance map[string]bool `json:"acceptance"`
	}
	require.NoError(t, json.Unmarshal(contents, &protocol))

	require.Equal(t, "suffix-route-cache-feasibility-v1", protocol.Schema)
	require.Equal(t, "frozen_preimplementation_feasibility", protocol.Status)
	require.Equal(t, "off", protocol.ProductionDefault)
	require.Equal(t, "postgres_sql", protocol.Scope.Backend)
	require.Contains(t, protocol.Scope.Transaction, "read-only")
	require.Contains(t, protocol.Scope.Transaction, "repeatable_read")
	require.Contains(t, protocol.Scope.Location, "exactly one active PostgreSQL transaction")
	require.ElementsMatch(t, []string{
		"opaque transaction-owner token minted after BEGIN",
		"graph_id",
		"normalized Cypher shape fingerprint",
		"canonical parameter names, types, and values fingerprint",
		"frozen routing-policy identity and threshold version",
		"transaction-local invalidation generation",
	}, protocol.CacheKey.Required)
	require.Contains(t, protocol.CacheKey.Snapshot, "no process, pool, connection")
	require.Contains(t, protocol.CacheKey.OnMissing, "ordinary incumbent")
	require.Contains(t, protocol.Ownership.Allowed, "no savepoint lifecycle")
	require.Contains(t, protocol.Ownership.Write, "invalidate every entry")
	require.Contains(t, protocol.Ownership.Commit, "no entry survives commit")
	require.Contains(t, protocol.Ownership.Rollback, "must not publish")
	require.Equal(t, "forbidden; a replacement transaction receives a new owner token and an empty cache", protocol.Ownership.Retry)
	require.Contains(t, protocol.Ownership.PoolRelease, "cannot carry an entry")
	require.Contains(t, protocol.MissHit.Miss, "exact ordinary EXPANSION-STEPWISE-FORWARD incumbent")
	require.Contains(t, protocol.MissHit.Hit, "same active owner transaction")
	require.Equal(t, int64(64), protocol.Resources.Entries)
	require.Equal(t, int64(65536), protocol.Resources.TotalBytes)
	require.Equal(t, int64(4096), protocol.Resources.EntryBytes)
	require.Contains(t, protocol.Resources.WAL, "zero cache-attributable WAL")
	require.Contains(t, protocol.Resources.State, "forbidden")
	for _, requirement := range []string{
		"all_misses_incumbent_only",
		"all_hits_owner_snapshot_key_and_generation_bound",
		"all_transaction_end_and_invalidation_boundaries_empty_cache",
		"all_memory_limits_observed",
		"zero_cache_attributable_wal_and_persistent_state",
		"cancellation_and_rollback_replay_exact",
		"no_selector_or_translation_cache_change",
	} {
		require.True(t, protocol.Acceptance[requirement], requirement)
	}
	require.False(t, slices.Contains(protocol.CacheKey.Required, "backend_pid"))
}

func TestTopologySelectedRoutingV1FreezesDefaultOffSnapshotContract(t *testing.T) {
	contents, err := os.ReadFile("../../benchmark/testdata/scale/protocols/topology_selected_routing_v1.json")
	require.NoError(t, err)

	var protocol struct {
		Schema            string `json:"schema"`
		Status            string `json:"status"`
		ProductionDefault string `json:"production_default"`
		Transaction       struct {
			Isolation []string `json:"required_isolation"`
			ReadOnly  bool     `json:"read_only"`
			SameSnap  bool     `json:"same_snapshot_synopsis_read"`
		} `json:"transaction"`
		Selector struct {
			EstimatorVersion string `json:"estimator_version"`
			MaximumDensity   int64  `json:"maximum_edge_to_node_ratio_per_mille"`
			Comparison       string `json:"comparison"`
		} `json:"selector"`
		RouteCache struct {
			Scope       string   `json:"scope"`
			Entries     int64    `json:"maximum_entries"`
			TotalBytes  int64    `json:"maximum_total_bytes"`
			EntryBytes  int64    `json:"maximum_entry_bytes"`
			Miss        string   `json:"miss"`
			Invalidates []string `json:"invalidation"`
		} `json:"route_cache"`
		Execution struct {
			SingleArm bool   `json:"single_arm"`
			Fallback  string `json:"fallback"`
		} `json:"execution"`
	}
	require.NoError(t, json.Unmarshal(contents, &protocol))
	require.Equal(t, "topology-selected-routing-v1", protocol.Schema)
	require.Equal(t, "frozen_implementation_protocol", protocol.Status)
	require.Equal(t, "off", protocol.ProductionDefault)
	require.ElementsMatch(t, []string{"repeatable_read", "serializable"}, protocol.Transaction.Isolation)
	require.True(t, protocol.Transaction.ReadOnly)
	require.True(t, protocol.Transaction.SameSnap)
	require.Equal(t, "topology-fixed-suffix-counts-v1", protocol.Selector.EstimatorVersion)
	require.Equal(t, int64(1000), protocol.Selector.MaximumDensity)
	require.Contains(t, protocol.Selector.Comparison, "edge_count * 1000")
	require.Equal(t, "one_active_transaction", protocol.RouteCache.Scope)
	require.Equal(t, int64(64), protocol.RouteCache.Entries)
	require.Equal(t, int64(65536), protocol.RouteCache.TotalBytes)
	require.Equal(t, int64(4096), protocol.RouteCache.EntryBytes)
	require.Equal(t, "incumbent_only", protocol.RouteCache.Miss)
	require.Contains(t, protocol.RouteCache.Invalidates, "cancellation")
	require.True(t, protocol.Execution.SingleArm)
	require.Equal(t, "exact_forward_same_snapshot", protocol.Execution.Fallback)
}
