// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/stretchr/testify/require"
)

func TestLoadBenchmarkTraversalPolicyBindsExactManifestBytes(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	raw := []byte(`{"candidate":"ASP-I1-U-DAG+MAT-M0","buckets":[{"query_sha256":["` + pg.TraversalPolicyQuerySHA256(query) + `"]}]}`)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	policy, err := loadBenchmarkTraversalPolicy(path, 42)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	require.Equal(t, uint64(42), policy.Generation)
	require.Equal(t, hex.EncodeToString(digest[:]), policy.PromotionManifestSHA256)
	require.Equal(t, raw, []byte(policy.PromotionManifestJSON))
	require.Equal(t, []string{pg.TraversalPolicyQuerySHA256(query)}, policy.QuerySHA256Allowlist)
	require.Equal(t, optimize.ShortestPathExecutorASPI1DAG, policy.ShortestPathExecutor)
}

func TestLoadBenchmarkTraversalPolicyRejectsMissingInputs(t *testing.T) {
	_, err := loadBenchmarkTraversalPolicy("", 1)
	require.ErrorContains(t, err, "path is required")

	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"candidate":"ASP-I1-U-DAG+MAT-M0","buckets":[]}`), 0o600))
	_, err = loadBenchmarkTraversalPolicy(path, 0)
	require.ErrorContains(t, err, "generation must be nonzero")
	_, err = loadBenchmarkTraversalPolicy(path, 1)
	require.ErrorContains(t, err, "must authorize at least one query")
}

func TestSelectTraversalPolicyScenariosRequiresOneExactMatch(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	policy := pg.TraversalPolicy{QuerySHA256Allowlist: []string{pg.TraversalPolicyQuerySHA256(query)}}
	scenarios := []Scenario{
		cypherScenario("Traversal", "fixture", "other", "MATCH (n) RETURN n"),
		cypherScenarioWithParameters("Shortest Paths", "fixture", "candidate", query, map[string]any{"start_id": 1, "end_id": 2}),
	}

	selected, err := selectTraversalPolicyScenarios(scenarios, policy)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "candidate", selected[0].Label)

	_, err = selectTraversalPolicyScenarios(scenarios[:1], policy)
	require.ErrorContains(t, err, "matched 0")
	_, err = selectTraversalPolicyScenarios(append(scenarios, scenarios[1]), policy)
	require.ErrorContains(t, err, "matched 2")
}
