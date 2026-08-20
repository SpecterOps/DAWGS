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
	"encoding/json"
	"fmt"
	"os"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg"
)

// traversalPolicyBenchmarkDriver exposes the production policy installation
// seam required by a policy-path benchmark.
type traversalPolicyBenchmarkDriver interface {
	SetTraversalPolicy(pg.TraversalPolicy) error
}

// benchmarkTraversalPromotionManifest retains only the manifest fields needed
// to assemble the driver's public TraversalPolicy. The driver independently
// decodes and validates the complete raw document before accepting it.
type benchmarkTraversalPromotionManifest struct {
	Candidate string `json:"candidate"`
	Buckets   []struct {
		QuerySHA256 []string `json:"query_sha256"`
	} `json:"buckets"`
}

// loadBenchmarkTraversalPolicy loads an immutable manifest verbatim and binds
// its exact bytes, candidate, and query authorization set into a V2 policy.
// It intentionally does not attempt to verify qualification evidence; that is
// the driver's strict installation contract and the GraphBench verifier's job.
func loadBenchmarkTraversalPolicy(path string, generation uint64) (pg.TraversalPolicy, error) {
	if path == "" {
		return pg.TraversalPolicy{}, fmt.Errorf("traversal policy manifest path is required")
	}
	if generation == 0 {
		return pg.TraversalPolicy{}, fmt.Errorf("traversal policy generation must be nonzero")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return pg.TraversalPolicy{}, fmt.Errorf("read traversal policy manifest: %w", err)
	}
	var manifest benchmarkTraversalPromotionManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return pg.TraversalPolicy{}, fmt.Errorf("decode traversal policy manifest binding: %w", err)
	}
	if manifest.Candidate == "" {
		return pg.TraversalPolicy{}, fmt.Errorf("traversal policy manifest candidate is required")
	}

	queries := make([]string, 0)
	for _, bucket := range manifest.Buckets {
		queries = append(queries, bucket.QuerySHA256...)
	}
	if len(queries) == 0 {
		return pg.TraversalPolicy{}, fmt.Errorf("traversal policy manifest must authorize at least one query")
	}

	digest := sha256.Sum256(raw)
	return pg.TraversalPolicy{
		Generation:              generation,
		PromotionManifestSHA256: hex.EncodeToString(digest[:]),
		PromotionManifestJSON:   raw,
		QuerySHA256Allowlist:    queries,
		ShortestPathExecutor:    optimize.ShortestPathExecutor(manifest.Candidate),
	}, nil
}

// selectTraversalPolicyScenarios selects the sole exact Cypher query that a
// promotion manifest authorizes. Current production canaries intentionally
// authorize exactly one query, so accepting zero or multiple scenarios would
// make a benchmark's reported policy path ambiguous.
func selectTraversalPolicyScenarios(scenarios []Scenario, policy pg.TraversalPolicy) ([]Scenario, error) {
	allowed := make(map[string]struct{}, len(policy.QuerySHA256Allowlist))
	for _, digest := range policy.QuerySHA256Allowlist {
		allowed[digest] = struct{}{}
	}

	selected := make([]Scenario, 0, 1)
	for _, scenario := range scenarios {
		if scenario.Cypher == "" {
			continue
		}
		if _, found := allowed[pg.TraversalPolicyQuerySHA256(scenario.Cypher)]; found {
			selected = append(selected, scenario)
		}
	}
	if len(selected) != 1 {
		return nil, fmt.Errorf("traversal policy must match exactly one scenario in the selected dataset, matched %d", len(selected))
	}
	return selected, nil
}
