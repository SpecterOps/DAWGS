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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
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
	Version         int                     `json:"version"`
	Candidate       string                  `json:"candidate"`
	SelectorVersion string                  `json:"selector_version"`
	Caps            map[string]int64        `json:"caps"`
	Buckets         []benchmarkPolicyBucket `json:"buckets"`
}

type benchmarkPolicyBucket struct {
	Name                  string   `json:"name"`
	QuerySHA256           []string `json:"query_sha256"`
	Direction             string   `json:"direction"`
	ObservationMode       string   `json:"observation_mode"`
	MinimumDepth          int64    `json:"minimum_depth"`
	MaximumDepth          int64    `json:"maximum_depth"`
	RelationshipKindCount int      `json:"relationship_kind_count"`
	UntypedRelationship   bool     `json:"untyped_relationship"`
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
	raw, manifest, err := loadBenchmarkTraversalPromotionManifest(path)
	if err != nil {
		return pg.TraversalPolicy{}, err
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

func loadBenchmarkTraversalPromotionManifest(path string) ([]byte, benchmarkTraversalPromotionManifest, error) {
	if path == "" {
		return nil, benchmarkTraversalPromotionManifest{}, fmt.Errorf("traversal policy manifest path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, benchmarkTraversalPromotionManifest{}, fmt.Errorf("read traversal policy manifest: %w", err)
	}
	var manifest benchmarkTraversalPromotionManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, benchmarkTraversalPromotionManifest{}, fmt.Errorf("decode traversal policy manifest binding: %w", err)
	}
	return raw, manifest, nil
}

// productionOptions derives the production translation input from the one
// exact query bucket in a provisional manifest. It is used only for SQL-anchor
// preflight; SetTraversalPolicy remains the only authorization path.
func (s benchmarkTraversalPromotionManifest) productionOptions(cypherQuery string) (translate.ProductionOptions, error) {
	if s.Candidate == "" || s.SelectorVersion == "" {
		return translate.ProductionOptions{}, fmt.Errorf("provisional traversal policy manifest requires candidate and selector version")
	}
	digest := pg.TraversalPolicyQuerySHA256(cypherQuery)
	for _, bucket := range s.Buckets {
		for _, allowed := range bucket.QuerySHA256 {
			if allowed != digest {
				continue
			}
			return translate.ProductionOptions{
				ShortestPathExecutor: optimize.ShortestPathExecutor(s.Candidate),
				ShortestPathCaps: &translate.ProductionShortestPathCaps{
					StateLimit:       s.Caps["state_limit"],
					FrontierLimit:    s.Caps["frontier_limit"],
					PredecessorLimit: s.Caps["predecessor_limit"],
					EnumerationLimit: s.Caps["enumeration_limit"],
					OutputBytesLimit: s.Caps["output_bytes_limit"],
				},
				AuthorizedBucket: &translate.ProductionTraversalBucket{
					Direction:             bucket.Direction,
					ObservationMode:       bucket.ObservationMode,
					MinimumDepth:          bucket.MinimumDepth,
					MaximumDepth:          bucket.MaximumDepth,
					RelationshipKindCount: bucket.RelationshipKindCount,
					UntypedRelationship:   bucket.UntypedRelationship,
				},
				SelectorVersion: s.SelectorVersion,
			}, nil
		}
	}
	return translate.ProductionOptions{}, fmt.Errorf("query SHA-256 %s is absent from provisional traversal policy manifest", digest)
}

// TraversalPolicyPreflight is a non-promotional record used to bind a formal
// manifest's SQL anchor after the benchmark graph and parameters are known.
type TraversalPolicyPreflight struct {
	Candidate       string                        `json:"candidate"`
	SelectorVersion string                        `json:"selector_version"`
	QuerySHA256     string                        `json:"query_sha256"`
	SQLSHA256       string                        `json:"operational_candidate_sql_sha256"`
	GraphID         int32                         `json:"graph_id"`
	Optimization    translate.OptimizationSummary `json:"optimization"`
}

// writeTraversalPolicyPreflight creates a new provenance record without ever
// replacing a provisional manifest or an earlier capture. A repeated preflight
// must use a new path so the record's filesystem lifetime remains one-to-one
// with the invocation that produced it.
func writeTraversalPolicyPreflight(manifestPath, outputPath string, preflight TraversalPolicyPreflight) error {
	manifestInfo, err := os.Stat(manifestPath)
	if err != nil {
		return fmt.Errorf("stat provisional traversal policy manifest: %w", err)
	}
	if outputInfo, err := os.Stat(outputPath); err == nil {
		if os.SameFile(manifestInfo, outputInfo) {
			return fmt.Errorf("preflight output must not overwrite the provisional traversal policy manifest")
		}
		return fmt.Errorf("preflight output already exists: %s", outputPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat preflight output: %w", err)
	}

	encoded, err := json.MarshalIndent(preflight, "", "  ")
	if err != nil {
		return fmt.Errorf("encode traversal policy preflight: %w", err)
	}
	output, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create preflight output: %w", err)
	}
	if _, err := output.Write(append(encoded, '\n')); err != nil {
		_ = output.Close()
		_ = os.Remove(outputPath)
		return fmt.Errorf("write preflight output: %w", err)
	}
	if err := output.Close(); err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("close preflight output: %w", err)
	}
	return nil
}

func renderTraversalPolicyPreflight(ctx context.Context, mapper pg.KindMapper, target model.Graph, scenario Scenario, manifest benchmarkTraversalPromotionManifest) (TraversalPolicyPreflight, error) {
	options, err := manifest.productionOptions(scenario.Cypher)
	if err != nil {
		return TraversalPolicyPreflight{}, err
	}
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), scenario.Cypher)
	if err != nil {
		return TraversalPolicyPreflight{}, fmt.Errorf("parse manifest-authorized Cypher: %w", err)
	}
	translation, err := translate.TranslateWithProductionOptions(ctx, regularQuery, mapper, scenario.Parameters, target.ID, options)
	if err != nil {
		return TraversalPolicyPreflight{}, fmt.Errorf("translate manifest-authorized Cypher: %w", err)
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		return TraversalPolicyPreflight{}, fmt.Errorf("render manifest-authorized SQL: %w", err)
	}
	return TraversalPolicyPreflight{
		Candidate:       manifest.Candidate,
		SelectorVersion: manifest.SelectorVersion,
		QuerySHA256:     pg.TraversalPolicyQuerySHA256(scenario.Cypher),
		SQLSHA256:       sqlFingerprint(sqlQuery),
		GraphID:         target.ID,
		Optimization:    translation.Optimization,
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
