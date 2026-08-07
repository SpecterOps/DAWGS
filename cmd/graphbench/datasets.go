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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
)

const defaultGraphName = "integration_test"

func scanDatasetKinds(datasetDir string, datasetNames []string) (graph.Kinds, graph.Kinds, error) {
	var nodeKinds, edgeKinds graph.Kinds

	for _, datasetName := range datasetNames {
		doc, err := parseDataset(datasetDir, datasetName)
		if err != nil {
			return nil, nil, err
		}

		nextNodeKinds, nextEdgeKinds := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nextNodeKinds...)
		edgeKinds = edgeKinds.Add(nextEdgeKinds...)
	}

	return nodeKinds, edgeKinds, nil
}

func parseDataset(datasetDir, name string) (opengraph.Document, error) {
	if fixture := generatedDataset(name); fixture != nil {
		return opengraph.Document{Graph: *fixture}, nil
	}

	path := filepath.Join(datasetDir, name+".json")
	f, err := os.Open(path)
	if err != nil {
		return opengraph.Document{}, fmt.Errorf("open dataset %s: %w", name, err)
	}
	defer f.Close()

	doc, err := opengraph.ParseDocument(f)
	if err != nil {
		return opengraph.Document{}, fmt.Errorf("parse dataset %s: %w", name, err)
	}

	return doc, nil
}

func loadDataset(ctx context.Context, db graph.Database, datasetDir, name string) (opengraph.IDMap, error) {
	if fixture := generatedDataset(name); fixture != nil {
		return opengraph.WriteGraph(ctx, db, fixture)
	}

	path := filepath.Join(datasetDir, name+".json")
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset %s: %w", name, err)
	}
	defer f.Close()

	idMap, err := opengraph.Load(ctx, db, f)
	if err != nil {
		return nil, fmt.Errorf("load dataset %s: %w", name, err)
	}

	return idMap, nil
}

func generatedDataset(name string) *opengraph.Graph {
	var shortestDepth, shortestFanout int
	if matched, _ := fmt.Sscanf(name, testutil.ShortestPathScaleDataset+"_d%d_f%d", &shortestDepth, &shortestFanout); matched == 2 && shortestDepth >= 1 && shortestFanout >= 1 && name == fmt.Sprintf(testutil.ShortestPathScaleDataset+"_d%d_f%d", shortestDepth, shortestFanout) {
		return testutil.NewShortestPathScaleFixture(testutil.ShortestPathScaleConfig{Depth: shortestDepth, Fanout: shortestFanout})
	}
	var adcsDepth, adcsFanout, adcsValidEvery, adcsPayload int
	if matched, _ := fmt.Sscanf(name, testutil.ADCSScaleDataset+"_d%d_f%d_v%d_p%d", &adcsDepth, &adcsFanout, &adcsValidEvery, &adcsPayload); matched == 4 && adcsDepth >= 0 && adcsFanout >= 1 && adcsValidEvery >= 1 && adcsPayload >= 0 && name == fmt.Sprintf(testutil.ADCSScaleDataset+"_d%d_f%d_v%d_p%d", adcsDepth, adcsFanout, adcsValidEvery, adcsPayload) {
		return testutil.NewADCSScaleFixture(testutil.ADCSScaleConfig{
			MemberOfDepth: adcsDepth, Fanout: adcsFanout, ValidSuffixEvery: adcsValidEvery, PropertyPayloadSize: adcsPayload,
		})
	}
	if config, ok := parseADCSV2DatasetName(name); ok {
		return testutil.NewADCSScaleFixture(config)
	}
	switch name {
	case testutil.ReconciliationScaleDataset:
		return testutil.NewReconciliationScaleFixture(128)
	case testutil.TrustPruningScaleDataset:
		return testutil.NewTrustPruningScaleFixture(128)
	case testutil.HopScaleDataset:
		return testutil.NewHopScaleFixture(128)
	case testutil.ScanLookupScaleDataset:
		return testutil.NewScanLookupScaleFixture(128)
	case testutil.ShortestPathScaleDataset:
		return testutil.NewShortestPathScaleFixture(testutil.ShortestPathScaleConfig{Depth: 16, Fanout: 128})
	case testutil.ADCSScaleDataset:
		return testutil.NewADCSScaleFixture(testutil.ADCSScaleConfig{MemberOfDepth: 8, Fanout: 100, ValidSuffixEvery: 10, PropertyPayloadSize: 4096})
	default:
		return nil
	}
}

type FixtureMetadata struct {
	Dataset           string                   `json:"dataset"`
	Checksum          string                   `json:"checksum"`
	NodeCount         int                      `json:"node_count"`
	EdgeCount         int                      `json:"edge_count"`
	PhysicalValidated bool                     `json:"physical_cardinality_validated,omitempty"`
	PhysicalNodeCount int64                    `json:"physical_node_count,omitempty"`
	PhysicalEdgeCount int64                    `json:"physical_edge_count,omitempty"`
	NodeRelationBytes int64                    `json:"node_relation_bytes,omitempty"`
	EdgeRelationBytes int64                    `json:"edge_relation_bytes,omitempty"`
	Configuration     string                   `json:"configuration,omitempty"`
	ADCS              *ADCSFixtureExpectations `json:"adcs,omitempty"`
}

type ADCSFixtureExpectations struct {
	RootSourceRows         int64 `json:"root_source_rows"`
	DistinctRoots          int64 `json:"distinct_roots"`
	ForwardMemberStates    int64 `json:"forward_member_states"`
	SuffixRows             int64 `json:"suffix_rows"`
	DistinctBoundaries     int64 `json:"distinct_boundaries"`
	ReachableBoundaries    int64 `json:"reachable_boundaries"`
	DisconnectedBoundaries int64 `json:"disconnected_boundaries"`
	ExpectedReverseStates  int64 `json:"expected_reverse_states"`
	CompleteOutputTrails   int64 `json:"complete_output_trails"`
}

func fixtureMetadata(datasetDir, name string) (FixtureMetadata, error) {
	doc, err := parseDataset(datasetDir, name)
	if err != nil {
		return FixtureMetadata{}, err
	}
	raw, err := json.Marshal(doc.Graph)
	if err != nil {
		return FixtureMetadata{}, fmt.Errorf("encode dataset %s for checksum: %w", name, err)
	}
	digest := sha256.Sum256(raw)
	configuration := "file"
	if generatedDataset(name) != nil {
		configuration = name
	}
	metadata := FixtureMetadata{
		Dataset: name, Checksum: hex.EncodeToString(digest[:]), NodeCount: len(doc.Graph.Nodes), EdgeCount: len(doc.Graph.Edges), Configuration: configuration,
	}
	if config, ok := parseADCSV2DatasetName(name); ok {
		metadata.ADCS = adcsFixtureExpectations(config)
	}
	return metadata, nil
}

func parseADCSV2DatasetName(name string) (testutil.ADCSScaleConfig, bool) {
	var depth, fanout, reachable, disconnected, fanIn, multiplicity, zeroDepth, payload int
	format := testutil.ADCSScaleDataset + "_v2_d%d_f%d_r%d_x%d_i%d_m%d_z%d_p%d"
	matched, _ := fmt.Sscanf(name, format, &depth, &fanout, &reachable, &disconnected, &fanIn, &multiplicity, &zeroDepth, &payload)
	if matched != 8 || depth < 0 || fanout < 1 || reachable < 0 || reachable > fanout || disconnected < 0 || fanIn < 0 || multiplicity < 1 || (zeroDepth != 0 && zeroDepth != 1) || payload < 0 || name != fmt.Sprintf(format, depth, fanout, reachable, disconnected, fanIn, multiplicity, zeroDepth, payload) {
		return testutil.ADCSScaleConfig{}, false
	}
	rootSuffix := zeroDepth == 1
	return testutil.ADCSScaleConfig{
		MemberOfDepth: depth, Fanout: fanout, ExactReachableSuffixSources: &reachable,
		DisconnectedSuffixSources: disconnected, ReverseFanIn: fanIn,
		SuffixPathsPerBoundary: multiplicity, RootMatchCount: 1,
		RootHasZeroDepthSuffix: &rootSuffix, PropertyPayloadSize: payload,
	}, true
}

func adcsFixtureExpectations(config testutil.ADCSScaleConfig) *ADCSFixtureExpectations {
	reachable := 0
	if config.ExactReachableSuffixSources != nil {
		reachable = *config.ExactReachableSuffixSources
	}
	rootSuffix := config.RootHasZeroDepthSuffix != nil && *config.RootHasZeroDepthSuffix
	zero := 0
	if rootSuffix {
		zero = 1
	}
	multiplicity := max(config.SuffixPathsPerBoundary, 1)
	rootCount := max(config.RootMatchCount, 1)
	productiveFanIn := 0
	if zero+reachable > 0 {
		productiveFanIn = config.ReverseFanIn
	}
	return &ADCSFixtureExpectations{
		RootSourceRows: int64(rootCount), DistinctRoots: int64(rootCount),
		ForwardMemberStates: int64(rootCount + config.Fanout*config.MemberOfDepth),
		SuffixRows:          int64((zero + reachable + config.DisconnectedSuffixSources) * multiplicity),
		DistinctBoundaries:  int64(zero + reachable + config.DisconnectedSuffixSources),
		ReachableBoundaries: int64(zero + reachable), DisconnectedBoundaries: int64(config.DisconnectedSuffixSources),
		ExpectedReverseStates: int64(zero + reachable*(config.MemberOfDepth+1) + config.DisconnectedSuffixSources + productiveFanIn),
		CompleteOutputTrails:  int64((zero + reachable) * multiplicity),
	}
}

func clearGraph(ctx context.Context, db graph.Database) error {
	if pgDriver, isPostgres := db.(*pg.Driver); isPostgres {
		graphTarget, hasDefaultGraph := pgDriver.DefaultGraph()
		if !hasDefaultGraph {
			return fmt.Errorf("PostgreSQL default graph is not set")
		}

		return clearPostgresGraph(ctx, db, graphTarget.ID)
	}

	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Relationships().Delete(); err != nil {
			return fmt.Errorf("delete relationships: %w", err)
		}

		if err := tx.Nodes().Delete(); err != nil {
			return fmt.Errorf("delete nodes: %w", err)
		}

		return nil
	})
}

func clearPostgresGraph(ctx context.Context, db graph.Database, graphID int32) error {
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		// Truncate the active child partitions together. The high-level
		// relationship query cannot see an already-orphaned edge, while DELETE
		// leaves heap and index size dependent on earlier benchmark fixtures.
		// Naming the children also avoids the node parent's cross-graph trigger.
		statement := fmt.Sprintf("truncate table edge_%d, node_%d", graphID, graphID)
		result := tx.Raw(statement, nil)
		result.Close()
		if err := result.Error(); err != nil {
			return fmt.Errorf("execute PostgreSQL graph reset: %w", err)
		}

		return nil
	})
}

func benchmarkSchema(nodeKinds, edgeKinds graph.Kinds) graph.Schema {
	return graph.Schema{
		Graphs: []graph.Graph{{
			Name:  defaultGraphName,
			Nodes: nodeKinds,
			Edges: edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: defaultGraphName},
	}
}

func resolveCaseParams(testCase ScaleCase, idMap opengraph.IDMap) (map[string]any, error) {
	return resolveParams(testCase.Name, testCase.Params, testCase.NodeParams, testCase.NodeListParams, testCase.GeneratedNodeListParams, idMap)
}

func resolveParams(caseName string, rawParams map[string]any, nodeParams map[string]string, nodeListParams map[string][]string, generatedNodeListParams map[string]testutil.GeneratedNodeListParam, idMap opengraph.IDMap) (map[string]any, error) {
	params := make(map[string]any, len(rawParams)+len(nodeParams)+len(nodeListParams)+len(generatedNodeListParams))
	for key, value := range rawParams {
		params[key] = value
	}

	for paramName, nodeName := range nodeParams {
		id, found := idMap[nodeName]
		if !found {
			return nil, fmt.Errorf("case %s references unknown dataset node %q", caseName, nodeName)
		}

		params[paramName] = id.Int64()
	}

	for paramName, nodeNames := range nodeListParams {
		ids := make([]int64, len(nodeNames))
		for idx, nodeName := range nodeNames {
			id, found := idMap[nodeName]
			if !found {
				return nil, fmt.Errorf("case %s references unknown dataset node %q in list parameter %q", caseName, nodeName, paramName)
			}

			ids[idx] = id.Int64()
		}

		params[paramName] = ids
	}

	for paramName, spec := range generatedNodeListParams {
		if spec.Count < 0 {
			return nil, fmt.Errorf("case %s generated node list parameter %q has negative count", caseName, paramName)
		}

		nodeNames := append([]string(nil), spec.Include...)
		nodeNames = append(nodeNames, testutil.FixtureNames(spec.Prefix, spec.Count)...)
		ids := make([]int64, len(nodeNames))
		for idx, nodeName := range nodeNames {
			id, found := idMap[nodeName]
			if !found {
				return nil, fmt.Errorf("case %s references unknown dataset node %q in generated list parameter %q", caseName, nodeName, paramName)
			}
			ids[idx] = id.Int64()
		}
		params[paramName] = ids
	}

	if len(params) == 0 {
		return nil, nil
	}

	return params, nil
}

func resolveWriteScenario(testCase ScaleCase, idMap opengraph.IDMap) (resolvedWriteScenario, error) {
	if testCase.WriteScenario == nil {
		return resolvedWriteScenario{}, nil
	}

	scenario := testCase.WriteScenario
	if scenario.ExpectedMatched == nil || scenario.ExpectedAffected == nil {
		return resolvedWriteScenario{}, fmt.Errorf("case %s has an incomplete write scenario", testCase.Name)
	}
	selectionParams, err := resolveParams(testCase.Name+" selection", scenario.Params, scenario.NodeParams, scenario.NodeListParams, scenario.GeneratedNodeListParams, idMap)
	if err != nil {
		return resolvedWriteScenario{}, err
	}

	resolved := resolvedWriteScenario{
		SelectionCypher:  scenario.SelectionCypher,
		SelectionParams:  selectionParams,
		AffectedEntity:   scenario.AffectedEntity,
		ExpectedMatched:  *scenario.ExpectedMatched,
		ExpectedAffected: *scenario.ExpectedAffected,
	}

	for _, postState := range scenario.PostState {
		params, err := resolveParams(testCase.Name+" post-state "+postState.Name, postState.Params, postState.NodeParams, postState.NodeListParams, postState.GeneratedNodeListParams, idMap)
		if err != nil {
			return resolvedWriteScenario{}, err
		}

		resolved.PostState = append(resolved.PostState, resolvedStateQuery{
			Name:     postState.Name,
			Cypher:   postState.Cypher,
			Params:   params,
			Expected: postState.Expected,
		})
	}

	return resolved, nil
}
