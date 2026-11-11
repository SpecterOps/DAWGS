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
	"strings"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
)

// defaultGraphName names the isolated graph populated with benchmark fixtures.
const defaultGraphName = "integration_test"

// scanDatasetKinds enumerates dataset kinds without changing the source data.
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

// parseDataset decodes a fixture document or dispatches to the requested generated dataset builder.
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

// loadDataset decodes and loads a named fixture dataset into an empty graph.
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

// generatedDataset constructs a named generated fixture and its shape-specific expectations.
func generatedDataset(name string) *opengraph.Graph {
	if config, ok := parseEndpointSeededExpansionDatasetName(name); ok {
		return testutil.NewEndpointSeededExpansionScaleFixture(config)
	}
	if config, ok := parseShortestPathV2DatasetName(name); ok {
		return testutil.NewShortestPathScaleV2Fixture(config)
	}
	var shortestDepth, shortestFanout int
	if matched, _ := fmt.Sscanf(name, testutil.ShortestPathScaleDataset+"_d%d_f%d", &shortestDepth, &shortestFanout); matched == 2 && shortestDepth >= 1 && shortestFanout >= 1 && name == fmt.Sprintf(testutil.ShortestPathScaleDataset+"_d%d_f%d", shortestDepth, shortestFanout) {
		return testutil.NewShortestPathScaleFixture(testutil.ShortestPathScaleConfig{
			Depth:  shortestDepth,
			Fanout: shortestFanout,
		})
	}
	var expansionDepth, expansionFanout, validSuffixEvery, expansionPayload int
	if matched, _ := fmt.Sscanf(name, testutil.FixedSuffixExpansionScaleDataset+"_d%d_f%d_v%d_p%d", &expansionDepth, &expansionFanout, &validSuffixEvery, &expansionPayload); matched == 4 && expansionDepth >= 0 && expansionFanout >= 1 && validSuffixEvery >= 1 && expansionPayload >= 0 && name == fmt.Sprintf(testutil.FixedSuffixExpansionScaleDataset+"_d%d_f%d_v%d_p%d", expansionDepth, expansionFanout, validSuffixEvery, expansionPayload) {
		return testutil.NewFixedSuffixExpansionScaleFixture(testutil.FixedSuffixExpansionScaleConfig{
			ExpansionDepth:      expansionDepth,
			Fanout:              expansionFanout,
			ValidSuffixEvery:    validSuffixEvery,
			PropertyPayloadSize: expansionPayload,
		})
	}
	if config, ok := parseFixedSuffixExpansionV3DatasetName(name); ok {
		return testutil.NewFixedSuffixExpansionScaleFixture(config)
	}
	if config, ok := parseFixedSuffixExpansionV2DatasetName(name); ok {
		return testutil.NewFixedSuffixExpansionScaleFixture(config)
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
		return testutil.NewShortestPathScaleFixture(testutil.ShortestPathScaleConfig{
			Depth:  16,
			Fanout: 128,
		})
	case testutil.FixedSuffixExpansionScaleDataset:
		return testutil.NewFixedSuffixExpansionScaleFixture(testutil.FixedSuffixExpansionScaleConfig{
			ExpansionDepth:      8,
			Fanout:              100,
			ValidSuffixEvery:    10,
			PropertyPayloadSize: 4096,
		})
	default:
		return nil
	}
}

// FixtureMetadata captures fixture cardinalities, checksums, and generated-shape expectations.
type FixtureMetadata struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Checksum identifies the fixture's canonical logical node and relationship contents.
	Checksum string `json:"checksum"`
	// NodeCount records logical fixture nodes declared or loaded.
	NodeCount int `json:"node_count"`
	// EdgeCount records logical fixture relationships declared or loaded.
	EdgeCount int `json:"edge_count"`
	// PhysicalValidated reports whether live database counts and checksum matched fixture metadata.
	PhysicalValidated bool `json:"physical_cardinality_validated,omitempty"`
	// PhysicalNodeCount records physical node rows present in the backend fixture.
	PhysicalNodeCount int64 `json:"physical_node_count,omitempty"`
	// PhysicalEdgeCount records physical relationship rows present in the backend fixture.
	PhysicalEdgeCount int64 `json:"physical_edge_count,omitempty"`
	// NodeRelationBytes supplies the node relation bytes input to the FixtureMetadata contract.
	NodeRelationBytes int64 `json:"node_relation_bytes,omitempty"`
	// EdgeRelationBytes supplies the edge relation bytes input to the FixtureMetadata contract.
	EdgeRelationBytes int64 `json:"edge_relation_bytes,omitempty"`
	// Configuration captures the generator parameters that define the fixture shape.
	Configuration string `json:"configuration,omitempty"`
	// Shortest contains expectations derived from a generated shortest-path fixture.
	Shortest *ShortestFixtureExpectations `json:"shortest,omitempty"`
	// FixedSuffixExpansion contains expectations derived from a fixed-suffix expansion fixture.
	FixedSuffixExpansion *FixedSuffixExpansionFixtureExpectations `json:"fixed_suffix_expansion,omitempty"`
	// EndpointSeededExpansion contains expectations derived from an endpoint-seeded expansion fixture.
	EndpointSeededExpansion *EndpointSeededExpansionFixtureExpectations `json:"endpoint_seeded_expansion,omitempty"`
}

// ShortestFixtureExpectations records expected distances, witnesses, and intermediate state for shortest-path fixtures.
type ShortestFixtureExpectations struct {
	// RootForwardDegree records outgoing relationships incident to the traversal root.
	RootForwardDegree int64 `json:"root_forward_degree"`
	// RootReverseDegree records incoming relationships incident to the traversal root.
	RootReverseDegree int64 `json:"root_reverse_degree"`
	// MaximumIntermediateForwardByLevel maps traversal depth to the largest expected forward frontier.
	MaximumIntermediateForwardByLevel map[string]int64 `json:"maximum_intermediate_forward_by_level"`
	// MaximumIntermediateReverseByLevel maps traversal depth to the largest expected reverse frontier.
	MaximumIntermediateReverseByLevel map[string]int64 `json:"maximum_intermediate_reverse_by_level"`
	// PhysicalTraversableEdgesByKind maps relationship kind to physical traversable edge count.
	PhysicalTraversableEdgesByKind map[string]int64 `json:"physical_traversable_edges_by_kind"`
	// DistinctReachableNodesByLevel maps traversal depth to distinct reachable node count.
	DistinctReachableNodesByLevel map[string]int64 `json:"distinct_reachable_nodes_by_level"`
	// ExpectedMinimumDistance supplies the expected minimum distance input to the ShortestFixtureExpectations contract.
	ExpectedMinimumDistance int64 `json:"expected_minimum_distance"`
	// ExpectedOnePathCardinality supplies the expected one path cardinality input to the ShortestFixtureExpectations contract.
	ExpectedOnePathCardinality int64 `json:"expected_one_path_cardinality"`
	// ExpectedAllShortestCardinality supplies the expected all shortest cardinality input to the ShortestFixtureExpectations contract.
	ExpectedAllShortestCardinality int64 `json:"expected_all_shortest_cardinality"`
	// ExpectedPredecessorEdges records predecessor edges expected in the shortest-path DAG.
	ExpectedPredecessorEdges int64 `json:"expected_relationship_distinct_predecessor_edges"`
	// DisconnectedStateCardinality records recursive states belonging to disconnected shortest-path regions.
	DisconnectedStateCardinality int64 `json:"disconnected_state_cardinality"`
	// ParallelPhysicalEdges records physical parallel relationships in the generated fixture.
	ParallelPhysicalEdges int64 `json:"parallel_physical_edges"`
	// ParallelDistinctTargets records distinct targets reached by parallel fixture edges.
	ParallelDistinctTargets int64 `json:"parallel_distinct_targets"`
}

// FixedSuffixExpansionFixtureExpectations records expected state and output sizes for fixed-suffix expansion fixtures.
type FixedSuffixExpansionFixtureExpectations struct {
	// RootSourceRows records rows selected as expansion roots.
	RootSourceRows int64 `json:"root_source_rows"`
	// DistinctRoots records unique root nodes in the generated fixture.
	DistinctRoots int64 `json:"distinct_roots"`
	// ForwardExpansionStates records recursive states visited by forward fixed-suffix expansion.
	ForwardExpansionStates int64 `json:"forward_expansion_states"`
	// SuffixRows records rows belonging to the fixed suffix of generated paths.
	SuffixRows int64 `json:"suffix_rows"`
	// DistinctBoundaries records unique terminal boundaries in the generated fixture.
	DistinctBoundaries int64 `json:"distinct_boundaries"`
	// ReachableBoundaries records terminal boundaries reachable in the generated fixture.
	ReachableBoundaries int64 `json:"reachable_boundaries"`
	// DisconnectedBoundaries records terminal boundaries intentionally disconnected from traversal roots.
	DisconnectedBoundaries int64 `json:"disconnected_boundaries"`
	// ExpectedReverseStates supplies the expected reverse states input to the FixedSuffixExpansionFixtureExpectations contract.
	ExpectedReverseStates int64 `json:"expected_reverse_states"`
	// CompleteOutputTrails records output trails before fixture eligibility filters are applied.
	CompleteOutputTrails int64 `json:"complete_output_trails"`
	// ProductiveBoundaryCycleEdges records the two relationship-distinct Expand
	// relationships forming the optional productive-boundary cycle.
	ProductiveBoundaryCycleEdges int64 `json:"productive_boundary_cycle_edges,omitempty"`
	// ProductiveBoundarySelfLoopEdges records the optional productive-boundary
	// Expand self-loop.
	ProductiveBoundarySelfLoopEdges int64 `json:"productive_boundary_self_loop_edges,omitempty"`
}

// EndpointSeededExpansionFixtureExpectations records expected state and output sizes for endpoint-seeded expansion fixtures.
type EndpointSeededExpansionFixtureExpectations struct {
	// MatchingEndpoints records endpoints satisfying the generated fixture predicate.
	MatchingEndpoints int64 `json:"matching_endpoints"`
	// OtherEndpoints records nonmatching endpoint nodes in an endpoint-seeded fixture.
	OtherEndpoints int64 `json:"other_endpoints"`
	// EligiblePrefixRows records prefix rows that can connect to the required suffix.
	EligiblePrefixRows int64 `json:"eligible_prefix_rows"`
	// MatchingIneligibleLanes records matching lanes excluded by endpoint eligibility filters.
	MatchingIneligibleLanes int64 `json:"matching_ineligible_lanes"`
	// ExpectedReverseStates supplies the expected reverse states input to the EndpointSeededExpansionFixtureExpectations contract.
	ExpectedReverseStates int64 `json:"expected_reverse_states"`
	// ExpectedOutputTrails records result trails expected from the generated expansion fixture.
	ExpectedOutputTrails int64 `json:"expected_output_trails"`
}

// fixtureMetadata derives fixture counts, checksums, and generated-shape expectations from a graph.
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
		Dataset:       name,
		Checksum:      hex.EncodeToString(digest[:]),
		NodeCount:     len(doc.Graph.Nodes),
		EdgeCount:     len(doc.Graph.Edges),
		Configuration: configuration,
	}
	if config, ok := parseFixedSuffixExpansionV3DatasetName(name); ok {
		metadata.FixedSuffixExpansion = fixedSuffixExpansionV3FixtureExpectations(doc.Graph, config)
	} else if config, ok := parseFixedSuffixExpansionV2DatasetName(name); ok {
		metadata.FixedSuffixExpansion = fixedSuffixExpansionV2FixtureExpectations(config)
	}
	if config, ok := parseShortestPathV2DatasetName(name); ok {
		metadata.Shortest = shortestFixtureExpectations(doc.Graph, config)
	}
	if config, ok := parseEndpointSeededExpansionDatasetName(name); ok {
		metadata.EndpointSeededExpansion = endpointSeededExpansionFixtureExpectations(doc.Graph, config)
	}
	return metadata, nil
}

// parseEndpointSeededExpansionDatasetName decodes and validates every scale parameter embedded in an endpoint-seeded dataset name.
func parseEndpointSeededExpansionDatasetName(name string) (testutil.EndpointSeededExpansionScaleConfig, bool) {
	var depth, matchingEndpoints, otherEndpoints, matchingEligible, otherEligible, matchingIneligible, parallel, cycle, payload int
	format := testutil.EndpointSeededExpansionScaleDataset + "_d%d_e%d_q%d_w%d_o%d_x%d_m%d_c%d_p%d"
	matched, _ := fmt.Sscanf(name, format, &depth, &matchingEndpoints, &otherEndpoints, &matchingEligible, &otherEligible, &matchingIneligible, &parallel, &cycle, &payload)
	config := testutil.EndpointSeededExpansionScaleConfig{
		Depth:                   depth,
		MatchingEndpoints:       matchingEndpoints,
		OtherEndpoints:          otherEndpoints,
		MatchingEligibleLanes:   matchingEligible,
		OtherEligibleLanes:      otherEligible,
		MatchingIneligibleLanes: matchingIneligible,
		ParallelEdges:           parallel,
		AddCycle:                cycle == 1,
		PropertyPayloadSize:     payload,
	}
	if matched != 9 || (cycle != 0 && cycle != 1) || testutil.ValidateEndpointSeededExpansionScaleConfig(config) != nil || name != endpointSeededExpansionDatasetName(config) {
		return testutil.EndpointSeededExpansionScaleConfig{}, false
	}
	return config, true
}

// endpointSeededExpansionDatasetName encodes endpoint-seeded scale parameters in their canonical dataset name.
func endpointSeededExpansionDatasetName(config testutil.EndpointSeededExpansionScaleConfig) string {
	cycle := 0
	if config.AddCycle {
		cycle = 1
	}
	return fmt.Sprintf(testutil.EndpointSeededExpansionScaleDataset+"_d%d_e%d_q%d_w%d_o%d_x%d_m%d_c%d_p%d",
		config.Depth, config.MatchingEndpoints, config.OtherEndpoints, config.MatchingEligibleLanes,
		config.OtherEligibleLanes, config.MatchingIneligibleLanes, config.ParallelEdges, cycle, config.PropertyPayloadSize)
}

// endpointSeededExpansionFixtureExpectations derives reverse-search state and output counts from an endpoint-seeded fixture.
func endpointSeededExpansionFixtureExpectations(fixture opengraph.Graph, config testutil.EndpointSeededExpansionScaleConfig) *EndpointSeededExpansionFixtureExpectations {
	incoming := map[string][]int{}
	matching := map[string]bool{}
	eligibleUsers := map[string]bool{}
	for _, node := range fixture.Nodes {
		if objectID, ok := node.Properties["objectid"].(string); ok && strings.HasSuffix(objectID, "-512") {
			matching[node.ID] = true
		}
	}
	for edgeIdx, edge := range fixture.Edges {
		if edge.Kind == "MemberOf" {
			incoming[edge.EndID] = append(incoming[edge.EndID], edgeIdx)
		} else if edge.Kind == "HasSession" {
			eligibleUsers[edge.EndID] = true
		}
	}
	var (
		states, outputs int64
		visit           func(string, int, map[int]bool)
	)
	visit = func(nodeID string, depth int, used map[int]bool) {
		states++
		if depth > 0 && eligibleUsers[nodeID] {
			outputs++
		}
		if depth == 64 {
			return
		}
		for _, edgeIdx := range incoming[nodeID] {
			if used[edgeIdx] {
				continue
			}
			used[edgeIdx] = true
			visit(fixture.Edges[edgeIdx].StartID, depth+1, used)
			delete(used, edgeIdx)
		}
	}
	for endpoint := range matching {
		visit(endpoint, 0, map[int]bool{})
	}
	return &EndpointSeededExpansionFixtureExpectations{
		MatchingEndpoints:       int64(config.MatchingEndpoints),
		OtherEndpoints:          int64(config.OtherEndpoints),
		EligiblePrefixRows:      int64(config.MatchingEligibleLanes + config.OtherEligibleLanes),
		MatchingIneligibleLanes: int64(config.MatchingIneligibleLanes),
		ExpectedReverseStates:   states,
		ExpectedOutputTrails:    outputs,
	}
}

// parseShortestPathV2DatasetName decodes and validates every scale parameter embedded in a shortest-path dataset name.
func parseShortestPathV2DatasetName(name string) (testutil.ShortestPathScaleV2Config, bool) {
	var (
		depth, rootOut, rootIn, intermediateOut, intermediateIn, level  int
		kinds, targets, diamond, disconnected, payload, cycle, selfLoop int
	)

	format := testutil.ShortestPathScaleV2Dataset + "_d%d_o%d_r%d_fo%d_fi%d_l%d_k%d_t%d_w%d_x%d_p%d_c%d_s%d"
	matched, _ := fmt.Sscanf(name, format, &depth, &rootOut, &rootIn, &intermediateOut, &intermediateIn, &level, &kinds, &targets, &diamond, &disconnected, &payload, &cycle, &selfLoop)
	if matched != 13 || (cycle != 0 && cycle != 1) || (selfLoop != 0 && selfLoop != 1) {
		return testutil.ShortestPathScaleV2Config{}, false
	}
	config := testutil.ShortestPathScaleV2Config{
		Depth:                    depth,
		ForwardRootFanOut:        rootOut,
		ReverseRootFanIn:         rootIn,
		IntermediateFanOut:       intermediateOut,
		IntermediateReverseFanIn: intermediateIn,
		FanInLevel:               level,
		ParallelKindCount:        kinds,
		ParallelTargetCount:      targets,
		DiamondWidth:             diamond,
		DisconnectedWidth:        disconnected,
		PropertyPayloadSize:      payload,
		AddCycle:                 cycle == 1,
		AddSelfLoop:              selfLoop == 1,
	}
	if err := testutil.ValidateShortestPathScaleV2Config(config); err != nil || name != shortestPathV2DatasetName(config) {
		return testutil.ShortestPathScaleV2Config{}, false
	}
	return config, true
}

// shortestPathV2DatasetName encodes shortest-path scale parameters in their canonical dataset name.
func shortestPathV2DatasetName(config testutil.ShortestPathScaleV2Config) string {
	cycle, selfLoop := 0, 0
	if config.AddCycle {
		cycle = 1
	}
	if config.AddSelfLoop {
		selfLoop = 1
	}
	return fmt.Sprintf(testutil.ShortestPathScaleV2Dataset+"_d%d_o%d_r%d_fo%d_fi%d_l%d_k%d_t%d_w%d_x%d_p%d_c%d_s%d",
		config.Depth, config.ForwardRootFanOut, config.ReverseRootFanIn,
		config.IntermediateFanOut, config.IntermediateReverseFanIn, config.FanInLevel,
		config.ParallelKindCount, config.ParallelTargetCount, config.DiamondWidth,
		config.DisconnectedWidth, config.PropertyPayloadSize, cycle, selfLoop)
}

// shortestFixtureExpectations derives shortest distance, path cardinality, and intermediate-state expectations from a fixture.
func shortestFixtureExpectations(fixture opengraph.Graph, config testutil.ShortestPathScaleV2Config) *ShortestFixtureExpectations {
	expectations := &ShortestFixtureExpectations{
		MaximumIntermediateForwardByLevel: map[string]int64{},
		MaximumIntermediateReverseByLevel: map[string]int64{},
		PhysicalTraversableEdgesByKind:    map[string]int64{},
		DistinctReachableNodesByLevel:     map[string]int64{},
		ExpectedMinimumDistance:           int64(config.Depth),
		ExpectedOnePathCardinality:        1,
		ExpectedAllShortestCardinality:    1,
		ExpectedPredecessorEdges:          int64(config.Depth),
		DisconnectedStateCardinality:      int64(config.DisconnectedWidth + 1),
		ParallelPhysicalEdges:             int64(config.ParallelKindCount * config.ParallelTargetCount),
		ParallelDistinctTargets:           int64(config.ParallelTargetCount),
	}
	outgoing, incoming := map[string][]string{}, map[string][]string{}
	for _, edge := range fixture.Edges {
		expectations.PhysicalTraversableEdgesByKind[edge.Kind]++
		outgoing[edge.StartID] = append(outgoing[edge.StartID], edge.EndID)
		incoming[edge.EndID] = append(incoming[edge.EndID], edge.StartID)
	}
	expectations.RootForwardDegree = int64(len(outgoing["sp-v2-start"]))
	expectations.RootReverseDegree = int64(len(incoming["sp-v2-inbound-root"]))
	for level := 1; level < config.Depth; level++ {
		id, key := fmt.Sprintf("sp-v2-linear-%02d", level), fmt.Sprintf("%d", level)
		expectations.MaximumIntermediateForwardByLevel[key] = int64(len(outgoing[id]))
		expectations.MaximumIntermediateReverseByLevel[key] = int64(len(incoming[fmt.Sprintf("sp-v2-inbound-linear-%02d", level)]))
	}
	seen := map[string]bool{"sp-v2-start": true}
	frontier := []string{"sp-v2-start"}
	for level := 0; len(frontier) > 0 && level <= 64; level++ {
		expectations.DistinctReachableNodesByLevel[fmt.Sprintf("%d", level)] = int64(len(frontier))
		next := []string{}
		for _, source := range frontier {
			for _, target := range outgoing[source] {
				if !seen[target] {
					seen[target] = true
					next = append(next, target)
				}
			}
		}
		frontier = next
	}
	return expectations
}

// parseFixedSuffixExpansionV3DatasetName decodes the exact fixed-suffix
// grammar with independently encoded matching roots and productive-boundary
// cycle/self-loop controls.
func parseFixedSuffixExpansionV3DatasetName(name string) (testutil.FixedSuffixExpansionScaleConfig, bool) {
	var depth, fanout, reachable, disconnected, fanIn, multiplicity, roots, zeroDepth, cycle, selfLoop, payload int
	format := testutil.FixedSuffixExpansionScaleV3Dataset + "_d%d_f%d_r%d_x%d_i%d_m%d_q%d_z%d_c%d_s%d_p%d"
	matched, _ := fmt.Sscanf(name, format, &depth, &fanout, &reachable, &disconnected, &fanIn, &multiplicity, &roots, &zeroDepth, &cycle, &selfLoop, &payload)
	if matched != 11 || (zeroDepth != 0 && zeroDepth != 1) || (cycle != 0 && cycle != 1) || (selfLoop != 0 && selfLoop != 1) {
		return testutil.FixedSuffixExpansionScaleConfig{}, false
	}

	rootSuffix := zeroDepth == 1
	config := testutil.FixedSuffixExpansionScaleConfig{
		ExpansionDepth:                depth,
		Fanout:                        fanout,
		ExactReachableSuffixSources:   &reachable,
		DisconnectedSuffixSources:     disconnected,
		ReverseFanIn:                  fanIn,
		SuffixPathsPerBoundary:        multiplicity,
		RootMatchCount:                roots,
		RootHasZeroDepthSuffix:        &rootSuffix,
		AddProductiveBoundaryCycle:    cycle == 1,
		AddProductiveBoundarySelfLoop: selfLoop == 1,
		PropertyPayloadSize:           payload,
	}
	if testutil.ValidateFixedSuffixExpansionScaleV3Config(config) != nil || name != fixedSuffixExpansionV3DatasetName(config) {
		return testutil.FixedSuffixExpansionScaleConfig{}, false
	}
	return config, true
}

// fixedSuffixExpansionV3DatasetName encodes every v3 fixture dimension in its
// canonical, round-trippable dataset name.
func fixedSuffixExpansionV3DatasetName(config testutil.FixedSuffixExpansionScaleConfig) string {
	reachable, zeroDepth, cycle, selfLoop := 0, 0, 0, 0
	if config.ExactReachableSuffixSources != nil {
		reachable = *config.ExactReachableSuffixSources
	}
	if config.RootHasZeroDepthSuffix != nil && *config.RootHasZeroDepthSuffix {
		zeroDepth = 1
	}
	if config.AddProductiveBoundaryCycle {
		cycle = 1
	}
	if config.AddProductiveBoundarySelfLoop {
		selfLoop = 1
	}
	return fmt.Sprintf(testutil.FixedSuffixExpansionScaleV3Dataset+"_d%d_f%d_r%d_x%d_i%d_m%d_q%d_z%d_c%d_s%d_p%d",
		config.ExpansionDepth, config.Fanout, reachable, config.DisconnectedSuffixSources,
		config.ReverseFanIn, config.SuffixPathsPerBoundary, config.RootMatchCount,
		zeroDepth, cycle, selfLoop, config.PropertyPayloadSize)
}

// parseFixedSuffixExpansionV2DatasetName decodes and validates every scale parameter embedded in a fixed-suffix dataset name.
func parseFixedSuffixExpansionV2DatasetName(name string) (testutil.FixedSuffixExpansionScaleConfig, bool) {
	var depth, fanout, reachable, disconnected, fanIn, multiplicity, zeroDepth, payload int
	format := testutil.FixedSuffixExpansionScaleDataset + "_v2_d%d_f%d_r%d_x%d_i%d_m%d_z%d_p%d"
	matched, _ := fmt.Sscanf(name, format, &depth, &fanout, &reachable, &disconnected, &fanIn, &multiplicity, &zeroDepth, &payload)
	if matched != 8 || depth < 0 || fanout < 1 || reachable < 0 || reachable > fanout || disconnected < 0 || fanIn < 0 || multiplicity < 1 || (zeroDepth != 0 && zeroDepth != 1) || payload < 0 || name != fmt.Sprintf(format, depth, fanout, reachable, disconnected, fanIn, multiplicity, zeroDepth, payload) {
		return testutil.FixedSuffixExpansionScaleConfig{}, false
	}
	rootSuffix := zeroDepth == 1
	return testutil.FixedSuffixExpansionScaleConfig{
		ExpansionDepth:              depth,
		Fanout:                      fanout,
		ExactReachableSuffixSources: &reachable,
		DisconnectedSuffixSources:   disconnected,
		ReverseFanIn:                fanIn,
		SuffixPathsPerBoundary:      multiplicity,
		RootMatchCount:              1,
		RootHasZeroDepthSuffix:      &rootSuffix,
		PropertyPayloadSize:         payload,
	}, true
}

// fixedSuffixExpansionV2FixtureExpectations preserves the exact v2 metadata
// contract for every existing fixture name.
func fixedSuffixExpansionV2FixtureExpectations(config testutil.FixedSuffixExpansionScaleConfig) *FixedSuffixExpansionFixtureExpectations {
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
	return &FixedSuffixExpansionFixtureExpectations{
		RootSourceRows:         int64(rootCount),
		DistinctRoots:          int64(rootCount),
		ForwardExpansionStates: int64(rootCount + config.Fanout*config.ExpansionDepth),
		SuffixRows:             int64((zero + reachable + config.DisconnectedSuffixSources) * multiplicity),
		DistinctBoundaries:     int64(zero + reachable + config.DisconnectedSuffixSources),
		ReachableBoundaries:    int64(zero + reachable),
		DisconnectedBoundaries: int64(config.DisconnectedSuffixSources),
		ExpectedReverseStates:  int64(zero + reachable*(config.ExpansionDepth+1) + config.DisconnectedSuffixSources + productiveFanIn),
		CompleteOutputTrails:   int64((zero + reachable) * multiplicity),
	}
}

// fixedSuffixExpansionV3FixtureExpectations derives exact forward and reverse
// relationship-distinct states and output trails from a v3 fixture graph.
func fixedSuffixExpansionV3FixtureExpectations(fixture opengraph.Graph, config testutil.FixedSuffixExpansionScaleConfig) *FixedSuffixExpansionFixtureExpectations {
	// adjacentEdge identifies one indexed transition in the generated fixture.
	type adjacentEdge struct {
		// index retains the index while adjacentEdge is assembled or evaluated.
		index int
		// next retains the next while adjacentEdge is assembled or evaluated.
		next string
	}

	nodeKinds := map[string]map[string]bool{}
	roots := []string{}
	for _, node := range fixture.Nodes {
		kinds := map[string]bool{}
		for _, kind := range node.Kinds {
			kinds[kind] = true
		}
		nodeKinds[node.ID] = kinds
		if kinds["ExpansionRoot"] && node.Properties["root_key"] == "generated-fse-root" {
			roots = append(roots, node.ID)
		}
	}

	expandForward := map[string][]adjacentEdge{}
	expandReverse := map[string][]adjacentEdge{}
	edgesByStart := map[string][]int{}
	for edgeIdx, edge := range fixture.Edges {
		edgesByStart[edge.StartID] = append(edgesByStart[edge.StartID], edgeIdx)
		if edge.Kind == "Expand" {
			expandForward[edge.StartID] = append(expandForward[edge.StartID], adjacentEdge{
				index: edgeIdx,
				next:  edge.EndID,
			})
			expandReverse[edge.EndID] = append(expandReverse[edge.EndID], adjacentEdge{
				index: edgeIdx,
				next:  edge.StartID,
			})
		}
	}

	suffixPaths := map[string]int64{}
	for _, enter := range fixture.Edges {
		if enter.Kind != "EnterSuffix" || !nodeKinds[enter.EndID]["SuffixHead"] {
			continue
		}
		for _, continueIdx := range edgesByStart[enter.EndID] {
			continuation := fixture.Edges[continueIdx]
			if continuation.Kind != "ContinueSuffix" || !nodeKinds[continuation.EndID]["SuffixMiddle"] {
				continue
			}
			for _, completeIdx := range edgesByStart[continuation.EndID] {
				completion := fixture.Edges[completeIdx]
				if completion.Kind == "CompleteSuffix" && nodeKinds[completion.EndID]["SuffixTerminal"] {
					suffixPaths[enter.StartID]++
				}
			}
		}
	}

	used := make([]bool, len(fixture.Edges))
	var enumerate func(map[string][]adjacentEdge, string, int, func(string)) int64
	enumerate = func(adjacency map[string][]adjacentEdge, nodeID string, depth int, observe func(string)) int64 {
		states := int64(1)
		observe(nodeID)
		if depth == config.ExpansionDepth {
			return states
		}
		for _, edge := range adjacency[nodeID] {
			if used[edge.index] {
				continue
			}
			used[edge.index] = true
			states += enumerate(adjacency, edge.next, depth+1, observe)
			used[edge.index] = false
		}
		return states
	}

	boundaryVisits := map[string]int64{}
	forwardStates := int64(0)
	for _, root := range roots {
		forwardStates += enumerate(expandForward, root, 0, func(nodeID string) {
			if suffixPaths[nodeID] > 0 {
				boundaryVisits[nodeID]++
			}
		})
	}

	reverseStates := int64(0)
	for boundary := range suffixPaths {
		reverseStates += enumerate(expandReverse, boundary, 0, func(string) {})
	}

	suffixRows, outputTrails := int64(0), int64(0)
	for boundary, pathCount := range suffixPaths {
		suffixRows += pathCount
		outputTrails += boundaryVisits[boundary] * pathCount
	}
	cycleEdges, selfLoopEdges := int64(0), int64(0)
	if config.AddProductiveBoundaryCycle {
		cycleEdges = 2
	}
	if config.AddProductiveBoundarySelfLoop {
		selfLoopEdges = 1
	}
	return &FixedSuffixExpansionFixtureExpectations{
		RootSourceRows:                  int64(len(roots)),
		DistinctRoots:                   int64(len(roots)),
		ForwardExpansionStates:          forwardStates,
		SuffixRows:                      suffixRows,
		DistinctBoundaries:              int64(len(suffixPaths)),
		ReachableBoundaries:             int64(len(boundaryVisits)),
		DisconnectedBoundaries:          int64(len(suffixPaths) - len(boundaryVisits)),
		ExpectedReverseStates:           reverseStates,
		CompleteOutputTrails:            outputTrails,
		ProductiveBoundaryCycleEdges:    cycleEdges,
		ProductiveBoundarySelfLoopEdges: selfLoopEdges,
	}
}

// clearGraph removes relationships before nodes, using PostgreSQL partition truncation when available.
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

// clearPostgresGraph truncates one PostgreSQL graph's edge and node partitions in a transaction.
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

// benchmarkSchema returns the SQL schema used to isolate benchmark preparation from timed execution.
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

// resolveCaseParams resolves a scale case's scalar, node-key, node-list, and generated-node parameters.
func resolveCaseParams(testCase ScaleCase, idMap opengraph.IDMap) (map[string]any, error) {
	return resolveParams(testCase.Name, testCase.Params, testCase.NodeParams, testCase.NodeListParams, testCase.GeneratedNodeListParams, idMap)
}

// resolveParams copies literal parameters and replaces symbolic node keys with database identifiers.
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

// resolveWriteScenario resolves selection and post-state parameters while preserving the write expectation contract.
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
