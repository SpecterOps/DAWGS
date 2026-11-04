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
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

// Measurement pairs a benchmark duration with the number of rows observed.
type Measurement struct {
	// RowCount records the number of rows produced.
	RowCount int64
	// DistinctRowCount records unique rows returned by the benchmark scenario.
	DistinctRowCount *int64
	// DuplicateRowCount records repeated rows retained by the benchmark scenario.
	DuplicateRowCount *int64
}

// Scenario defines one query, its parameters, and expected cardinality.
type Scenario struct {
	// Section groups baseline rows under a Markdown summary section.
	Section string // grouping key in the report (e.g. "Match Nodes")
	// Dataset identifies the fixture dataset.
	Dataset string
	// Label provides the benchfmt label for the benchmark scenario.
	Label string // human-readable row label
	// ExpectedRows sets the row count required for a scenario to succeed.
	ExpectedRows *int64
	// Cypher contains the Cypher statement under test.
	Cypher string
	// Parameters supplies the immutable parameters bound when Cypher is executed.
	Parameters map[string]any
	// Query executes the scenario in a transaction and returns its duration and observed row count.
	Query func(tx graph.Transaction) (Measurement, error)
}

// traversalShapesDataset is the fixture key shared by traversal-shape scenario selection and dataset loading.
const traversalShapesDataset = "traversal_shapes"

// defaultDatasets is the set of datasets committed to the repo.
var defaultDatasets = []string{"base", "fixed_suffix_expansion_fanout", traversalShapesDataset}

// scenariosForDataset returns all benchmark scenarios for a given dataset and its loaded ID map.
func scenariosForDataset(dataset string, idMap opengraph.IDMap) []Scenario {
	switch dataset {
	case "base":
		return baseScenarios(idMap)
	case "fixed_suffix_expansion_fanout":
		return fixedSuffixExpansionFanoutScenarios()
	case traversalShapesDataset:
		return traversalShapesScenarios(idMap)
	case "local/phantom":
		return phantomScenarios(idMap)
	default:
		return nil
	}
}

// expectRows returns an addressable row expectation so zero expected rows remains distinguishable from an unspecified expectation.
func expectRows(rows int64) *int64 {
	return &rows
}

// countNodes measures the transaction-visible node cardinality for dataset sanity benchmarks.
func countNodes(tx graph.Transaction) (int64, error) {
	return tx.Nodes().Count()
}

// countEdges measures the transaction-visible relationship cardinality for dataset sanity benchmarks.
func countEdges(tx graph.Transaction) (int64, error) {
	return tx.Relationships().Count()
}

// cypherQuery adapts Cypher text into a benchmark callback that drains the result and records returned row count.
func cypherQuery(cypher string) func(tx graph.Transaction) (Measurement, error) {
	return cypherQueryWithParameters(cypher, nil)
}

// cypherQueryWithParameters adapts parameterized Cypher text into a benchmark
// callback that drains the result and records returned row count.
func cypherQueryWithParameters(cypher string, parameters map[string]any) func(tx graph.Transaction) (Measurement, error) {
	return func(tx graph.Transaction) (Measurement, error) {
		result := tx.Query(cypher, parameters)
		defer result.Close()

		var rowCount int64
		for result.Next() {
			rowCount++
		}

		return Measurement{RowCount: rowCount}, result.Error()
	}
}

// countQuery adapts a cardinality callback into a benchmark Measurement while preserving the callback error.
func countQuery(query func(tx graph.Transaction) (int64, error)) func(tx graph.Transaction) (Measurement, error) {
	return func(tx graph.Transaction) (Measurement, error) {
		rowCount, err := query(tx)
		if err != nil {
			return Measurement{}, err
		}

		return Measurement{RowCount: rowCount}, nil
	}
}

// cypherScenario builds a row-counting Scenario from its corpus identity and Cypher text.
func cypherScenario(section, dataset, label, cypher string) Scenario {
	return cypherScenarioWithParameters(section, dataset, label, cypher, nil)
}

// cypherScenarioWithParameters builds a row-counting Scenario whose Cypher
// identity remains stable while its endpoint values vary with fixture loading.
// That stability is required by the exact-query traversal-policy allowlist.
func cypherScenarioWithParameters(section, dataset, label, cypher string, parameters map[string]any) Scenario {
	return Scenario{
		Section:    section,
		Dataset:    dataset,
		Label:      label,
		Cypher:     cypher,
		Parameters: parameters,
		Query:      cypherQueryWithParameters(cypher, parameters),
	}
}

// cypherPathScenario builds a Scenario that validates and counts path-valued columns while consuming results.
func cypherPathScenario(section, dataset, label, cypher string, pathColumns int) Scenario {
	return Scenario{
		Section: section,
		Dataset: dataset,
		Label:   label,
		Cypher:  cypher,
		Query:   cypherPathQuery(cypher, pathColumns),
	}
}

// expectScenarioRows returns scenario with an explicit correctness expectation attached.
func expectScenarioRows(scenario Scenario, rows int64) Scenario {
	scenario.ExpectedRows = expectRows(rows)
	return scenario
}

// cypherPathQuery adapts Cypher text into a benchmark callback that validates path columns and hashes their node/edge identities while draining rows.
func cypherPathQuery(cypher string, pathColumns int) func(tx graph.Transaction) (Measurement, error) {
	return func(tx graph.Transaction) (Measurement, error) {
		result := tx.Query(cypher, nil)
		defer result.Close()

		var (
			rowCount int64
			seen     = map[string]struct{}{}
		)

		for result.Next() {
			rowCount++

			var (
				values  = make([]graph.Path, pathColumns)
				targets = make([]any, pathColumns)
			)

			for idx := range values {
				targets[idx] = &values[idx]
			}

			if err := result.Scan(targets...); err != nil {
				return Measurement{}, err
			}

			seen[pathRowKey(values)] = struct{}{}
		}

		if err := result.Error(); err != nil {
			return Measurement{}, err
		}

		var (
			distinctRowCount  = int64(len(seen))
			duplicateRowCount = rowCount - distinctRowCount
		)

		return Measurement{
			RowCount:          rowCount,
			DistinctRowCount:  &distinctRowCount,
			DuplicateRowCount: &duplicateRowCount,
		}, nil
	}
}

// pathRowKey serializes path node and edge IDs into an unambiguous key used to prevent result materialization from being optimized away.
func pathRowKey(paths []graph.Path) string {
	var builder strings.Builder

	for pathIdx, path := range paths {
		if pathIdx > 0 {
			builder.WriteByte('|')
		}

		builder.WriteByte('n')
		for _, node := range path.Nodes {
			builder.WriteByte(':')
			if node == nil {
				builder.WriteString("nil")
				continue
			}

			builder.WriteString(strconv.FormatUint(node.ID.Uint64(), 10))
		}

		builder.WriteString(";e")
		for _, edge := range path.Edges {
			builder.WriteByte(':')
			if edge == nil {
				builder.WriteString("nil")
				continue
			}

			builder.WriteString(strconv.FormatUint(edge.ID.Uint64(), 10))
			builder.WriteByte(',')
			builder.WriteString(strconv.FormatUint(edge.StartID.Uint64(), 10))
			builder.WriteByte(',')
			builder.WriteString(strconv.FormatUint(edge.EndID.Uint64(), 10))
			builder.WriteByte(',')
			builder.WriteString(edge.Kind.String())
		}
	}

	return builder.String()
}

// --- Base dataset scenarios (n1 -> n2 -> n3) ---

// baseScenarios defines cardinality, lookup, and one-hop checks for the three-node base fixture.
func baseScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "base"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, ExpectedRows: expectRows(3), Query: countQuery(countNodes)},
		{Section: "Match Edges", Dataset: ds, Label: ds, ExpectedRows: expectRows(2), Query: countQuery(countEdges)},
		expectScenarioRows(cypherScenarioWithParameters("Shortest Paths", ds, "n1 -> n3",
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p",
			map[string]any{"start_id": idMap["n1"], "end_id": idMap["n3"]},
		), 1),
		expectScenarioRows(cypherScenario("Traversal", ds, "n1", fmt.Sprintf(
			"MATCH (s)-[*1..]->(e) WHERE id(s) = %d RETURN e",
			idMap["n1"],
		)), 2),
		expectScenarioRows(cypherScenario("Match Return", ds, "n1", fmt.Sprintf(
			"MATCH (s)-[]->(e) WHERE id(s) = %d RETURN e",
			idMap["n1"],
		)), 1),
		expectScenarioRows(cypherScenario("Filter By Kind", ds, "NodeKind1", "MATCH (n:NodeKind1) RETURN n"), 2),
		expectScenarioRows(cypherScenario("Filter By Kind", ds, "NodeKind2", "MATCH (n:NodeKind2) RETURN n"), 2),
	}
}

// fixedSuffixFanoutRootKey identifies the fanout fixture root whose generated ID is injected into fixed-suffix scenarios.
const fixedSuffixFanoutRootKey = "fixed-suffix-fanout-root"

// fixedSuffixExpansionFanoutScenarios exercises bounded reverse-suffix expansion at increasing depths and with path projection enabled.
func fixedSuffixExpansionFanoutScenarios() []Scenario {
	var (
		ds = "fixed_suffix_expansion_fanout"
		p1 = fmt.Sprintf(`
		MATCH (root:ExpansionRoot) WHERE root.root_key = '%s'
		MATCH p1 = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN p1
		`, fixedSuffixFanoutRootKey)
		p2 = fmt.Sprintf(`
		MATCH (root:ExpansionRoot) WHERE root.root_key = '%s'
		MATCH p2 = (root)-[:Expand*0..16]->()-[:OptionA|OptionB|OptionC]->(predicate:PredicateNode)-[:JoinSuffix]->(head:SuffixHead)-[:HeadToBridge|HeadToAlternateBridge*1..16]->(:BridgeNode)-[:ReachTerminal]->(terminal:SuffixTerminal)
		WHERE predicate.eligible = true
		AND predicate.requires_review = false
		AND predicate.allows_direct = true
		AND (predicate.version = 1 OR predicate.required_approvals = 0)
		RETURN p2
		`, fixedSuffixFanoutRootKey)
		combinedMatch = fmt.Sprintf(`
		MATCH (root:ExpansionRoot) WHERE root.root_key = '%s'
		MATCH p1 = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		MATCH p2 = (root)-[:Expand*0..16]->()-[:OptionA|OptionB|OptionC]->(predicate:PredicateNode)-[:JoinSuffix]->(head)-[:HeadToBridge|HeadToAlternateBridge*1..16]->(:BridgeNode)-[:ReachTerminal]->(terminal)
		WHERE predicate.eligible = true
		AND predicate.requires_review = false
		AND predicate.allows_direct = true
		AND (predicate.version = 1 OR predicate.required_approvals = 0)
		`, fixedSuffixFanoutRootKey)
	)

	return []Scenario{
		cypherPathScenario("Fixed Suffix Expansion Fanout", ds, "p1 only", p1, 1),
		cypherPathScenario("Fixed Suffix Expansion Fanout", ds, "p2 only", p2, 1),
		cypherPathScenario("Fixed Suffix Expansion Fanout", ds, "combined", combinedMatch+"RETURN p1,p2", 2),
		cypherScenario("Fixed Suffix Expansion Fanout", ds, "combined endpoints", combinedMatch+"RETURN id(head), id(terminal), id(predicate)"),
	}
}

// --- Traversal shape scenarios ---

// traversalShapesScenarios covers single-hop, bounded variable-length, shortest-path, and repeated-edge traversal forms over the shared fixture.
func traversalShapesScenarios(idMap opengraph.IDMap) []Scenario {
	ds := traversalShapesDataset
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, ExpectedRows: expectRows(45), Query: countQuery(countNodes)},
		{Section: "Match Edges", Dataset: ds, Label: ds, ExpectedRows: expectRows(41), Query: countQuery(countEdges)},
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "chain depth 1", fmt.Sprintf(
			"MATCH (s)-[:ChainEdge*1..1]->(e) WHERE id(s) = %d RETURN e",
			idMap["c0"],
		)), 1),
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "chain depth 3", fmt.Sprintf(
			"MATCH (s)-[:ChainEdge*1..3]->(e) WHERE id(s) = %d RETURN e",
			idMap["c0"],
		)), 3),
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "chain depth 10", fmt.Sprintf(
			"MATCH (s)-[:ChainEdge*1..10]->(e) WHERE id(s) = %d RETURN e",
			idMap["c0"],
		)), 10),
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "fanout depth 1", fmt.Sprintf(
			"MATCH (s)-[:FanoutEdge*1..1]->(e) WHERE id(s) = %d RETURN e",
			idMap["f0"],
		)), 3),
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "fanout depth 2", fmt.Sprintf(
			"MATCH (s)-[:FanoutEdge*1..2]->(e) WHERE id(s) = %d RETURN e",
			idMap["f0"],
		)), 9),
		expectScenarioRows(cypherScenario("Traversal Depth", ds, "fanout depth 3", fmt.Sprintf(
			"MATCH (s)-[:FanoutEdge*1..3]->(e) WHERE id(s) = %d RETURN e",
			idMap["f0"],
		)), 15),
		expectScenarioRows(cypherScenario("Traversal Cycle", ds, "bounded cycle", fmt.Sprintf(
			"MATCH (s)-[:CycleEdge*1..4]->(e) WHERE id(s) = %d RETURN e",
			idMap["y0"],
		)), 4),
		expectScenarioRows(cypherScenario("Traversal Dead End", ds, "chain terminal", fmt.Sprintf(
			"MATCH (s)-[:ChainEdge*1..]->(e) WHERE id(s) = %d RETURN e",
			idMap["c10"],
		)), 0),
		expectScenarioRows(cypherScenario("Edge Kind Traversal", ds, "Allowed", fmt.Sprintf(
			"MATCH (s)-[:Allowed*1..]->(e) WHERE id(s) = %d RETURN e",
			idMap["s0"],
		)), 3),
		expectScenarioRows(cypherScenario("Edge Kind Traversal", ds, "all kinds", fmt.Sprintf(
			"MATCH (s)-[*1..]->(e) WHERE id(s) = %d RETURN e",
			idMap["s0"],
		)), 6),
		expectScenarioRows(cypherScenarioWithParameters("Shortest Paths", ds, "diamond many paths",
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p",
			map[string]any{"start_id": idMap["d0"], "end_id": idMap["d4"]},
		), 3),
		expectScenarioRows(cypherScenarioWithParameters("Shortest Paths", ds, "disconnected",
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = $disconnected_start_id AND id(e) = $disconnected_end_id RETURN p",
			map[string]any{"disconnected_start_id": idMap["x0"], "disconnected_end_id": idMap["x1"]},
		), 0),
	}
}

// --- Phantom scenarios (hardcoded node IDs from the dataset) ---

// phantomScenarios preserves legacy benchmark cases that intentionally address the phantom fixture by its stable generated IDs.
func phantomScenarios(idMap opengraph.IDMap) []Scenario {
	var (
		ds        = "local/phantom"
		scenarios = []Scenario{
			{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countQuery(countNodes)},
			{Section: "Match Edges", Dataset: ds, Label: ds, Query: countQuery(countEdges)},
		}
	)

	for _, kind := range []string{"User", "Group", "Computer"} {
		k := kind
		scenarios = append(scenarios, cypherScenario("Filter By Kind", ds, k, fmt.Sprintf("MATCH (n:%s) RETURN n", k)))
	}

	if _, ok := idMap["41"]; ok {
		for _, depth := range []int{1, 2, 3} {
			d := depth
			scenarios = append(scenarios, cypherScenario(
				"Traversal Depth",
				ds,
				fmt.Sprintf("depth %d", d),
				fmt.Sprintf(
					"MATCH (s)-[*1..%d]->(e) WHERE id(s) = %d RETURN e",
					d, idMap["41"],
				),
			))
		}

		for _, ek := range []string{"MemberOf", "GenericAll", "HasSession"} {
			edgeKind := ek
			scenarios = append(scenarios, cypherScenario(
				"Edge Kind Traversal",
				ds,
				edgeKind,
				fmt.Sprintf(
					"MATCH (s)-[:%s*1..]->(e) WHERE id(s) = %d RETURN e",
					edgeKind, idMap["41"],
				),
			))
		}
	}

	if _, ok := idMap["41"]; ok {
		if _, ok := idMap["587"]; ok {
			scenarios = append(scenarios, cypherScenario(
				"Shortest Paths",
				ds,
				"41 -> 587",
				fmt.Sprintf(
					"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
					idMap["41"], idMap["587"],
				),
			))
		}
	}

	return scenarios
}
