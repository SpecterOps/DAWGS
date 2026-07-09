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

// Measurement captures the warm-up result shape for a benchmark scenario.
type Measurement struct {
	RowCount          int64
	DistinctRowCount  *int64
	DuplicateRowCount *int64
}

// Scenario defines a single benchmark query to run against a loaded dataset.
type Scenario struct {
	Section      string // grouping key in the report (e.g. "Match Nodes")
	Dataset      string
	Label        string // human-readable row label
	ExpectedRows *int64
	Cypher       string
	Query        func(tx graph.Transaction) (Measurement, error)
}

const traversalShapesDataset = "traversal_shapes"

// defaultDatasets is the set of datasets committed to the repo.
var defaultDatasets = []string{"base", "adcs_fanout", traversalShapesDataset}

// scenariosForDataset returns all benchmark scenarios for a given dataset and its loaded ID map.
func scenariosForDataset(dataset string, idMap opengraph.IDMap) []Scenario {
	switch dataset {
	case "base":
		return baseScenarios(idMap)
	case "adcs_fanout":
		return adcsFanoutScenarios()
	case traversalShapesDataset:
		return traversalShapesScenarios(idMap)
	case "local/phantom":
		return phantomScenarios(idMap)
	default:
		return nil
	}
}

func expectRows(rows int64) *int64 {
	return &rows
}

func countNodes(tx graph.Transaction) (int64, error) {
	return tx.Nodes().Count()
}

func countEdges(tx graph.Transaction) (int64, error) {
	return tx.Relationships().Count()
}

func cypherQuery(cypher string) func(tx graph.Transaction) (Measurement, error) {
	return func(tx graph.Transaction) (Measurement, error) {
		result := tx.Query(cypher, nil)
		defer result.Close()

		var rowCount int64
		for result.Next() {
			rowCount++
		}

		return Measurement{RowCount: rowCount}, result.Error()
	}
}

func countQuery(query func(tx graph.Transaction) (int64, error)) func(tx graph.Transaction) (Measurement, error) {
	return func(tx graph.Transaction) (Measurement, error) {
		rowCount, err := query(tx)
		if err != nil {
			return Measurement{}, err
		}

		return Measurement{RowCount: rowCount}, nil
	}
}

func cypherScenario(section, dataset, label, cypher string) Scenario {
	return Scenario{
		Section: section,
		Dataset: dataset,
		Label:   label,
		Cypher:  cypher,
		Query:   cypherQuery(cypher),
	}
}

func cypherPathScenario(section, dataset, label, cypher string, pathColumns int) Scenario {
	return Scenario{
		Section: section,
		Dataset: dataset,
		Label:   label,
		Cypher:  cypher,
		Query:   cypherPathQuery(cypher, pathColumns),
	}
}

func expectScenarioRows(scenario Scenario, rows int64) Scenario {
	scenario.ExpectedRows = expectRows(rows)
	return scenario
}

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

func baseScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "base"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, ExpectedRows: expectRows(3), Query: countQuery(countNodes)},
		{Section: "Match Edges", Dataset: ds, Label: ds, ExpectedRows: expectRows(2), Query: countQuery(countEdges)},
		expectScenarioRows(cypherScenario("Shortest Paths", ds, "n1 -> n3", fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			idMap["n1"], idMap["n3"],
		)), 1),
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

const adcsFanoutObjectID = "S-1-5-21-2643190041-1319121918-239771340-513"

func adcsFanoutScenarios() []Scenario {
	var (
		ds = "adcs_fanout"
		p1 = fmt.Sprintf(`
		MATCH (n:Group) WHERE n.objectid = '%s'
		MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
		RETURN p1
		`, adcsFanoutObjectID)
		p2 = fmt.Sprintf(`
		MATCH (n:Group) WHERE n.objectid = '%s'
		MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d:Domain)
		WHERE ct.authenticationenabled = true
		AND ct.requiresmanagerapproval = false
		AND ct.enrolleesuppliessubject = true
		AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
		RETURN p2
		`, adcsFanoutObjectID)
		combinedMatch = fmt.Sprintf(`
		MATCH (n:Group) WHERE n.objectid = '%s'
		MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
		MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d)
		WHERE ct.authenticationenabled = true
		AND ct.requiresmanagerapproval = false
		AND ct.enrolleesuppliessubject = true
		AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
		`, adcsFanoutObjectID)
	)

	return []Scenario{
		cypherPathScenario("ADCS Fanout", ds, "p1 only", p1, 1),
		cypherPathScenario("ADCS Fanout", ds, "p2 only", p2, 1),
		cypherPathScenario("ADCS Fanout", ds, "combined", combinedMatch+"RETURN p1,p2", 2),
		cypherScenario("ADCS Fanout", ds, "combined endpoints", combinedMatch+"RETURN id(ca), id(d), id(ct)"),
	}
}

// --- Traversal shape scenarios ---

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
		expectScenarioRows(cypherScenario("Shortest Paths", ds, "diamond many paths", fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			idMap["d0"], idMap["d4"],
		)), 3),
		expectScenarioRows(cypherScenario("Shortest Paths", ds, "disconnected", fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			idMap["x0"], idMap["x1"],
		)), 0),
	}
}

// --- Phantom scenarios (hardcoded node IDs from the dataset) ---

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
