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

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

// Scenario defines a single benchmark query to run against a loaded dataset.
type Scenario struct {
	Section string // grouping key in the report (e.g. "Match Nodes")
	Dataset string
	Label   string // human-readable row label
	Query   func(tx graph.Transaction) error
}

// smallDatasets is the set of datasets committed to the repo.
var smallDatasets = []string{"diamond", "linear", "wide_diamond", "disconnected", "dead_end", "direct_shortcut"}

// scenariosForDataset returns all benchmark scenarios for a given dataset and its loaded ID map.
func scenariosForDataset(dataset string, idMap opengraph.IDMap) []Scenario {
	switch dataset {
	case "diamond":
		return diamondScenarios(idMap)
	case "linear":
		return linearScenarios(idMap)
	case "wide_diamond":
		return wideDiamondScenarios(idMap)
	case "disconnected":
		return disconnectedScenarios(idMap)
	case "dead_end":
		return deadEndScenarios(idMap)
	case "direct_shortcut":
		return directShortcutScenarios(idMap)
	case "local/phantom":
		return phantomScenarios(idMap)
	default:
		return nil
	}
}

func countNodes(tx graph.Transaction) error {
	_, err := tx.Nodes().Count()
	return err
}

func countEdges(tx graph.Transaction) error {
	_, err := tx.Relationships().Count()
	return err
}

func cypherQuery(cypher string) func(tx graph.Transaction) error {
	return func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()
		for result.Next() {
			// drain results
		}
		return result.Error()
	}
}

func shortestPathQuery(idMap opengraph.IDMap, start, end string) func(tx graph.Transaction) error {
	return cypherQuery(fmt.Sprintf(
		"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
		idMap[start], idMap[end],
	))
}

func traversalQuery(idMap opengraph.IDMap, start string) func(tx graph.Transaction) error {
	return cypherQuery(fmt.Sprintf(
		"MATCH (s)-[:EdgeKind1*1..]->(e) WHERE id(s) = %d RETURN e",
		idMap[start],
	))
}

func matchReturnQuery(idMap opengraph.IDMap, start string) func(tx graph.Transaction) error {
	return cypherQuery(fmt.Sprintf(
		"MATCH (s)-[:EdgeKind1]->(e) WHERE id(s) = %d RETURN e",
		idMap[start],
	))
}

// --- Small dataset scenarios ---

func diamondScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "diamond"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> d", Query: shortestPathQuery(idMap, "a", "d")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
		{Section: "Match Return", Dataset: ds, Label: "a", Query: matchReturnQuery(idMap, "a")},
	}
}

func linearScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "linear"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> c", Query: shortestPathQuery(idMap, "a", "c")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
		{Section: "Match Return", Dataset: ds, Label: "a", Query: matchReturnQuery(idMap, "a")},
	}
}

func wideDiamondScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "wide_diamond"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> e", Query: shortestPathQuery(idMap, "a", "e")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
		{Section: "Match Return", Dataset: ds, Label: "a", Query: matchReturnQuery(idMap, "a")},
	}
}

func disconnectedScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "disconnected"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> b", Query: shortestPathQuery(idMap, "a", "b")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
	}
}

func deadEndScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "dead_end"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> c", Query: shortestPathQuery(idMap, "a", "c")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
	}
}

func directShortcutScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "direct_shortcut"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "a -> d", Query: shortestPathQuery(idMap, "a", "d")},
		{Section: "Traversal", Dataset: ds, Label: "a", Query: traversalQuery(idMap, "a")},
	}
}

// --- Phantom scenarios (hardcoded node IDs from the dataset) ---

func phantomScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "local/phantom"

	scenarios := []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
	}

	// Kind-filtered counts
	for _, kind := range []string{"User", "Group", "Computer"} {
		k := kind
		scenarios = append(scenarios, Scenario{
			Section: "Filter By Kind",
			Dataset: ds,
			Label:   k,
			Query:   cypherQuery(fmt.Sprintf("MATCH (n:%s) RETURN n", k)),
		})
	}

	// Traversal at increasing depths from a known User node (ID "41" = SVC_DOMAINJOIN)
	if _, ok := idMap["41"]; ok {
		for _, depth := range []int{1, 2, 3} {
			d := depth
			scenarios = append(scenarios, Scenario{
				Section: "Traversal Depth",
				Dataset: ds,
				Label:   fmt.Sprintf("depth %d", d),
				Query: cypherQuery(fmt.Sprintf(
					"MATCH (s)-[*1..%d]->(e) WHERE id(s) = %d RETURN e",
					d, idMap["41"],
				)),
			})
		}

		// Edge-kind-specific traversals
		for _, ek := range []string{"MemberOf", "GenericAll", "HasSession"} {
			edgeKind := ek
			scenarios = append(scenarios, Scenario{
				Section: "Edge Kind Traversal",
				Dataset: ds,
				Label:   edgeKind,
				Query: cypherQuery(fmt.Sprintf(
					"MATCH (s)-[:%s*1..]->(e) WHERE id(s) = %d RETURN e",
					edgeKind, idMap["41"],
				)),
			})
		}
	}

	// Shortest path: User "41" to Domain "587"
	if _, ok := idMap["41"]; ok {
		if _, ok := idMap["587"]; ok {
			scenarios = append(scenarios, Scenario{
				Section: "Shortest Paths",
				Dataset: ds,
				Label:   "41 -> 587",
				Query:   shortestPathQuery(idMap, "41", "587"),
			})
		}
	}

	return scenarios
}
