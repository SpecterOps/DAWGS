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
	Query   func(tx graph.Transaction) (int64, error)
}

// defaultDatasets is the set of datasets committed to the repo.
var defaultDatasets = []string{"base", "adcs_fanout"}

// scenariosForDataset returns all benchmark scenarios for a given dataset and its loaded ID map.
func scenariosForDataset(dataset string, idMap opengraph.IDMap) []Scenario {
	switch dataset {
	case "base":
		return baseScenarios(idMap)
	case "adcs_fanout":
		return adcsFanoutScenarios()
	case "local/phantom":
		return phantomScenarios(idMap)
	default:
		return nil
	}
}

func countNodes(tx graph.Transaction) (int64, error) {
	return tx.Nodes().Count()
}

func countEdges(tx graph.Transaction) (int64, error) {
	return tx.Relationships().Count()
}

func cypherQuery(cypher string) func(tx graph.Transaction) (int64, error) {
	return func(tx graph.Transaction) (int64, error) {
		result := tx.Query(cypher, nil)
		defer result.Close()

		var rowCount int64
		for result.Next() {
			rowCount++
		}

		return rowCount, result.Error()
	}
}

// --- Base dataset scenarios (n1 -> n2 -> n3) ---

func baseScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "base"
	return []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
		{Section: "Shortest Paths", Dataset: ds, Label: "n1 -> n3", Query: cypherQuery(fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			idMap["n1"], idMap["n3"],
		))},
		{Section: "Traversal", Dataset: ds, Label: "n1", Query: cypherQuery(fmt.Sprintf(
			"MATCH (s)-[*1..]->(e) WHERE id(s) = %d RETURN e",
			idMap["n1"],
		))},
		{Section: "Match Return", Dataset: ds, Label: "n1", Query: cypherQuery(fmt.Sprintf(
			"MATCH (s)-[]->(e) WHERE id(s) = %d RETURN e",
			idMap["n1"],
		))},
		{Section: "Filter By Kind", Dataset: ds, Label: "NodeKind1", Query: cypherQuery("MATCH (n:NodeKind1) RETURN n")},
		{Section: "Filter By Kind", Dataset: ds, Label: "NodeKind2", Query: cypherQuery("MATCH (n:NodeKind2) RETURN n")},
	}
}

const adcsFanoutObjectID = "S-1-5-21-2643190041-1319121918-239771340-513"

func adcsFanoutScenarios() []Scenario {
	ds := "adcs_fanout"

	p1 := fmt.Sprintf(`
MATCH (n:Group) WHERE n.objectid = '%s'
MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
RETURN p1
`, adcsFanoutObjectID)

	p2 := fmt.Sprintf(`
MATCH (n:Group) WHERE n.objectid = '%s'
MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d:Domain)
WHERE ct.authenticationenabled = true
AND ct.requiresmanagerapproval = false
AND ct.enrolleesuppliessubject = true
AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
RETURN p2
`, adcsFanoutObjectID)

	combinedMatch := fmt.Sprintf(`
MATCH (n:Group) WHERE n.objectid = '%s'
MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d)
WHERE ct.authenticationenabled = true
AND ct.requiresmanagerapproval = false
AND ct.enrolleesuppliessubject = true
AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
`, adcsFanoutObjectID)

	return []Scenario{
		{Section: "ADCS Fanout", Dataset: ds, Label: "p1 only", Query: cypherQuery(p1)},
		{Section: "ADCS Fanout", Dataset: ds, Label: "p2 only", Query: cypherQuery(p2)},
		{Section: "ADCS Fanout", Dataset: ds, Label: "combined", Query: cypherQuery(combinedMatch + "RETURN p1,p2")},
	}
}

// --- Phantom scenarios (hardcoded node IDs from the dataset) ---

func phantomScenarios(idMap opengraph.IDMap) []Scenario {
	ds := "local/phantom"

	scenarios := []Scenario{
		{Section: "Match Nodes", Dataset: ds, Label: ds, Query: countNodes},
		{Section: "Match Edges", Dataset: ds, Label: ds, Query: countEdges},
	}

	for _, kind := range []string{"User", "Group", "Computer"} {
		k := kind
		scenarios = append(scenarios, Scenario{
			Section: "Filter By Kind",
			Dataset: ds,
			Label:   k,
			Query:   cypherQuery(fmt.Sprintf("MATCH (n:%s) RETURN n", k)),
		})
	}

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

	if _, ok := idMap["41"]; ok {
		if _, ok := idMap["587"]; ok {
			scenarios = append(scenarios, Scenario{
				Section: "Shortest Paths",
				Dataset: ds,
				Label:   "41 -> 587",
				Query: cypherQuery(fmt.Sprintf(
					"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
					idMap["41"], idMap["587"],
				)),
			})
		}
	}

	return scenarios
}
