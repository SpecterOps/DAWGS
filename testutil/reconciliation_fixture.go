// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package testutil

import (
	"fmt"

	"github.com/specterops/dawgs/opengraph"
)

const (
	ReconciliationScaleDataset = "generated_reconciliation"
	TrustPruningScaleDataset   = "generated_trust_pruning"
	HopScaleDataset            = "generated_hops"
)

// GeneratedNodeListParam resolves optional fixture IDs followed by a
// deterministic prefix/count sequence. It keeps high-cardinality database-ID
// parameters out of handwritten JSON.
type GeneratedNodeListParam struct {
	Prefix  string   `json:"prefix"`
	Count   int      `json:"count"`
	Include []string `json:"include,omitempty"`
}

// FixtureNames returns stable, zero-padded fixture IDs.
func FixtureNames(prefix string, count int) []string {
	if count < 0 {
		count = 0
	}

	width := len(fmt.Sprintf("%d", max(count-1, 0)))
	if width < 2 {
		width = 2
	}

	values := make([]string, count)
	for idx := range count {
		values[idx] = fmt.Sprintf("%s-%0*d", prefix, width, idx)
	}
	return values
}

// NewReconciliationScaleFixture returns the deterministic graphbench fixture
// for the ingestion reconciliation forms. fanout controls the degree of the
// REC-08 detach-delete target.
func NewReconciliationScaleFixture(fanout int) *opengraph.Graph {
	if fanout < 1 {
		fanout = 128
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "source", Kinds: []string{"Source"}, Properties: map[string]any{"objectid": "source"}},
			{ID: "sink", Kinds: []string{"Destination"}, Properties: map[string]any{"objectid": "sink"}},
			{ID: "inbound-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "rec-in"}},
			{ID: "outbound-target", Kinds: []string{"ADEntity", "Computer"}, Properties: map[string]any{"objectid": "rec-out"}},
			{ID: "list-target", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "rec-list"}},
			{ID: "template", Kinds: []string{"CertTemplate"}, Properties: map[string]any{"objectid": "template"}},
			{ID: "agent", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "agent"}},
			{ID: "delete-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "delete-target"}},
			{ID: "survivor", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "survivor"}},
		},
		Edges: []opengraph.Edge{
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-01-a"}},
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-01-b"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-02-a"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-02-b"}},
			{StartID: "source", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-a"}},
			{StartID: "source", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-b"}},
			{StartID: "agent", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-a"}},
			{StartID: "agent", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-b"}},
			{StartID: "source", EndID: "survivor", Kind: "Survivor", Properties: map[string]any{"marker": "survivor"}},
		},
	}

	for _, templateID := range FixtureNames("scale-template", 2_000) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         templateID,
			Kinds:      []string{"CertTemplate"},
			Properties: map[string]any{"objectid": templateID},
		})
	}

	// Ensure every relationship kind in the 30-kind disjunction is registered,
	// while anchoring each decoy away from the REC-01/REC-02 target endpoints.
	for idx := 2; idx < 30; idx++ {
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    "source",
			EndID:      "survivor",
			Kind:       fmt.Sprintf("RecKind%02d", idx),
			Properties: map[string]any{"marker": fmt.Sprintf("kind-decoy-%02d", idx)},
		})
	}

	for idx := range fanout {
		neighborID := fmt.Sprintf("detach-neighbor-%04d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         neighborID,
			Kinds:      []string{"ADEntity"},
			Properties: map[string]any{"objectid": neighborID},
		})

		startID, endID := "delete-target", neighborID
		if idx%2 == 0 {
			startID, endID = neighborID, "delete-target"
		}
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    startID,
			EndID:      endID,
			Kind:       "Incident",
			Properties: map[string]any{"marker": fmt.Sprintf("incident-%04d", idx)},
		})
	}

	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID:    "delete-target",
		EndID:      "delete-target",
		Kind:       "Incident",
		Properties: map[string]any{"marker": "incident-self"},
	})
	return fixture
}

// NewTrustPruningScaleFixture returns deterministic dense trust and pruning
// shapes without changing the cardinalities of the reconciliation fixture.
func NewTrustPruningScaleFixture(fanout int) *opengraph.Graph {
	if fanout < 1 {
		fanout = 128
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "trust-early", Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-02T00:00:00Z"}},
			{ID: "trust-late-a", Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-04T00:00:00Z"}},
			{ID: "trust-late-b", Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-04T00:00:00Z"}},
			{ID: "trust-equal-a", Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-03T00:00:00Z"}},
			{ID: "trust-equal-b", Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-03T00:00:00Z"}},
			{ID: "prune-a", Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": "a"}},
			{ID: "prune-b", Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": "b"}},
			{ID: "prune-missing", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": "missing"}},
			{ID: "prune-null", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": "null", "lastseen": nil}},
			{ID: "prune-protected", Kinds: []string{"PruneCandidate", "Domain"}, Properties: map[string]any{"name": "protected", "lastseen": "2026-01-02T00:00:00Z"}},
			{ID: "orphan-missing", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"objectid": "S-1-5-100"}},
			{ID: "orphan-null", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": nil, "objectid": "S-1-5-101"}},
			{ID: "orphan-named", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": "named", "objectid": "S-1-5-102"}},
			{ID: "orphan-wrong-prefix", Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"objectid": "X-1-5-103"}},
			{ID: "prune-batch-high", Kinds: []string{"PruneBatchNode"}, Properties: map[string]any{"remove": true}},
			{ID: "prune-batch-survivor", Kinds: []string{"PruneBatchNode"}, Properties: map[string]any{"remove": false}},
		},
		Edges: []opengraph.Edge{
			{StartID: "trust-equal-a", EndID: "trust-equal-b", Kind: "SameForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "same-equal"}},
			{StartID: "trust-equal-a", EndID: "trust-equal-b", Kind: "CrossForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "cross-equal"}},
			{StartID: "trust-late-a", EndID: "trust-late-b", Kind: "SameForestTrust", Properties: map[string]any{"lastseen": "2026-01-05T00:00:00Z", "marker": "same-new"}},
			{StartID: "trust-late-a", EndID: "trust-late-b", Kind: "CrossForestTrust", Properties: map[string]any{"lastseen": "2026-01-05T00:00:00Z", "marker": "cross-new"}},
			{StartID: "trust-late-a", EndID: "trust-late-b", Kind: "AbuseTGTDelegation", Properties: map[string]any{"marker": "valid-forward-abuse"}},
			{StartID: "trust-late-b", EndID: "trust-late-a", Kind: "SpoofSIDHistory", Properties: map[string]any{"marker": "valid-reverse-spoof"}},
			{StartID: "trust-late-a", EndID: "trust-late-b", Kind: "SpoofSIDHistory", Properties: map[string]any{"marker": "invalid-forward-spoof"}},
			{StartID: "trust-late-b", EndID: "trust-late-a", Kind: "AbuseTGTDelegation", Properties: map[string]any{"marker": "invalid-reverse-abuse"}},
			{StartID: "prune-a", EndID: "prune-b", Kind: "PruneBatchSurvivor", Properties: map[string]any{"remove": false}},
			{StartID: "prune-a", EndID: "prune-b", Kind: "MetaIncludes", Properties: map[string]any{"lastseen": "2026-01-02T00:00:00Z", "marker": "protected-meta-includes"}},
		},
	}

	for idx := range fanout {
		suffix := fmt.Sprintf("%04d", idx)
		oldNodeID := "prune-old-" + suffix
		newNodeID := "prune-new-" + suffix
		orphanNodeID := "orphan-scale-" + suffix
		batchNodeID := "prune-batch-" + suffix
		neighborID := "prune-neighbor-" + suffix

		fixture.Nodes = append(fixture.Nodes,
			opengraph.Node{ID: oldNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": oldNodeID, "lastseen": "2026-01-02T00:00:00Z"}},
			opengraph.Node{ID: newNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": newNodeID, "lastseen": "2026-01-04T00:00:00Z"}},
			opengraph.Node{ID: orphanNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"objectid": "S-1-5-" + suffix}},
			opengraph.Node{ID: batchNodeID, Kinds: []string{"PruneBatchNode"}, Properties: map[string]any{"remove": idx%2 == 0}},
			opengraph.Node{ID: neighborID, Kinds: []string{"PruneNeighbor"}, Properties: map[string]any{"name": neighborID}},
		)

		fixture.Edges = append(fixture.Edges,
			opengraph.Edge{StartID: "trust-late-a", EndID: "trust-early", Kind: "SameForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "same-old-" + suffix}},
			opengraph.Edge{StartID: "trust-late-a", EndID: "trust-early", Kind: "CrossForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "cross-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "CandidateRel", Properties: map[string]any{"lastseen": "2026-01-02T00:00:00Z", "marker": "candidate-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "CandidateRel", Properties: map[string]any{"lastseen": "2026-01-04T00:00:00Z", "marker": "candidate-new-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "HasSession", Properties: map[string]any{"marker": "session-missing-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "HasSession", Properties: map[string]any{"lastseen": "2026-01-02T00:00:00Z", "marker": "session-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "HasSession", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "session-equal-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: "prune-b", Kind: "PruneBatch", Properties: map[string]any{"remove": true, "marker": "batch-" + suffix}},
			opengraph.Edge{StartID: "prune-batch-high", EndID: neighborID, Kind: "PruneIncident", Properties: map[string]any{"marker": "incident-" + suffix}},
		)
	}

	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID:    "prune-batch-high",
		EndID:      "prune-batch-high",
		Kind:       "PruneIncident",
		Properties: map[string]any{"marker": "incident-self"},
	})
	return fixture
}

// NewHopScaleFixture returns deterministic one-hop fanout, kind-cardinality,
// endpoint-list, predicate-selectivity, and two-sided set shapes.
func NewHopScaleFixture(fanout int) *opengraph.Graph {
	if fanout < 30 {
		fanout = 128
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "hop-out-root", Kinds: []string{"HopAnchor"}, Properties: map[string]any{"name": "hop-out-root"}},
			{ID: "hop-in-root", Kinds: []string{"HopAnchor"}, Properties: map[string]any{"name": "hop-in-root"}},
			{ID: "hop-kind-root", Kinds: []string{"HopAnchor"}, Properties: map[string]any{"name": "hop-kind-root"}},
			{ID: "hop-decoy-root", Kinds: []string{"HopAnchor"}, Properties: map[string]any{"name": "hop-decoy-root"}},
		},
	}

	for idx := range fanout {
		suffix := fmt.Sprintf("%04d", idx)
		peerID := "hop-peer-" + suffix
		sourceID := "hop-source-" + suffix
		properties := map[string]any{
			"name":                    peerID,
			"requiresmanagerapproval": false,
			"authenticationenabled":   true,
		}
		switch idx % 4 {
		case 0:
			properties["schemaversion"] = 2
			properties["authorizedsignatures"] = 0
		case 1:
			properties["schemaversion"] = 1
			properties["authorizedsignatures"] = 9
		case 2:
			properties["schemaversion"] = 2
			properties["authorizedsignatures"] = 1
		case 3:
			properties["schemaversion"] = 2
			properties["authorizedsignatures"] = 0
			properties["authenticationenabled"] = false
		}

		peerKinds := []string{"HopEndpoint", "HopEndA", "HopTemplate"}
		if idx%2 == 0 {
			peerKinds = append(peerKinds, "HopEndB")
		}
		fixture.Nodes = append(fixture.Nodes,
			opengraph.Node{ID: peerID, Kinds: peerKinds, Properties: properties},
			opengraph.Node{ID: sourceID, Kinds: []string{"HopSource"}, Properties: map[string]any{"name": sourceID}},
		)
		fixture.Edges = append(fixture.Edges,
			opengraph.Edge{StartID: "hop-out-root", EndID: peerID, Kind: "HopKind01", Properties: map[string]any{"marker": "out-" + suffix}},
			opengraph.Edge{StartID: sourceID, EndID: "hop-in-root", Kind: "HopKind01", Properties: map[string]any{"marker": "in-" + suffix}},
			opengraph.Edge{StartID: "hop-kind-root", EndID: peerID, Kind: fmt.Sprintf("HopKind%02d", idx%30+1), Properties: map[string]any{"marker": "kind-" + suffix}},
			opengraph.Edge{StartID: "hop-out-root", EndID: peerID, Kind: "HopTypedEdge", Properties: map[string]any{"marker": "typed-" + suffix}},
			opengraph.Edge{StartID: "hop-out-root", EndID: peerID, Kind: "HopNestedEdge", Properties: map[string]any{"marker": "nested-" + suffix}},
		)
	}

	for idx, targetID := range FixtureNames("hop-id-target", 1_000) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         targetID,
			Kinds:      []string{"HopIDEndpoint"},
			Properties: map[string]any{"name": targetID},
		})
		if idx < fanout {
			fixture.Edges = append(fixture.Edges, opengraph.Edge{
				StartID:    "hop-out-root",
				EndID:      targetID,
				Kind:       "HopIDEdge",
				Properties: map[string]any{"marker": fmt.Sprintf("id-%04d", idx)},
			})
		}
	}

	setStarts := FixtureNames("hop-set-start", 32)
	setEnds := FixtureNames("hop-set-end", 32)
	for _, startID := range setStarts {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: startID, Kinds: []string{"HopSetStart"}, Properties: map[string]any{"name": startID}})
	}
	for _, endID := range setEnds {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: endID, Kinds: []string{"HopSetEnd"}, Properties: map[string]any{"name": endID}})
	}
	for _, startID := range setStarts {
		for _, endID := range setEnds {
			fixture.Edges = append(fixture.Edges, opengraph.Edge{
				StartID:    startID,
				EndID:      endID,
				Kind:       "HopSetEdge",
				Properties: map[string]any{"marker": startID + "-" + endID},
			})
		}
	}
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{StartID: "hop-decoy-root", EndID: "hop-peer-0000", Kind: "HopTypedEdge", Properties: map[string]any{"marker": "wrong-root"}},
		opengraph.Edge{StartID: "hop-peer-0000", EndID: "hop-out-root", Kind: "HopTypedEdge", Properties: map[string]any{"marker": "wrong-direction"}},
		opengraph.Edge{StartID: setStarts[0], EndID: setEnds[0], Kind: "HopWrongSetEdge", Properties: map[string]any{"marker": "wrong-set-kind"}},
		opengraph.Edge{StartID: setEnds[0], EndID: setStarts[0], Kind: "HopSetEdge", Properties: map[string]any{"marker": "wrong-set-direction"}},
	)
	return fixture
}
