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
	ScanLookupScaleDataset     = "generated_scan_lookups"
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

// NewDirectWriteScaleFixture returns a deterministic graph for direct batch
// mutation tests. The requested number of target nodes is used exactly so that
// callers can exercise batch-flush boundaries without fixture rounding.
//
// Every target has one deletable relationship and one relationship-upsert
// baseline. Deletion directions alternate, while the first two targets (when
// present) provide self-connected and high-degree cascade shapes. Separate
// root-to-survivor relationships are never incident to a target, including a
// same-kind survivor for exact delete-set assertions.
func NewDirectWriteScaleFixture(targets int) *opengraph.Graph {
	if targets < 0 {
		targets = 0
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "write-root", Kinds: []string{"WriteEndpoint"}, Properties: map[string]any{"objectid": "write-root", "role": "root"}},
			{ID: "write-survivor", Kinds: []string{"WriteEndpoint"}, Properties: map[string]any{"objectid": "write-survivor", "role": "survivor"}},
		},
		Edges: []opengraph.Edge{
			{StartID: "write-root", EndID: "write-survivor", Kind: "WriteSurvivor", Properties: map[string]any{"marker": "survivor"}},
			{StartID: "write-root", EndID: "write-survivor", Kind: "WriteDeleteRelationship", Properties: map[string]any{"deletebatch": false, "marker": "same-kind-survivor"}},
		},
	}

	targetIDs := FixtureNames("write-target", targets)
	for idx, targetID := range targetIDs {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    targetID,
			Kinds: []string{"WriteDeleteNode", "WriteUpdateNode"},
			Properties: map[string]any{
				"objectid":    targetID,
				"deletebatch": true,
				"lastseen":    "2026-01-01T00:00:00Z",
				"ordinal":     idx,
			},
		})

		startID, endID := "write-root", targetID
		if idx%2 == 1 {
			startID, endID = targetID, "write-root"
		}
		fixture.Edges = append(fixture.Edges,
			opengraph.Edge{
				StartID: startID,
				EndID:   endID,
				Kind:    "WriteDeleteRelationship",
				Properties: map[string]any{
					"deletebatch": true,
					"marker":      targetID,
				},
			},
			opengraph.Edge{
				StartID: "write-root",
				EndID:   targetID,
				Kind:    "WriteUpdateRelationship",
				Properties: map[string]any{
					"lastseen": "2026-01-01T00:00:00Z",
					"marker":   targetID,
				},
			},
		)
	}

	if targets > 0 {
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    targetIDs[0],
			EndID:      targetIDs[0],
			Kind:       "WriteIncident",
			Properties: map[string]any{"marker": "self"},
		})
	}
	if targets > 1 {
		for idx, targetID := range targetIDs {
			fixture.Edges = append(fixture.Edges, opengraph.Edge{
				StartID:    targetIDs[1],
				EndID:      targetID,
				Kind:       "WriteIncident",
				Properties: map[string]any{"marker": fmt.Sprintf("high-%04d", idx)},
			})
		}
	}

	return fixture
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
			{ID: "list-source-duplicate", Kinds: []string{"Source"}, Properties: map[string]any{"objectid": "list-source-duplicate"}},
			{ID: "sink", Kinds: []string{"Destination"}, Properties: map[string]any{"objectid": "sink"}},
			{ID: "inbound-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "rec-in"}},
			{ID: "outbound-target", Kinds: []string{"ADEntity", "Computer"}, Properties: map[string]any{"objectid": "rec-out"}},
			{ID: "list-target", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "rec-list"}},
			{ID: "template", Kinds: []string{"CertTemplate"}, Properties: map[string]any{"objectid": "template"}},
			{ID: "agent", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "agent"}},
			{ID: "agent-duplicate", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "agent-duplicate"}},
			{ID: "delete-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "delete-target"}},
			{ID: "survivor", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "survivor"}},
		},
		Edges: []opengraph.Edge{
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-01-a"}},
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-01-b"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-02-a"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-02-b"}},
			{StartID: "source", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-a"}},
			{StartID: "list-source-duplicate", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-b"}},
			{StartID: "agent", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-a"}},
			{StartID: "agent-duplicate", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-b"}},
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
		trustEarlyID := "trust-early-" + suffix
		oldNodeID := "prune-old-" + suffix
		newNodeID := "prune-new-" + suffix
		orphanNodeID := "orphan-scale-" + suffix
		batchNodeID := "prune-batch-" + suffix
		neighborID := "prune-neighbor-" + suffix
		candidateOldTargetID := "candidate-old-target-" + suffix
		candidateNewTargetID := "candidate-new-target-" + suffix
		sessionMissingTargetID := "session-missing-target-" + suffix
		sessionOldTargetID := "session-old-target-" + suffix
		sessionEqualTargetID := "session-equal-target-" + suffix
		batchEdgeTargetID := "prune-batch-edge-target-" + suffix

		fixture.Nodes = append(fixture.Nodes,
			opengraph.Node{ID: trustEarlyID, Kinds: []string{"Domain"}, Properties: map[string]any{"lastcollected": "2026-01-02T00:00:00Z"}},
			opengraph.Node{ID: oldNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": oldNodeID, "lastseen": "2026-01-02T00:00:00Z"}},
			opengraph.Node{ID: newNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"name": newNodeID, "lastseen": "2026-01-04T00:00:00Z"}},
			opengraph.Node{ID: orphanNodeID, Kinds: []string{"PruneCandidate"}, Properties: map[string]any{"objectid": "S-1-5-" + suffix}},
			opengraph.Node{ID: batchNodeID, Kinds: []string{"PruneBatchNode"}, Properties: map[string]any{"remove": idx%2 == 0}},
			opengraph.Node{ID: neighborID, Kinds: []string{"PruneNeighbor"}, Properties: map[string]any{"name": neighborID}},
			opengraph.Node{ID: candidateOldTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": candidateOldTargetID}},
			opengraph.Node{ID: candidateNewTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": candidateNewTargetID}},
			opengraph.Node{ID: sessionMissingTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": sessionMissingTargetID}},
			opengraph.Node{ID: sessionOldTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": sessionOldTargetID}},
			opengraph.Node{ID: sessionEqualTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": sessionEqualTargetID}},
			opengraph.Node{ID: batchEdgeTargetID, Kinds: []string{"PruneEndpoint"}, Properties: map[string]any{"name": batchEdgeTargetID}},
		)

		fixture.Edges = append(fixture.Edges,
			opengraph.Edge{StartID: "trust-late-a", EndID: trustEarlyID, Kind: "SameForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "same-old-" + suffix}},
			opengraph.Edge{StartID: "trust-late-a", EndID: trustEarlyID, Kind: "CrossForestTrust", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "cross-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: candidateOldTargetID, Kind: "CandidateRel", Properties: map[string]any{"lastseen": "2026-01-02T00:00:00Z", "marker": "candidate-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: candidateNewTargetID, Kind: "CandidateRel", Properties: map[string]any{"lastseen": "2026-01-04T00:00:00Z", "marker": "candidate-new-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: sessionMissingTargetID, Kind: "HasSession", Properties: map[string]any{"marker": "session-missing-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: sessionOldTargetID, Kind: "HasSession", Properties: map[string]any{"lastseen": "2026-01-02T00:00:00Z", "marker": "session-old-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: sessionEqualTargetID, Kind: "HasSession", Properties: map[string]any{"lastseen": "2026-01-03T00:00:00Z", "marker": "session-equal-" + suffix}},
			opengraph.Edge{StartID: "prune-a", EndID: batchEdgeTargetID, Kind: "PruneBatch", Properties: map[string]any{"remove": true, "marker": "batch-" + suffix}},
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

// NewScanLookupScaleFixture returns deterministic wide-scan, large lookup,
// adjacency, ordering, and count shapes for the scan/lookup regression corpus.
func NewScanLookupScaleFixture(fanout int) *opengraph.Graph {
	if fanout < 9 {
		fanout = 128
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "scan-base-root", Kinds: []string{"ADBase"}, Properties: map[string]any{"name": "scan-base-root"}},
			{ID: "scan-tracker-root", Kinds: []string{"Plain"}, Properties: map[string]any{"name": "scan-tracker-root"}},
			{ID: "scan-adcs-target", Kinds: []string{"Computer"}, Properties: map[string]any{"name": "scan-adcs-target"}},
			{ID: "scan-local-target", Kinds: []string{"Computer"}, Properties: map[string]any{"name": "scan-local-target"}},
			{ID: "lookup-tenant", Kinds: []string{"Tenant"}, Properties: map[string]any{"name": "lookup-tenant", "objectid": "tenant-scale"}},
			{ID: "lookup-local-target", Kinds: []string{"Computer"}, Properties: map[string]any{"name": "lookup-local-target"}},
		},
	}

	escalationKinds := []string{"GenericAll", "GenericWrite", "Owns", "WriteOwner", "WriteDACL", "WritePublicInformation"}
	victimIDs := FixtureNames("scan-victim", max(1_000, fanout))
	for idx := range fanout {
		suffix := fmt.Sprintf("%04d", idx)
		scanEndID := "scan-end-" + suffix
		scanEntityID := "scan-entity-" + suffix
		lookupObjectID := "lookup-object-" + suffix
		lookupStringID := "lookup-string-" + suffix
		lookupLocalID := "lookup-local-" + suffix
		ntlmID := "lookup-ntlm-" + suffix

		entityKinds := []string{"Entity"}
		switch idx % 3 {
		case 0:
			entityKinds = append(entityKinds, "Group")
		case 1:
			entityKinds = append(entityKinds, "User")
		case 2:
			entityKinds = append(entityKinds, "Computer")
		}

		lookupObjectSuffix := "-513"
		if idx%2 == 0 {
			lookupObjectSuffix = "-512"
		}
		lookupName := fmt.Sprintf("Remote Desktop Users %04d", idx)
		if idx%2 == 1 {
			lookupName = fmt.Sprintf("rEmOtE dEsKtOp UsErS %04d", idx)
		}

		fixture.Nodes = append(fixture.Nodes,
			opengraph.Node{ID: scanEndID, Kinds: []string{"AZBase", "Plain"}, Properties: map[string]any{"name": scanEndID}},
			opengraph.Node{ID: scanEntityID, Kinds: entityKinds, Properties: map[string]any{"name": scanEntityID}},
			opengraph.Node{ID: lookupObjectID, Kinds: []string{"Computer"}, Properties: map[string]any{"name": lookupObjectID, "objectid": "S-1-5-21-scale", "enabled": true}},
			opengraph.Node{ID: lookupStringID, Kinds: []string{"Group", "Entity"}, Properties: map[string]any{"name": lookupName, "objectid": "S-1-5-21" + lookupObjectSuffix, "domainsid": "S-1-5-21"}},
			opengraph.Node{ID: lookupLocalID, Kinds: []string{"LocalGroup", "Entity"}, Properties: map[string]any{"name": lookupLocalID, "objectid": "S-1-5-21-555"}},
			opengraph.Node{ID: ntlmID, Kinds: []string{"Computer"}, Properties: map[string]any{"name": ntlmID, "domainsid": "S-1-5-21", "isdc": true, "ldapavailable": true, "ldapsigning": false}},
		)

		migrationProperties := map[string]any{"marker": "migration-" + suffix}
		if idx%2 == 0 {
			migrationProperties["lastseen"] = "2026-01-03T00:00:00Z"
		} else if idx%4 == 1 {
			migrationProperties["lastseen"] = nil
		}

		victimID := victimIDs[idx]
		fixture.Edges = append(fixture.Edges,
			opengraph.Edge{StartID: "scan-base-root", EndID: scanEndID, Kind: "ScanPostProcessed", Properties: map[string]any{"marker": "post-" + suffix}},
			opengraph.Edge{StartID: "scan-tracker-root", EndID: scanEndID, Kind: "TrackerA", Properties: map[string]any{"marker": "tracker-a-" + suffix}},
			opengraph.Edge{StartID: "scan-tracker-root", EndID: scanEndID, Kind: "TrackerB", Properties: map[string]any{"marker": "tracker-b-" + suffix}},
			opengraph.Edge{StartID: "scan-tracker-root", EndID: scanEndID, Kind: "MigratedEdge", Properties: migrationProperties},
			opengraph.Edge{StartID: scanEntityID, EndID: scanEndID, Kind: "OwnsRaw", Properties: map[string]any{"marker": "owns-" + suffix}},
			opengraph.Edge{StartID: scanEntityID, EndID: "scan-adcs-target", Kind: fmt.Sprintf("ADCSEdge%02d", idx%9+1), Properties: map[string]any{"marker": "adcs-" + suffix}},
			opengraph.Edge{StartID: scanEntityID, EndID: "scan-local-target", Kind: "LocalToComputer", Properties: map[string]any{"marker": "scan-local-" + suffix}},
			opengraph.Edge{StartID: scanEntityID, EndID: scanEndID, Kind: "MemberOf", Properties: map[string]any{"marker": "member-" + suffix}},
			opengraph.Edge{StartID: scanEntityID, EndID: scanEndID, Kind: "MemberOfLocalGroup", Properties: map[string]any{"marker": "member-local-" + suffix}},
			opengraph.Edge{StartID: scanEntityID, EndID: victimID, Kind: escalationKinds[idx%len(escalationKinds)], Properties: map[string]any{"marker": "esc-" + suffix}},
			opengraph.Edge{StartID: lookupLocalID, EndID: "lookup-local-target", Kind: "LocalToComputer", Properties: map[string]any{"marker": "lookup-local-" + suffix}},
		)
	}

	for idx, victimID := range victimIDs {
		victimKinds := []string{"Other"}
		if idx%2 == 0 {
			victimKinds = []string{"Computer"}
		}
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: victimID, Kinds: victimKinds, Properties: map[string]any{"name": victimID}})
	}

	for _, targetID := range FixtureNames("lookup-id-target", 1_000) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: targetID, Kinds: []string{"Hydrate"}, Properties: map[string]any{"name": targetID}})
	}

	for idx, roleID := range FixtureNames("lookup-role", 1_000) {
		roleTemplateID := fmt.Sprintf("role-template-%03d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: roleID, Kinds: []string{"AZRole"}, Properties: map[string]any{"name": roleID, "roletemplateid": roleTemplateID, "enabled": true}})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: "lookup-tenant", EndID: roleID, Kind: "Contains", Properties: map[string]any{"marker": roleID}})
	}

	return fixture
}
