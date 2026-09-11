// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

// TestVerifyPromotionManifestRequiresQualifiedSPI2Caps verifies that final
// promotion evidence cannot authorize cap values outside the qualified study.
func TestVerifyPromotionManifestRequiresQualifiedSPI2Caps(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion:   optimize.ShortestPathSelectorStaticV8HiddenFanIn,
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ShortestPathExecutorS4CanonicalDistance),
		SourceCommit:      "deadbeef",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      spI2FullCorpusSHA256,
		Caps:              spI2PromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "hidden-fan-in-depth32",
			QuerySHA256:           []string{spI2QuerySHA256},
			Direction:             "inbound",
			ObservationMode:       string(optimize.ShortestPathObservationDistance),
			MinimumDepth:          1,
			MaximumDepth:          32,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}

	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, base))
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)

	for name, test := range map[string]struct {
		capName string
		value   int64
	}{
		"non-qualified state cap":    {capName: "state_limit", value: 1000},
		"non-qualified frontier cap": {capName: "frontier_limit", value: 100},
	} {
		t.Run(name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			manifest.Caps[test.capName] = test.value

			verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, verification.Reasons, fmt.Sprintf(
				"SP-I2 distance cap %s must equal %d",
				test.capName,
				spI2PromotionCaps()[test.capName],
			))
		})
	}
}
