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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig verifies odd/even round rotation without modifying the configured backend order.
func TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig(t *testing.T) {
	modes := []ExecutionMode{ModePostgresSQL, ModeNeo4j}

	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modesForRound(modes, 1))
	require.Equal(t, []ExecutionMode{ModeNeo4j, ModePostgresSQL}, modesForRound(modes, 2))
	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modes)
}

// TestParseConfigRequiresCompleteGateInputs verifies that baseline gating cannot be enabled without its paired candidate artifact.
func TestParseConfigRequiresCompleteGateInputs(t *testing.T) {
	_, err := parseConfig([]string{"-gate-baseline", "baseline.jsonl"}, func(string) string { return "" })

	require.ErrorContains(t, err, "must be supplied together")
}

// TestParseConfigDefaultsQualificationConfidence verifies every statistical workflow starts at the frozen 97.5% policy.
func TestParseConfigDefaultsQualificationConfidence(t *testing.T) {
	cfg, err := parseConfig(nil, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, defaultConfidenceLevel, cfg.Confidence)
	require.Equal(t, minimumTimingNoiseRatio, cfg.Regression)
}

// TestParseConfigRequiresGateAAForPromotion verifies only explicit diagnostic comparisons may omit host calibration.
func TestParseConfigRequiresGateAAForPromotion(t *testing.T) {
	_, err := parseConfig([]string{"-gate-baseline", "baseline.jsonl", "-gate-candidate", "candidate.jsonl"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires gate-aa")

	cfg, err := parseConfig([]string{
		"-gate-baseline", "baseline.jsonl", "-gate-candidate", "candidate.jsonl", "-gate-aa", "aa.json",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "aa.json", cfg.GateAA)
}

// TestParseConfigAcceptsNamedBundleEvidence verifies repeatable name=path inputs are retained for capture without conflating their host paths with evidence names.
func TestParseConfigAcceptsNamedBundleEvidence(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-bundle-dir", "capture",
		"-bundle-evidence", "host-aa=.coverage/aa.json",
		"-bundle-evidence", "plan-delta=.coverage/plan-delta.json",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, []CaptureBundleEvidenceInput{
		{
			Name: "host-aa",
			Path: ".coverage/aa.json",
		},
		{
			Name: "plan-delta",
			Path: ".coverage/plan-delta.json",
		},
	}, cfg.BundleEvidence)
}

// TestParseConfigAcceptsStandaloneBundleVerification verifies portable verification can run without a benchmark connection and optionally enforce clean-source provenance.
func TestParseConfigAcceptsStandaloneBundleVerification(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-bundle-verify", "capture",
		"-bundle-verify-output", "verification.json",
		"-bundle-require-clean",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "capture", cfg.BundleVerify)
	require.Equal(t, "verification.json", cfg.BundleVerifyOutput)
	require.True(t, cfg.BundleRequireClean)
}

// TestParseConfigAcceptsOnlyStandalonePromotionManifestVerification verifies parse config accepts only standalone promotion manifest verification behavior.
func TestParseConfigAcceptsOnlyStandalonePromotionManifestVerification(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-promotion-manifest", "promotion.json",
		"-promotion-manifest-output", "verification.json",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "promotion.json", cfg.PromotionManifest)
	require.Equal(t, "verification.json", cfg.PromotionManifestOutput)

	for _, args := range [][]string{
		{"-promotion-manifest-output", "verification.json"},
		{"-promotion-manifest", "promotion.json", "-bundle-verify", "capture"},
		{"-promotion-manifest", "promotion.json", "-resource-artifact", "resources.jsonl"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigRejectsMalformedOrOrphanedBundleFlags verifies capture and verification inputs fail before any artifact or database is touched.
func TestParseConfigRejectsMalformedOrOrphanedBundleFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-bundle-evidence", "host-aa=aa.json"},
		{"-bundle-dir", "capture", "-bundle-evidence", "missing-separator"},
		{"-bundle-dir", "capture", "-bundle-evidence", "../escape=aa.json"},
		{"-bundle-dir", "capture", "-bundle-evidence", "host-aa=one.json", "-bundle-evidence", "host-aa=two.json"},
		{"-bundle-verify-output", "verification.json"},
		{"-bundle-require-clean"},
		{"-bundle-verify", "capture", "-bundle-dir", "new-capture"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsExpandIntoStudyProtocols verifies standalone three-arm reports expose the frozen discovery and confirmation evidence contracts.
func TestParseConfigAcceptsExpandIntoStudyProtocols(t *testing.T) {
	for _, protocol := range []string{referencePairProtocolDiscovery, referencePairProtocolConfirmation} {
		t.Run(protocol, func(t *testing.T) {
			cfg, err := parseConfig([]string{
				"-expand-into-artifact", "expand-into.jsonl",
				"-expand-into-output", "expand-into.json",
				"-expand-into-protocol", protocol,
			}, func(string) string { return "" })

			require.NoError(t, err)
			require.Equal(t, "expand-into.jsonl", cfg.ExpandIntoArtifact)
			require.Equal(t, "expand-into.json", cfg.ExpandIntoOutput)
			require.Equal(t, protocol, cfg.ExpandIntoProtocol)
		})
	}
}

// TestParseConfigAcceptsOrientationSelectorReport verifies the matched shadow,
// incumbent, forced-reverse, and A/A artifacts form one standalone workflow.
func TestParseConfigAcceptsOrientationSelectorReport(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-orientation-shadow-artifact", "shadow.jsonl",
		"-orientation-incumbent-artifact", "incumbent.jsonl",
		"-orientation-reverse-artifact", "reverse.jsonl",
		"-orientation-aa", "aa.json",
		"-orientation-output", "orientation.json",
		"-orientation-protocol", referencePairProtocolConfirmation,
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "shadow.jsonl", cfg.OrientationShadowArtifact)
	require.Equal(t, "incumbent.jsonl", cfg.OrientationIncumbentArtifact)
	require.Equal(t, "reverse.jsonl", cfg.OrientationReverseArtifact)
	require.Equal(t, "aa.json", cfg.OrientationAA)
	require.Equal(t, "orientation.json", cfg.OrientationOutput)
	require.Equal(t, referencePairProtocolConfirmation, cfg.OrientationProtocol)
}

// TestParseConfigAcceptsOrientationSelectorV2Report verifies parse config accepts orientation selector v2 report behavior.
func TestParseConfigAcceptsOrientationSelectorV2Report(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-orientation-v2-shadow-artifact", "shadow-v2.jsonl",
		"-orientation-v2-incumbent-artifact", "incumbent.jsonl",
		"-orientation-v2-reverse-artifact", "reverse.jsonl",
		"-orientation-v2-guarded-artifact", "guarded-v2.jsonl",
		"-orientation-v2-aa", "aa.json",
		"-orientation-v2-freeze", "orientation-v2-freeze.json",
		"-orientation-v2-discovery-report", "orientation-v2-discovery.json",
		"-orientation-v2-output", "orientation-v2.json",
		"-orientation-v2-protocol", referencePairProtocolConfirmation,
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "shadow-v2.jsonl", cfg.OrientationV2ShadowArtifact)
	require.Equal(t, "incumbent.jsonl", cfg.OrientationV2IncumbentArtifact)
	require.Equal(t, "reverse.jsonl", cfg.OrientationV2ReverseArtifact)
	require.Equal(t, "guarded-v2.jsonl", cfg.OrientationV2GuardedArtifact)
	require.Equal(t, "aa.json", cfg.OrientationV2AA)
	require.Equal(t, "orientation-v2-freeze.json", cfg.OrientationV2Freeze)
	require.Equal(t, "orientation-v2-discovery.json", cfg.OrientationV2DiscoveryReport)
	require.Equal(t, "orientation-v2.json", cfg.OrientationV2Output)
}

// TestParseConfigAcceptsOrientationSelectorV2DiscoveryFreeze verifies parse config accepts orientation selector v2 discovery freeze behavior.
func TestParseConfigAcceptsOrientationSelectorV2DiscoveryFreeze(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-orientation-v2-shadow-artifact", "shadow-v2.jsonl",
		"-orientation-v2-incumbent-artifact", "incumbent.jsonl",
		"-orientation-v2-reverse-artifact", "reverse.jsonl",
		"-orientation-v2-guarded-artifact", "guarded-v2.jsonl",
		"-orientation-v2-aa", "aa.json",
		"-orientation-v2-output", "orientation-v2-discovery.json",
		"-orientation-v2-freeze-output", "orientation-v2-freeze.json",
		"-orientation-v2-protocol", referencePairProtocolDiscovery,
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, referencePairProtocolDiscovery, cfg.OrientationV2Protocol)
	require.Equal(t, "orientation-v2-discovery.json", cfg.OrientationV2Output)
	require.Equal(t, "orientation-v2-freeze.json", cfg.OrientationV2FreezeOutput)
}

// TestParseConfigRejectsIncompleteOrMixedOrientationSelectorV2Report verifies parse config rejects incomplete or mixed orientation selector v2 report behavior.
func TestParseConfigRejectsIncompleteOrMixedOrientationSelectorV2Report(t *testing.T) {
	complete := []string{
		"-orientation-v2-shadow-artifact", "shadow-v2.jsonl",
		"-orientation-v2-incumbent-artifact", "incumbent.jsonl",
		"-orientation-v2-reverse-artifact", "reverse.jsonl",
		"-orientation-v2-guarded-artifact", "guarded-v2.jsonl",
		"-orientation-v2-aa", "aa.json",
		"-orientation-v2-freeze", "orientation-v2-freeze.json",
		"-orientation-v2-discovery-report", "orientation-v2-discovery.json",
	}
	for _, args := range [][]string{
		{"-orientation-v2-shadow-artifact", "shadow-v2.jsonl"},
		{
			"-orientation-v2-shadow-artifact", "shadow-v2.jsonl", "-orientation-v2-incumbent-artifact", "incumbent.jsonl",
			"-orientation-v2-reverse-artifact", "reverse.jsonl", "-orientation-v2-guarded-artifact", "guarded-v2.jsonl",
			"-orientation-v2-aa", "aa.json", "-orientation-v2-output", "report.json",
		},
		append(append([]string(nil), complete...), "-orientation-v2-protocol", "exploratory"),
		append(append([]string(nil), complete...), "-orientation-shadow-artifact", "shadow-v1.jsonl", "-orientation-incumbent-artifact", "incumbent.jsonl", "-orientation-reverse-artifact", "reverse.jsonl", "-orientation-aa", "aa.json"),
		append(append([]string(nil), complete...), "-expand-into-artifact", "expand.jsonl"),
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsSPI1StagedDiscoveryAndConfirmation verifies parse config accepts spi1 staged discovery and confirmation behavior.
func TestParseConfigAcceptsSPI1StagedDiscoveryAndConfirmation(t *testing.T) {
	discovery, err := parseConfig([]string{
		"-sp-i1-baseline-artifact", "s4-training.jsonl",
		"-sp-i1-candidate-artifact", "i1-training.jsonl",
		"-sp-i1-resource-report", "i1-training-resource.json",
		"-sp-i1-output", "sp-i1-discovery.json",
		"-sp-i1-freeze-output", "sp-i1-freeze.json",
		"-sp-i1-protocol", referencePairProtocolDiscovery,
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "s4-training.jsonl", discovery.SPI1BaselineArtifact)
	require.Equal(t, "i1-training.jsonl", discovery.SPI1CandidateArtifact)
	require.Equal(t, "i1-training-resource.json", discovery.SPI1ResourceReport)
	require.Equal(t, "sp-i1-freeze.json", discovery.SPI1FreezeOutput)

	confirmation, err := parseConfig([]string{
		"-sp-i1-baseline-artifact", "s4-confirmation.jsonl",
		"-sp-i1-candidate-artifact", "i1-confirmation.jsonl",
		"-sp-i1-resource-report", "i1-confirmation-resource.json",
		"-sp-i1-output", "sp-i1-confirmation.json",
		"-sp-i1-freeze", "sp-i1-freeze.json",
		"-sp-i1-discovery-report", "sp-i1-discovery.json",
		"-sp-i1-training-baseline-artifact", "s4-training.jsonl",
		"-sp-i1-training-candidate-artifact", "i1-training.jsonl",
		"-sp-i1-training-resource-report", "i1-training-resource.json",
		"-sp-i1-protocol", referencePairProtocolConfirmation,
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "sp-i1-freeze.json", confirmation.SPI1Freeze)
	require.Equal(t, "sp-i1-discovery.json", confirmation.SPI1DiscoveryReport)
}

func TestParseConfigAcceptsSPI2StagedDiscoveryAndConfirmation(t *testing.T) {
	discovery, err := parseConfig([]string{
		"-sp-i2-baseline-artifact", "s4-distance-training.jsonl",
		"-sp-i2-candidate-artifact", "i2-training.jsonl",
		"-sp-i2-resource-report", "i2-training-resource.json",
		"-sp-i2-output", "sp-i2-discovery.json",
		"-sp-i2-freeze-output", "sp-i2-freeze.json",
		"-sp-i2-protocol", referencePairProtocolDiscovery,
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "s4-distance-training.jsonl", discovery.SPI2BaselineArtifact)
	require.Equal(t, "i2-training.jsonl", discovery.SPI2CandidateArtifact)
	require.Equal(t, "i2-training-resource.json", discovery.SPI2ResourceReport)
	require.Equal(t, "sp-i2-freeze.json", discovery.SPI2FreezeOutput)

	confirmation, err := parseConfig([]string{
		"-sp-i2-baseline-artifact", "s4-distance-confirmation.jsonl",
		"-sp-i2-candidate-artifact", "i2-confirmation.jsonl",
		"-sp-i2-resource-report", "i2-confirmation-resource.json",
		"-sp-i2-output", "sp-i2-confirmation.json",
		"-sp-i2-freeze", "sp-i2-freeze.json",
		"-sp-i2-discovery-report", "sp-i2-discovery.json",
		"-sp-i2-training-baseline-artifact", "s4-distance-training.jsonl",
		"-sp-i2-training-candidate-artifact", "i2-training.jsonl",
		"-sp-i2-training-resource-report", "i2-training-resource.json",
		"-sp-i2-protocol", referencePairProtocolConfirmation,
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "sp-i2-freeze.json", confirmation.SPI2Freeze)
	require.Equal(t, "sp-i2-discovery.json", confirmation.SPI2DiscoveryReport)
}

func TestParseConfigRejectsIncompleteOrMixedSPI2StagedWorkflow(t *testing.T) {
	discovery := []string{
		"-sp-i2-baseline-artifact", "s4.jsonl",
		"-sp-i2-candidate-artifact", "i2.jsonl",
		"-sp-i2-resource-report", "resource.json",
		"-sp-i2-output", "report.json",
		"-sp-i2-freeze-output", "freeze.json",
		"-sp-i2-protocol", referencePairProtocolDiscovery,
	}
	for _, args := range [][]string{
		{"-sp-i2-baseline-artifact", "s4.jsonl"},
		{"-sp-i2-freeze", "freeze.json"},
		append(append([]string(nil), discovery...), "-sp-i2-freeze", "old-freeze.json", "-sp-i2-discovery-report", "old-report.json"),
		append(append([]string(nil), discovery...), "-sp-i2-protocol", "exploratory"),
		append(append([]string(nil), discovery...), "-sp-i2-output", "s4.jsonl"),
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsSPI1HoldoutCaptureAuthorization verifies parse config accepts spi1 holdout capture authorization behavior.
func TestParseConfigAcceptsSPI1HoldoutCaptureAuthorization(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-sp-i1-freeze", "sp-i1-freeze.json",
		"-sp-i1-discovery-report", "sp-i1-discovery.json",
		"-sp-i1-training-baseline-artifact", "s4-training.jsonl",
		"-sp-i1-training-candidate-artifact", "i1-training.jsonl",
		"-sp-i1-training-resource-report", "i1-training-resource.json",
		"-tags", "sp-i1-inbound-v1-training,sp-i1-inbound-v1-holdout",
		"-iterations", "50",
		"-warmup-iterations", "20",
		"-round", "1",
		"-block", "1",
		"-arm", "sp-i1-s4",
		"-arm-order", "1",
		"-run-uuid", "sp-i1-confirmation-run",
		"-postgres-force-shortest-executor", "SP-S4-C-WE+MAT-M0",
		"-postgres-repeatable-read",
		"-postgres-traversal-telemetry", postgresTraversalTelemetryDiagnostic,
		"-jsonl-output", "sp-i1-s4-confirmation.jsonl",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "sp-i1-freeze.json", cfg.SPI1Freeze)
	require.Empty(t, cfg.SPI1BaselineArtifact)
}

// TestParseConfigRejectsIncompleteOrMixedSPI1StagedWorkflow verifies parse config rejects incomplete or mixed spi1 staged workflow behavior.
func TestParseConfigRejectsIncompleteOrMixedSPI1StagedWorkflow(t *testing.T) {
	discovery := []string{
		"-sp-i1-baseline-artifact", "s4.jsonl",
		"-sp-i1-candidate-artifact", "i1.jsonl",
		"-sp-i1-resource-report", "resource.json",
		"-sp-i1-output", "report.json",
		"-sp-i1-freeze-output", "freeze.json",
		"-sp-i1-protocol", referencePairProtocolDiscovery,
	}
	for _, args := range [][]string{
		{"-sp-i1-baseline-artifact", "s4.jsonl"},
		{"-sp-i1-freeze", "freeze.json"},
		append(append([]string(nil), discovery...), "-sp-i1-freeze", "old-freeze.json", "-sp-i1-discovery-report", "old-report.json"),
		append(append([]string(nil), discovery...), "-sp-i1-protocol", "exploratory"),
		append(append([]string(nil), discovery...), "-resource-artifact", "other.jsonl"),
		append(append([]string(nil), discovery...), "-sp-i1-output", "s4.jsonl"),
		append(append([]string(nil), discovery...), "-seed", "2"),
		append(append([]string(nil), discovery...), "-confidence-level", "0.95"),
		{"-sp-i1-freeze", "freeze.json", "-sp-i1-discovery-report", "discovery.json", "-resource-artifact", "candidate.jsonl"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsProductionManifestAndRejectsToolMixing verifies parse config accepts production manifest and rejects tool mixing behavior.
func TestParseConfigAcceptsProductionManifestAndRejectsToolMixing(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-production-manifest", "provisional.json"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "provisional.json", cfg.PostgresProductionManifest)

	_, err = parseConfig([]string{
		"-postgres-production-manifest", "provisional.json",
		"-postgres-force-shortest-executor", "ASP-I1-U-DAG+MAT-M0",
	}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")

	cfg, err = parseConfig([]string{"-postgres-repeatable-read"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresRepeatableRead)
	_, err = parseConfig([]string{"-postgres-production-manifest", "provisional.json", "-postgres-repeatable-read"}, func(string) string { return "" })
	require.ErrorContains(t, err, "already implies Repeatable Read")
}

// TestParseConfigRejectsIncompleteOrientationSelectorReport verifies the
// report cannot silently omit an exact comparator, A/A floor, or standalone
// workflow boundary.
func TestParseConfigRejectsIncompleteOrientationSelectorReport(t *testing.T) {
	complete := []string{
		"-orientation-shadow-artifact", "shadow.jsonl",
		"-orientation-incumbent-artifact", "incumbent.jsonl",
		"-orientation-reverse-artifact", "reverse.jsonl",
		"-orientation-aa", "aa.json",
	}
	for _, args := range [][]string{
		{"-orientation-shadow-artifact", "shadow.jsonl"},
		append(append([]string(nil), complete...), "-orientation-protocol", "exploratory"),
		append(append([]string(nil), complete...), "-expand-into-artifact", "expand.jsonl"),
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigRejectsInvalidExpandIntoStudyMode verifies report output, protocol, and standalone-mode exclusivity fail closed.
func TestParseConfigRejectsInvalidExpandIntoStudyMode(t *testing.T) {
	for _, args := range [][]string{
		{"-expand-into-output", "expand-into.json"},
		{"-expand-into-artifact", "expand-into.jsonl", "-expand-into-protocol", "exploratory"},
		{"-expand-into-artifact", "expand-into.jsonl", "-bundle-verify", "capture"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsPoolAndConcurrencySmokeLevels verifies numeric pool parsing and stable deduplication of requested concurrency levels.
func TestParseConfigAcceptsPoolAndConcurrencySmokeLevels(t *testing.T) {
	cfg, err := parseConfig([]string{"-pool-size", "4", "-concurrency", "1,4,8,4"}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, 4, cfg.PoolSize)
	require.Equal(t, []int{1, 4, 8}, cfg.Concurrency)
}

// TestParseConfigAcceptsReferencePairDiscoveryProtocol verifies that the discovery protocol flag selects the corresponding reference-pair workflow.
func TestParseConfigAcceptsReferencePairDiscoveryProtocol(t *testing.T) {
	cfg, err := parseConfig([]string{"-reference-pair-protocol", "discovery"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, referencePairProtocolDiscovery, cfg.ReferencePairProtocol)
}

// TestParseConfigAcceptsReferenceTournament verifies a predeclared arm order
// is preserved because the first arm defines the incumbent.
func TestParseConfigAcceptsReferenceTournament(t *testing.T) {
	arms := "expand_into_pair_join,expand_into_lower_degree_scan,expand_into_pair_cache"
	cfg, err := parseConfig([]string{
		"-reference-tournament-artifact", "tournament.jsonl",
		"-reference-tournament-output", "tournament.json",
		"-reference-tournament-arms", arms,
		"-reference-tournament-protocol", referencePairProtocolConfirmation,
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, []string{"expand_into_pair_join", "expand_into_lower_degree_scan", "expand_into_pair_cache"}, cfg.ReferenceTournamentArms)
	require.Equal(t, referencePairProtocolConfirmation, cfg.ReferenceTournamentProtocol)
}

// TestParseConfigRejectsInvalidReferenceTournament verifies parse config rejects invalid reference tournament behavior.
func TestParseConfigRejectsInvalidReferenceTournament(t *testing.T) {
	for _, args := range [][]string{
		{"-reference-tournament-output", "tournament.json"},
		{"-reference-tournament-artifact", "tournament.jsonl"},
		{"-reference-tournament-artifact", "tournament.jsonl", "-reference-tournament-arms", "expand_into_pair_join,expand_into_pair_cache"},
		{"-reference-tournament-artifact", "tournament.jsonl", "-reference-tournament-arms", "expand_into_pair_join,unknown,expand_into_pair_cache"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigRejectsPoolMemoryBelowPerSessionBudget verifies that the pool ceiling must cover the per-session budget for every configured connection.
func TestParseConfigRejectsPoolMemoryBelowPerSessionBudget(t *testing.T) {
	_, err := parseConfig([]string{
		"-pool-size", "4",
		"-session-memory-ceiling-bytes", "100",
		"-pool-memory-ceiling-bytes", "399",
	}, func(string) string { return "" })

	require.ErrorContains(t, err, "session memory ceiling times pool size")
}

// TestParseConfigAcceptsDiagnosticSelectorsAndRunMetadata verifies parsing of case filters, warmups, arm identity, and block metadata used to reproduce diagnostic runs.
func TestParseConfigAcceptsDiagnosticSelectorsAndRunMetadata(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-cases", "case-a,case-b", "-datasets", "fixture", "-categories", "lookup", "-tags", "primary,control",
		"-warmup-iterations", "20", "-arm", "candidate", "-arm-order", "2", "-block", "7", "-run-uuid", "run-1",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, []string{"case-a", "case-b"}, cfg.Cases)
	require.Equal(t, 20, cfg.WarmupIterations)
	require.Equal(t, "candidate", cfg.Arm)
	require.Equal(t, 7, cfg.Block)
}

// TestParseConfigRejectsDuplicateExactSelectors verifies that repeated exact case names are rejected before corpus selection.
func TestParseConfigRejectsDuplicateExactSelectors(t *testing.T) {
	_, err := parseConfig([]string{"-cases", "case-a,case-a"}, func(string) string { return "" })
	require.ErrorContains(t, err, "duplicate case selector")
}

// TestParseConfigAcceptsOnlyQualifiedForcedShortestExecutor verifies the supported shortest-executor allowlist and rejects an incomplete strategy name.
func TestParseConfigAcceptsOnlyQualifiedForcedShortestExecutor(t *testing.T) {
	for _, executor := range []string{
		"SP-S0",
		"SP-S0-DIRECT",
		"SP-S3-U-D",
		"SP-S3-U-E+MAT-M0",
		"SP-S4-C-D",
		"SP-S4-C-WE+MAT-M0",
		"SP-I1-C-D",
		"SP-I1-U-E+MAT-M0",
		"SP-I1-C-WE+MAT-M0",
		"SP-B1-C-ALT-NODE-D",
		"SP-B1-C-ALT-NODE-WE+MAT-M0",
		"SP-B2-C-MIN-LEVEL-D",
		"SP-B2-C-MIN-LEVEL-WE+MAT-M0",
		"ASP-A1-DAG",
		"ASP-I1-U-DAG+MAT-M0",
		"ASP-B1-DAG-ALT-NODE",
		"ASP-B2-DAG-MIN-LEVEL",
	} {
		t.Run(executor, func(t *testing.T) {
			cfg, err := parseConfig([]string{"-postgres-force-shortest-executor", executor}, func(string) string { return "" })
			require.NoError(t, err)
			require.Equal(t, executor, cfg.PostgresForceShortest)
		})
	}

	_, err := parseConfig([]string{"-postgres-force-shortest-executor", "SP-S1"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unsupported PostgreSQL forced shortest executor")
}

// TestParseConfigExistingGraphWorkflow verifies that a fully specified live-graph discovery run retains checkpoint, resume, progress, timeout, and sampling settings.
func TestParseConfigExistingGraphWorkflow(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-existing-graph", "-anchor-manifest", "anchors.json", "-checkpoint", "checkpoint.json",
		"-resume", "-progress", "progress.jsonl", "-discovery", "-timeout-classes", "100ms,1s",
		"-discovery-sample-floor", "2",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.ExistingGraph)
	require.True(t, cfg.Resume)
	require.True(t, cfg.Discovery)
	require.Equal(t, []time.Duration{100 * time.Millisecond, time.Second}, cfg.TimeoutClasses)
	require.Equal(t, 2, cfg.DiscoverySampleFloor)
}

// TestParseConfigRejectsUnsafeExistingGraphCombinations verifies that live-graph mode requires an anchor manifest and disallows mismatched backends or orphaned resume/discovery flags.
func TestParseConfigRejectsUnsafeExistingGraphCombinations(t *testing.T) {
	for _, args := range [][]string{
		{"-existing-graph"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-modes", "postgres_sql,neo4j"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-resume"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-timeout-classes", "1s"},
		{"-anchor-manifest", "anchors.json"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsOnlyQualifiedForcedExpansionSearch verifies the expansion-strategy allowlist and prevents simultaneous forced expansion and shortest-path strategies.
func TestParseConfigAcceptsOnlyQualifiedForcedExpansionSearch(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "EXPANSION-SUFFIX-SEEDED-REVERSE", cfg.PostgresForceExpansion)
	cfg, err = parseConfig([]string{"-postgres-force-expansion-search", "EXPANSION-ENDPOINT-SEEDED-REVERSE"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "EXPANSION-ENDPOINT-SEEDED-REVERSE", cfg.PostgresForceExpansion)

	_, err = parseConfig([]string{"-postgres-force-expansion-search", "unknown-strategy"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unsupported PostgreSQL forced expansion search")

	_, err = parseConfig([]string{
		"-postgres-force-shortest-executor", "SP-S3-U-D",
		"-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE",
	}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestParseConfigRequiresOutputForJSONLAppend verifies that append mode names a destination and is retained once that destination is present.
func TestParseConfigRequiresOutputForJSONLAppend(t *testing.T) {
	_, err := parseConfig([]string{"-append-jsonl"}, func(string) string { return "" })
	require.ErrorContains(t, err, "append-jsonl requires jsonl-output")

	cfg, err := parseConfig([]string{"-append-jsonl", "-jsonl-output", "rounds.jsonl"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.AppendJSONL)
}

// TestParseConfigAcceptsMultipleAAArtifacts verifies independently captured
// A/A arms can be passed to the reporter without an unvalidated external merge.
func TestParseConfigAcceptsMultipleAAArtifacts(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-aa-artifact", "aa-a.jsonl",
		"-aa-artifact", "aa-b.jsonl",
		"-aa-output", "aa.json",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, []string{"aa-a.jsonl", "aa-b.jsonl"}, cfg.AAArtifacts)
}

// TestParseConfigAcceptsReferenceClosureMode verifies reference-closure artifact parsing, confidence propagation, required output pairing, and exclusion of incompatible A/A mode.
func TestParseConfigAcceptsReferenceClosureMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-reference-closure-artifact", "reference.jsonl",
		"-reference-closure-output", "report.json",
		"-reference-closure-arm", "s3_unidirectional_trail_cte",
		"-confidence-level", "0.975",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "reference.jsonl", cfg.ReferenceClosureArtifact)
	require.Equal(t, 0.975, cfg.Confidence)

	_, err = parseConfig([]string{"-reference-closure-output", "report.json"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires reference-closure-artifact")
	_, err = parseConfig([]string{"-reference-closure-artifact", "reference.jsonl", "-aa-artifact", "aa.jsonl"}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestParseConfigAcceptsReferencePairMode verifies that pair-report configuration retains its artifact and explicit baseline/candidate arm names.
func TestParseConfigAcceptsReferencePairMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-reference-pair-artifact", "pair.jsonl",
		"-reference-pair-baseline", "s3",
		"-reference-pair-candidate", "s1",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "pair.jsonl", cfg.ReferencePairArtifact)
	require.Equal(t, "s3", cfg.ReferencePairBaseline)
	require.Equal(t, "s1", cfg.ReferencePairCandidate)
}
