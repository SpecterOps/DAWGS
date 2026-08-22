// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestSPI2QualificationDiscoveryPassesTrainingWithoutOpeningHoldout verifies spi2 qualification discovery passes training without opening holdout behavior.
func TestSPI2QualificationDiscoveryPassesTrainingWithoutOpeningHoldout(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	report, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	require.True(t, report.EvidencePassed)
	require.True(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
	require.Equal(t, 6, report.TrainingCases)
	require.Zero(t, report.HoldoutCases)
	require.Len(t, report.Cases, 6)
	require.Equal(t, spI2QualificationCaps(), report.Caps)
	for _, gateCase := range report.Cases {
		require.True(t, gateCase.Passed, gateCase.Reasons)
		require.Equal(t, "training", gateCase.QualificationSplit)
		require.LessOrEqual(t, gateCase.MedianRatio.Upper, 0.95)
		require.LessOrEqual(t, gateCase.P95Ratio.Upper, 1.05)
	}
}

// TestTimedRuntimeAttestationIdentityIncludesExactS4Baseline verifies timed runtime attestation identity includes exact s4 baseline behavior.
func TestTimedRuntimeAttestationIdentityIncludesExactS4DistanceBaseline(t *testing.T) {
	baseline := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	translation := translate.Result{Optimization: translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
		Family:   "SP",
		Selected: baseline,
	}}}}
	require.Equal(t, baseline, timedRuntimeAttestationIdentity(translation))
}

// TestSPI2QualificationConfirmationRequiresAndPassesFrozenDiscovery verifies spi2 qualification confirmation requires and passes frozen discovery behavior.
func TestSPI2QualificationConfirmationRequiresAndPassesFrozenDiscovery(t *testing.T) {
	trainingBaseline, trainingCandidate, trainingResource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	discovery, err := buildSPI2QualificationReport(trainingBaseline, trainingCandidate, trainingResource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	discovery.BaselineArtifactSHA256 = strings.Repeat("1", 64)
	discovery.CandidateArtifactSHA256 = strings.Repeat("2", 64)
	discovery.ResourceReportSHA256 = strings.Repeat("3", 64)
	freeze := spI2QualificationTestFreeze(t, discovery)

	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolConfirmation)
	report, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolConfirmation,
		SourceArchiveSHA256: strings.Repeat("a", 64),
		Freeze:              &freeze,
		Discovery:           &discovery,
	})
	require.NoError(t, err)
	require.True(t, report.TrainingPassed)
	require.True(t, report.HoldoutPassed)
	require.True(t, report.QualificationPassed)
	require.Equal(t, 6, report.TrainingCases)
	require.Equal(t, 4, report.HoldoutCases)
	require.Len(t, report.Cases, 10)
}

// TestSPI2QualificationRejectsUnattestedCandidateAndFreezeMutation verifies spi2 qualification rejects unattested candidate and freeze mutation behavior.
func TestSPI2QualificationRejectsUnattestedCandidateAndFreezeMutation(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	candidate[0].Stats.Samples[1].RuntimeAttestation = "same_case_invocation_local_replay"
	candidate[0].Stats.Samples[1].RuntimeReceiptEvents = nil
	_, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.ErrorContains(t, err, "timed-invocation attribution")

	trainingBaseline, trainingCandidate, trainingResource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	discovery, err := buildSPI2QualificationReport(trainingBaseline, trainingCandidate, trainingResource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	discovery.BaselineArtifactSHA256 = strings.Repeat("1", 64)
	discovery.CandidateArtifactSHA256 = strings.Repeat("2", 64)
	discovery.ResourceReportSHA256 = strings.Repeat("3", 64)
	freeze := spI2QualificationTestFreeze(t, discovery)
	freeze.QuerySHA256 = strings.Repeat("f", 64)
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	require.Error(t, validateSPI2FrozenDiscovery(&freeze, &discovery, cohort))
}

// TestSPI2QualificationClassifiesBoundResourceFailure verifies spi2 qualification classifies bound resource failure behavior.
func TestSPI2QualificationClassifiesBoundResourceFailure(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	candidate[0].PostgresMetrics.Buffers.TempWritten = 1
	resource.Cases[0] = evaluateProductionResourceGateCase(candidate[0])
	resource.Passed = false
	report, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	require.False(t, report.TrainingPassed)
	require.False(t, report.QualificationPassed)
	found := false
	for _, gateCase := range report.Cases {
		found = found || strings.Contains(strings.Join(gateCase.Reasons, "\n"), "candidate resource evidence did not pass")
	}
	require.True(t, found)
}

func TestSPI2QualificationAppliesPreregisteredAdverseControlGate(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	for index := range candidate {
		if !strings.Contains(candidate[index].Name, "cycle-control") {
			continue
		}
		candidate[index].Stats.Median = 10500 * time.Microsecond
		candidate[index].Stats.P95 = 10500 * time.Microsecond
		for sampleIndex := range candidate[index].Stats.Samples {
			if candidate[index].Stats.Samples[sampleIndex].Classification == "warm" {
				candidate[index].Stats.Samples[sampleIndex].Duration = 10500 * time.Microsecond
			}
		}
	}
	report, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: defaultBootstrapCount,
		Protocol: referencePairProtocolDiscovery, SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	require.True(t, report.TrainingPassed)
	for _, gateCase := range report.Cases {
		if strings.Contains(gateCase.Name, "cycle-control") {
			require.Equal(t, "adverse_control", gateCase.QualificationRole)
			require.True(t, gateCase.Material)
			require.Greater(t, gateCase.MedianRatio.Upper, report.MaterialityRatio)
			require.LessOrEqual(t, gateCase.MedianRatio.Upper, report.AdverseRatioLimit)
		} else {
			require.Equal(t, "target", gateCase.QualificationRole)
		}
	}
}

// TestSPI2QualificationRejectsCanonicalEvidenceAndScheduleTampering verifies spi2 qualification rejects canonical evidence and schedule tampering behavior.
func TestSPI2QualificationRejectsCanonicalEvidenceAndScheduleTampering(t *testing.T) {
	tests := map[string]func([]CaseResult, []CaseResult, *ResourceGateReport){
		"canonical observation": func(baseline, _ []CaseResult, _ *ResourceGateReport) {
			baseline[0].ObservedRows = []string{`[{"nodes":[],"relationships":[]}]`}
		},
		"duplicate warm iteration": func(_ []CaseResult, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].Stats.Samples[2].Iteration = 1
		},
		"duplicate timed invocation": func(_ []CaseResult, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].Stats.Samples[2].RuntimeInvocationID = candidate[0].Stats.Samples[1].RuntimeInvocationID
			candidate[0].Stats.Samples[2].RuntimeReceiptEvents[0].InvocationID = candidate[0].Stats.Samples[1].RuntimeInvocationID
		},
		"cross-record timed invocation replay": func(baseline, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[1].Stats.Samples[1].RuntimeInvocationID = baseline[0].Stats.Samples[1].RuntimeInvocationID
			candidate[1].Stats.Samples[1].RuntimeReceiptEvents[0].InvocationID = baseline[0].Stats.Samples[1].RuntimeInvocationID
		},
		"contradictory arm chronology": func(baseline, candidate []CaseResult, _ *ResourceGateReport) {
			started := baseline[0].Environment.StartedAt.Add(-2 * time.Second)
			for index := range candidate {
				if candidate[index].Environment.Round == 1 {
					candidate[index].Environment.StartedAt = started
					candidate[index].Environment.EndedAt = started.Add(time.Second)
				}
			}
		},
		"block differs from round": func(baseline, _ []CaseResult, _ *ResourceGateReport) {
			for index := range baseline {
				if baseline[index].Environment.Round == 2 {
					baseline[index].Environment.Block = 1
				}
			}
		},
		"unbound resource round": func(_, _ []CaseResult, resource *ResourceGateReport) {
			resource.Cases[0].Round = 99
		},
		"substituted resource receipt": func(_, _ []CaseResult, resource *ResourceGateReport) {
			resource.Cases[0].RuntimeReceiptChains[0][0].RuntimeBranch = "substituted"
		},
		"cleared resource spill": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].PostgresMetrics.Buffers.TempWritten = 1
		},
		"reachable relabeled no path": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].TraversalTelemetry.Summary.RuntimeBranch = "inline_canonical_distance_no_path"
			for index := range candidate[0].Stats.Samples {
				if candidate[0].Stats.Samples[index].Classification == "warm" {
					candidate[0].Stats.Samples[index].RuntimeBranch = "inline_canonical_distance_no_path"
					candidate[0].Stats.Samples[index].RuntimeReceiptEvents[0].RuntimeBranch = "inline_canonical_distance_no_path"
				}
			}
		},
		"no path relabeled witness": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			for recordIndex := range candidate {
				if !strings.HasSuffix(candidate[recordIndex].Name, "-disconnected") {
					continue
				}
				candidate[recordIndex].TraversalTelemetry.Summary.RuntimeBranch = "inline_canonical_distance"
				for sampleIndex := range candidate[recordIndex].Stats.Samples {
					if candidate[recordIndex].Stats.Samples[sampleIndex].Classification == "warm" {
						candidate[recordIndex].Stats.Samples[sampleIndex].RuntimeBranch = "inline_canonical_distance"
						candidate[recordIndex].Stats.Samples[sampleIndex].RuntimeReceiptEvents[0].RuntimeBranch = "inline_canonical_distance"
					}
				}
				return
			}
		},
		"output counter differs from observation": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			*candidate[0].TraversalTelemetry.Diagnostic.Counters.InlineShortestDistance.OutputRows = 0
		},
		"supplemental planned arm": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].TraversalTelemetry.Summary.PlannedIdentities = append(candidate[0].TraversalTelemetry.Summary.PlannedIdentities, "SP-B1-extra")
		},
		"reduced planned search space": func(baseline, _ []CaseResult, _ *ResourceGateReport) {
			baseline[0].TraversalTelemetry.Summary.PlannedIdentities = []string{
				string(optimize.ShortestPathExecutorS4CanonicalDistance),
				string(optimize.ShortestPathExecutorIncumbentWorkspace),
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
			mutate(baseline, candidate, &resource)
			_, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
				Seed:                1,
				Confidence:          defaultConfidenceLevel,
				BootstrapCount:      defaultBootstrapCount,
				Protocol:            referencePairProtocolDiscovery,
				SourceArchiveSHA256: strings.Repeat("a", 64),
			})
			require.Error(t, err)
		})
	}
}

// TestSPI2QualificationFreezesStatisticalPolicyAndDiscoverySemantics verifies spi2 qualification freezes statistical policy and discovery semantics behavior.
func TestSPI2QualificationFreezesStatisticalPolicyAndDiscoverySemantics(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	for _, options := range []SPI2QualificationOptions{
		{
			Seed:           2,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: defaultBootstrapCount,
		},
		{
			Seed:           1,
			Confidence:     0.95,
			BootstrapCount: defaultBootstrapCount,
		},
		{
			Seed:           1,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 1,
		},
	} {
		options.Protocol = referencePairProtocolDiscovery
		options.SourceArchiveSHA256 = strings.Repeat("a", 64)
		_, err := buildSPI2QualificationReport(baseline, candidate, resource, options)
		require.Error(t, err)
	}

	discovery, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	discovery.BaselineArtifactSHA256 = strings.Repeat("1", 64)
	discovery.CandidateArtifactSHA256 = strings.Repeat("2", 64)
	discovery.ResourceReportSHA256 = strings.Repeat("3", 64)
	freeze := spI2QualificationTestFreeze(t, discovery)
	discovery.Cases[0].P95Ratio.Upper = 2
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	require.Error(t, validateSPI2FrozenDiscovery(&freeze, &discovery, cohort))
}

// TestSPI2FrozenTrainingEvidenceIsRecomputedFromNamedArtifacts verifies spi2 frozen training evidence is recomputed from named artifacts behavior.
func TestSPI2FrozenTrainingEvidenceIsRecomputedFromNamedArtifacts(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	directory := t.TempDir()
	baselinePath := filepath.Join(directory, "s4.jsonl")
	candidatePath := filepath.Join(directory, "i1.jsonl")
	resourcePath := filepath.Join(directory, "resource.json")
	require.NoError(t, writeJSONLFile(baselinePath, baseline))
	require.NoError(t, writeJSONLFile(candidatePath, candidate))
	candidateSHA256, err := fileSHA256(candidatePath)
	require.NoError(t, err)
	resource.ArtifactSHA256 = candidateSHA256
	resourceRaw, err := json.MarshalIndent(resource, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(resourcePath, append(resourceRaw, '\n'), 0o600))

	discovery, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	discovery.BaselineArtifactSHA256, err = fileSHA256(baselinePath)
	require.NoError(t, err)
	discovery.CandidateArtifactSHA256 = candidateSHA256
	discovery.ResourceReportSHA256, err = fileSHA256(resourcePath)
	require.NoError(t, err)
	freeze := spI2QualificationTestFreeze(t, discovery)
	require.NoError(t, validateSPI2FrozenTrainingEvidence(&freeze, &discovery, baselinePath, candidatePath, resourcePath))

	forged := discovery
	forged.Cases = append([]SPI2QualificationCase(nil), discovery.Cases...)
	forged.Cases[0].MedianRatio = RatioInterval{
		Lower:    0.801,
		Estimate: 0.801,
		Upper:    0.801,
	}
	require.ErrorContains(t,
		validateSPI2FrozenTrainingEvidence(&freeze, &forged, baselinePath, candidatePath, resourcePath),
		"differs from its recomputed",
	)
}

// TestSPI2FreezeManifestRequiresPassingDiscovery verifies that failed discovery
// evidence cannot leave behind a holdout-authorization artifact.
func TestSPI2FreezeManifestRequiresPassingDiscovery(t *testing.T) {
	baseline, candidate, resource := spI2QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	discovery, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	discovery.BaselineArtifactSHA256 = strings.Repeat("1", 64)
	discovery.CandidateArtifactSHA256 = strings.Repeat("2", 64)
	discovery.ResourceReportSHA256 = strings.Repeat("3", 64)

	for name, mutate := range map[string]func(*SPI2QualificationReport){
		"failed evidence": func(report *SPI2QualificationReport) {
			report.EvidencePassed = false
		},
		"failed training": func(report *SPI2QualificationReport) {
			report.TrainingPassed = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			report := discovery
			mutate(&report)
			directory := t.TempDir()
			reportPath := filepath.Join(directory, "discovery.json")
			freezePath := filepath.Join(directory, "freeze.json")
			require.NoError(t, writeSPI2QualificationReport(reportPath, report))
			require.ErrorContains(t, writeSPI2FreezeManifest(freezePath, reportPath, report), "exact passing clean training-only report")
			_, statErr := os.Stat(freezePath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}

	t.Run("passing discovery", func(t *testing.T) {
		directory := t.TempDir()
		reportPath := filepath.Join(directory, "discovery.json")
		freezePath := filepath.Join(directory, "freeze.json")
		require.NoError(t, writeSPI2QualificationReport(reportPath, discovery))
		require.NoError(t, writeSPI2FreezeManifest(freezePath, reportPath, discovery))
		freeze, _, err := loadSPI2FreezeManifest(freezePath)
		require.NoError(t, err)
		require.True(t, freeze.TrainingPassed)
		require.Equal(t, discovery.BaselineArtifactSHA256, freeze.BaselineArtifactSHA256)
		require.Equal(t, discovery.CandidateArtifactSHA256, freeze.CandidateArtifactSHA256)
		require.Equal(t, discovery.ResourceReportSHA256, freeze.ResourceReportSHA256)
	})
}

// TestSPI2PathsRejectHardlinkAliases verifies spi2 paths reject hardlink aliases behavior.
func TestSPI2PathsRejectHardlinkAliases(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.json")
	alias := filepath.Join(directory, "alias.json")
	require.NoError(t, os.WriteFile(input, []byte("{}"), 0o600))
	require.NoError(t, os.Link(input, alias))
	require.Error(t, validateDistinctSPI2Paths(map[string]string{"input": input, "output": alias}))
}

// TestSPI2HoldoutCaptureProfileAcceptsBothBalancedArmsAndRejectsDrift verifies spi2 holdout capture profile accepts both balanced arms and rejects drift behavior.
func TestSPI2HoldoutCaptureProfileAcceptsBothBalancedArmsAndRejectsDrift(t *testing.T) {
	baseline := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	candidate := string(optimize.ShortestPathExecutorI2GuardedDistance)
	valid := func(executor, arm string, round, order int) config {
		return config{
			Modes:                      []ExecutionMode{ModePostgresSQL},
			Iterations:                 50,
			WarmupIterations:           20,
			Round:                      round,
			Block:                      round,
			Arm:                        arm,
			ArmOrder:                   order,
			RunUUID:                    "sp-i2-confirmation",
			PoolSize:                   1,
			PostgresForceShortest:      executor,
			PostgresRepeatableRead:     true,
			PostgresTraversalTelemetry: postgresTraversalTelemetryDiagnostic,
			OutputJSONL:                fmt.Sprintf(".coverage/sp-i2-%s-%d.jsonl", arm, round),
			AppendJSONL:                round > 1,
			SPI2Freeze:                 ".coverage/sp-i2-freeze.json",
			SPI2DiscoveryReport:        ".coverage/sp-i2-discovery.json",
			SPI2TrainingBaseline:       ".coverage/sp-i2-training-s4.jsonl",
			SPI2TrainingCandidate:      ".coverage/sp-i2-training-i2.jsonl",
			SPI2TrainingResource:       ".coverage/sp-i2-training-resource.json",
		}
	}
	for _, cfg := range []config{
		valid(baseline, "sp-i2-s4", 1, 1),
		valid(candidate, "sp-i2-candidate", 1, 2),
		valid(baseline, "sp-i2-s4", 2, 2),
		valid(candidate, "sp-i2-candidate", 2, 1),
	} {
		require.NoError(t, validateSPI2HoldoutCaptureConfig(cfg))
	}

	tests := map[string]func(*config){
		"wrong backend":           func(cfg *config) { cfg.Modes = []ExecutionMode{ModeNeo4j} },
		"existing graph":          func(cfg *config) { cfg.ExistingGraph = true },
		"too few samples":         func(cfg *config) { cfg.Iterations = 49 },
		"too few warmups":         func(cfg *config) { cfg.WarmupIterations = 19 },
		"pool larger than one":    func(cfg *config) { cfg.PoolSize = 2 },
		"concurrency":             func(cfg *config) { cfg.Concurrency = []int{2} },
		"round above maximum":     func(cfg *config) { cfg.Round, cfg.Block = 21, 21 },
		"mismatched block":        func(cfg *config) { cfg.Block = 2 },
		"missing run UUID":        func(cfg *config) { cfg.RunUUID = "" },
		"wrong arm label":         func(cfg *config) { cfg.Arm = "baseline" },
		"wrong arm order":         func(cfg *config) { cfg.ArmOrder = 2 },
		"wrong executor":          func(cfg *config) { cfg.PostgresForceShortest = "SP-S3-U-E+MAT-M0" },
		"read committed":          func(cfg *config) { cfg.PostgresRepeatableRead = false },
		"summary telemetry":       func(cfg *config) { cfg.PostgresTraversalTelemetry = postgresTraversalTelemetrySummary },
		"supplemental references": func(cfg *config) { cfg.PostgresReferences = true },
		"missing output":          func(cfg *config) { cfg.OutputJSONL = "" },
		"path alias":              func(cfg *config) { cfg.OutputJSONL = cfg.SPI2Freeze },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid(baseline, "sp-i2-s4", 1, 1)
			mutate(&cfg)
			require.Error(t, validateSPI2HoldoutCaptureConfig(cfg))
		})
	}
	t.Run("round after one requires append", func(t *testing.T) {
		cfg := valid(baseline, "sp-i2-s4", 2, 2)
		cfg.AppendJSONL = false
		require.Error(t, validateSPI2HoldoutCaptureConfig(cfg))
	})
}

// TestSPI2HoldoutDetectionAndCorpusBindingIgnoreMutableTagAlone verifies spi2 holdout detection and corpus binding ignore mutable tag alone behavior.
func TestSPI2HoldoutDetectionAndCorpusBindingIgnoreMutableTagAlone(t *testing.T) {
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	training, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{spI2TrainingTag}})
	require.NoError(t, err)
	require.False(t, selectedCorpusContainsSPI2Holdout(training))

	confirmation, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{spI2TrainingTag, spI2HoldoutTag}})
	require.NoError(t, err)
	require.True(t, selectedCorpusContainsSPI2Holdout(confirmation))
	for index := range confirmation.Cases {
		confirmation.Cases[index].Source = strings.TrimPrefix(confirmation.Cases[index].Source, "../../")
		confirmation.Cases[index].Tags = nil
	}
	require.True(t, selectedCorpusContainsSPI2Holdout(confirmation), "canonical key detection must not depend on tags")

	// Restore exact declarations before checking the complete frozen corpus.
	exact, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{spI2TrainingTag, spI2HoldoutTag}})
	require.NoError(t, err)
	for index := range exact.Cases {
		exact.Cases[index].Source = strings.TrimPrefix(exact.Cases[index].Source, "../../")
	}
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	require.NoError(t, validateSPI2Corpus(exact, cohort))

	omitted := ScaleCorpus{Cases: append([]ScaleCase(nil), exact.Cases[:len(exact.Cases)-1]...)}
	require.Error(t, validateSPI2Corpus(omitted, cohort))
	mutated := ScaleCorpus{Cases: append([]ScaleCase(nil), exact.Cases...)}
	mutated.Cases[0].Cypher += " "
	require.Error(t, validateSPI2Corpus(mutated, cohort))
}

// TestRunnableCorpusExcludesSPI2HoldoutUntilExactOptIn verifies runnable corpus excludes spi2 holdout until exact opt in behavior.
func TestRunnableCorpusExcludesSPI2HoldoutUntilExactOptIn(t *testing.T) {
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	ordinary, manifest, err := selectRunnableScaleCorpusWithSPI2Protection(full, CorpusSelectors{})
	require.NoError(t, err)
	require.False(t, selectedCorpusContainsSPI2Holdout(ordinary))
	require.False(t, manifest.DiagnosticOnly)
	require.Equal(t, manifest.FullDeclarationCount, manifest.SelectedDeclarationCount+manifest.OmittedDeclarationCount)
	require.Equal(t, 26, manifest.OmittedDeclarationCount)
	require.Equal(t, 26, manifest.ProtectedDeclarationCount)
	require.True(t, lowercaseSHA256(manifest.ProtectedDeclarationSHA256))
	require.True(t, selectedCorpusContainsTag(ordinary, spI2TrainingTag))

	for name, selectors := range map[string]CorpusSelectors{
		"generic holdout tag": {Tags: []string{"holdout"}},
		"broad category":      {Categories: []string{"generated_shortest_path_v2"}},
	} {
		t.Run(name, func(t *testing.T) {
			selected, _, err := selectRunnableScaleCorpusWithSPI2Protection(full, selectors)
			require.NoError(t, err)
			require.False(t, selectedCorpusContainsSPI2Holdout(selected))
		})
	}

	exactTag, _, err := selectRunnableScaleCorpusWithSPI2Protection(full, CorpusSelectors{Tags: []string{spI2HoldoutTag}})
	require.NoError(t, err)
	require.Len(t, exactTag.Cases, 4)
	require.True(t, selectedCorpusContainsSPI2Holdout(exactTag))

	exactCase, _, err := selectRunnableScaleCorpusWithSPI2Protection(full, CorpusSelectors{Cases: []string{spI2CanonicalCases[6].name}})
	require.NoError(t, err)
	require.Len(t, exactCase.Cases, 1)
	require.True(t, selectedCorpusContainsSPI2Holdout(exactCase))
}

// spI2QualificationTestArtifacts prepares or inspects test evidence for sp i2 qualification test artifacts.
func spI2QualificationTestArtifacts(t *testing.T, protocol string) ([]CaseResult, []CaseResult, ResourceGateReport) {
	t.Helper()
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	tags := []string{spI2TrainingTag}
	rounds, samples, warmups := 5, 10, 5
	corpusSHA256 := spI2TrainingCorpusSHA256
	if protocol == referencePairProtocolConfirmation {
		tags = append(tags, spI2HoldoutTag)
		rounds, samples, warmups = 10, 50, 20
		corpusSHA256 = spI2FullCorpusSHA256
	}
	selected, selection, err := selectRunnableScaleCorpusWithSPI2Protection(full, CorpusSelectors{Tags: tags})
	require.NoError(t, err)

	var baseline, candidate []CaseResult
	resource := ResourceGateReport{
		Version:        resourceGateVersion,
		ArtifactSHA256: strings.Repeat("9", 64),
		Passed:         true,
	}
	for _, testCase := range selected.Cases {
		fixture, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err)
		fixture.PhysicalValidated = true
		fixture.PhysicalNodeCount = int64(fixture.NodeCount)
		fixture.PhysicalEdgeCount = int64(fixture.EdgeCount)
		fixture.NodeRelationBytes = int64(fixture.NodeCount) * 1024
		fixture.EdgeRelationBytes = int64(fixture.EdgeCount) * 1024
		for round := 1; round <= rounds; round++ {
			left, right := spI2QualificationTestRecords(t, testCase, fixture, selection, corpusSHA256, round, samples, warmups)
			baseline = append(baseline, left)
			candidate = append(candidate, right)
			resource.Cases = append(resource.Cases, evaluateProductionResourceGateCase(right))
		}
	}
	return baseline, candidate, resource
}

// spI2QualificationTestRecords prepares or inspects test evidence for sp i2 qualification test records.
func spI2QualificationTestRecords(
	t *testing.T,
	testCase ScaleCase,
	fixture FixtureMetadata,
	selection SelectionManifest,
	corpusSHA256 string,
	round, samples, warmups int,
) (CaseResult, CaseResult) {
	t.Helper()
	baselineIdentity := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	candidateIdentity := string(optimize.ShortestPathExecutorI2GuardedDistance)
	baselineOrder, candidateOrder := 1, 2
	if round%2 == 0 {
		baselineOrder, candidateOrder = 2, 1
	}
	rowCount := int64(1)
	var observed []string
	if testCase.Expected.ScalarInt != nil {
		observed = []string{fmt.Sprintf("[%d]", *testCase.Expected.ScalarInt)}
	}
	if strings.HasSuffix(testCase.Name, "-disconnected") {
		rowCount, observed = 0, nil
	}
	falseValue, trueValue := false, true
	makeSamples := func(arm string, order int, duration time.Duration, requested, branch, attestation string) []LatencySample {
		result := make([]LatencySample, samples+1)
		result[0] = LatencySample{
			Round:          round,
			Block:          round,
			Arm:            arm,
			ArmOrder:       order,
			RunUUID:        "sp-i2-test-run",
			Iteration:      0,
			Case:           testCase.Name,
			Dataset:        testCase.Dataset,
			Backend:        ModePostgresSQL,
			ConnectionID:   "101",
			Classification: "cold",
			Duration:       2 * duration,
		}
		for index := range samples {
			invocationID := fmt.Sprintf("sp-i2-test-%s-%s-%d-%d", arm, testCase.Name, round, index+1)
			result[index+1] = LatencySample{
				Round:               round,
				Block:               round,
				Arm:                 arm,
				ArmOrder:            order,
				RunUUID:             "sp-i2-test-run",
				Iteration:           index + 1,
				Case:                testCase.Name,
				Dataset:             testCase.Dataset,
				Backend:             ModePostgresSQL,
				ConnectionID:        "101",
				Classification:      "warm",
				Duration:            duration,
				RequestedIdentity:   requested,
				RuntimeIdentity:     requested,
				RuntimeBranch:       branch,
				FallbackExecuted:    &falseValue,
				RuntimeAttestation:  attestation,
				RuntimeInvocationID: invocationID,
			}
			if attestation == "timed_invocation" {
				result[index+1].RuntimeReceiptEvents = []RuntimeReceiptEvent{{
					InvocationID:     invocationID,
					Ordinal:          1,
					RuntimeIdentity:  requested,
					RuntimeBranch:    branch,
					FallbackExecuted: false,
				}}
			}
		}
		return result
	}
	baseEnvironment := RunEnvironment{
		ArtifactSchemaVersion: 2,
		CorpusSHA256:          corpusSHA256,
		SourceCommit:          "deadbeef",
		DirtyDiffSHA256:       cleanWorkingTreeSHA256(),
		BinarySHA256:          strings.Repeat("b", 64),
		GOOS:                  "linux",
		GOARCH:                "amd64",
		CPUCount:              8,
		CPUModel:              "test-cpu",
		Kernel:                "test-kernel",
		CgroupCPU:             "max 100000",
		CgroupMemory:          "max",
		CPUGovernor:           "performance",
		RunUUID:               "sp-i2-test-run",
		Block:                 round,
		Round:                 round,
		WarmupIterations:      warmups,
		Selection:             &selection,
		PoolSize:              1,
		Protocol:              "fixed_confirmation",
	}
	postgresEnvironment := &PostgresEnvironment{
		Version:              "PostgreSQL test",
		Database:             "dawgs",
		PlanCacheMode:        "auto",
		TransactionIsolation: "repeatable read",
		WorkMem:              "64MB",
		TempFileLimit:        "1GB",
		GraphPartitionCount:  1,
		DatabaseOID:          42,
		Autovacuum:           "on",
		NodeRelationBytes:    fixture.NodeRelationBytes,
		EdgeRelationBytes:    fixture.EdgeRelationBytes,
		AnalyzeState:         "edge:analyzed,node:analyzed",
		SchemaFingerprint:    strings.Repeat("c", 64),
		IndexFingerprint:     strings.Repeat("d", 64),
	}
	base := newCaseResult(testCase, ModePostgresSQL, nil)
	base.RowCount = rowCount
	base.ObservedRows = append([]string(nil), observed...)
	base.Status = StatusOK
	base.WorkloadSHA256 = scaleCaseWorkloadIdentity(testCase, ModePostgresSQL)
	attachFixtureMetadata(&base, fixture)
	base.PostgresEnvironment = postgresEnvironment

	baseline := base
	baseline.Environment = cloneSPI2TestEnvironment(baseEnvironment, "sp-i2-s4", baselineOrder)
	firstStarted := time.Unix(1_700_000_000+int64(round)*10, 0)
	baselineStarted, candidateStarted := firstStarted, firstStarted.Add(2*time.Second)
	if candidateOrder == 1 {
		candidateStarted, baselineStarted = firstStarted, firstStarted.Add(2*time.Second)
	}
	baseline.Environment.StartedAt, baseline.Environment.EndedAt = baselineStarted, baselineStarted.Add(time.Second)
	baseline.SQL = "select 's4:' || " + fmt.Sprintf("%q", testCase.Name)
	baseline.SQLFingerprint = sqlFingerprint(baseline.SQL)
	baselineBranch := "compact_workspace_witness"
	if rowCount == 0 {
		baselineBranch = "compact_no_path"
	} else if strings.Contains(testCase.Name, "cycle-control") {
		baselineBranch = "one_hop_preflight"
	} else if strings.Contains(testCase.Name, "early-d02") {
		baselineBranch = "two_hop_preflight"
	}
	baseline.Stats = DurationStats{
		Iterations:       samples,
		WarmupIterations: warmups,
		Median:           10 * time.Millisecond,
		P95:              10 * time.Millisecond,
		Samples:          makeSamples("sp-i2-s4", baselineOrder, 10*time.Millisecond, baselineIdentity, baselineBranch, "timed_invocation"),
	}
	baselineOutcome := translate.TargetLoweringOutcome{
		Lowering:               optimize.LoweringShortestPathExecutor,
		TargetKind:             "traversal",
		Family:                 "SP",
		Selected:               baselineIdentity,
		Applied:                baselineIdentity,
		Fallback:               "SP-S0",
		PlannedCandidates:      spI2ShortestPathPlannedIdentities(),
		SelectorVersion:        "sp-tool-v1",
		ExecutionBoundary:      "stored_helper",
		ObservationMode:        "distance",
		Scheduler:              "single_ended_level",
		Direction:              "inbound",
		PhysicalExpansion:      "end_id",
		RelationshipKindCount:  1,
		TopologyClassification: "physical_inbound_deep",
		SelectionMode:          "forced_tool",
		Eligible:               &trueValue,
		StaticallyEligible:     &trueValue,
		MinimumDepth:           traversalTelemetryPointer(int64(1)),
		MaximumDepth:           traversalTelemetryPointer(int64(64)),
		StateLimit:             100_000,
		FrontierLimit:          100_000,
		PredecessorLimit:       100_000,
		EnumerationLimit:       100_000,
		OutputBytesLimit:       64 * 1024 * 1024,
	}
	baseline.Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{baselineOutcome}}
	baselineMetrics := PostgresPlanMetrics{Provenance: map[string]string{}}
	baseline.PostgresMetrics = &baselineMetrics
	baselineTelemetry, err := buildPostgresCaseTraversalTelemetry(*baseline.Optimization, baselineMetrics, "101", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	baseline.TraversalTelemetry = baselineTelemetry

	candidate := base
	candidate.Environment = cloneSPI2TestEnvironment(baseEnvironment, "sp-i2-candidate", candidateOrder)
	candidate.Environment.StartedAt, candidate.Environment.EndedAt = candidateStarted, candidateStarted.Add(time.Second)
	candidate.SQL = "select 'i2:' || " + fmt.Sprintf("%q", testCase.Name)
	candidate.SQLFingerprint = sqlFingerprint(candidate.SQL)
	candidateBranch := "inline_canonical_distance"
	if rowCount == 0 {
		candidateBranch = "inline_canonical_distance_no_path"
	}
	candidate.Stats = DurationStats{
		Iterations:       samples,
		WarmupIterations: warmups,
		Median:           8 * time.Millisecond,
		P95:              8 * time.Millisecond,
		Samples:          makeSamples("sp-i2-candidate", candidateOrder, 8*time.Millisecond, candidateIdentity, candidateBranch, "timed_invocation"),
	}
	candidateOutcome := translate.TargetLoweringOutcome{
		Lowering:               optimize.LoweringShortestPathExecutor,
		TargetKind:             "traversal",
		Family:                 "SP",
		Candidate:              candidateIdentity,
		Selected:               candidateIdentity,
		Applied:                candidateIdentity,
		Fallback:               baselineIdentity,
		PlannedCandidates:      spI2ShortestPathPlannedIdentities(),
		EmittedCandidates:      []string{candidateIdentity, baselineIdentity},
		EmittedPolicy:          optimize.ShortestPathPolicyI2DistanceGuardedV1,
		SelectorVersion:        optimize.ShortestPathSelectorStaticV8HiddenFanIn,
		ExecutionBoundary:      optimize.ExpansionSearchExecutionBoundaryGuardedDualArm,
		ObservationMode:        "distance",
		Scheduler:              "single_ended_level",
		Direction:              "inbound",
		PhysicalExpansion:      "end_id",
		RelationshipKindCount:  1,
		TopologyClassification: "physical_inbound_deep",
		SelectionMode:          "forced_tool",
		Eligible:               &trueValue,
		StaticallyEligible:     &trueValue,
		MinimumDepth:           traversalTelemetryPointer(int64(1)),
		MaximumDepth:           traversalTelemetryPointer(int64(64)),
		StateLimit:             100_000,
		FrontierLimit:          100_000,
	}
	candidate.Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{candidateOutcome}}
	ids := map[string]int64{
		"sp_i2_distance_bounded": 1, "sp_i2_target": 2,
		"sp_i2_candidate_marker": 3, "sp_i2_fallback_marker": 4,
		"sp_i2_candidate_rows": 5, "sp_i2_fallback_rows": 6,
	}
	planNode := func(name string, rows int64) PostgresPlanNodeMetric {
		return PostgresPlanNodeMetric{PlanNodeID: ids[name], NodeType: "Result", SubplanName: "CTE " + name, ActualRows: rows, ActualLoops: 1}
	}
	markerGate := func(branch string, rows int64) PostgresPlanNodeMetric {
		body := ids["sp_i2_"+branch+"_rows"]
		return PostgresPlanNodeMetric{PlanNodeID: body + 100, ParentPlanNodeID: body, ParentRelationship: "Outer", NodeType: "CTE Scan", CTEName: "sp_i2_" + branch + "_marker", ActualRows: rows, ActualLoops: 1}
	}
	executor := func(branch string, loops int64) PostgresPlanNodeMetric {
		body := ids["sp_i2_"+branch+"_rows"]
		return PostgresPlanNodeMetric{PlanNodeID: body + 200, ParentPlanNodeID: body, ParentRelationship: "Inner", NodeType: "Result", ActualLoops: loops}
	}
	candidateMetrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		planNode("sp_i2_distance_bounded", 32), planNode("sp_i2_target", rowCount),
		planNode("sp_i2_candidate_marker", 1), planNode("sp_i2_fallback_marker", 0),
		planNode("sp_i2_candidate_rows", rowCount), planNode("sp_i2_fallback_rows", 0),
		markerGate("candidate", 1), markerGate("fallback", 0), executor("candidate", 1), executor("fallback", 0),
	}}
	candidate.PostgresMetrics = &candidateMetrics
	candidateTelemetry, err := buildPostgresCaseTraversalTelemetry(*candidate.Optimization, candidateMetrics, "101", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichInlineDistanceTraversalTelemetry(candidateTelemetry, rowCount)
	require.NoError(t, candidateTelemetry.Validate())
	candidate.TraversalTelemetry = candidateTelemetry
	return baseline, candidate
}

// cloneSPI2TestEnvironment returns an independent copy of spi2 test environment.
func cloneSPI2TestEnvironment(environment RunEnvironment, arm string, order int) *RunEnvironment {
	copy := environment
	copy.Arm = arm
	copy.ArmOrder = order
	return &copy
}

// spI2QualificationTestFreeze prepares or inspects test evidence for sp i2 qualification test freeze.
func spI2QualificationTestFreeze(t *testing.T, discovery SPI2QualificationReport) SPI2QualificationFreezeManifest {
	t.Helper()
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	return SPI2QualificationFreezeManifest{
		Version:                   spI2FreezeVersion,
		Baseline:                  discovery.Baseline,
		Candidate:                 discovery.Candidate,
		Policy:                    discovery.Policy,
		QuerySHA256:               discovery.QuerySHA256,
		Caps:                      discovery.Caps,
		Seed:                      discovery.Seed,
		Confidence:                discovery.Confidence,
		BootstrapCount:            discovery.BootstrapCount,
		SourceCommit:              discovery.SourceCommit,
		SourceArchiveSHA256:       discovery.SourceArchiveSHA256,
		DirtyDiffSHA256:           discovery.DirtyDiffSHA256,
		BinarySHA256:              discovery.BinarySHA256,
		TrainingDeclarationSHA256: cohort.trainingDeclarationSHA256,
		HoldoutDeclarationSHA256:  cohort.holdoutDeclarationSHA256,
		FullDeclarationSHA256:     cohort.declarationSHA256,
		TrainingCorpusSHA256:      cohort.trainingCorpusSHA256,
		FullCorpusSHA256:          cohort.fullCorpusSHA256,
		TrainingResolvedSHA256:    cohort.trainingResolvedSHA256,
		FullResolvedSHA256:        cohort.fullResolvedSHA256,
		BaselineArtifactSHA256:    discovery.BaselineArtifactSHA256,
		CandidateArtifactSHA256:   discovery.CandidateArtifactSHA256,
		ResourceReportSHA256:      discovery.ResourceReportSHA256,
		DiscoveryReportSHA256:     strings.Repeat("4", 64),
		TrainingPassed:            discovery.TrainingPassed,
	}
}
