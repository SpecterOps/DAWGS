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

// TestSPI1QualificationDiscoveryPassesTrainingWithoutOpeningHoldout verifies spi1 qualification discovery passes training without opening holdout behavior.
func TestSPI1QualificationDiscoveryPassesTrainingWithoutOpeningHoldout(t *testing.T) {
	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	report, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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
	require.Equal(t, 4, report.TrainingCases)
	require.Zero(t, report.HoldoutCases)
	require.Len(t, report.Cases, 4)
	require.Equal(t, spI1QualificationCaps(), report.Caps)
	for _, gateCase := range report.Cases {
		require.True(t, gateCase.Passed, gateCase.Reasons)
		require.Equal(t, "training", gateCase.QualificationSplit)
		require.LessOrEqual(t, gateCase.MedianRatio.Upper, 0.95)
		require.LessOrEqual(t, gateCase.P95Ratio.Upper, 1.05)
	}
}

// TestTimedRuntimeAttestationIdentityIncludesExactS4Baseline verifies timed runtime attestation identity includes exact s4 baseline behavior.
func TestTimedRuntimeAttestationIdentityIncludesExactS4Baseline(t *testing.T) {
	baseline := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	translation := translate.Result{Optimization: translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
		Family:   "SP",
		Selected: baseline,
	}}}}
	require.Equal(t, baseline, timedRuntimeAttestationIdentity(translation))
}

// TestSPI1QualificationConfirmationRequiresAndPassesFrozenDiscovery verifies spi1 qualification confirmation requires and passes frozen discovery behavior.
func TestSPI1QualificationConfirmationRequiresAndPassesFrozenDiscovery(t *testing.T) {
	trainingBaseline, trainingCandidate, trainingResource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	discovery, err := buildSPI1QualificationReport(trainingBaseline, trainingCandidate, trainingResource, SPI1QualificationOptions{
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
	freeze := spI1QualificationTestFreeze(t, discovery)

	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolConfirmation)
	report, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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
	require.Equal(t, 4, report.TrainingCases)
	require.Equal(t, 3, report.HoldoutCases)
	require.Len(t, report.Cases, 7)
}

// TestSPI1QualificationRejectsUnattestedCandidateAndFreezeMutation verifies spi1 qualification rejects unattested candidate and freeze mutation behavior.
func TestSPI1QualificationRejectsUnattestedCandidateAndFreezeMutation(t *testing.T) {
	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	candidate[0].Stats.Samples[1].RuntimeAttestation = "same_case_invocation_local_replay"
	candidate[0].Stats.Samples[1].RuntimeReceiptEvents = nil
	_, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
		Seed:                1,
		Confidence:          defaultConfidenceLevel,
		BootstrapCount:      defaultBootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: strings.Repeat("a", 64),
	})
	require.ErrorContains(t, err, "timed-invocation attribution")

	trainingBaseline, trainingCandidate, trainingResource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	discovery, err := buildSPI1QualificationReport(trainingBaseline, trainingCandidate, trainingResource, SPI1QualificationOptions{
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
	freeze := spI1QualificationTestFreeze(t, discovery)
	freeze.QuerySHA256 = strings.Repeat("f", 64)
	cohort, err := canonicalSPI1Cohort()
	require.NoError(t, err)
	require.Error(t, validateSPI1FrozenDiscovery(&freeze, &discovery, cohort))
}

// TestSPI1QualificationClassifiesBoundResourceFailure verifies spi1 qualification classifies bound resource failure behavior.
func TestSPI1QualificationClassifiesBoundResourceFailure(t *testing.T) {
	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	candidate[0].PostgresMetrics.Buffers.TempWritten = 1
	resource.Cases[0] = evaluateProductionResourceGateCase(candidate[0])
	resource.Passed = false
	report, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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

// TestSPI1QualificationRejectsCanonicalEvidenceAndScheduleTampering verifies spi1 qualification rejects canonical evidence and schedule tampering behavior.
func TestSPI1QualificationRejectsCanonicalEvidenceAndScheduleTampering(t *testing.T) {
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
			candidate[0].TraversalTelemetry.Summary.RuntimeBranch = "inline_canonical_no_path"
			for index := range candidate[0].Stats.Samples {
				if candidate[0].Stats.Samples[index].Classification == "warm" {
					candidate[0].Stats.Samples[index].RuntimeBranch = "inline_canonical_no_path"
					candidate[0].Stats.Samples[index].RuntimeReceiptEvents[0].RuntimeBranch = "inline_canonical_no_path"
				}
			}
		},
		"no path relabeled witness": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			for recordIndex := range candidate {
				if !strings.HasSuffix(candidate[recordIndex].Name, "-disconnected") {
					continue
				}
				candidate[recordIndex].TraversalTelemetry.Summary.RuntimeBranch = "inline_canonical_witness"
				for sampleIndex := range candidate[recordIndex].Stats.Samples {
					if candidate[recordIndex].Stats.Samples[sampleIndex].Classification == "warm" {
						candidate[recordIndex].Stats.Samples[sampleIndex].RuntimeBranch = "inline_canonical_witness"
						candidate[recordIndex].Stats.Samples[sampleIndex].RuntimeReceiptEvents[0].RuntimeBranch = "inline_canonical_witness"
					}
				}
				return
			}
		},
		"output counter differs from observation": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			*candidate[0].TraversalTelemetry.Diagnostic.Counters.InlineShortestPath.OutputPaths = 0
		},
		"supplemental planned arm": func(_, candidate []CaseResult, _ *ResourceGateReport) {
			candidate[0].TraversalTelemetry.Summary.PlannedIdentities = append(candidate[0].TraversalTelemetry.Summary.PlannedIdentities, "SP-B1-extra")
		},
		"reduced planned search space": func(baseline, _ []CaseResult, _ *ResourceGateReport) {
			baseline[0].TraversalTelemetry.Summary.PlannedIdentities = []string{
				string(optimize.ShortestPathExecutorS4CanonicalWitness),
				string(optimize.ShortestPathExecutorIncumbentWorkspace),
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
			mutate(baseline, candidate, &resource)
			_, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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

// TestSPI1QualificationFreezesStatisticalPolicyAndDiscoverySemantics verifies spi1 qualification freezes statistical policy and discovery semantics behavior.
func TestSPI1QualificationFreezesStatisticalPolicyAndDiscoverySemantics(t *testing.T) {
	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
	for _, options := range []SPI1QualificationOptions{
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
		_, err := buildSPI1QualificationReport(baseline, candidate, resource, options)
		require.Error(t, err)
	}

	discovery, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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
	freeze := spI1QualificationTestFreeze(t, discovery)
	discovery.Cases[0].P95Ratio.Upper = 2
	cohort, err := canonicalSPI1Cohort()
	require.NoError(t, err)
	require.Error(t, validateSPI1FrozenDiscovery(&freeze, &discovery, cohort))
}

// TestSPI1FrozenTrainingEvidenceIsRecomputedFromNamedArtifacts verifies spi1 frozen training evidence is recomputed from named artifacts behavior.
func TestSPI1FrozenTrainingEvidenceIsRecomputedFromNamedArtifacts(t *testing.T) {
	baseline, candidate, resource := spI1QualificationTestArtifacts(t, referencePairProtocolDiscovery)
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

	discovery, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
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
	freeze := spI1QualificationTestFreeze(t, discovery)
	require.NoError(t, validateSPI1FrozenTrainingEvidence(&freeze, &discovery, baselinePath, candidatePath, resourcePath))

	forged := discovery
	forged.Cases = append([]SPI1QualificationCase(nil), discovery.Cases...)
	forged.Cases[0].MedianRatio = RatioInterval{
		Lower:    0.801,
		Estimate: 0.801,
		Upper:    0.801,
	}
	require.ErrorContains(t,
		validateSPI1FrozenTrainingEvidence(&freeze, &forged, baselinePath, candidatePath, resourcePath),
		"differs from its recomputed",
	)
}

// TestSPI1PathsRejectHardlinkAliases verifies spi1 paths reject hardlink aliases behavior.
func TestSPI1PathsRejectHardlinkAliases(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.json")
	alias := filepath.Join(directory, "alias.json")
	require.NoError(t, os.WriteFile(input, []byte("{}"), 0o600))
	require.NoError(t, os.Link(input, alias))
	require.Error(t, validateDistinctSPI1Paths(map[string]string{"input": input, "output": alias}))
}

// TestSPI1HoldoutCaptureProfileAcceptsBothBalancedArmsAndRejectsDrift verifies spi1 holdout capture profile accepts both balanced arms and rejects drift behavior.
func TestSPI1HoldoutCaptureProfileAcceptsBothBalancedArmsAndRejectsDrift(t *testing.T) {
	baseline := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	candidate := string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
	valid := func(executor, arm string, round, order int) config {
		return config{
			Modes:                      []ExecutionMode{ModePostgresSQL},
			Iterations:                 50,
			WarmupIterations:           20,
			Round:                      round,
			Block:                      round,
			Arm:                        arm,
			ArmOrder:                   order,
			RunUUID:                    "sp-i1-confirmation",
			PoolSize:                   1,
			PostgresForceShortest:      executor,
			PostgresRepeatableRead:     true,
			PostgresTraversalTelemetry: postgresTraversalTelemetryDiagnostic,
			OutputJSONL:                fmt.Sprintf(".coverage/sp-i1-%s-%d.jsonl", arm, round),
			AppendJSONL:                round > 1,
			SPI1Freeze:                 ".coverage/sp-i1-freeze.json",
			SPI1DiscoveryReport:        ".coverage/sp-i1-discovery.json",
			SPI1TrainingBaseline:       ".coverage/sp-i1-training-s4.jsonl",
			SPI1TrainingCandidate:      ".coverage/sp-i1-training-i1.jsonl",
			SPI1TrainingResource:       ".coverage/sp-i1-training-resource.json",
		}
	}
	for _, cfg := range []config{
		valid(baseline, "sp-i1-s4", 1, 1),
		valid(candidate, "sp-i1-candidate", 1, 2),
		valid(baseline, "sp-i1-s4", 2, 2),
		valid(candidate, "sp-i1-candidate", 2, 1),
	} {
		require.NoError(t, validateSPI1HoldoutCaptureConfig(cfg))
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
		"path alias":              func(cfg *config) { cfg.OutputJSONL = cfg.SPI1Freeze },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid(baseline, "sp-i1-s4", 1, 1)
			mutate(&cfg)
			require.Error(t, validateSPI1HoldoutCaptureConfig(cfg))
		})
	}
	t.Run("round after one requires append", func(t *testing.T) {
		cfg := valid(baseline, "sp-i1-s4", 2, 2)
		cfg.AppendJSONL = false
		require.Error(t, validateSPI1HoldoutCaptureConfig(cfg))
	})
}

// TestSPI1HoldoutDetectionAndCorpusBindingIgnoreMutableTagAlone verifies spi1 holdout detection and corpus binding ignore mutable tag alone behavior.
func TestSPI1HoldoutDetectionAndCorpusBindingIgnoreMutableTagAlone(t *testing.T) {
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	training, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{"sp-i1-inbound-v1-training"}})
	require.NoError(t, err)
	require.False(t, selectedCorpusContainsSPI1Holdout(training))

	confirmation, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{"sp-i1-inbound-v1-training", "sp-i1-inbound-v1-holdout"}})
	require.NoError(t, err)
	require.True(t, selectedCorpusContainsSPI1Holdout(confirmation))
	for index := range confirmation.Cases {
		confirmation.Cases[index].Source = strings.TrimPrefix(confirmation.Cases[index].Source, "../../")
		confirmation.Cases[index].Tags = nil
	}
	require.True(t, selectedCorpusContainsSPI1Holdout(confirmation), "canonical key detection must not depend on tags")

	// Restore exact declarations before checking the complete frozen corpus.
	exact, _, err := selectScaleCorpus(full, CorpusSelectors{Tags: []string{"sp-i1-inbound-v1-training", "sp-i1-inbound-v1-holdout"}})
	require.NoError(t, err)
	for index := range exact.Cases {
		exact.Cases[index].Source = strings.TrimPrefix(exact.Cases[index].Source, "../../")
	}
	cohort, err := canonicalSPI1Cohort()
	require.NoError(t, err)
	require.NoError(t, validateSPI1Corpus(exact, cohort))

	omitted := ScaleCorpus{Cases: append([]ScaleCase(nil), exact.Cases[:len(exact.Cases)-1]...)}
	require.Error(t, validateSPI1Corpus(omitted, cohort))
	mutated := ScaleCorpus{Cases: append([]ScaleCase(nil), exact.Cases...)}
	mutated.Cases[0].Cypher += " "
	require.Error(t, validateSPI1Corpus(mutated, cohort))
}

// TestRunnableCorpusExcludesSPI1HoldoutUntilExactOptIn verifies runnable corpus excludes spi1 holdout until exact opt in behavior.
func TestRunnableCorpusExcludesSPI1HoldoutUntilExactOptIn(t *testing.T) {
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	ordinary, manifest, err := selectRunnableScaleCorpus(full, CorpusSelectors{})
	require.NoError(t, err)
	require.False(t, selectedCorpusContainsSPI1Holdout(ordinary))
	require.False(t, manifest.DiagnosticOnly)
	require.Equal(t, manifest.FullDeclarationCount, manifest.SelectedDeclarationCount+manifest.OmittedDeclarationCount)
	require.Equal(t, 6, manifest.OmittedDeclarationCount)
	require.Equal(t, 6, manifest.ProtectedDeclarationCount)
	require.True(t, lowercaseSHA256(manifest.ProtectedDeclarationSHA256))
	require.True(t, selectedCorpusContainsTag(ordinary, spI1TrainingTag))

	for name, selectors := range map[string]CorpusSelectors{
		"generic holdout tag": {Tags: []string{"holdout"}},
		"broad category":      {Categories: []string{"generated_shortest_path_v2"}},
	} {
		t.Run(name, func(t *testing.T) {
			selected, _, err := selectRunnableScaleCorpus(full, selectors)
			require.NoError(t, err)
			require.False(t, selectedCorpusContainsSPI1Holdout(selected))
		})
	}

	exactTag, _, err := selectRunnableScaleCorpus(full, CorpusSelectors{Tags: []string{spI1HoldoutTag}})
	require.NoError(t, err)
	require.Len(t, exactTag.Cases, 3)
	require.True(t, selectedCorpusContainsSPI1Holdout(exactTag))

	exactCase, _, err := selectRunnableScaleCorpus(full, CorpusSelectors{Cases: []string{spI1CanonicalCases[4].name}})
	require.NoError(t, err)
	require.Len(t, exactCase.Cases, 1)
	require.True(t, selectedCorpusContainsSPI1Holdout(exactCase))
}

// spI1QualificationTestArtifacts prepares or inspects test evidence for sp i1 qualification test artifacts.
func spI1QualificationTestArtifacts(t *testing.T, protocol string) ([]CaseResult, []CaseResult, ResourceGateReport) {
	t.Helper()
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	tags := []string{"sp-i1-inbound-v1-training"}
	rounds, samples, warmups := 5, 10, 5
	corpusSHA256 := spI1TrainingCorpusSHA256
	if protocol == referencePairProtocolConfirmation {
		tags = append(tags, "sp-i1-inbound-v1-holdout")
		rounds, samples, warmups = 10, 50, 20
		corpusSHA256 = spI1FullCorpusSHA256
	}
	selected, selection, err := selectRunnableScaleCorpus(full, CorpusSelectors{Tags: tags})
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
			left, right := spI1QualificationTestRecords(t, testCase, fixture, selection, corpusSHA256, round, samples, warmups)
			baseline = append(baseline, left)
			candidate = append(candidate, right)
			allObserved := traversalNumericObservations(right.TraversalTelemetry.Diagnostic.Counters)
			observed := make(map[string]int64, len(spI1TelemetryCaps()))
			for name := range spI1TelemetryCaps() {
				observed[name] = allObserved[name]
			}
			resource.Cases = append(resource.Cases, ResourceGateCase{
				Dataset:              right.Dataset,
				Name:                 right.Name,
				Tier:                 right.Shape.FixtureTier,
				Round:                right.Environment.Round,
				Block:                right.Environment.Block,
				RunUUID:              right.Environment.RunUUID,
				Arm:                  right.Environment.Arm,
				ArmOrder:             right.Environment.ArmOrder,
				QualificationSplit:   right.Shape.QualificationSplit,
				Architecture:         string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
				Passed:               true,
				NumericLimits:        spI1TelemetryCaps(),
				NumericObserved:      observed,
				RuntimeReceiptChains: runtimeReceiptChains(right.Stats.Samples),
			})
		}
	}
	return baseline, candidate, resource
}

// spI1QualificationTestRecords prepares or inspects test evidence for sp i1 qualification test records.
func spI1QualificationTestRecords(
	t *testing.T,
	testCase ScaleCase,
	fixture FixtureMetadata,
	selection SelectionManifest,
	corpusSHA256 string,
	round, samples, warmups int,
) (CaseResult, CaseResult) {
	t.Helper()
	baselineIdentity := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	candidateIdentity := string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
	baselineOrder, candidateOrder := 1, 2
	if round%2 == 0 {
		baselineOrder, candidateOrder = 2, 1
	}
	rowCount := int64(1)
	var observed []string
	if len(testCase.Expected.PathRows) == 1 {
		expectedPath := testCase.Expected.PathRows[0]
		path := stablePathObservation{
			Nodes:         make([]stableNodeObservation, len(expectedPath.Nodes)),
			Relationships: make([]stableRelationshipObservation, len(expectedPath.RelationshipKinds)),
		}
		for index, identity := range expectedPath.Nodes {
			path.Nodes[index].Identity = identity
		}
		for index, kind := range expectedPath.RelationshipKinds {
			path.Relationships[index] = stableRelationshipObservation{
				Identity: expectedPath.RelationshipKeys[index],
				Start:    expectedPath.Nodes[index],
				End:      expectedPath.Nodes[index+1],
				Kind:     kind,
			}
		}
		raw, err := json.Marshal([]any{path})
		require.NoError(t, err)
		observed = []string{string(raw)}
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
			RunUUID:        "sp-i1-test-run",
			Iteration:      0,
			Case:           testCase.Name,
			Dataset:        testCase.Dataset,
			Backend:        ModePostgresSQL,
			ConnectionID:   "101",
			Classification: "cold",
			Duration:       2 * duration,
		}
		for index := range samples {
			invocationID := fmt.Sprintf("sp-i1-test-%s-%s-%d-%d", arm, testCase.Name, round, index+1)
			result[index+1] = LatencySample{
				Round:               round,
				Block:               round,
				Arm:                 arm,
				ArmOrder:            order,
				RunUUID:             "sp-i1-test-run",
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
		RunUUID:               "sp-i1-test-run",
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
	baseline.Environment = cloneSPI1TestEnvironment(baseEnvironment, "sp-i1-s4", baselineOrder)
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
	}
	baseline.Stats = DurationStats{
		Iterations:       samples,
		WarmupIterations: warmups,
		Median:           10 * time.Millisecond,
		P95:              10 * time.Millisecond,
		Samples:          makeSamples("sp-i1-s4", baselineOrder, 10*time.Millisecond, baselineIdentity, baselineBranch, "timed_invocation"),
	}
	baselineOutcome := translate.TargetLoweringOutcome{
		Lowering:               optimize.LoweringShortestPathExecutor,
		TargetKind:             "traversal",
		Family:                 "SP",
		Selected:               baselineIdentity,
		Applied:                baselineIdentity,
		Fallback:               "SP-S0",
		PlannedCandidates:      spI1ShortestPathPlannedIdentities(),
		SelectorVersion:        "sp-tool-v1",
		ExecutionBoundary:      "stored_helper",
		ObservationMode:        "one_path",
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
	candidate.Environment = cloneSPI1TestEnvironment(baseEnvironment, "sp-i1-candidate", candidateOrder)
	candidate.Environment.StartedAt, candidate.Environment.EndedAt = candidateStarted, candidateStarted.Add(time.Second)
	candidate.SQL = "select 'i1:' || " + fmt.Sprintf("%q", testCase.Name)
	candidate.SQLFingerprint = sqlFingerprint(candidate.SQL)
	candidateBranch := "inline_canonical_witness"
	if rowCount == 0 {
		candidateBranch = "inline_canonical_no_path"
	}
	candidate.Stats = DurationStats{
		Iterations:       samples,
		WarmupIterations: warmups,
		Median:           8 * time.Millisecond,
		P95:              8 * time.Millisecond,
		Samples:          makeSamples("sp-i1-candidate", candidateOrder, 8*time.Millisecond, candidateIdentity, candidateBranch, "timed_invocation"),
	}
	candidateOutcome := translate.TargetLoweringOutcome{
		Lowering:               optimize.LoweringShortestPathExecutor,
		TargetKind:             "traversal",
		Family:                 "SP",
		Candidate:              candidateIdentity,
		Selected:               candidateIdentity,
		Applied:                candidateIdentity,
		Fallback:               baselineIdentity,
		PlannedCandidates:      spI1ShortestPathPlannedIdentities(),
		EmittedCandidates:      []string{candidateIdentity, baselineIdentity},
		EmittedPolicy:          optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		SelectorVersion:        "sp-i1-canonical-tool-v1",
		ExecutionBoundary:      optimize.ExpansionSearchExecutionBoundaryGuardedDualArm,
		ObservationMode:        "one_path",
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
		PredecessorLimit:       100_000,
		EnumerationLimit:       100_000,
		OutputBytesLimit:       64 * 1024 * 1024,
	}
	candidate.Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{candidateOutcome}}
	candidateMetrics := PostgresPlanMetrics{
		Provenance:     map[string]string{},
		HydrationRows:  rowCount,
		HydrationLoops: rowCount,
		PlanNodes: []PostgresPlanNodeMetric{
			inlinePredecessorPlanNode("asp_i1_distance_bounded", 32, 1),
			inlinePredecessorPlanNode("asp_i1_predecessor_bounded", 16, 1),
			inlinePredecessorPlanNode("asp_i1_paths_bounded", rowCount, 1),
			inlinePredecessorPlanNode("asp_i1_shortest", rowCount, 1),
			inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
			inlinePredecessorPlanNode("asp_i1_fallback_marker", 0, 1),
			inlinePredecessorPlanNode("asp_i1_candidate_rows", rowCount, 1),
			inlinePredecessorPlanNode("asp_i1_fallback_rows", 0, 1),
			inlinePredecessorMarkerGateNode("candidate", 1, 1),
			inlinePredecessorMarkerGateNode("fallback", 0, 1),
			inlinePredecessorExecutorNode("candidate", 1),
			inlinePredecessorExecutorNode("fallback", 0),
		},
	}
	candidate.PostgresMetrics = &candidateMetrics
	candidateTelemetry, err := buildPostgresCaseTraversalTelemetry(*candidate.Optimization, candidateMetrics, "101", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichInlinePredecessorTraversalTelemetry(candidateTelemetry, candidateMetrics, rowCount, observed)
	require.NoError(t, candidateTelemetry.Validate())
	candidate.TraversalTelemetry = candidateTelemetry
	return baseline, candidate
}

// cloneSPI1TestEnvironment returns an independent copy of spi1 test environment.
func cloneSPI1TestEnvironment(environment RunEnvironment, arm string, order int) *RunEnvironment {
	copy := environment
	copy.Arm = arm
	copy.ArmOrder = order
	return &copy
}

// spI1QualificationTestFreeze prepares or inspects test evidence for sp i1 qualification test freeze.
func spI1QualificationTestFreeze(t *testing.T, discovery SPI1QualificationReport) SPI1QualificationFreezeManifest {
	t.Helper()
	cohort, err := canonicalSPI1Cohort()
	require.NoError(t, err)
	return SPI1QualificationFreezeManifest{
		Version:                   spI1FreezeVersion,
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
