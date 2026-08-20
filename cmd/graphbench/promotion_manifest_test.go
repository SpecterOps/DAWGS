// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	pgdriver "github.com/specterops/dawgs/drivers/pg"
	"github.com/stretchr/testify/require"
)

// writePromotionManifestWithPassingEvidence writes promotion manifest with passing evidence.
func writePromotionManifestWithPassingEvidence(t *testing.T, manifest PromotionManifest) string {
	t.Helper()
	if manifest.OperationalCandidateSQLSHA256 == "" {
		manifest.OperationalCandidateSQLSHA256 = sqlFingerprint(operationalTestSQL)
	}
	directory := t.TempDir()
	manifest.Evidence = map[string]PromotionEvidenceReference{}
	for _, role := range requiredPromotionEvidenceRoles {
		document := passingPromotionEvidenceDocument(t, manifest, role)
		raw, err := json.Marshal(document)
		require.NoError(t, err)
		path := role + ".json"
		require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
		digest := sha256.Sum256(raw)
		manifest.Evidence[role] = PromotionEvidenceReference{
			Path:   path,
			SHA256: hex.EncodeToString(digest[:]),
		}
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(directory, "promotion.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

func passingPromotionEvidenceDocument(t *testing.T, manifest PromotionManifest, role string) any {
	t.Helper()
	identity := promotionEvidenceIdentity(manifest)
	switch role {
	case "aa":
		return passingPromotionAAReport(identity)
	case "confirmation":
		return passingPromotionConfirmationReport(t, identity)
	case "performance":
		return passingPromotionPerformanceReport(t, identity)
	case "resource":
		return passingPromotionResourceReport(identity)
	case "reference_closure":
		return passingPromotionReferenceClosureReport(identity)
	case "operational":
		return passingPromotionOperationalReport(t, identity)
	default:
		t.Fatalf("unknown promotion evidence role %q", role)
		return nil
	}
}

func passingPromotionResourceReport(identity PromotionEvidenceIdentity) promotionResourceReport {
	report := promotionTestResourceReport(identity)
	nativeRaw, err := json.Marshal(report)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	return promotionResourceReport{
		ResourceGateReport: report, PromotionIdentity: identity,
		NativeReportSHA256: hex.EncodeToString(digest[:]), NativeReportBase64: base64.StdEncoding.EncodeToString(nativeRaw),
	}
}

func promotionTestResourceReport(identity PromotionEvidenceIdentity) ResourceGateReport {
	limits, supported := promotionResourceNumericLimits(identity)
	if !supported {
		panic("unsupported promotion test resource candidate")
	}
	report := ResourceGateReport{Version: resourceGateVersion, ArtifactSHA256: strings.Repeat("2", 64), Passed: true}
	architecture := identity.Candidate
	if isOrientationProbePolicy(identity.Candidate) {
		architecture = string(optimize.ExpansionSearchSuffixSeededReverse)
	}
	for index, cohortCase := range promotionTestPerformanceCohort(identity.Candidate) {
		observed := make(map[string]int64, len(limits))
		for name := range limits {
			observed[name] = 1
		}
		chains := promotionTestReceiptChains(identity.Candidate, fmt.Sprintf("candidate-%d", index), 500)
		for round := 1; round <= 10; round++ {
			report.Cases = append(report.Cases, ResourceGateCase{
				Dataset: cohortCase.dataset, Name: cohortCase.name, Round: round, Block: round,
				RunUUID: fmt.Sprintf("resource-run-%d", index), Arm: "candidate", ArmOrder: 1 + (round+1)%2,
				Tier: "normal", QualificationSplit: cohortCase.split, Architecture: architecture, Passed: true,
				NumericLimits: limits, NumericObserved: observed,
				RuntimeReceiptChains: chains[(round-1)*50 : round*50],
			})
		}
	}
	return report
}

func promotionTestNativeResourceSHA256(identity PromotionEvidenceIdentity) string {
	raw, err := json.Marshal(promotionTestResourceReport(identity))
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func passingPromotionReferenceClosureReport(identity PromotionEvidenceIdentity) promotionReferenceClosureReport {
	report := promotionTestReferenceClosureReport(identity)
	nativeRaw, err := json.Marshal(report)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	return promotionReferenceClosureReport{
		ReferenceClosureReport: report, PromotionIdentity: identity,
		NativeReportSHA256: hex.EncodeToString(digest[:]), NativeReportBase64: base64.StdEncoding.EncodeToString(nativeRaw),
	}
}

func promotionTestReferenceClosureReport(identity PromotionEvidenceIdentity) ReferenceClosureReport {
	query := ""
	for _, bucket := range identity.Buckets {
		if len(bucket.QuerySHA256) > 0 {
			query = bucket.QuerySHA256[0]
			break
		}
	}
	report := ReferenceClosureReport{
		Version: referenceClosureReportVersion, Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: defaultBootstrapCount,
		ArtifactSHA256: strings.Repeat("2", 64), Candidate: identity.Candidate, SourceCommit: identity.SourceCommit,
		DirtyDiffSHA256: cleanWorkingTreeSHA256(), BinarySHA256: identity.BinarySHA256, CorpusSHA256: identity.CorpusSHA256,
		ReferenceName: "s3_unidirectional_trail_cte", Passed: true,
	}
	for index, cohortCase := range promotionTestPerformanceCohort(identity.Candidate) {
		report.Cases = append(report.Cases, ReferenceClosureCase{
			Dataset: cohortCase.dataset, Name: cohortCase.name, QualificationSplit: cohortCase.split,
			WorkloadSHA256: promotionTestWorkloadSHA256(cohortCase), QuerySHA256: query,
			ReferenceName: report.ReferenceName, ReferenceArchitecture: "SP-S3-U-D",
			Rounds: 10, ProductionSamples: 500, ReferenceSamples: 500,
			MedianRatio: RatioInterval{Estimate: 1, Lower: 0.99, Upper: 1.01}, MedianChange: DurationInterval{Estimate: 0, Lower: -time.Microsecond, Upper: time.Microsecond},
			AbsoluteGapUpper: time.Microsecond, RatioUpperLimit: 1.10, AbsoluteFloor: 100 * time.Microsecond,
			AbsoluteResolution: 100 * time.Microsecond, Passed: true,
			ProductionRuntimeReceiptChains: promotionTestReceiptChains(identity.Candidate, fmt.Sprintf("closure-%d", index), 500),
		})
	}
	return report
}

func passingPromotionAAReport(identity PromotionEvidenceIdentity) promotionAAResolutionReport {
	report := promotionTestAAResolutionReport(identity)
	nativeRaw, err := json.Marshal(report)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	return promotionAAResolutionReport{
		AAResolutionReport: report,
		PromotionIdentity:  identity,
		NativeReportSHA256: hex.EncodeToString(digest[:]),
		NativeReportBase64: base64.StdEncoding.EncodeToString(nativeRaw),
	}
}

func promotionTestAAResolutionReport(identity PromotionEvidenceIdentity) AAResolutionReport {
	artifact := strings.Repeat("1", 64)
	metric := AAMetricResolution{
		Ratio:          RatioInterval{Estimate: 1, Lower: 1, Upper: 1},
		AbsoluteChange: DurationInterval{},
	}
	report := AAResolutionReport{
		Version: aaReportVersion, Seed: 1, Confidence: defaultConfidenceLevel,
		ArtifactSHA256: artifact, HostFingerprint: strings.Repeat("2", 64),
		MinimumRounds: minimumGateRounds, MinimumSamplesPerArmPerRound: 10, OrderBalanced: true,
		PhysicalChronology: &AAPhysicalChronology{
			Version: aaPhysicalChronologyVersion, Validated: true, ArtifactSHA256: artifact,
			Rounds: minimumGateRounds, Arms: []string{"aa-a", "aa-b"},
		},
		MinimumP99SamplesPerArm: 10_000,
	}
	for _, cohortCase := range promotionTestPerformanceCohort(identity.Candidate) {
		report.Cases = append(report.Cases, AAResolutionCase{
			Dataset: cohortCase.dataset, Name: cohortCase.name, Backend: ModePostgresSQL,
			WorkloadSHA256: promotionTestWorkloadSHA256(cohortCase), PostgresEnvironmentSHA256: strings.Repeat("4", 64), FixtureSHA256: strings.Repeat("5", 64),
			Rounds: minimumGateRounds, SamplesPerArm: minimumGateRounds * 10, P50: metric, P95: metric,
			P99Reason: "diagnostic only: insufficient samples",
		})
	}
	return report
}

func passingPromotionConfirmationReport(t *testing.T, identity PromotionEvidenceIdentity) any {
	t.Helper()
	switch identity.Candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return passingPromotionSPI1Confirmation(t, identity)
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return passingPromotionSPI2Confirmation(t, identity)
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1):
		return passingPromotionOrientationConfirmation(identity)
	case string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return passingPromotionOrientationV2Confirmation(identity)
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return passingPromotionGenericConfirmation(identity)
	default:
		t.Fatalf("no promotion confirmation fixture for candidate %q", identity.Candidate)
		return nil
	}
}

func passingPromotionSPI1Confirmation(t *testing.T, identity PromotionEvidenceIdentity) promotionSPI1QualificationReport {
	t.Helper()
	cohort, err := canonicalSPI1Cohort()
	require.NoError(t, err)
	report := SPI1QualificationReport{
		Version: spI1QualificationVersion, Protocol: referencePairProtocolConfirmation,
		Baseline: string(optimize.ShortestPathExecutorS4CanonicalWitness), Candidate: identity.Candidate, Policy: optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		QuerySHA256: spI1QuerySHA256, Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: defaultBootstrapCount,
		MaterialityRatio: 0.95, MaterialityAbsolute: 100 * time.Microsecond, P95RatioLimit: 1.05, Caps: spI1QualificationCaps(),
		SourceCommit: identity.SourceCommit, SourceArchiveSHA256: identity.SourceSHA256, DirtyDiffSHA256: cleanWorkingTreeSHA256(),
		BinarySHA256: identity.BinarySHA256, CorpusSHA256: identity.CorpusSHA256,
		CohortDeclarationSHA256: cohort.declarationSHA256, ResolvedSelectionSHA256: cohort.fullResolvedSHA256,
		TrainingDeclarationSHA256: cohort.trainingDeclarationSHA256, HoldoutDeclarationSHA256: cohort.holdoutDeclarationSHA256,
		FullDeclarationSHA256: cohort.declarationSHA256, TrainingCorpusSHA256: cohort.trainingCorpusSHA256, FullCorpusSHA256: cohort.fullCorpusSHA256,
		BaselineArtifactSHA256: strings.Repeat("1", 64), CandidateArtifactSHA256: strings.Repeat("2", 64),
		ResourceReportSHA256: promotionTestNativeResourceSHA256(identity), FreezeManifestSHA256: strings.Repeat("4", 64),
		EvidencePassed: true, TrainingCases: len(cohort.trainingKeys), HoldoutCases: len(cohort.holdoutKeys),
		TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
	}
	for _, declaration := range spI1CanonicalCases {
		report.Cases = append(report.Cases, SPI1QualificationCase{
			Dataset: declaration.dataset, Name: declaration.name, QualificationSplit: declaration.split,
			Rounds: 10, BaselineSamples: 500, CandidateSamples: 500,
			MedianRatio:  RatioInterval{Estimate: 0.5, Lower: 0.4, Upper: 0.6},
			MedianSaving: DurationInterval{Estimate: 300 * time.Microsecond, Lower: 200 * time.Microsecond, Upper: 400 * time.Microsecond},
			P95Ratio:     RatioInterval{Estimate: 0.7, Lower: 0.6, Upper: 0.8},
			Material:     true, P95Contained: true, ResourcePassed: true, RuntimeBranch: "canonical_predecessor_witness", Passed: true,
		})
	}
	return promotionSPI1QualificationReport{SPI1QualificationReport: report, PromotionIdentity: identity}
}

func passingPromotionSPI2Confirmation(t *testing.T, identity PromotionEvidenceIdentity) promotionSPI2QualificationReport {
	t.Helper()
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	report := SPI2QualificationReport{
		Version: spI2QualificationVersion, Protocol: referencePairProtocolConfirmation,
		Baseline: string(optimize.ShortestPathExecutorS4CanonicalDistance), Candidate: identity.Candidate, Policy: optimize.ShortestPathPolicyI2DistanceGuardedV1,
		QuerySHA256: spI2QuerySHA256, Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: defaultBootstrapCount,
		MaterialityRatio: 0.95, MaterialityAbsolute: 100 * time.Microsecond, P95RatioLimit: 1.05,
		AdverseRatioLimit: 1.10, AdverseAbsoluteLimit: 100 * time.Microsecond, Caps: spI2QualificationCaps(),
		SourceCommit: identity.SourceCommit, SourceArchiveSHA256: identity.SourceSHA256, DirtyDiffSHA256: cleanWorkingTreeSHA256(),
		BinarySHA256: identity.BinarySHA256, CorpusSHA256: identity.CorpusSHA256,
		CohortDeclarationSHA256: cohort.declarationSHA256, ResolvedSelectionSHA256: cohort.fullResolvedSHA256,
		TrainingDeclarationSHA256: cohort.trainingDeclarationSHA256, HoldoutDeclarationSHA256: cohort.holdoutDeclarationSHA256,
		FullDeclarationSHA256: cohort.declarationSHA256, TrainingCorpusSHA256: cohort.trainingCorpusSHA256, FullCorpusSHA256: cohort.fullCorpusSHA256,
		BaselineArtifactSHA256: strings.Repeat("1", 64), CandidateArtifactSHA256: strings.Repeat("2", 64),
		ResourceReportSHA256: promotionTestNativeResourceSHA256(identity), FreezeManifestSHA256: strings.Repeat("4", 64),
		EvidencePassed: true, TrainingCases: len(cohort.trainingKeys), HoldoutCases: len(cohort.holdoutKeys),
		TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
	}
	for _, declaration := range spI2CanonicalCases {
		role := "target"
		if strings.Contains(declaration.name, "cycle-control") {
			role = "adverse_control"
		}
		report.Cases = append(report.Cases, SPI2QualificationCase{
			Dataset: declaration.dataset, Name: declaration.name, QualificationSplit: declaration.split, QualificationRole: role,
			Rounds: 10, BaselineSamples: 500, CandidateSamples: 500,
			MedianRatio:  RatioInterval{Estimate: 0.5, Lower: 0.4, Upper: 0.6},
			MedianSaving: DurationInterval{Estimate: 300 * time.Microsecond, Lower: 200 * time.Microsecond, Upper: 400 * time.Microsecond},
			P95Ratio:     RatioInterval{Estimate: 0.7, Lower: 0.6, Upper: 0.8},
			Material:     true, P95Contained: true, ResourcePassed: true, RuntimeBranch: "inline_canonical_distance", Passed: true,
		})
	}
	return promotionSPI2QualificationReport{SPI2QualificationReport: report, PromotionIdentity: identity}
}

func passingPromotionOrientationConfirmation(identity PromotionEvidenceIdentity) promotionOrientationSelectorReport {
	gate := promotionTestOrientationGate("baseline", "candidate", 500)
	report := OrientationSelectorReport{
		Version: orientationSelectorReportVersion, Policy: identity.Candidate, Protocol: referencePairProtocolConfirmation,
		Seed: 1, Confidence: defaultConfidenceLevel,
		ShadowArtifactSHA256: strings.Repeat("1", 64), IncumbentArtifactSHA256: strings.Repeat("2", 64),
		ReverseArtifactSHA256: strings.Repeat("3", 64), AAReportSHA256: promotionTestNativeAASHA256(identity),
		SelectorRegretRatioLimit: 1.10, ProbeOverheadRatioLimit: 1.10, ProbeOverheadAbsoluteLimit: 100 * time.Microsecond,
		EvidencePassed: true, TrainingCases: 1, HoldoutCases: 1, TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
		Cases: []OrientationSelectorCase{
			{Dataset: "orientation-training", Name: "training", QualificationSplit: "training", QualificationRole: "qualification", QualificationEligible: true, Rounds: 10, WouldSelectIdentity: "forward", FastestExactIdentity: "forward", ExactObservationsMatched: true, SelectorRegret: gate, ProbeOverhead: gate, Passed: true},
			{Dataset: "orientation-holdout", Name: "holdout", QualificationSplit: "holdout", QualificationRole: "qualification", QualificationEligible: true, Rounds: 10, WouldSelectIdentity: "reverse", FastestExactIdentity: "reverse", ExactObservationsMatched: true, SelectorRegret: gate, ProbeOverhead: gate, Passed: true},
		},
	}
	return promotionOrientationSelectorReport{OrientationSelectorReport: report, PromotionIdentity: identity}
}

func passingPromotionOrientationV2Confirmation(identity PromotionEvidenceIdentity) promotionOrientationSelectorV2Report {
	cohort, err := canonicalOrientationV2Cohort()
	if err != nil {
		panic(err)
	}
	report := OrientationSelectorV2Report{
		Version: orientationSelectorReportV2Version, Policy: identity.Candidate, Protocol: referencePairProtocolConfirmation,
		Seed: 1, Confidence: defaultConfidenceLevel,
		SourceCommit: identity.SourceCommit, DirtyDiffSHA256: cleanWorkingTreeSHA256(), BinarySHA256: identity.BinarySHA256, CorpusSHA256: identity.CorpusSHA256,
		CohortDeclarationSHA256: cohort.declarationSHA256, FreezeManifestSHA256: strings.Repeat("6", 64),
		Formula: "F2=root_rows+maximum_depth*forward_degree_rows;R2=suffix_rows+boundary_rows+reverse_degree_rows;reverse=complete&&4*R2<3*F2",
		Caps:    orientationPromotionCaps(), ShadowArtifactSHA256: strings.Repeat("1", 64), IncumbentArtifactSHA256: strings.Repeat("3", 64),
		ReverseArtifactSHA256: strings.Repeat("4", 64), GuardedArtifactSHA256: strings.Repeat("2", 64), AAReportSHA256: promotionTestNativeAASHA256(identity),
		ShadowForwardRatioLimit: 1.10, GuardedSelectedRatioLimit: 1.10, GuardedFastestRatioLimit: 1.10, OverheadAbsoluteLimit: 100 * time.Microsecond,
		EvidencePassed: true, TrainingCases: 8, HoldoutCases: 4, TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
	}
	for _, declaration := range orientationV2CanonicalCases {
		role, tuning, _ := orientationQualificationRole(declaration.split, referencePairProtocolConfirmation)
		runtimeIdentity := string(optimize.ExpansionSearchSuffixSeededReverse)
		observedIdentity := identity.Candidate + ":" + runtimeIdentity
		report.Cases = append(report.Cases, OrientationSelectorV2Case{
			Dataset: declaration.dataset, Name: declaration.name,
			QualificationSplit: declaration.split, QualificationRole: role, ThresholdTuningEligible: tuning, QualificationEligible: true, Rounds: 10,
			WouldSelectIdentity: runtimeIdentity, FastestExactIdentity: runtimeIdentity,
			GuardedRuntimeIdentity: runtimeIdentity, GuardedRuntimeBranch: "suffix_seeded_reverse",
			ExactObservationsMatched: true,
			ShadowForwardOverhead: OrientationLatencyGateV2{
				Applicable: false,
				OrientationLatencyGate: promotionTestOrientationGate(
					string(optimize.ExpansionSearchStepwiseForward), identity.Candidate+":shadow", 500,
				),
			},
			GuardedSelectedOverhead: promotionTestOrientationGate(runtimeIdentity, observedIdentity, 500),
			GuardedFastestRegret:    promotionTestOrientationGate(runtimeIdentity, observedIdentity, 500),
			Passed:                  true,
		})
	}
	return promotionOrientationSelectorV2Report{OrientationSelectorV2Report: report, PromotionIdentity: identity}
}

func passingPromotionGenericConfirmation(identity PromotionEvidenceIdentity) promotionConfirmationReport {
	report := ConfirmationReport{
		Version: confirmationReportVersion, Kind: "causal_confirmation", Seed: 1, Confidence: defaultConfidenceLevel,
		LeftArm: "incumbent", RightArm: "candidate", LeftSHA256: strings.Repeat("1", 64), RightSHA256: strings.Repeat("2", 64),
		AAReport: "aa.json", AAReportSHA256: promotionTestNativeAASHA256(identity), PromotionEligible: true,
		QualificationRequired: true, TrainingCases: 1, HoldoutCases: 1, TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
		QualificationFamilies: []TraversalQualificationStatus{{
			Family: identity.Candidate, TrainingCases: 1, HoldoutCases: 1, TrainingPassed: true, HoldoutPassed: true, Passed: true,
		}},
	}
	for index, split := range []string{"training", "holdout"} {
		report.Cases = append(report.Cases, ConfirmationCase{
			Dataset: "fixture", Name: split, Backend: ModePostgresSQL, Tier: "normal", QualificationSplit: split, TimingGated: true,
			MatchedRounds: 10, LeftSamples: 500, RightSamples: 500, Comparable: true,
			P50: promotionTestConfirmationMetric("cleared_non_inferior"), P95: promotionTestConfirmationMetric("cleared_non_inferior"),
			Disposition:               "cleared_non_inferior",
			RightRuntimeReceiptChains: promotionTestReceiptChains(identity.Candidate, fmt.Sprintf("confirmation-%d", index), 500),
		})
	}
	return promotionConfirmationReport{ConfirmationReport: report, PromotionIdentity: identity}
}

func passingPromotionPerformanceReport(t *testing.T, identity PromotionEvidenceIdentity) promotionPerfGateReport {
	t.Helper()
	materialityRatio := 0.95
	materialityAbsolute := 100 * time.Microsecond
	cohort := promotionTestPerformanceCohort(identity.Candidate)
	trainingCases, holdoutCases := 0, 0
	for _, gateCase := range cohort {
		if gateCase.split == "training" {
			trainingCases++
		} else {
			holdoutCases++
		}
	}
	report := PerfGateReport{
		Version: perfGateVersion, Seed: 1, Confidence: defaultConfidenceLevel, RegressionThreshold: minimumTimingNoiseRatio,
		BaselineSHA256: strings.Repeat("1", 64), CandidateSHA256: strings.Repeat("2", 64), AAReportSHA256: promotionTestNativeAASHA256(identity),
		DeclarationSHA256: promotionTestDeclarationSHA256(identity.Candidate), Passed: true, PromotionEligible: true,
		MaterialityRequired: true, MaterialityTargets: 1, MaterialityPassed: true,
		QualificationRequired: true, TrainingCases: trainingCases, HoldoutCases: holdoutCases, TrainingPassed: true, HoldoutPassed: true, QualificationPassed: true,
		QualificationFamilies: []TraversalQualificationStatus{{
			Family: identity.Candidate, TrainingCases: trainingCases, HoldoutCases: holdoutCases, TrainingPassed: true, HoldoutPassed: true, Passed: true,
		}},
	}
	for index, cohortCase := range cohort {
		saving := DurationInterval{Estimate: 300 * time.Microsecond, Lower: 200 * time.Microsecond, Upper: 400 * time.Microsecond}
		change := negateDurationInterval(saving)
		p95Ratio := RatioInterval{Estimate: 0.7, Lower: 0.6, Upper: 0.8}
		p95Change := DurationInterval{Estimate: -200 * time.Microsecond, Lower: -300 * time.Microsecond, Upper: -100 * time.Microsecond}
		gateCase := PerfGateCase{
			Dataset: cohortCase.dataset, Name: cohortCase.name, Backend: ModePostgresSQL, Tier: "normal", QualificationSplit: cohortCase.split, TimingGated: true,
			Rounds: 10, BaselineSamples: 500, CandidateSamples: 500, BaselineStatus: string(StatusOK), CandidateStatus: string(StatusOK),
			MedianRatio: RatioInterval{Estimate: 0.5, Lower: 0.4, Upper: 0.6}, P95Ratio: &p95Ratio,
			MedianSaving: &saving, MedianChange: &change, P95Change: &p95Change,
			P50NoiseRatio: minimumTimingNoiseRatio, P50NoiseAbsolute: minimumTimingNoiseAbsolute,
			P95NoiseRatio: minimumTimingNoiseRatio, P95NoiseAbsolute: minimumTimingNoiseAbsolute, Passed: true,
			CandidateRuntimeReceiptChains: promotionTestReceiptChains(identity.Candidate, fmt.Sprintf("candidate-%d", index), 500),
		}
		if index == 0 {
			gateCase.MaterialityRatio = &materialityRatio
			gateCase.MaterialityAbsolute = &materialityAbsolute
		}
		report.Cases = append(report.Cases, gateCase)
	}
	return promotionPerfGateReport{PerfGateReport: report, PromotionIdentity: identity}
}

type promotionTestCohortCase struct {
	dataset string
	name    string
	split   string
}

func promotionTestPerformanceCohort(candidate string) []promotionTestCohortCase {
	switch candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		cohort := make([]promotionTestCohortCase, 0, len(spI1CanonicalCases))
		for _, gateCase := range spI1CanonicalCases {
			cohort = append(cohort, promotionTestCohortCase{dataset: gateCase.dataset, name: gateCase.name, split: gateCase.split})
		}
		return cohort
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		cohort := make([]promotionTestCohortCase, 0, len(spI2CanonicalCases))
		for _, gateCase := range spI2CanonicalCases {
			cohort = append(cohort, promotionTestCohortCase{dataset: gateCase.dataset, name: gateCase.name, split: gateCase.split})
		}
		return cohort
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1):
		return []promotionTestCohortCase{
			{dataset: "orientation-training", name: "training", split: "training"},
			{dataset: "orientation-holdout", name: "holdout", split: "holdout"},
		}
	case string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		cohort := make([]promotionTestCohortCase, 0, len(orientationV2CanonicalCases))
		for _, gateCase := range orientationV2CanonicalCases {
			cohort = append(cohort, promotionTestCohortCase{
				dataset: gateCase.dataset,
				name:    gateCase.name,
				split:   gateCase.split,
			})
		}
		return cohort
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return []promotionTestCohortCase{
			{dataset: "fixture", name: "training", split: "training"},
			{dataset: "fixture", name: "holdout", split: "holdout"},
		}
	default:
		return nil
	}
}

func promotionTestReceiptChains(candidate, prefix string, count int) [][]RuntimeReceiptEvent {
	runtimeIdentity := candidate
	if mapped, supported := operationalCandidateRuntimeIdentity(candidate); supported {
		runtimeIdentity = mapped
	}
	chains := make([][]RuntimeReceiptEvent, 0, count)
	for index := 0; index < count; index++ {
		invocation := fmt.Sprintf("%s-%d", prefix, index)
		chains = append(chains, []RuntimeReceiptEvent{{
			InvocationID: invocation, Ordinal: 1, RuntimeIdentity: runtimeIdentity, RuntimeBranch: promotionTestReceiptBranch(candidate, runtimeIdentity),
		}})
	}
	return chains
}

func promotionTestReceiptBranch(candidate, runtimeIdentity string) string {
	switch candidate {
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return "inline_predecessor_dag"
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return "inline_canonical_witness"
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return "inline_canonical_distance"
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1), string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		if runtimeIdentity == string(optimize.ExpansionSearchStepwiseForward) {
			return "exact_forward_incumbent"
		}
		return "suffix_seeded_reverse"
	default:
		return "selected"
	}
}

func promotionTestWorkloadSHA256(gateCase promotionTestCohortCase) string {
	return sqlFingerprint(gateCase.dataset + "\x00" + gateCase.name)
}

func promotionTestOrientationGate(baseline, observed string, samples int) OrientationLatencyGate {
	return OrientationLatencyGate{
		BaselineIdentity: baseline, ObservedIdentity: observed, BaselineSamples: samples, ObservedSamples: samples,
		Ratio: RatioInterval{Estimate: 1, Lower: 0.99, Upper: 1.01}, AbsoluteChange: DurationInterval{},
		RatioUpperLimit: 1.10, AbsoluteFloor: 100 * time.Microsecond, Passed: true,
	}
}

func promotionTestConfirmationMetric(classification string) ConfirmationMetric {
	return ConfirmationMetric{
		Ratio: RatioInterval{Estimate: 1, Lower: 0.99, Upper: 1.01}, AbsoluteChange: DurationInterval{},
		NoiseRatio: 0.05, NoiseAbsolute: 100 * time.Microsecond, Classification: classification,
	}
}

func promotionTestNativeAASHA256(identity PromotionEvidenceIdentity) string {
	raw, err := json.Marshal(promotionTestAAResolutionReport(identity))
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func promotionTestDeclarationSHA256(candidate string) string {
	switch candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		cohort, _ := canonicalSPI1Cohort()
		return cohort.declarationSHA256
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		cohort, _ := canonicalSPI2Cohort()
		return cohort.declarationSHA256
	case string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		cohort, _ := canonicalOrientationV2Cohort()
		return cohort.declarationSHA256
	default:
		return strings.Repeat("8", 64)
	}
}

func TestTopologyFixedSuffixBucketUsesDriverStructuralContract(t *testing.T) {
	shape := pgdriver.TraversalShape{
		Version:           pgdriver.TraversalFixedSuffixShapeVersion,
		Family:            "fixed_suffix_expansion",
		Direction:         "outbound",
		ObservationMode:   string(optimize.ExpansionSearchObservationFullPath),
		MinimumDepth:      0,
		MaximumDepth:      16,
		SuffixLength:      3,
		CandidateStrategy: string(optimize.ExpansionSearchSuffixSeededReverse),
	}
	shape.Fingerprint = pgdriver.TraversalShapeFingerprint(shape)
	manifest := PromotionManifest{
		Version:           topologyPromotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1),
		SelectorVersion:   string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1),
		ExecutionBoundary: "transaction_retry",
		TopologyThresholds: map[string]int64{
			"maximum_edge_to_node_ratio_per_mille": 1000,
		},
	}
	bucket := PromotionBucket{
		Name:                   "fixed-suffix",
		QuerySHA256:            []string{strings.Repeat("a", 64)},
		QualificationSplit:     []string{"training", "holdout"},
		Direction:              shape.Direction,
		ObservationMode:        shape.ObservationMode,
		MinimumDepth:           int(shape.MinimumDepth),
		MaximumDepth:           int(shape.MaximumDepth),
		SuffixLength:           shape.SuffixLength,
		CandidateStrategy:      shape.CandidateStrategy,
		StructuralShapeVersion: shape.Version,
		StructuralFamily:       shape.Family,
		StructuralShapeSHA256:  shape.Fingerprint,
	}
	bucket.SQLTemplateSHA256 = pgdriver.TraversalSQLTemplateSHA256(manifest.Candidate, manifest.SelectorVersion, manifest.ExecutionBoundary, shape)
	require.NoError(t, validateTopologyFixedSuffixBucket(manifest, bucket))

	bucket.SuffixLength = 2
	require.ErrorContains(t, validateTopologyFixedSuffixBucket(manifest, bucket), "classifier envelope")
	bucket.SuffixLength = 3
	bucket.SQLTemplateSHA256 = strings.Repeat("b", 64)
	require.ErrorContains(t, validateTopologyFixedSuffixBucket(manifest, bucket), "SQL template digest")
}

// TestVerifyPromotionManifestRequiresExactOrientationProbeContract verifies verify promotion manifest requires exact orientation probe contract behavior.
func TestVerifyPromotionManifestRequiresExactOrientationProbeContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		SelectorVersion:   "orientation-probe-v1",
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:      "deadbeef",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "fixed-suffix",
			QuerySHA256:           []string{pgdriver.TraversalPolicyQuerySHA256(operationalTestOrientationCypher)},
			Direction:             "outbound",
			ObservationMode:       "endpoint_ids",
			MinimumDepth:          0,
			MaximumDepth:          16,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}

	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, base))
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "confirmation: orientation-probe-v1 promotion is disabled because its report schema cannot bind source, corpus, and frozen cohort identity")

	tests := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*PromotionManifest)
		// reason retains the reason while anonymous record is assembled or evaluated.
		reason string
	}{
		{
			name:   "boundary",
			mutate: func(manifest *PromotionManifest) { manifest.ExecutionBoundary = "inline_statement" },
			reason: "orientation-probe-v1 requires the guarded_dual_arm production boundary",
		},
		{
			name:   "fallback",
			mutate: func(manifest *PromotionManifest) { manifest.FallbackExecutor = "EXPANSION-SUFFIX-SEEDED-REVERSE" },
			reason: "orientation-probe-v1 requires EXPANSION-STEPWISE-FORWARD as its exact fallback",
		},
		{
			name:   "extra cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["extra_limit"] = 1 },
			reason: "orientation-probe-v1 requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		{
			name:   "missing cap",
			mutate: func(manifest *PromotionManifest) { delete(manifest.Caps, "root_row_limit") },
			reason: "orientation-probe-v1 requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		{
			name:   "root cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["root_row_limit"]-- },
			reason: "orientation-probe-v1 cap root_row_limit must equal 512",
		},
		{
			name:   "reverse seed cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["reverse_seed_row_limit"]-- },
			reason: "orientation-probe-v1 cap reverse_seed_row_limit must equal 512",
		},
		{
			name:   "directional degree cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["directional_degree_row_limit"]-- },
			reason: "orientation-probe-v1 cap directional_degree_row_limit must equal 16384",
		},
		{
			name:   "state cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["state_limit"]-- },
			reason: "orientation-probe-v1 cap state_limit must equal 4096",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			test.mutate(&manifest)
			verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, verification.Reasons, test.reason)
		})
	}
}

// TestVerifyPromotionManifestRejectsTerminalOrientationProbeV2Contract verifies
// structurally valid v2 evidence remains readable but cannot authorize the
// terminal policy generation after its immutable training overhead gate failed.
func TestVerifyPromotionManifestRejectsTerminalOrientationProbeV2Contract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		SelectorVersion:   string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:      "deadbeef",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "fixed-suffix-v2",
			QuerySHA256:           []string{pgdriver.TraversalPolicyQuerySHA256(operationalTestOrientationCypher)},
			Direction:             "outbound",
			ObservationMode:       "endpoint_ids",
			MinimumDepth:          0,
			MaximumDepth:          16,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "orientation-probe-v2 is terminally rejected because its immutable training overhead gate failed; authorization requires a new policy generation")

	manifest.SelectorVersion = string(optimize.ExpansionSearchPolicyOrientationProbeV1)
	verification, err = verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "orientation-probe-v2 requires the same selector version")
}

// TestVerifyPromotionManifestRequiresStaticV6CanonicalInboundContract verifies verify promotion manifest requires static v6 canonical inbound contract behavior.
func TestVerifyPromotionManifestRequiresStaticV6CanonicalInboundContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		SelectorVersion:   optimize.ShortestPathSelectorStaticV6,
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ShortestPathExecutorS4CanonicalWitness),
		SourceCommit:      "deadbeef",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      spI1FullCorpusSHA256,
		Caps:              map[string]int64{"state_limit": 100_000, "predecessor_limit": 100_000, "enumeration_limit": 100_000, "output_bytes_limit": 64 << 20},
		Buckets: []PromotionBucket{{
			Name:                  "canonical-inbound-depth64",
			QuerySHA256:           []string{spI1QuerySHA256},
			Direction:             "inbound",
			ObservationMode:       "one_path",
			MinimumDepth:          1,
			MaximumDepth:          64,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}

	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, base))
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)

	tests := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*PromotionManifest)
		// reason retains the reason while anonymous record is assembled or evaluated.
		reason string
	}{
		{
			name:   "selector",
			mutate: func(manifest *PromotionManifest) { manifest.SelectorVersion = "sp-static-v5-contained" },
			reason: "SP-I1 canonical witness requires selector sp-static-v6",
		},
		{
			name:   "outbound",
			mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].Direction = "outbound" },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		{
			name:   "maximum",
			mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].MaximumDepth = 63 },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		{
			name:   "kinds",
			mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].RelationshipKindCount = 2 },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			manifest.Buckets = clonePromotionBuckets(base.Buckets)
			test.mutate(&manifest)
			verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, verification.Reasons, test.reason)
		})
	}
}

// TestVerifyPromotionManifestRequiresCompleteImmutableEvidenceClosure verifies verify promotion manifest requires complete immutable evidence closure behavior.
func TestVerifyPromotionManifestRequiresCompleteImmutableEvidenceClosure(t *testing.T) {
	directory := t.TempDir()
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := PromotionManifest{
		Version:                       promotionManifestVersion,
		Candidate:                     string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion:               optimize.ShortestPathSelectorStaticV8HiddenFanIn,
		ExecutionBoundary:             "guarded_dual_arm",
		FallbackExecutor:              "SP-S4-C-D",
		SourceCommit:                  "deadbeef",
		SourceSHA256:                  digest,
		BinarySHA256:                  digest,
		CorpusSHA256:                  spI2FullCorpusSHA256,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL),
		Caps:                          spI2PromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "deep-inbound-distance",
			QuerySHA256:           []string{spI2QuerySHA256},
			Direction:             "inbound",
			ObservationMode:       "distance",
			MinimumDepth:          1,
			MaximumDepth:          16,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	evidence := map[string]PromotionEvidenceReference{}
	for _, role := range requiredPromotionEvidenceRoles {
		document := passingPromotionEvidenceDocument(t, manifest, role)
		raw, err := json.Marshal(document)
		require.NoError(t, err)
		path := role + ".json"
		require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
		digest := sha256.Sum256(raw)
		evidence[role] = PromotionEvidenceReference{
			Path:   path,
			SHA256: hex.EncodeToString(digest[:]),
		}
	}
	manifest.Evidence = evidence
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "promotion.json")
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)
	require.NotEmpty(t, verification.ManifestSHA256)

	delete(manifest.Evidence, "operational")
	raw, err = json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))
	verification, err = verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "required evidence role operational is missing")
}

// TestVerifyPromotionEvidenceRejectsEveryCrossBindingMismatch verifies verify promotion evidence rejects every cross binding mismatch behavior.
func TestVerifyPromotionEvidenceRejectsEveryCrossBindingMismatch(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion:   optimize.ShortestPathSelectorStaticV8HiddenFanIn,
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ShortestPathExecutorS4CanonicalDistance),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              spI2PromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "bucket",
			QuerySHA256:           []string{digest},
			Direction:             "outbound",
			ObservationMode:       "one_path",
			MinimumDepth:          1,
			MaximumDepth:          4,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	tests := map[string]func(*PromotionEvidenceIdentity){
		"candidate":       func(identity *PromotionEvidenceIdentity) { identity.Candidate = "candidate-b" },
		"selector":        func(identity *PromotionEvidenceIdentity) { identity.SelectorVersion = "other-selector" },
		"boundary":        func(identity *PromotionEvidenceIdentity) { identity.ExecutionBoundary = "stored_helper" },
		"fallback":        func(identity *PromotionEvidenceIdentity) { identity.FallbackExecutor = "other-incumbent" },
		"source commit":   func(identity *PromotionEvidenceIdentity) { identity.SourceCommit = "other-commit" },
		"source digest":   func(identity *PromotionEvidenceIdentity) { identity.SourceSHA256 = strings.Repeat("1", 64) },
		"binary digest":   func(identity *PromotionEvidenceIdentity) { identity.BinarySHA256 = strings.Repeat("2", 64) },
		"corpus digest":   func(identity *PromotionEvidenceIdentity) { identity.CorpusSHA256 = strings.Repeat("3", 64) },
		"cap":             func(identity *PromotionEvidenceIdentity) { identity.Caps["state_limit"]++ },
		"bucket envelope": func(identity *PromotionEvidenceIdentity) { identity.Buckets[0].MaximumDepth = 8 },
		"query cohort": func(identity *PromotionEvidenceIdentity) {
			identity.Buckets[0].QuerySHA256[0] = strings.Repeat("4", 64)
		},
		"qualification split": func(identity *PromotionEvidenceIdentity) {
			identity.Buckets[0].QualificationSplit = []string{"training"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			wrong := promotionEvidenceIdentity(manifest)
			mutate(&wrong)
			document := passingPromotionEvidenceDocument(t, manifest, "resource").(promotionResourceReport)
			document.PromotionIdentity = wrong
			raw, err := json.Marshal(document)
			require.NoError(t, err)
			path := "resource.json"
			require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
			sum := sha256.Sum256(raw)
			reference := PromotionEvidenceReference{
				Path:   path,
				SHA256: hex.EncodeToString(sum[:]),
			}
			err = verifyPromotionEvidence(directory, "resource", reference, promotionEvidenceIdentity(manifest))
			require.EqualError(t, err, "promotion identity does not match manifest")
		})
	}
}

func TestVerifyPromotionEvidenceStrictlyValidatesVersionedGateRoles(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256, Caps: spI2PromotionCaps(),
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL),
		Buckets: []PromotionBucket{{
			Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance",
			MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}
	identity := promotionEvidenceIdentity(manifest)
	for _, role := range requiredPromotionEvidenceRoles {
		t.Run(role+" valid", func(t *testing.T) {
			reference, directory := writePromotionEvidenceReference(t, role, passingPromotionEvidenceDocument(t, manifest, role))
			require.NoError(t, verifyPromotionEvidence(directory, role, reference, identity))
		})
		t.Run(role+" minimal forgery", func(t *testing.T) {
			reference, directory := writePromotionEvidenceReference(t, role, map[string]any{"passed": true, "promotion_identity": identity})
			err := verifyPromotionEvidence(directory, role, reference, identity)
			require.Error(t, err)
			require.NotErrorIs(t, err, os.ErrNotExist)
		})
		t.Run(role+" unknown field", func(t *testing.T) {
			raw, err := json.Marshal(passingPromotionEvidenceDocument(t, manifest, role))
			require.NoError(t, err)
			var document map[string]any
			require.NoError(t, json.Unmarshal(raw, &document))
			document["unrecognized_proof"] = true
			reference, directory := writePromotionEvidenceReference(t, role, document)
			require.ErrorContains(t, verifyPromotionEvidence(directory, role, reference, identity), "unknown field")
		})
	}
}

func TestVerifyPromotionEvidenceRejectsInternallyIncompleteGateReports(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256, Caps: spI2PromotionCaps(),
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL),
		Buckets:                       []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}
	identity := promotionEvidenceIdentity(manifest)
	tests := []struct {
		name   string
		role   string
		mutate func(any) any
		reason string
	}{
		{name: "resource version", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Version--
			return rebindPromotionTestResourceReport(report)
		}, reason: "resource report version"},
		{name: "A/A embedded producer mismatch", role: "aa", mutate: func(value any) any {
			report := value.(promotionAAResolutionReport)
			report.NativeReportSHA256 = strings.Repeat("f", 64)
			return report
		}, reason: "does not match its embedded bytes"},
		{name: "A/A chronology artifact drift", role: "aa", mutate: func(value any) any {
			report := value.(promotionAAResolutionReport)
			report.PhysicalChronology.ArtifactSHA256 = strings.Repeat("f", 64)
			return rebindPromotionTestAAReport(report)
		}, reason: "artifact-bound physical chronology"},
		{name: "confirmation forged material pass", role: "confirmation", mutate: func(value any) any {
			report := value.(promotionSPI2QualificationReport)
			report.Cases[0].MedianRatio = RatioInterval{Estimate: 1, Lower: 1, Upper: 1}
			report.Cases[0].MedianSaving = DurationInterval{}
			return report
		}, reason: "incomplete or contradictory evidence"},
		{name: "performance seed drift", role: "performance", mutate: func(value any) any {
			report := value.(promotionPerfGateReport)
			report.Seed++
			return report
		}, reason: "frozen settings"},
		{name: "performance missing noise floor", role: "performance", mutate: func(value any) any {
			report := value.(promotionPerfGateReport)
			report.Cases[0].P50NoiseRatio = 0.01
			return report
		}, reason: "minimum finite noise floors"},
		{name: "performance forged p50 pass", role: "performance", mutate: func(value any) any {
			report := value.(promotionPerfGateReport)
			report.Cases[0].MedianRatio = RatioInterval{Estimate: 1.2, Lower: 1.1, Upper: 1.3}
			report.Cases[0].MedianChange = &DurationInterval{Estimate: 300 * time.Microsecond, Lower: 200 * time.Microsecond, Upper: 400 * time.Microsecond}
			report.Cases[0].MedianSaving = &DurationInterval{Estimate: -300 * time.Microsecond, Lower: -400 * time.Microsecond, Upper: -200 * time.Microsecond}
			return report
		}, reason: "noise-adjusted p50 regression"},
		{name: "performance receipt terminal drift", role: "performance", mutate: func(value any) any {
			report := value.(promotionPerfGateReport)
			report.Cases[0].CandidateRuntimeReceiptChains[0][0].RuntimeIdentity = "forged-runtime"
			return report
		}, reason: "terminal identity differs"},
		{name: "performance receipt branch forgery", role: "performance", mutate: func(value any) any {
			report := value.(promotionPerfGateReport)
			report.Cases[0].CandidateRuntimeReceiptChains[0][0].RuntimeBranch = "invented-authorization-branch"
			return report
		}, reason: "is not authorized"},
		{name: "resource empty", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases = nil
			return rebindPromotionTestResourceReport(report)
		}, reason: "resource report has no cases"},
		{name: "resource contradictory case", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases[0].Passed = false
			return rebindPromotionTestResourceReport(report)
		}, reason: "contradicts case"},
		{name: "resource observed over limit", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases[0].NumericObserved["state_rows"] = report.Cases[0].NumericLimits["state_rows"] + 1
			return rebindPromotionTestResourceReport(report)
		}, reason: "exceeds limit"},
		{name: "resource cap drift", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases[0].NumericLimits["state_rows"]--
			return rebindPromotionTestResourceReport(report)
		}, reason: "exact numeric limits"},
		{name: "resource missing receipts", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases[0].RuntimeReceiptChains = report.Cases[0].RuntimeReceiptChains[:49]
			return rebindPromotionTestResourceReport(report)
		}, reason: "lacks at least 50"},
		{name: "resource receipt terminal drift", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.Cases[0].RuntimeReceiptChains[0][0].RuntimeIdentity = "forged-runtime"
			return rebindPromotionTestResourceReport(report)
		}, reason: "terminal identity differs"},
		{name: "resource native digest drift", role: "resource", mutate: func(value any) any {
			report := value.(promotionResourceReport)
			report.NativeReportSHA256 = strings.Repeat("f", 64)
			return report
		}, reason: "does not match its embedded bytes"},
		{name: "closure version", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Version++
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "reference-closure report version"},
		{name: "closure empty", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases = nil
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "reference-closure report has no cases"},
		{name: "closure insufficient samples", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].ProductionSamples = 1
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "lacks the required rounds or samples"},
		{name: "closure forged pass", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].MedianRatio.Upper = 2
			report.Cases[0].MedianRatio.Estimate = 1.5
			report.Cases[0].AbsoluteGapUpper = time.Second
			report.Cases[0].MedianChange.Upper = time.Second
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "contradicts case"},
		{name: "closure seed drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Seed++
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "invalid frozen settings"},
		{name: "closure bootstrap drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.BootstrapCount--
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "invalid frozen settings"},
		{name: "closure candidate drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Candidate = "forged-candidate"
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "differs from the manifest"},
		{name: "closure source drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.SourceCommit = "forged-commit"
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "differs from the manifest"},
		{name: "closure query drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].QuerySHA256 = strings.Repeat("f", 64)
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "incomplete case identity"},
		{name: "closure ratio threshold drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].RatioUpperLimit = 2
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "invalid statistical evidence"},
		{name: "closure absolute threshold drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].AbsoluteFloor++
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "invalid statistical evidence"},
		{name: "closure receipt count drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].ProductionRuntimeReceiptChains = report.Cases[0].ProductionRuntimeReceiptChains[1:]
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "receipt count differs"},
		{name: "closure receipt terminal drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.Cases[0].ProductionRuntimeReceiptChains[0][0].RuntimeIdentity = "forged-runtime"
			return rebindPromotionTestReferenceClosureReport(report)
		}, reason: "terminal identity differs"},
		{name: "closure native digest drift", role: "reference_closure", mutate: func(value any) any {
			report := value.(promotionReferenceClosureReport)
			report.NativeReportSHA256 = strings.Repeat("f", 64)
			return report
		}, reason: "does not match its embedded bytes"},
		{name: "operational version", role: "operational", mutate: func(value any) any { report := value.(OperationalGateReport); report.Version++; return report }, reason: "operational report version"},
		{name: "operational incomplete coverage", role: "operational", mutate: func(value any) any {
			report := value.(OperationalGateReport)
			report.Coverage.CancellationReplay = false
			return report
		}, reason: "coverage differs from recomputed input"},
		{name: "operational missing matrix record", role: "operational", mutate: func(value any) any {
			report := value.(OperationalGateReport)
			report.Records = report.Records[1:]
			return report
		}, reason: "record decisions differ from recomputed input"},
		{name: "operational duplicate matrix cell", role: "operational", mutate: func(value any) any {
			report := value.(OperationalGateReport)
			report.Records[1].PoolSize = report.Records[0].PoolSize
			report.Records[1].Concurrency = report.Records[0].Concurrency
			report.Records[1].PlanCacheMode = report.Records[0].PlanCacheMode
			return report
		}, reason: "record decisions differ from recomputed input"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := test.mutate(passingPromotionEvidenceDocument(t, manifest, test.role))
			reference, directory := writePromotionEvidenceReference(t, test.role, document)
			require.ErrorContains(t, verifyPromotionEvidence(directory, test.role, reference, identity), test.reason)
		})
	}
}

func TestVerifyPromotionGenericConfirmationRecomputesClassification(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorASPI1DAG),
		SelectorVersion: "asp-static-v1", ExecutionBoundary: "guarded_dual_arm", FallbackExecutor: "ASP-A1-DAG",
		SourceCommit: "deadbeef", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: map[string]int64{"state_limit": 1},
		Buckets: []PromotionBucket{{Name: "asp", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
	}
	identity := promotionEvidenceIdentity(manifest)
	report := passingPromotionGenericConfirmation(identity)
	report.Cases[0].P95.Ratio = RatioInterval{Estimate: 1.2, Lower: 1.1, Upper: 1.3}
	report.Cases[0].P95.AbsoluteChange = DurationInterval{Estimate: 300 * time.Microsecond, Lower: 200 * time.Microsecond, Upper: 400 * time.Microsecond}

	reference, directory := writePromotionEvidenceReference(t, "confirmation", report)
	require.ErrorContains(t, verifyPromotionEvidence(directory, "confirmation", reference, identity), "classification contradicts")
}

func TestVerifyPromotionOrientationV2RequiresCanonicalCohortAndRuntimeTuple(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		SelectorVersion: string(optimize.ExpansionSearchPolicyOrientationProbeV2), ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ExpansionSearchStepwiseForward), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: orientationPromotionCaps(),
		Buckets: []PromotionBucket{{Name: "orientation", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
	}
	identity := promotionEvidenceIdentity(manifest)

	t.Run("case substitution", func(t *testing.T) {
		report := passingPromotionOrientationV2Confirmation(identity)
		report.Cases[0].Name = "invented-holdout"
		reference, directory := writePromotionEvidenceReference(t, "confirmation", report)
		require.ErrorContains(t, verifyPromotionEvidence(directory, "confirmation", reference, identity), "outside the frozen V3 corpus")
	})
	t.Run("runtime branch forgery", func(t *testing.T) {
		report := passingPromotionOrientationV2Confirmation(identity)
		report.Cases[0].GuardedRuntimeBranch = "invented"
		reference, directory := writePromotionEvidenceReference(t, "confirmation", report)
		require.ErrorContains(t, verifyPromotionEvidence(directory, "confirmation", reference, identity), "frozen runtime qualification evidence")
	})
}

func TestPromotionReceiptTerminalsAreCandidateSpecific(t *testing.T) {
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	policy := string(optimize.ExpansionSearchPolicyOrientationProbeV2)
	require.True(t, promotionReceiptTerminalAllowed(policy, forward))
	require.True(t, promotionReceiptTerminalAllowed(policy, reverse))
	require.False(t, promotionReceiptTerminalAllowed(policy, policy))
	require.True(t, promotionReceiptTerminalAllowed(string(optimize.ShortestPathExecutorI2GuardedDistance), string(optimize.ShortestPathExecutorI2GuardedDistance)))
	require.False(t, promotionReceiptTerminalAllowed(string(optimize.ShortestPathExecutorI2GuardedDistance), forward))
	require.True(t, promotionReceiptBranchAllowed(string(optimize.ShortestPathExecutorI2GuardedDistance), string(optimize.ShortestPathExecutorI2GuardedDistance), "inline_canonical_distance"))
	require.False(t, promotionReceiptBranchAllowed(string(optimize.ShortestPathExecutorI2GuardedDistance), string(optimize.ShortestPathExecutorI2GuardedDistance), "invented"))
	require.True(t, promotionReceiptBranchAllowed(string(optimize.ShortestPathExecutorASPI1DAG), string(optimize.ShortestPathExecutorASPI1DAG), "inline_no_path"))
}

func TestVerifyPromotionEvidenceClosureRequiresExactCrossRoleCohort(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifestPath := writePromotionManifestWithPassingEvidence(t, PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256, Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	})
	manifestRaw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	var manifest PromotionManifest
	require.NoError(t, json.Unmarshal(manifestRaw, &manifest))
	performancePath := filepath.Join(filepath.Dir(manifestPath), manifest.Evidence["performance"].Path)
	performanceRaw, err := os.ReadFile(performancePath)
	require.NoError(t, err)
	var performance promotionPerfGateReport
	require.NoError(t, json.Unmarshal(performanceRaw, &performance))
	performance.Cases[0].Name += "-substituted"
	performanceRaw, err = json.Marshal(performance)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(performancePath, performanceRaw, 0o600))
	performanceDigest := sha256.Sum256(performanceRaw)
	manifest.Evidence["performance"] = PromotionEvidenceReference{Path: manifest.Evidence["performance"].Path, SHA256: hex.EncodeToString(performanceDigest[:])}
	manifestRaw, err = json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "evidence closure: performance and confirmation reports do not contain the same exact promotion cohort")
}

func TestVerifyPromotionEvidenceClosureBindsNativeResourceAndCandidateArtifacts(t *testing.T) {
	newManifest := func(t *testing.T) (string, PromotionManifest) {
		t.Helper()
		digest := strings.Repeat("a", 64)
		path := writePromotionManifestWithPassingEvidence(t, PromotionManifest{
			Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
			SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
			FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
			SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256, Caps: spI2PromotionCaps(),
			Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
		})
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var manifest PromotionManifest
		require.NoError(t, json.Unmarshal(raw, &manifest))
		return path, manifest
	}

	t.Run("confirmation native resource mismatch", func(t *testing.T) {
		path, manifest := newManifest(t)
		var report promotionSPI2QualificationReport
		readPromotionTestEvidence(t, path, manifest, "confirmation", &report)
		report.ResourceReportSHA256 = strings.Repeat("f", 64)
		rewritePromotionTestEvidence(t, path, &manifest, "confirmation", report)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, verification.Reasons, "evidence closure: SP-I2 confirmation does not use the manifest's exact native resource report")
	})

	t.Run("resource candidate artifact mismatch", func(t *testing.T) {
		path, manifest := newManifest(t)
		var report promotionResourceReport
		readPromotionTestEvidence(t, path, manifest, "resource", &report)
		report.ArtifactSHA256 = strings.Repeat("f", 64)
		report = rebindPromotionTestResourceReport(report)
		rewritePromotionTestEvidence(t, path, &manifest, "resource", report)

		// Keep the confirmation-to-native-resource binding intact so this
		// mutation reaches the independent candidate-artifact closure check.
		var confirmation promotionSPI2QualificationReport
		readPromotionTestEvidence(t, path, manifest, "confirmation", &confirmation)
		confirmation.ResourceReportSHA256 = report.NativeReportSHA256
		rewritePromotionTestEvidence(t, path, &manifest, "confirmation", confirmation)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, verification.Reasons, "evidence closure: confirmation, performance, and resource reports do not bind the same exact candidate artifact")
	})

	t.Run("reference is an independently bound capture", func(t *testing.T) {
		path, manifest := newManifest(t)
		var report promotionReferenceClosureReport
		readPromotionTestEvidence(t, path, manifest, "reference_closure", &report)
		report.ArtifactSHA256 = strings.Repeat("9", 64)
		report = rebindPromotionTestReferenceClosureReport(report)
		rewritePromotionTestEvidence(t, path, &manifest, "reference_closure", report)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.True(t, verification.Passed, verification.Reasons)
	})

	t.Run("reference cohort substitution", func(t *testing.T) {
		path, manifest := newManifest(t)
		var report promotionReferenceClosureReport
		readPromotionTestEvidence(t, path, manifest, "reference_closure", &report)
		report.Cases[0].Name += "-substituted"
		report = rebindPromotionTestReferenceClosureReport(report)
		rewritePromotionTestEvidence(t, path, &manifest, "reference_closure", report)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, verification.Reasons, "evidence closure: reference-closure and confirmation reports do not contain the same exact promotion cohort")
	})

	t.Run("reference workload differs from native A/A", func(t *testing.T) {
		path, manifest := newManifest(t)
		var report promotionReferenceClosureReport
		readPromotionTestEvidence(t, path, manifest, "reference_closure", &report)
		report.Cases[0].WorkloadSHA256 = strings.Repeat("f", 64)
		report = rebindPromotionTestReferenceClosureReport(report)
		rewritePromotionTestEvidence(t, path, &manifest, "reference_closure", report)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, strings.Join(verification.Reasons, "\n"), "evidence closure: reference-closure workload identity differs from native A/A")
	})

	t.Run("native A/A omits a promotion workload", func(t *testing.T) {
		path, manifest := newManifest(t)
		var aa promotionAAResolutionReport
		readPromotionTestEvidence(t, path, manifest, "aa", &aa)
		aa.Cases = aa.Cases[1:]
		aa = rebindPromotionTestAAReport(aa)
		rewritePromotionTestEvidence(t, path, &manifest, "aa", aa)

		var performance promotionPerfGateReport
		readPromotionTestEvidence(t, path, manifest, "performance", &performance)
		performance.AAReportSHA256 = aa.NativeReportSHA256
		rewritePromotionTestEvidence(t, path, &manifest, "performance", performance)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, strings.Join(verification.Reasons, "\n"), "evidence closure: native A/A report must contain exactly one PostgreSQL workload identity")
	})

	t.Run("resource omits a measured round", func(t *testing.T) {
		path, manifest := newManifest(t)
		var resource promotionResourceReport
		readPromotionTestEvidence(t, path, manifest, "resource", &resource)
		resource.Cases = resource.Cases[1:]
		resource = rebindPromotionTestResourceReport(resource)
		rewritePromotionTestEvidence(t, path, &manifest, "resource", resource)

		var confirmation promotionSPI2QualificationReport
		readPromotionTestEvidence(t, path, manifest, "confirmation", &confirmation)
		confirmation.ResourceReportSHA256 = resource.NativeReportSHA256
		rewritePromotionTestEvidence(t, path, &manifest, "confirmation", confirmation)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, strings.Join(verification.Reasons, "\n"), "evidence closure: resource report must contain exactly 10 rounds")
	})

	t.Run("resource substitutes a candidate receipt", func(t *testing.T) {
		path, manifest := newManifest(t)
		var resource promotionResourceReport
		readPromotionTestEvidence(t, path, manifest, "resource", &resource)
		resource.Cases[0].RuntimeReceiptChains[0][0].InvocationID = "substituted-invocation"
		resource = rebindPromotionTestResourceReport(resource)
		rewritePromotionTestEvidence(t, path, &manifest, "resource", resource)

		var confirmation promotionSPI2QualificationReport
		readPromotionTestEvidence(t, path, manifest, "confirmation", &confirmation)
		confirmation.ResourceReportSHA256 = resource.NativeReportSHA256
		rewritePromotionTestEvidence(t, path, &manifest, "confirmation", confirmation)

		verification, err := verifyPromotionManifest(path)
		require.NoError(t, err)
		require.Contains(t, strings.Join(verification.Reasons, "\n"), "evidence closure: resource and performance reports do not bind the same exact candidate receipt chains")
	})
}

func readPromotionTestEvidence(t *testing.T, manifestPath string, manifest PromotionManifest, role string, destination any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(manifestPath), manifest.Evidence[role].Path))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, destination))
}

func rewritePromotionTestEvidence(t *testing.T, manifestPath string, manifest *PromotionManifest, role string, document any) {
	t.Helper()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	reference := manifest.Evidence[role]
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(manifestPath), reference.Path), raw, 0o600))
	digest := sha256.Sum256(raw)
	reference.SHA256 = hex.EncodeToString(digest[:])
	manifest.Evidence[role] = reference
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
}

func rebindPromotionTestAAReport(report promotionAAResolutionReport) promotionAAResolutionReport {
	nativeRaw, err := json.Marshal(report.AAResolutionReport)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	report.NativeReportSHA256 = hex.EncodeToString(digest[:])
	report.NativeReportBase64 = base64.StdEncoding.EncodeToString(nativeRaw)
	return report
}

func rebindPromotionTestResourceReport(report promotionResourceReport) promotionResourceReport {
	nativeRaw, err := json.Marshal(report.ResourceGateReport)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	report.NativeReportSHA256 = hex.EncodeToString(digest[:])
	report.NativeReportBase64 = base64.StdEncoding.EncodeToString(nativeRaw)
	return report
}

func rebindPromotionTestReferenceClosureReport(report promotionReferenceClosureReport) promotionReferenceClosureReport {
	nativeRaw, err := json.Marshal(report.ReferenceClosureReport)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(nativeRaw)
	report.NativeReportSHA256 = hex.EncodeToString(digest[:])
	report.NativeReportBase64 = base64.StdEncoding.EncodeToString(nativeRaw)
	return report
}

func writePromotionEvidenceReference(t *testing.T, role string, document any) (PromotionEvidenceReference, string) {
	t.Helper()
	directory := t.TempDir()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	path := role + ".json"
	require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
	digest := sha256.Sum256(raw)
	return PromotionEvidenceReference{Path: path, SHA256: hex.EncodeToString(digest[:])}, directory
}

// TestBindPromotionEvidenceReportCopiesCompleteManifestIdentity verifies bind promotion evidence report copies complete manifest identity behavior.
func TestBindPromotionEvidenceReportCopiesCompleteManifestIdentity(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion:   optimize.ShortestPathSelectorStaticV8HiddenFanIn,
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ShortestPathExecutorS4CanonicalDistance),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              spI2PromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:               "bucket",
			QuerySHA256:        []string{digest},
			QualificationSplit: []string{"training", "holdout"},
		}},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "manifest.json")
	inputPath := filepath.Join(directory, "input.json")
	outputPath := filepath.Join(directory, "output.json")
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
	inputRaw, err := json.Marshal(passingPromotionEvidenceDocument(t, manifest, "resource").(promotionResourceReport).ResourceGateReport)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(inputPath, inputRaw, 0o600))
	require.NoError(t, bindPromotionEvidenceReport(manifestPath, "resource", inputPath, outputPath))

	boundRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var bound promotionResourceReport
	require.NoError(t, json.Unmarshal(boundRaw, &bound))
	require.True(t, bound.Passed)
	require.Equal(t, promotionEvidenceIdentity(manifest), bound.PromotionIdentity)
	nativeDigest := sha256.Sum256(inputRaw)
	require.Equal(t, hex.EncodeToString(nativeDigest[:]), bound.NativeReportSHA256)
	require.Equal(t, base64.StdEncoding.EncodeToString(inputRaw), bound.NativeReportBase64)
}

func TestBindPromotionAAEmbedsExactNativeProducerBytes(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	nativeRaw, err := json.Marshal(promotionTestAAResolutionReport(promotionEvidenceIdentity(manifest)))
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "manifest.json")
	inputPath := filepath.Join(directory, "aa-native.json")
	outputPath := filepath.Join(directory, "aa-bound.json")
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
	require.NoError(t, os.WriteFile(inputPath, nativeRaw, 0o600))
	require.NoError(t, bindPromotionEvidenceReport(manifestPath, "aa", inputPath, outputPath))

	boundRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var bound promotionAAResolutionReport
	require.NoError(t, json.Unmarshal(boundRaw, &bound))
	require.Equal(t, base64.StdEncoding.EncodeToString(nativeRaw), bound.NativeReportBase64)
	nativeDigest := sha256.Sum256(nativeRaw)
	require.Equal(t, hex.EncodeToString(nativeDigest[:]), bound.NativeReportSHA256)
	require.NoError(t, validatePromotionAAReport(boundRaw, promotionEvidenceIdentity(manifest)))
}

func TestBindPromotionReferenceClosureEmbedsExactNativeProducerBytes(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	nativeRaw, err := json.Marshal(promotionTestReferenceClosureReport(promotionEvidenceIdentity(manifest)))
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "manifest.json")
	inputPath := filepath.Join(directory, "reference-native.json")
	outputPath := filepath.Join(directory, "reference-bound.json")
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
	require.NoError(t, os.WriteFile(inputPath, nativeRaw, 0o600))
	require.NoError(t, bindPromotionEvidenceReport(manifestPath, "reference_closure", inputPath, outputPath))

	boundRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var bound promotionReferenceClosureReport
	require.NoError(t, json.Unmarshal(boundRaw, &bound))
	nativeDigest := sha256.Sum256(nativeRaw)
	require.Equal(t, hex.EncodeToString(nativeDigest[:]), bound.NativeReportSHA256)
	require.Equal(t, base64.StdEncoding.EncodeToString(nativeRaw), bound.NativeReportBase64)
	require.Equal(t, promotionEvidenceIdentity(manifest), bound.PromotionIdentity)
}

// TestVerifyPromotionManifestRejectsVersionOne verifies verify promotion manifest rejects version one behavior.
func TestVerifyPromotionManifestRejectsVersionOne(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":1}`), 0o600))
	verification, err := verifyPromotionManifest(path)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "manifest version must be 2, 3, or 4")
}

func TestPromotionManifestDecodingRejectsUnknownAndTrailingJSON(t *testing.T) {
	directory := t.TempDir()
	for name, raw := range map[string][]byte{
		"unknown":  []byte(`{"version":2,"invented_authorization":true}`),
		"trailing": []byte(`{"version":2}{"version":2}`),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(directory, name+".json")
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			_, err := verifyPromotionManifest(path)
			require.Error(t, err)
		})
	}
}

func TestPromotionManifestDecodingRejectsDuplicateJSONKeys(t *testing.T) {
	for name, raw := range map[string][]byte{
		"top level": []byte(`{"version":2,"version":2}`),
		"nested":    []byte(`{"version":2,"caps":{"state_limit":1,"state_limit":2}}`),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			_, err := verifyPromotionManifest(path)
			require.ErrorContains(t, err, "duplicate JSON object key")
		})
	}
}

func TestVerifyPromotionManifestRejectsNonExactAuthorizationSets(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: spI2FullCorpusSHA256,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}

	tests := map[string]struct {
		mutate func(*PromotionManifest)
		reason string
	}{
		"extra evidence role": {
			mutate: func(manifest *PromotionManifest) {
				manifest.Evidence["invented"] = PromotionEvidenceReference{Path: "invented.json", SHA256: digest}
			},
			reason: "unsupported evidence role invented is present",
		},
		"duplicate split": {
			mutate: func(manifest *PromotionManifest) {
				manifest.Buckets[0].QualificationSplit = []string{"training", "training", "holdout"}
			},
			reason: "must bind exactly one training and one holdout qualification split",
		},
		"extra split": {
			mutate: func(manifest *PromotionManifest) {
				manifest.Buckets[0].QualificationSplit = []string{"training", "holdout", "diagnostic"}
			},
			reason: "must bind exactly one training and one holdout qualification split",
		},
		"reordered split": {
			mutate: func(manifest *PromotionManifest) {
				manifest.Buckets[0].QualificationSplit = []string{"holdout", "training"}
			},
			reason: "canonical order",
		},
		"duplicate query": {
			mutate: func(manifest *PromotionManifest) {
				manifest.Buckets[0].QuerySHA256 = append(manifest.Buckets[0].QuerySHA256, spI2QuerySHA256)
			},
			reason: "duplicates query digest",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			path := writePromotionManifestWithPassingEvidence(t, base)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			var manifest PromotionManifest
			require.NoError(t, json.Unmarshal(raw, &manifest))
			manifest.Buckets = clonePromotionBuckets(manifest.Buckets)
			test.mutate(&manifest)
			raw, err = json.Marshal(manifest)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			verification, err := verifyPromotionManifest(path)
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, strings.Join(verification.Reasons, "\n"), test.reason)
		})
	}
}

// TestVerifyPromotionManifestRequiresOperationalSQLAnchor verifies final
// authorization cannot delegate its SQL identity to the operational report.
func TestVerifyPromotionManifestRequiresOperationalSQLAnchor(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate", SelectorVersion: "selector",
		ExecutionBoundary: "inline_statement", SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps:    map[string]int64{"cap": 1},
		Buckets: []PromotionBucket{{Name: "bucket", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
	}
	directory := t.TempDir()
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(directory, "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	verification, err := verifyPromotionManifest(path)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "operational_candidate_sql_sha256 must be a lowercase SHA-256 digest")
}

// TestVerifyPromotionManifestSQLAnchorIsUnambiguous verifies one scalar SQL
// digest cannot authorize a final manifest containing several query texts.
func TestVerifyPromotionManifestSQLAnchorIsUnambiguous(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate", SelectorVersion: "selector",
		ExecutionBoundary: "inline_statement", SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		OperationalCandidateSQLSHA256: strings.Repeat("d", 64), Caps: map[string]int64{"cap": 1},
		Buckets: []PromotionBucket{{Name: "bucket", QuerySHA256: []string{digest, strings.Repeat("b", 64)}, QualificationSplit: []string{"training", "holdout"}}},
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	verification, err := verifyPromotionManifest(path)
	require.NoError(t, err)
	require.Contains(t, verification.Reasons, "operational SQL anchor requires exactly one authorized query digest")
}

// TestVerifyPromotionManifestRejectsEscapingOrMutatedEvidence verifies verify promotion manifest rejects escaping or mutated evidence behavior.
func TestVerifyPromotionManifestRejectsEscapingOrMutatedEvidence(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "promotion.json")
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         "candidate",
		SelectorVersion:   "selector",
		ExecutionBoundary: "inline_statement",
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              map[string]int64{"cap": 1},
		Buckets: []PromotionBucket{{
			Name:               "bucket",
			QuerySHA256:        []string{digest},
			QualificationSplit: []string{"training", "holdout"},
		}},
		Evidence: map[string]PromotionEvidenceReference{},
	}
	for _, role := range requiredPromotionEvidenceRoles {
		manifest.Evidence[role] = PromotionEvidenceReference{
			Path:   "../outside.json",
			SHA256: digest,
		}
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	for _, role := range requiredPromotionEvidenceRoles {
		require.Contains(t, verification.Reasons, role+": path escapes the manifest directory")
	}
}

func TestVerifyPromotionManifestRejectsEvidenceSymlinkEscape(t *testing.T) {
	outside := t.TempDir()
	manifestPath := writePromotionManifestWithPassingEvidence(t, PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "commit",
		SourceSHA256: strings.Repeat("a", 64), BinarySHA256: strings.Repeat("a", 64), CorpusSHA256: spI2FullCorpusSHA256,
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL), Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{Name: "hidden-fan-in", QuerySHA256: []string{spI2QuerySHA256}, Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	})
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	var manifest PromotionManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))

	role := "aa"
	original := filepath.Join(filepath.Dir(manifestPath), manifest.Evidence[role].Path)
	external := filepath.Join(outside, "external-aa.json")
	externalRaw, err := os.ReadFile(original)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(external, externalRaw, 0o600))
	require.NoError(t, os.Remove(original))
	require.NoError(t, os.Symlink(external, original))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "aa: path escapes the manifest directory through a symlink")
}

func TestBindPromotionEvidencePreservesLargeIntegersExactly(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate", SelectorVersion: "selector",
		ExecutionBoundary: "guarded_dual_arm", SourceCommit: "commit", SourceSHA256: digest,
		BinarySHA256: digest, CorpusSHA256: digest, Caps: map[string]int64{"cap": 1},
		Buckets: []PromotionBucket{{Name: "bucket", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "manifest.json")
	inputPath := filepath.Join(directory, "input.json")
	outputPath := filepath.Join(directory, "output.json")
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
	require.NoError(t, os.WriteFile(inputPath, []byte(`{"version":9007199254740993}`), 0o600))
	require.NoError(t, bindPromotionEvidenceReport(manifestPath, "resource", inputPath, outputPath))
	boundRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Contains(t, string(boundRaw), `"version": 9007199254740993`)
	require.NotContains(t, string(boundRaw), `9007199254740992`)

	require.NoError(t, os.WriteFile(inputPath, []byte(`{"version":1,"version":2}`), 0o600))
	require.ErrorContains(t, bindPromotionEvidenceReport(manifestPath, "resource", inputPath, outputPath), "duplicate JSON object key")
}
