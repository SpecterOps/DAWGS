// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// Bound promotion reports embed the producer's native schema and add only the
// authorization identity installed by bindPromotionEvidenceReport. Keeping
// these wrappers concrete makes DisallowUnknownFields effective; decoding into
// map[string]any would silently accept misspelled or invented proof fields.
type promotionAAResolutionReport struct {
	AAResolutionReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
	// NativeReportSHA256 binds the wrapped document to the exact producer report
	// before promotion_identity is attached. Other roles use this digest when
	// they declare which A/A report supplied their statistical floor.
	NativeReportSHA256 string `json:"native_report_sha256"`
	// NativeReportBase64 preserves the producer's exact bytes so verification can
	// recompute the digest rather than trusting a digest-shaped assertion.
	NativeReportBase64 string `json:"native_report_base64"`
}

type promotionConfirmationReport struct {
	ConfirmationReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

type promotionPerfGateReport struct {
	PerfGateReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

type promotionSPI1QualificationReport struct {
	SPI1QualificationReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

type promotionSPI2QualificationReport struct {
	SPI2QualificationReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

type promotionOrientationSelectorReport struct {
	OrientationSelectorReport
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

type promotionOrientationSelectorV2Report struct {
	OrientationSelectorV2Report
	PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
}

func validatePromotionAAReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionAAResolutionReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("A/A report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("A/A report promotion identity does not match manifest")
	}
	report := bound.AAResolutionReport
	nativeRaw, err := base64.StdEncoding.DecodeString(bound.NativeReportBase64)
	if err != nil || len(nativeRaw) == 0 {
		return fmt.Errorf("A/A report does not contain decodable native producer bytes")
	}
	nativeDigest := sha256.Sum256(nativeRaw)
	if hex.EncodeToString(nativeDigest[:]) != bound.NativeReportSHA256 {
		return fmt.Errorf("A/A native producer report SHA-256 does not match its embedded bytes")
	}
	var native AAResolutionReport
	if err := decodePromotionEvidence(nativeRaw, &native); err != nil {
		return fmt.Errorf("A/A native producer report: %w", err)
	}
	if !reflect.DeepEqual(native, report) {
		return fmt.Errorf("A/A bound projection differs from its native producer report")
	}
	if report.Version != aaReportVersion {
		return fmt.Errorf("A/A report version must be %d", aaReportVersion)
	}
	if !lowercaseSHA256(report.ArtifactSHA256) || !lowercaseSHA256(report.HostFingerprint) {
		return fmt.Errorf("A/A report lacks canonical artifact and host digests")
	}
	if report.Confidence != defaultConfidenceLevel || math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) || !lowercaseSHA256(bound.NativeReportSHA256) {
		return fmt.Errorf("A/A report has invalid frozen confidence or native report digest")
	}
	if !report.OrderBalanced || report.MinimumRounds != minimumGateRounds || report.MinimumSamplesPerArmPerRound != 10 || report.MinimumP99SamplesPerArm != 10_000 {
		return fmt.Errorf("A/A report lacks the promotion-grade balanced sampling contract")
	}
	chronology := report.PhysicalChronology
	if chronology == nil || chronology.Version != aaPhysicalChronologyVersion || !chronology.Validated ||
		chronology.ArtifactSHA256 != report.ArtifactSHA256 || chronology.Rounds < report.MinimumRounds ||
		len(chronology.Arms) != 2 || strings.TrimSpace(chronology.Arms[0]) == "" || strings.TrimSpace(chronology.Arms[1]) == "" || chronology.Arms[0] == chronology.Arms[1] {
		return fmt.Errorf("A/A report lacks artifact-bound physical chronology")
	}
	if len(report.Cases) == 0 {
		return fmt.Errorf("A/A report has no cases")
	}
	seen := map[string]struct{}{}
	for _, gateCase := range report.Cases {
		key := gateCase.Dataset + "\x00" + gateCase.Name
		if strings.TrimSpace(gateCase.Dataset) == "" || strings.TrimSpace(gateCase.Name) == "" || gateCase.Backend != ModePostgresSQL ||
			!lowercaseSHA256(gateCase.WorkloadSHA256) || !lowercaseSHA256(gateCase.PostgresEnvironmentSHA256) || !lowercaseSHA256(gateCase.FixtureSHA256) {
			return fmt.Errorf("A/A report contains an incomplete case identity")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("A/A report duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seen[key] = struct{}{}
		if gateCase.Rounds != chronology.Rounds || gateCase.Rounds < report.MinimumRounds ||
			gateCase.SamplesPerArm < gateCase.Rounds*report.MinimumSamplesPerArmPerRound {
			return fmt.Errorf("A/A case %s/%s lacks the declared rounds or samples", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionAAMetric(gateCase.P50); err != nil {
			return fmt.Errorf("A/A case %s/%s p50: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if err := validatePromotionAAMetric(gateCase.P95); err != nil {
			return fmt.Errorf("A/A case %s/%s p95: %w", gateCase.Dataset, gateCase.Name, err)
		}
		expectedP99 := gateCase.SamplesPerArm >= report.MinimumP99SamplesPerArm
		if report.MinimumP99SamplesPerArm <= 0 || gateCase.P99Gated != expectedP99 || expectedP99 && gateCase.P99Reason != "" || !expectedP99 && strings.TrimSpace(gateCase.P99Reason) == "" {
			return fmt.Errorf("A/A case %s/%s has contradictory p99 gating", gateCase.Dataset, gateCase.Name)
		}
	}
	return nil
}

func validatePromotionAAMetric(metric AAMetricResolution) error {
	if !validRatioInterval(metric.Ratio) || !validDurationInterval(metric.AbsoluteChange) ||
		math.IsNaN(metric.RatioResolution) || math.IsInf(metric.RatioResolution, 0) || metric.RatioResolution < 0 || metric.AbsoluteResolution < 0 {
		return fmt.Errorf("invalid statistical interval")
	}
	expectedRatio := math.Max(math.Abs(1-metric.Ratio.Lower), math.Abs(metric.Ratio.Upper-1))
	expectedAbsolute := max(absDuration(metric.AbsoluteChange.Lower), absDuration(metric.AbsoluteChange.Upper))
	if metric.RatioResolution != expectedRatio || metric.AbsoluteResolution != expectedAbsolute {
		return fmt.Errorf("derived resolution contradicts its interval")
	}
	return nil
}

func validatePromotionConfirmationReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	switch expectedIdentity.Candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return validatePromotionSPI1Confirmation(raw, expectedIdentity)
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return validatePromotionSPI2Confirmation(raw, expectedIdentity)
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1):
		return fmt.Errorf("orientation-probe-v1 promotion is disabled because its report schema cannot bind source, corpus, and frozen cohort identity")
	case string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return validatePromotionOrientationV2Confirmation(raw, expectedIdentity)
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return validatePromotionGenericConfirmation(raw, expectedIdentity)
	default:
		return fmt.Errorf("candidate %q has no registered confirmation-report schema", expectedIdentity.Candidate)
	}
}

func validatePromotionGenericConfirmation(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionConfirmationReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("confirmation report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("confirmation report promotion identity does not match manifest")
	}
	report := bound.ConfirmationReport
	if report.Version != confirmationReportVersion || report.Kind != "causal_confirmation" {
		return fmt.Errorf("confirmation report is not schema-v%d causal confirmation", confirmationReportVersion)
	}
	if report.Seed != 1 || report.Confidence != defaultConfidenceLevel || math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) ||
		strings.TrimSpace(report.LeftArm) == "" || strings.TrimSpace(report.RightArm) == "" || report.LeftArm == report.RightArm ||
		!lowercaseSHA256(report.LeftSHA256) || !lowercaseSHA256(report.RightSHA256) || !lowercaseSHA256(report.AAReportSHA256) || strings.TrimSpace(report.AAReport) == "" {
		return fmt.Errorf("confirmation report lacks immutable arm and A/A identity")
	}
	if !report.PromotionEligible || !report.QualificationRequired || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases <= 0 || report.HoldoutCases <= 0 || len(report.Cases) == 0 {
		return fmt.Errorf("confirmation report did not pass complete training and holdout qualification")
	}
	if err := validatePromotionQualificationFamilies(report.QualificationFamilies, expectedIdentity.Candidate, report.TrainingCases, report.HoldoutCases); err != nil {
		return fmt.Errorf("confirmation report: %w", err)
	}
	seenCases := map[string]struct{}{}
	seenInvocations := map[string]struct{}{}
	trainingCases, holdoutCases := 0, 0
	for _, gateCase := range report.Cases {
		key := fmt.Sprintf("%s\x00%s\x00%s", gateCase.Dataset, gateCase.Name, gateCase.Backend)
		if strings.TrimSpace(gateCase.Dataset) == "" || strings.TrimSpace(gateCase.Name) == "" {
			return fmt.Errorf("confirmation report contains an incomplete case identity")
		}
		if _, duplicate := seenCases[key]; duplicate {
			return fmt.Errorf("confirmation report duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seenCases[key] = struct{}{}
		if !gateCase.TimingGated {
			continue
		}
		if gateCase.Backend != ModePostgresSQL || (gateCase.QualificationSplit != "training" && gateCase.QualificationSplit != "holdout") ||
			gateCase.MatchedRounds < 10 || gateCase.MatchedRounds > 20 || gateCase.LeftSamples < gateCase.MatchedRounds*50 || gateCase.RightSamples < gateCase.MatchedRounds*50 ||
			!gateCase.Comparable || len(gateCase.Comparability) != 0 || gateCase.P95.Classification != "cleared_non_inferior" || gateCase.Disposition != gateCase.P95.Classification {
			return fmt.Errorf("confirmation case %s/%s lacks passing promotion evidence", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionConfirmationMetric(gateCase.P50); err != nil {
			return fmt.Errorf("confirmation case %s/%s p50: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if err := validatePromotionConfirmationMetric(gateCase.P95); err != nil {
			return fmt.Errorf("confirmation case %s/%s p95: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if len(gateCase.RightRuntimeReceiptChains) != gateCase.RightSamples {
			return fmt.Errorf("confirmation case %s/%s runtime receipt count differs from right-arm samples", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionReceiptChains(gateCase.RightRuntimeReceiptChains, expectedIdentity.Candidate, seenInvocations); err != nil {
			return fmt.Errorf("confirmation case %s/%s: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if gateCase.QualificationSplit == "training" {
			trainingCases++
		} else {
			holdoutCases++
		}
	}
	if trainingCases != report.TrainingCases || holdoutCases != report.HoldoutCases {
		return fmt.Errorf("confirmation report split counts contradict its cases")
	}
	return nil
}

func validatePromotionSPI1Confirmation(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionSPI1QualificationReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("SP-I1 confirmation report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("SP-I1 confirmation promotion identity does not match manifest")
	}
	report := bound.SPI1QualificationReport
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return fmt.Errorf("SP-I1 confirmation cohort: %w", err)
	}
	if report.Version != spI1QualificationVersion || report.Protocol != referencePairProtocolConfirmation ||
		report.Baseline != string(optimize.ShortestPathExecutorS4CanonicalWitness) || report.Candidate != expectedIdentity.Candidate ||
		report.Policy != optimize.ShortestPathPolicyI1CanonicalGuardedV1 || report.QuerySHA256 != spI1QuerySHA256 {
		return fmt.Errorf("SP-I1 confirmation report has the wrong version, protocol, or candidate contract")
	}
	if expectedIdentity.SelectorVersion != optimize.ShortestPathSelectorStaticV6 || expectedIdentity.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
		expectedIdentity.FallbackExecutor != report.Baseline || promotionIdentityQueryCount(expectedIdentity, report.QuerySHA256) != 1 {
		return fmt.Errorf("SP-I1 confirmation report does not match the manifest selector, fallback, or exact query cohort")
	}
	if report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.BootstrapCount != defaultBootstrapCount ||
		report.MaterialityRatio != 0.95 || report.MaterialityAbsolute != 100*time.Microsecond || report.P95RatioLimit != 1.05 ||
		!exactPromotionCaps(report.Caps, spI1QualificationCaps()) || !exactPromotionCaps(report.Caps, expectedIdentity.Caps) {
		return fmt.Errorf("SP-I1 confirmation report changes frozen statistical or cap settings")
	}
	if report.SourceCommit != expectedIdentity.SourceCommit || report.SourceArchiveSHA256 != expectedIdentity.SourceSHA256 ||
		report.BinarySHA256 != expectedIdentity.BinarySHA256 || report.CorpusSHA256 != expectedIdentity.CorpusSHA256 ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || report.CorpusSHA256 != spI1FullCorpusSHA256 ||
		report.CohortDeclarationSHA256 != cohort.declarationSHA256 || report.ResolvedSelectionSHA256 != cohort.fullResolvedSHA256 ||
		report.TrainingDeclarationSHA256 != cohort.trainingDeclarationSHA256 || report.HoldoutDeclarationSHA256 != cohort.holdoutDeclarationSHA256 ||
		report.FullDeclarationSHA256 != cohort.declarationSHA256 || report.TrainingCorpusSHA256 != cohort.trainingCorpusSHA256 || report.FullCorpusSHA256 != cohort.fullCorpusSHA256 {
		return fmt.Errorf("SP-I1 confirmation report source, corpus, or cohort identity differs from the manifest and frozen protocol")
	}
	if !lowercaseSHA256(report.BaselineArtifactSHA256) || !lowercaseSHA256(report.CandidateArtifactSHA256) ||
		!lowercaseSHA256(report.ResourceReportSHA256) || !lowercaseSHA256(report.FreezeManifestSHA256) {
		return fmt.Errorf("SP-I1 confirmation report lacks checksummed artifacts and freeze")
	}
	if !report.EvidencePassed || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases != len(cohort.trainingKeys) || report.HoldoutCases != len(cohort.holdoutKeys) || len(report.Cases) != len(cohort.keys) {
		return fmt.Errorf("SP-I1 confirmation report did not pass the complete frozen cohort")
	}
	seen := map[string]struct{}{}
	for _, gateCase := range report.Cases {
		key := performanceKey{dataset: gateCase.Dataset, name: gateCase.Name, backend: ModePostgresSQL}
		if _, expected := cohort.keys[key]; !expected {
			return fmt.Errorf("SP-I1 confirmation report contains unexpected case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		caseKey := promotionCaseKey(gateCase.Dataset, gateCase.Name)
		if _, duplicate := seen[caseKey]; duplicate {
			return fmt.Errorf("SP-I1 confirmation report duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seen[caseKey] = struct{}{}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		material := validRatioInterval(gateCase.MedianRatio) && validDurationInterval(gateCase.MedianSaving) &&
			(gateCase.MedianRatio.Upper <= report.MaterialityRatio || gateCase.MedianSaving.Lower >= report.MaterialityAbsolute)
		p95Contained := validRatioInterval(gateCase.P95Ratio) && gateCase.P95Ratio.Upper <= report.P95RatioLimit
		if gateCase.QualificationSplit != expectedSplit || gateCase.Rounds < 10 || gateCase.Rounds > 20 ||
			gateCase.BaselineSamples < gateCase.Rounds*50 || gateCase.CandidateSamples < gateCase.Rounds*50 ||
			!material || gateCase.Material != material || !p95Contained || gateCase.P95Contained != p95Contained ||
			!gateCase.ResourcePassed || strings.TrimSpace(gateCase.RuntimeBranch) == "" || !gateCase.Passed || len(gateCase.Reasons) != 0 {
			return fmt.Errorf("SP-I1 confirmation case %s/%s has incomplete or contradictory evidence", gateCase.Dataset, gateCase.Name)
		}
	}
	return nil
}

func validatePromotionSPI2Confirmation(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionSPI2QualificationReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("SP-I2 confirmation report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("SP-I2 confirmation promotion identity does not match manifest")
	}
	report := bound.SPI2QualificationReport
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return fmt.Errorf("SP-I2 confirmation cohort: %w", err)
	}
	if report.Version != spI2QualificationVersion || report.Protocol != referencePairProtocolConfirmation ||
		report.Baseline != string(optimize.ShortestPathExecutorS4CanonicalDistance) || report.Candidate != expectedIdentity.Candidate ||
		report.Policy != optimize.ShortestPathPolicyI2DistanceGuardedV1 || report.QuerySHA256 != spI2QuerySHA256 {
		return fmt.Errorf("SP-I2 confirmation report has the wrong version, protocol, or candidate contract")
	}
	if expectedIdentity.SelectorVersion != optimize.ShortestPathSelectorStaticV8HiddenFanIn || expectedIdentity.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
		expectedIdentity.FallbackExecutor != report.Baseline || promotionIdentityQueryCount(expectedIdentity, report.QuerySHA256) != 1 {
		return fmt.Errorf("SP-I2 confirmation report does not match the manifest selector, fallback, or exact query cohort")
	}
	if report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.BootstrapCount != defaultBootstrapCount ||
		report.MaterialityRatio != 0.95 || report.MaterialityAbsolute != 100*time.Microsecond || report.P95RatioLimit != 1.05 ||
		report.AdverseRatioLimit != 1.10 || report.AdverseAbsoluteLimit != 100*time.Microsecond ||
		!exactPromotionCaps(report.Caps, spI2QualificationCaps()) || !exactPromotionCaps(report.Caps, expectedIdentity.Caps) {
		return fmt.Errorf("SP-I2 confirmation report changes frozen statistical or cap settings")
	}
	if report.SourceCommit != expectedIdentity.SourceCommit || report.SourceArchiveSHA256 != expectedIdentity.SourceSHA256 ||
		report.BinarySHA256 != expectedIdentity.BinarySHA256 || report.CorpusSHA256 != expectedIdentity.CorpusSHA256 ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || report.CorpusSHA256 != spI2FullCorpusSHA256 ||
		report.CohortDeclarationSHA256 != cohort.declarationSHA256 || report.ResolvedSelectionSHA256 != cohort.fullResolvedSHA256 ||
		report.TrainingDeclarationSHA256 != cohort.trainingDeclarationSHA256 || report.HoldoutDeclarationSHA256 != cohort.holdoutDeclarationSHA256 ||
		report.FullDeclarationSHA256 != cohort.declarationSHA256 || report.TrainingCorpusSHA256 != cohort.trainingCorpusSHA256 || report.FullCorpusSHA256 != cohort.fullCorpusSHA256 {
		return fmt.Errorf("SP-I2 confirmation report source, corpus, or cohort identity differs from the manifest and frozen protocol")
	}
	if !lowercaseSHA256(report.BaselineArtifactSHA256) || !lowercaseSHA256(report.CandidateArtifactSHA256) ||
		!lowercaseSHA256(report.ResourceReportSHA256) || !lowercaseSHA256(report.FreezeManifestSHA256) {
		return fmt.Errorf("SP-I2 confirmation report lacks checksummed artifacts and freeze")
	}
	if !report.EvidencePassed || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases != len(cohort.trainingKeys) || report.HoldoutCases != len(cohort.holdoutKeys) || len(report.Cases) != len(cohort.keys) {
		return fmt.Errorf("SP-I2 confirmation report did not pass the complete frozen cohort")
	}
	seen := map[string]struct{}{}
	for _, gateCase := range report.Cases {
		key := performanceKey{dataset: gateCase.Dataset, name: gateCase.Name, backend: ModePostgresSQL}
		if _, expected := cohort.keys[key]; !expected {
			return fmt.Errorf("SP-I2 confirmation report contains unexpected case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		caseKey := promotionCaseKey(gateCase.Dataset, gateCase.Name)
		if _, duplicate := seen[caseKey]; duplicate {
			return fmt.Errorf("SP-I2 confirmation report duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seen[caseKey] = struct{}{}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		expectedRole := "target"
		material := validRatioInterval(gateCase.MedianRatio) && validDurationInterval(gateCase.MedianSaving) &&
			(gateCase.MedianRatio.Upper <= report.MaterialityRatio || gateCase.MedianSaving.Lower >= report.MaterialityAbsolute)
		if strings.Contains(gateCase.Name, "cycle-control") {
			expectedRole = "adverse_control"
			material = validRatioInterval(gateCase.MedianRatio) && validDurationInterval(gateCase.MedianSaving) &&
				(gateCase.MedianRatio.Upper <= report.AdverseRatioLimit || gateCase.MedianSaving.Lower >= -report.AdverseAbsoluteLimit)
		}
		p95Contained := validRatioInterval(gateCase.P95Ratio) && gateCase.P95Ratio.Upper <= report.P95RatioLimit
		if gateCase.QualificationSplit != expectedSplit || gateCase.QualificationRole != expectedRole ||
			gateCase.Rounds < 10 || gateCase.Rounds > 20 || gateCase.BaselineSamples < gateCase.Rounds*50 || gateCase.CandidateSamples < gateCase.Rounds*50 ||
			!material || gateCase.Material != material || !p95Contained || gateCase.P95Contained != p95Contained ||
			!gateCase.ResourcePassed || strings.TrimSpace(gateCase.RuntimeBranch) == "" || !gateCase.Passed || len(gateCase.Reasons) != 0 {
			return fmt.Errorf("SP-I2 confirmation case %s/%s has incomplete or contradictory evidence", gateCase.Dataset, gateCase.Name)
		}
	}
	return nil
}

func validatePromotionOrientationConfirmation(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionOrientationSelectorReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("orientation confirmation report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("orientation confirmation promotion identity does not match manifest")
	}
	report := bound.OrientationSelectorReport
	if report.Version != orientationSelectorReportVersion || report.Policy != expectedIdentity.Candidate || report.Protocol != referencePairProtocolConfirmation ||
		report.Confidence <= 0 || report.Confidence >= 1 || math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) ||
		report.SelectorRegretRatioLimit != 1.10 || report.ProbeOverheadRatioLimit != 1.10 || report.ProbeOverheadAbsoluteLimit != 100*time.Microsecond {
		return fmt.Errorf("orientation confirmation report changes its version, protocol, policy, or frozen thresholds")
	}
	if expectedIdentity.SelectorVersion != expectedIdentity.Candidate || expectedIdentity.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
		expectedIdentity.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) || !exactPromotionCaps(expectedIdentity.Caps, orientationPromotionCaps()) {
		return fmt.Errorf("orientation confirmation report does not match the manifest selector, fallback, or cap contract")
	}
	if !lowercaseSHA256(report.ShadowArtifactSHA256) || !lowercaseSHA256(report.IncumbentArtifactSHA256) ||
		!lowercaseSHA256(report.ReverseArtifactSHA256) || !lowercaseSHA256(report.AAReportSHA256) {
		return fmt.Errorf("orientation confirmation report lacks checksummed arm and A/A artifacts")
	}
	if !report.EvidencePassed || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases <= 0 || report.HoldoutCases <= 0 || len(report.Cases) != report.TrainingCases+report.HoldoutCases {
		return fmt.Errorf("orientation confirmation report did not pass complete training and holdout evidence")
	}
	seen := map[string]struct{}{}
	trainingCases, holdoutCases := 0, 0
	for _, gateCase := range report.Cases {
		key := promotionCaseKey(gateCase.Dataset, gateCase.Name)
		if strings.TrimSpace(gateCase.Dataset) == "" || strings.TrimSpace(gateCase.Name) == "" {
			return fmt.Errorf("orientation confirmation contains an incomplete case identity")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("orientation confirmation duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seen[key] = struct{}{}
		if (gateCase.QualificationSplit != "training" && gateCase.QualificationSplit != "holdout") || !gateCase.QualificationEligible ||
			gateCase.Rounds < 10 || gateCase.Rounds > 20 || !gateCase.ExactObservationsMatched ||
			strings.TrimSpace(gateCase.WouldSelectIdentity) == "" || strings.TrimSpace(gateCase.FastestExactIdentity) == "" {
			return fmt.Errorf("orientation confirmation case %s/%s lacks frozen qualification evidence", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionOrientationLatencyGate(gateCase.SelectorRegret, gateCase.Rounds, 50, gateCase.SelectorRegret.BaselineIdentity, gateCase.SelectorRegret.ObservedIdentity, report.SelectorRegretRatioLimit, 0, false); err != nil {
			return fmt.Errorf("orientation confirmation case %s/%s selector regret: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if err := validatePromotionOrientationLatencyGate(gateCase.ProbeOverhead, gateCase.Rounds, 50, gateCase.ProbeOverhead.BaselineIdentity, gateCase.ProbeOverhead.ObservedIdentity, report.ProbeOverheadRatioLimit, report.ProbeOverheadAbsoluteLimit, true); err != nil {
			return fmt.Errorf("orientation confirmation case %s/%s probe overhead: %w", gateCase.Dataset, gateCase.Name, err)
		}
		expectedPassed := gateCase.SelectorRegret.Passed && gateCase.ProbeOverhead.Passed
		if !expectedPassed || gateCase.Passed != expectedPassed || len(gateCase.Reasons) != 0 {
			return fmt.Errorf("orientation confirmation case %s/%s has contradictory passing disposition", gateCase.Dataset, gateCase.Name)
		}
		if gateCase.QualificationSplit == "training" {
			trainingCases++
		} else {
			holdoutCases++
		}
	}
	if trainingCases != report.TrainingCases || holdoutCases != report.HoldoutCases {
		return fmt.Errorf("orientation confirmation split counts contradict its cases")
	}
	return nil
}

func validatePromotionOrientationV2Confirmation(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionOrientationSelectorV2Report
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("orientation-v2 confirmation report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("orientation-v2 confirmation promotion identity does not match manifest")
	}
	report := bound.OrientationSelectorV2Report
	canonical, err := canonicalOrientationV2Cohort()
	if err != nil {
		return fmt.Errorf("orientation-v2 canonical cohort: %w", err)
	}
	const formula = "F2=root_rows+maximum_depth*forward_degree_rows;R2=suffix_rows+boundary_rows+reverse_degree_rows;reverse=complete&&4*R2<3*F2"
	if report.Version != orientationSelectorReportV2Version || report.Policy != expectedIdentity.Candidate || report.Protocol != referencePairProtocolConfirmation ||
		report.Seed != 1 || report.Confidence != defaultConfidenceLevel || math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) || report.Formula != formula ||
		report.ShadowForwardRatioLimit != 1.10 || report.GuardedSelectedRatioLimit != 1.10 || report.GuardedFastestRatioLimit != 1.10 || report.OverheadAbsoluteLimit != 100*time.Microsecond {
		return fmt.Errorf("orientation-v2 confirmation changes its version, protocol, policy, formula, or frozen thresholds")
	}
	if expectedIdentity.SelectorVersion != expectedIdentity.Candidate || expectedIdentity.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
		expectedIdentity.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) ||
		!exactPromotionCaps(report.Caps, orientationPromotionCaps()) || !exactPromotionCaps(report.Caps, expectedIdentity.Caps) {
		return fmt.Errorf("orientation-v2 confirmation does not match the manifest selector, fallback, or cap contract")
	}
	if report.SourceCommit != expectedIdentity.SourceCommit || report.BinarySHA256 != expectedIdentity.BinarySHA256 || report.CorpusSHA256 != expectedIdentity.CorpusSHA256 ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || report.CohortDeclarationSHA256 != canonical.declarationSHA256 ||
		!lowercaseSHA256(report.ShadowArtifactSHA256) || !lowercaseSHA256(report.IncumbentArtifactSHA256) ||
		!lowercaseSHA256(report.ReverseArtifactSHA256) || !lowercaseSHA256(report.GuardedArtifactSHA256) ||
		!lowercaseSHA256(report.AAReportSHA256) || !lowercaseSHA256(report.FreezeManifestSHA256) {
		return fmt.Errorf("orientation-v2 confirmation lacks manifest-bound clean source and checksummed artifacts")
	}
	if !report.EvidencePassed || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases != 8 || report.HoldoutCases != 4 || len(report.Cases) != 12 {
		return fmt.Errorf("orientation-v2 confirmation did not pass its exact frozen cohort")
	}
	seen := map[string]struct{}{}
	trainingCases, holdoutCases := 0, 0
	for _, gateCase := range report.Cases {
		key := promotionCaseKey(gateCase.Dataset, gateCase.Name)
		performanceCaseKey := performanceKey{dataset: gateCase.Dataset, name: gateCase.Name, backend: ModePostgresSQL}
		if _, expected := canonical.keys[performanceCaseKey]; !expected {
			return fmt.Errorf("orientation-v2 confirmation contains a case outside the frozen V3 corpus")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("orientation-v2 confirmation duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seen[key] = struct{}{}
		expectedSplit, expectedRole, expectedTuning := "holdout", "frozen_evaluation", false
		if _, training := canonical.trainingKeys[performanceCaseKey]; training {
			expectedSplit, expectedRole, expectedTuning = "training", "selector_training", true
		}
		forward := string(optimize.ExpansionSearchStepwiseForward)
		expectedRuntimeBranch := "exact_forward_incumbent"
		if gateCase.GuardedRuntimeIdentity == string(optimize.ExpansionSearchSuffixSeededReverse) {
			expectedRuntimeBranch = "suffix_seeded_reverse"
		}
		if gateCase.QualificationSplit != expectedSplit || gateCase.QualificationRole != expectedRole || gateCase.ThresholdTuningEligible != expectedTuning || !gateCase.QualificationEligible ||
			gateCase.Rounds < 10 || gateCase.Rounds > 20 || !gateCase.ExactObservationsMatched || gateCase.Overflow || gateCase.FallbackExecuted ||
			!promotionOrientationRuntimeArm(gateCase.WouldSelectIdentity) || !promotionOrientationRuntimeArm(gateCase.FastestExactIdentity) ||
			!promotionOrientationRuntimeArm(gateCase.GuardedRuntimeIdentity) || gateCase.GuardedRuntimeIdentity != gateCase.WouldSelectIdentity || gateCase.GuardedRuntimeBranch != expectedRuntimeBranch ||
			gateCase.ShadowForwardOverhead.Applicable != (gateCase.WouldSelectIdentity == forward) {
			return fmt.Errorf("orientation-v2 confirmation case %s/%s lacks frozen runtime qualification evidence", gateCase.Dataset, gateCase.Name)
		}
		guardedObserved := expectedIdentity.Candidate + ":" + gateCase.GuardedRuntimeIdentity
		if err := validatePromotionOrientationLatencyGate(gateCase.ShadowForwardOverhead.OrientationLatencyGate, gateCase.Rounds, 50, forward, expectedIdentity.Candidate+":shadow", report.ShadowForwardRatioLimit, report.OverheadAbsoluteLimit, true); err != nil {
			return fmt.Errorf("orientation-v2 confirmation case %s/%s shadow overhead: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if err := validatePromotionOrientationLatencyGate(gateCase.GuardedSelectedOverhead, gateCase.Rounds, 50, gateCase.WouldSelectIdentity, guardedObserved, report.GuardedSelectedRatioLimit, report.OverheadAbsoluteLimit, true); err != nil {
			return fmt.Errorf("orientation-v2 confirmation case %s/%s selected overhead: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if err := validatePromotionOrientationLatencyGate(gateCase.GuardedFastestRegret, gateCase.Rounds, 50, gateCase.FastestExactIdentity, guardedObserved, report.GuardedFastestRatioLimit, report.OverheadAbsoluteLimit, false); err != nil {
			return fmt.Errorf("orientation-v2 confirmation case %s/%s fastest regret: %w", gateCase.Dataset, gateCase.Name, err)
		}
		expectedPassed := (!gateCase.ShadowForwardOverhead.Applicable || gateCase.ShadowForwardOverhead.Passed) &&
			gateCase.GuardedSelectedOverhead.Passed && gateCase.GuardedFastestRegret.Passed
		if !expectedPassed || gateCase.Passed != expectedPassed || len(gateCase.Reasons) != 0 {
			return fmt.Errorf("orientation-v2 confirmation case %s/%s has contradictory passing disposition", gateCase.Dataset, gateCase.Name)
		}
		if gateCase.QualificationSplit == "training" {
			trainingCases++
		} else {
			holdoutCases++
		}
	}
	if trainingCases != report.TrainingCases || holdoutCases != report.HoldoutCases {
		return fmt.Errorf("orientation-v2 confirmation split counts contradict its cases")
	}
	return nil
}

func validatePromotionOrientationLatencyGate(gate OrientationLatencyGate, rounds, minimumSamplesPerRound int, baselineIdentity, observedIdentity string, ratioLimit float64, absoluteFloor time.Duration, exactAbsoluteFloor bool) error {
	if gate.BaselineIdentity != baselineIdentity || gate.ObservedIdentity != observedIdentity || gate.RatioUpperLimit != ratioLimit ||
		gate.BaselineSamples < rounds*minimumSamplesPerRound || gate.ObservedSamples < rounds*minimumSamplesPerRound ||
		!validRatioInterval(gate.Ratio) || !validDurationInterval(gate.AbsoluteChange) || gate.RatioUpperLimit <= 0 || gate.AbsoluteFloor < absoluteFloor || exactAbsoluteFloor && gate.AbsoluteFloor != absoluteFloor {
		return fmt.Errorf("incomplete statistical evidence")
	}
	expectedGap := max(time.Duration(0), gate.AbsoluteChange.Upper)
	expectedPassed := gate.Ratio.Upper <= gate.RatioUpperLimit || gate.AbsoluteGapUpper <= gate.AbsoluteFloor
	if gate.AbsoluteGapUpper != expectedGap || gate.Passed != expectedPassed {
		return fmt.Errorf("derived evidence contradicts the reported gate decision")
	}
	return nil
}

func promotionOrientationRuntimeArm(identity string) bool {
	return identity == string(optimize.ExpansionSearchStepwiseForward) || identity == string(optimize.ExpansionSearchSuffixSeededReverse)
}

func validatePromotionConfirmationMetric(metric ConfirmationMetric) error {
	if !validRatioInterval(metric.Ratio) || !validDurationInterval(metric.AbsoluteChange) ||
		!validPromotionNoiseFloor(metric.NoiseRatio, metric.NoiseAbsolute) || strings.TrimSpace(metric.Classification) == "" {
		return fmt.Errorf("invalid statistical evidence")
	}
	if expected := classifyConfirmationMetric(metric.Ratio, metric.AbsoluteChange, metric.NoiseRatio, metric.NoiseAbsolute).Classification; metric.Classification != expected {
		return fmt.Errorf("classification contradicts ratio, change, and noise floors")
	}
	return nil
}

func validatePromotionPerformanceReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionPerfGateReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("performance report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("performance report promotion identity does not match manifest")
	}
	report := bound.PerfGateReport
	if report.Version != perfGateVersion {
		return fmt.Errorf("performance report version must be %d", perfGateVersion)
	}
	if report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.RegressionThreshold != minimumTimingNoiseRatio ||
		math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) ||
		math.IsNaN(report.RegressionThreshold) || math.IsInf(report.RegressionThreshold, 0) ||
		!lowercaseSHA256(report.BaselineSHA256) || !lowercaseSHA256(report.CandidateSHA256) || !lowercaseSHA256(report.AAReportSHA256) || !lowercaseSHA256(report.DeclarationSHA256) {
		return fmt.Errorf("performance report lacks immutable artifacts and frozen settings")
	}
	if !report.Passed || !report.PromotionEligible || !report.MaterialityRequired || !report.MaterialityPassed || report.MaterialityTargets <= 0 ||
		!report.QualificationRequired || !report.TrainingPassed || !report.HoldoutPassed || !report.QualificationPassed ||
		report.TrainingCases <= 0 || report.HoldoutCases <= 0 || len(report.Cases) == 0 {
		return fmt.Errorf("performance report is not complete promotion-eligible evidence")
	}
	if err := validatePromotionQualificationFamilies(report.QualificationFamilies, expectedIdentity.Candidate, report.TrainingCases, report.HoldoutCases); err != nil {
		return fmt.Errorf("performance report: %w", err)
	}
	seenCases := map[string]struct{}{}
	seenInvocations := map[string]struct{}{}
	trainingCases, holdoutCases, materialityTargets := 0, 0, 0
	for _, gateCase := range report.Cases {
		key := fmt.Sprintf("%s\x00%s\x00%s", gateCase.Dataset, gateCase.Name, gateCase.Backend)
		if strings.TrimSpace(gateCase.Dataset) == "" || strings.TrimSpace(gateCase.Name) == "" || strings.TrimSpace(gateCase.Tier) == "" {
			return fmt.Errorf("performance report contains an incomplete case identity")
		}
		if _, duplicate := seenCases[key]; duplicate {
			return fmt.Errorf("performance report duplicates case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		seenCases[key] = struct{}{}
		if !gateCase.Passed {
			return fmt.Errorf("performance report passing disposition contradicts case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		if !gateCase.TimingGated {
			continue
		}
		if gateCase.Backend != ModePostgresSQL || gateCase.OracleOnly || (gateCase.QualificationSplit != "training" && gateCase.QualificationSplit != "holdout") ||
			gateCase.BaselineStatus != string(StatusOK) || gateCase.CandidateStatus != string(StatusOK) || len(gateCase.Reasons) != 0 ||
			gateCase.Rounds < minimumGateRounds || gateCase.BaselineSamples < minimumP95Samples || gateCase.CandidateSamples < minimumP95Samples ||
			!validRatioInterval(gateCase.MedianRatio) || gateCase.P95Ratio == nil || !validRatioInterval(*gateCase.P95Ratio) ||
			gateCase.MedianSaving == nil || !validDurationInterval(*gateCase.MedianSaving) || gateCase.MedianChange == nil || !validDurationInterval(*gateCase.MedianChange) ||
			gateCase.P95Change == nil || !validDurationInterval(*gateCase.P95Change) {
			return fmt.Errorf("performance case %s/%s lacks complete passing timing evidence", gateCase.Dataset, gateCase.Name)
		}
		if gateCase.MedianChange.Estimate != -gateCase.MedianSaving.Estimate || gateCase.MedianChange.Lower != -gateCase.MedianSaving.Upper || gateCase.MedianChange.Upper != -gateCase.MedianSaving.Lower {
			return fmt.Errorf("performance case %s/%s has contradictory median change and saving", gateCase.Dataset, gateCase.Name)
		}
		if !validPromotionNoiseFloor(gateCase.P50NoiseRatio, gateCase.P50NoiseAbsolute) ||
			!validPromotionNoiseFloor(gateCase.P95NoiseRatio, gateCase.P95NoiseAbsolute) {
			return fmt.Errorf("performance case %s/%s changes or omits the minimum finite noise floors", gateCase.Dataset, gateCase.Name)
		}
		if gateCase.MedianRatio.Lower > 1+gateCase.P50NoiseRatio && gateCase.MedianChange.Lower > gateCase.P50NoiseAbsolute {
			return fmt.Errorf("performance case %s/%s contains a noise-adjusted p50 regression", gateCase.Dataset, gateCase.Name)
		}
		if gateCase.P95Ratio.Lower > 1+gateCase.P95NoiseRatio && gateCase.P95Change.Lower > gateCase.P95NoiseAbsolute {
			return fmt.Errorf("performance case %s/%s contains a noise-adjusted p95 regression", gateCase.Dataset, gateCase.Name)
		}
		if len(gateCase.CandidateRuntimeReceiptChains) != gateCase.CandidateSamples {
			return fmt.Errorf("performance case %s/%s runtime receipt count differs from candidate samples", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionReceiptChains(gateCase.CandidateRuntimeReceiptChains, expectedIdentity.Candidate, seenInvocations); err != nil {
			return fmt.Errorf("performance case %s/%s: %w", gateCase.Dataset, gateCase.Name, err)
		}
		if gateCase.MaterialityRatio != nil || gateCase.MaterialityAbsolute != nil {
			expectedRatio := min(0.95, 1-gateCase.P50NoiseRatio)
			expectedAbsolute := max(100*time.Microsecond, gateCase.P50NoiseAbsolute)
			if gateCase.MaterialityRatio == nil || gateCase.MaterialityAbsolute == nil || *gateCase.MaterialityRatio <= 0 ||
				*gateCase.MaterialityRatio != expectedRatio || *gateCase.MaterialityAbsolute != expectedAbsolute ||
				gateCase.MedianRatio.Upper > *gateCase.MaterialityRatio && gateCase.MedianSaving.Lower < *gateCase.MaterialityAbsolute {
				return fmt.Errorf("performance case %s/%s has contradictory materiality evidence", gateCase.Dataset, gateCase.Name)
			}
			materialityTargets++
		}
		if gateCase.QualificationSplit == "training" {
			trainingCases++
		} else {
			holdoutCases++
		}
	}
	if trainingCases != report.TrainingCases || holdoutCases != report.HoldoutCases || materialityTargets != report.MaterialityTargets {
		return fmt.Errorf("performance report aggregate counts contradict its cases")
	}
	return nil
}

// validatePromotionEvidenceClosure verifies relationships that no report can
// prove in isolation. In particular, the performance and candidate-specific
// confirmation reports must use the exact native A/A document that was wrapped
// into the manifest, and fixed-cohort candidates must share one declaration.
func validatePromotionEvidenceClosure(base string, evidence map[string]PromotionEvidenceReference, identity PromotionEvidenceIdentity) error {
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure"} {
		if _, found := evidence[role]; !found {
			return nil // The ordinary required-role check reports this more directly.
		}
	}
	read := func(role string) ([]byte, error) {
		reference := evidence[role]
		raw, err := readContainedPromotionEvidence(base, reference.Path)
		if err != nil {
			return nil, fmt.Errorf("read %s report: %w", role, err)
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != reference.SHA256 {
			return nil, fmt.Errorf("%s report changed while evidence closure was being verified", role)
		}
		return raw, nil
	}
	aaRaw, err := read("aa")
	if err != nil {
		return err
	}
	performanceRaw, err := read("performance")
	if err != nil {
		return err
	}
	confirmationRaw, err := read("confirmation")
	if err != nil {
		return err
	}
	resourceRaw, err := read("resource")
	if err != nil {
		return err
	}
	referenceRaw, err := read("reference_closure")
	if err != nil {
		return err
	}
	var aa promotionAAResolutionReport
	if err := decodePromotionEvidence(aaRaw, &aa); err != nil {
		return fmt.Errorf("decode A/A closure: %w", err)
	}
	var performance promotionPerfGateReport
	if err := decodePromotionEvidence(performanceRaw, &performance); err != nil {
		return fmt.Errorf("decode performance closure: %w", err)
	}
	if performance.AAReportSHA256 != aa.NativeReportSHA256 {
		return fmt.Errorf("performance report does not use the manifest's exact native A/A report")
	}
	var resource promotionResourceReport
	if err := decodePromotionEvidence(resourceRaw, &resource); err != nil {
		return fmt.Errorf("decode resource closure: %w", err)
	}
	var reference promotionReferenceClosureReport
	if err := decodePromotionEvidence(referenceRaw, &reference); err != nil {
		return fmt.Errorf("decode reference closure: %w", err)
	}

	expectedDeclaration := ""
	expectedCandidateArtifact := ""
	expectedCases := map[promotionCohortCase]struct{}{}
	switch identity.Candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		var confirmation promotionSPI1QualificationReport
		if err := decodePromotionEvidence(confirmationRaw, &confirmation); err != nil {
			return fmt.Errorf("decode SP-I1 confirmation closure: %w", err)
		}
		cohort, cohortErr := canonicalSPI1Cohort()
		if cohortErr != nil {
			return cohortErr
		}
		expectedDeclaration = cohort.declarationSHA256
		for _, gateCase := range spI1CanonicalCases {
			expectedCases[promotionCohortCase{dataset: gateCase.dataset, name: gateCase.name, split: gateCase.split}] = struct{}{}
		}
		expectedCandidateArtifact = confirmation.CandidateArtifactSHA256
		if confirmation.ResourceReportSHA256 != resource.NativeReportSHA256 {
			return fmt.Errorf("SP-I1 confirmation does not use the manifest's exact native resource report")
		}
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		var confirmation promotionSPI2QualificationReport
		if err := decodePromotionEvidence(confirmationRaw, &confirmation); err != nil {
			return fmt.Errorf("decode SP-I2 confirmation closure: %w", err)
		}
		cohort, cohortErr := canonicalSPI2Cohort()
		if cohortErr != nil {
			return cohortErr
		}
		expectedDeclaration = cohort.declarationSHA256
		for _, gateCase := range spI2CanonicalCases {
			expectedCases[promotionCohortCase{dataset: gateCase.dataset, name: gateCase.name, split: gateCase.split}] = struct{}{}
		}
		expectedCandidateArtifact = confirmation.CandidateArtifactSHA256
		if confirmation.ResourceReportSHA256 != resource.NativeReportSHA256 {
			return fmt.Errorf("SP-I2 confirmation does not use the manifest's exact native resource report")
		}
	case string(optimize.ShortestPathExecutorASPI1DAG):
		var confirmation promotionConfirmationReport
		if err := decodePromotionEvidence(confirmationRaw, &confirmation); err != nil {
			return fmt.Errorf("decode confirmation closure: %w", err)
		}
		if confirmation.AAReportSHA256 != aa.NativeReportSHA256 {
			return fmt.Errorf("confirmation report does not use the manifest's exact native A/A report")
		}
		expectedCases = genericPromotionConfirmationCohort(confirmation.ConfirmationReport)
		expectedCandidateArtifact = confirmation.RightSHA256
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1):
		var confirmation promotionOrientationSelectorReport
		if err := decodePromotionEvidence(confirmationRaw, &confirmation); err != nil {
			return fmt.Errorf("decode orientation confirmation closure: %w", err)
		}
		if confirmation.AAReportSHA256 != aa.NativeReportSHA256 {
			return fmt.Errorf("orientation confirmation does not use the manifest's exact native A/A report")
		}
		for _, gateCase := range confirmation.Cases {
			expectedCases[promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}] = struct{}{}
		}
	case string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		var confirmation promotionOrientationSelectorV2Report
		if err := decodePromotionEvidence(confirmationRaw, &confirmation); err != nil {
			return fmt.Errorf("decode orientation-v2 confirmation closure: %w", err)
		}
		if confirmation.AAReportSHA256 != aa.NativeReportSHA256 {
			return fmt.Errorf("orientation-v2 confirmation does not use the manifest's exact native A/A report")
		}
		expectedDeclaration = confirmation.CohortDeclarationSHA256
		expectedCandidateArtifact = confirmation.GuardedArtifactSHA256
		for _, gateCase := range confirmation.Cases {
			expectedCases[promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}] = struct{}{}
		}
	}
	if !lowercaseSHA256(expectedCandidateArtifact) || performance.CandidateSHA256 != expectedCandidateArtifact ||
		resource.ArtifactSHA256 != expectedCandidateArtifact {
		return fmt.Errorf("confirmation, performance, and resource reports do not bind the same exact candidate artifact")
	}
	if expectedDeclaration != "" && performance.DeclarationSHA256 != expectedDeclaration {
		return fmt.Errorf("performance and confirmation reports do not bind the same frozen cohort declaration")
	}
	performanceCases := map[promotionCohortCase]struct{}{}
	performanceCasesByKey := map[promotionCohortCase][]PerfGateCase{}
	for _, gateCase := range performance.Cases {
		if gateCase.TimingGated {
			key := promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}
			performanceCases[key] = struct{}{}
			performanceCasesByKey[key] = append(performanceCasesByKey[key], gateCase)
		}
	}
	if !reflect.DeepEqual(performanceCases, expectedCases) {
		return fmt.Errorf("performance and confirmation reports do not contain the same exact promotion cohort")
	}
	resourceCases := map[promotionCohortCase]struct{}{}
	resourceCasesByKey := map[promotionCohortCase][]ResourceGateCase{}
	for _, gateCase := range resource.Cases {
		key := promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}
		resourceCases[key] = struct{}{}
		resourceCasesByKey[key] = append(resourceCasesByKey[key], gateCase)
	}
	if !reflect.DeepEqual(resourceCases, expectedCases) {
		return fmt.Errorf("resource and confirmation reports do not contain the same exact promotion cohort")
	}
	referenceCases := map[promotionCohortCase]struct{}{}
	referenceCasesByKey := map[promotionCohortCase][]ReferenceClosureCase{}
	for _, gateCase := range reference.Cases {
		key := promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}
		referenceCases[key] = struct{}{}
		referenceCasesByKey[key] = append(referenceCasesByKey[key], gateCase)
	}
	if !reflect.DeepEqual(referenceCases, expectedCases) {
		return fmt.Errorf("reference-closure and confirmation reports do not contain the same exact promotion cohort")
	}

	// Resource evidence is produced once per round, while performance evidence
	// aggregates the same candidate invocations. Set equality at the case level
	// is insufficient: omitted rounds or a substituted receipt subset would
	// otherwise retain the same dataset/name/split keys.
	for key := range expectedCases {
		performanceForCase := performanceCasesByKey[key]
		if len(performanceForCase) != 1 {
			return fmt.Errorf("performance report must contain exactly one timing-gated case for %s/%s (%s)", key.dataset, key.name, key.split)
		}
		performanceCase := performanceForCase[0]
		resourcesForCase := resourceCasesByKey[key]
		if len(resourcesForCase) != performanceCase.Rounds {
			return fmt.Errorf("resource report must contain exactly %d rounds for %s/%s (%s)", performanceCase.Rounds, key.dataset, key.name, key.split)
		}
		rounds := make(map[int]struct{}, performanceCase.Rounds)
		resourceReceipts := make([][]RuntimeReceiptEvent, 0, performanceCase.CandidateSamples)
		for _, resourceCase := range resourcesForCase {
			if resourceCase.Round < 1 || resourceCase.Round > performanceCase.Rounds {
				return fmt.Errorf("resource report round %d is outside 1..%d for %s/%s (%s)", resourceCase.Round, performanceCase.Rounds, key.dataset, key.name, key.split)
			}
			if _, duplicate := rounds[resourceCase.Round]; duplicate {
				return fmt.Errorf("resource report duplicates round %d for %s/%s (%s)", resourceCase.Round, key.dataset, key.name, key.split)
			}
			rounds[resourceCase.Round] = struct{}{}
			resourceReceipts = append(resourceReceipts, resourceCase.RuntimeReceiptChains...)
		}
		performanceReceiptSet, setErr := promotionReceiptChainSet(performanceCase.CandidateRuntimeReceiptChains)
		if setErr != nil {
			return fmt.Errorf("performance report receipts for %s/%s (%s): %w", key.dataset, key.name, key.split, setErr)
		}
		resourceReceiptSet, setErr := promotionReceiptChainSet(resourceReceipts)
		if setErr != nil {
			return fmt.Errorf("resource report receipts for %s/%s (%s): %w", key.dataset, key.name, key.split, setErr)
		}
		if !reflect.DeepEqual(resourceReceiptSet, performanceReceiptSet) {
			return fmt.Errorf("resource and performance reports do not bind the same exact candidate receipt chains for %s/%s (%s)", key.dataset, key.name, key.split)
		}
	}

	// Reference closure and PostgreSQL A/A must name the same logical workload,
	// not merely cases with matching human-readable labels. Extra A/A cases are
	// harmless, but every promotion cohort workload must resolve exactly once.
	aaCasesByKey := map[promotionWorkloadCase][]AAResolutionCase{}
	for _, aaCase := range aa.Cases {
		if aaCase.Backend == ModePostgresSQL {
			key := promotionWorkloadCase{dataset: aaCase.Dataset, name: aaCase.Name}
			aaCasesByKey[key] = append(aaCasesByKey[key], aaCase)
		}
	}
	for key := range expectedCases {
		referenceForCase := referenceCasesByKey[key]
		if len(referenceForCase) != 1 {
			return fmt.Errorf("reference-closure report must contain exactly one workload identity for %s/%s (%s)", key.dataset, key.name, key.split)
		}
		workloadKey := promotionWorkloadCase{dataset: key.dataset, name: key.name}
		aaForCase := aaCasesByKey[workloadKey]
		if len(aaForCase) != 1 {
			return fmt.Errorf("native A/A report must contain exactly one PostgreSQL workload identity for %s/%s", key.dataset, key.name)
		}
		if referenceForCase[0].WorkloadSHA256 != aaForCase[0].WorkloadSHA256 {
			return fmt.Errorf("reference-closure workload identity differs from native A/A for %s/%s", key.dataset, key.name)
		}
	}
	return nil
}

type promotionCohortCase struct {
	dataset string
	name    string
	split   string
}

type promotionWorkloadCase struct {
	dataset string
	name    string
}

func promotionReceiptChainSet(chains [][]RuntimeReceiptEvent) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(chains))
	for _, chain := range chains {
		raw, err := json.Marshal(chain)
		if err != nil {
			return nil, err
		}
		key := string(raw)
		if _, duplicate := set[key]; duplicate {
			return nil, fmt.Errorf("contains a duplicate candidate receipt chain")
		}
		set[key] = struct{}{}
	}
	return set, nil
}

func genericPromotionConfirmationCohort(report ConfirmationReport) map[promotionCohortCase]struct{} {
	cohort := map[promotionCohortCase]struct{}{}
	for _, gateCase := range report.Cases {
		if gateCase.TimingGated {
			cohort[promotionCohortCase{dataset: gateCase.Dataset, name: gateCase.Name, split: gateCase.QualificationSplit}] = struct{}{}
		}
	}
	return cohort
}

func validatePromotionQualificationFamilies(statuses []TraversalQualificationStatus, candidate string, trainingCases, holdoutCases int) error {
	if len(statuses) == 0 {
		return fmt.Errorf("qualification family evidence is missing")
	}
	seen := map[string]struct{}{}
	totalTraining, totalHoldout := 0, 0
	candidateFound := false
	for _, status := range statuses {
		if strings.TrimSpace(status.Family) == "" || status.TrainingCases <= 0 || status.HoldoutCases <= 0 ||
			!status.TrainingPassed || !status.HoldoutPassed || !status.Passed {
			return fmt.Errorf("qualification family %q is incomplete or failing", status.Family)
		}
		if _, duplicate := seen[status.Family]; duplicate {
			return fmt.Errorf("qualification family %q is duplicated", status.Family)
		}
		seen[status.Family] = struct{}{}
		candidateFound = candidateFound || promotionFamilyMatches(status.Family, candidate)
		totalTraining += status.TrainingCases
		totalHoldout += status.HoldoutCases
	}
	if !candidateFound {
		return fmt.Errorf("qualification families do not identify candidate %q", candidate)
	}
	if totalTraining != trainingCases || totalHoldout != holdoutCases {
		return fmt.Errorf("qualification family counts contradict report aggregates")
	}
	return nil
}

func promotionFamilyMatches(family, candidate string) bool {
	return family == candidate || strings.HasPrefix(family, candidate+"@")
}

func validatePromotionReceiptChains(chains [][]RuntimeReceiptEvent, candidate string, seenInvocations map[string]struct{}) error {
	if len(chains) == 0 {
		return fmt.Errorf("candidate runtime receipt chains are missing")
	}
	for _, chain := range chains {
		if len(chain) == 0 {
			return fmt.Errorf("candidate runtime receipt chain is empty")
		}
		invocationID := strings.TrimSpace(chain[0].InvocationID)
		if invocationID == "" {
			return fmt.Errorf("candidate runtime receipt invocation is missing")
		}
		if _, duplicate := seenInvocations[invocationID]; duplicate {
			return fmt.Errorf("candidate runtime receipt invocation %q is reused", invocationID)
		}
		seenInvocations[invocationID] = struct{}{}
		for index, event := range chain {
			if event.Ordinal != index+1 || event.InvocationID != invocationID || strings.TrimSpace(event.RuntimeIdentity) == "" || strings.TrimSpace(event.RuntimeBranch) == "" || event.FallbackExecuted {
				return fmt.Errorf("candidate runtime receipt chain is non-canonical")
			}
		}
		if !promotionReceiptTerminalAllowed(candidate, chain[len(chain)-1].RuntimeIdentity) {
			return fmt.Errorf("candidate runtime receipt terminal identity differs from promotion candidate")
		}
		for _, event := range chain {
			if !promotionReceiptBranchAllowed(candidate, event.RuntimeIdentity, event.RuntimeBranch) {
				return fmt.Errorf("candidate runtime receipt branch %q is not authorized for %s", event.RuntimeBranch, candidate)
			}
		}
	}
	return nil
}

// promotionReceiptBranchAllowed freezes the successful non-fallback runtime
// tuples emitted by production candidates. Nested receipt chains remain
// supported, but every transition must itself be a recognized candidate arm.
func promotionReceiptBranchAllowed(candidate, runtimeIdentity, runtimeBranch string) bool {
	switch candidate {
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return runtimeIdentity == candidate && (runtimeBranch == "inline_predecessor_dag" || runtimeBranch == "inline_no_path")
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return runtimeIdentity == candidate && (runtimeBranch == "inline_canonical_witness" || runtimeBranch == "inline_canonical_no_path")
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return runtimeIdentity == candidate && (runtimeBranch == "inline_canonical_distance" || runtimeBranch == "inline_canonical_distance_no_path")
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1), string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return runtimeIdentity == string(optimize.ExpansionSearchSuffixSeededReverse) && runtimeBranch == "suffix_seeded_reverse" ||
			runtimeIdentity == string(optimize.ExpansionSearchStepwiseForward) && runtimeBranch == "exact_forward_incumbent"
	default:
		return false
	}
}

func promotionReceiptTerminalAllowed(candidate, runtimeIdentity string) bool {
	switch candidate {
	case string(optimize.ShortestPathExecutorASPI1DAG),
		string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		string(optimize.ShortestPathExecutorI2GuardedDistance):
		return runtimeIdentity == candidate
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return runtimeIdentity == string(optimize.ExpansionSearchSuffixSeededReverse) ||
			runtimeIdentity == string(optimize.ExpansionSearchStepwiseForward)
	default:
		return false
	}
}

func validPromotionNoiseFloor(ratio float64, absolute time.Duration) bool {
	return !math.IsNaN(ratio) && !math.IsInf(ratio, 0) && ratio >= minimumTimingNoiseRatio && absolute >= minimumTimingNoiseAbsolute
}

func promotionIdentityQueryCount(identity PromotionEvidenceIdentity, querySHA256 string) int {
	count := 0
	for _, bucket := range identity.Buckets {
		for _, query := range bucket.QuerySHA256 {
			if query == querySHA256 {
				count++
			}
		}
	}
	return count
}

func promotionCaseKey(dataset, name string) string {
	return dataset + "\x00" + name
}

func exactPromotionCaps(actual, expected map[string]int64) bool {
	return reflect.DeepEqual(actual, expected)
}
