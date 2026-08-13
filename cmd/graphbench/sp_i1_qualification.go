// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const (
	spI1QualificationVersion = 1
	spI1FreezeVersion        = 1
	spI1TrainingTag          = "sp-i1-inbound-v1-training"
	spI1HoldoutTag           = "sp-i1-inbound-v1-holdout"
	spI1QuerySHA256          = "1024577967901503995d4ec0c76540e96b65f4d25e015ccb6eeffb500a5596f9"
	spI1TrainingCorpusSHA256 = "3da3c4b1cea3fa64fbaa1958f7bf8048639241522ccf6e46defd10d2d8c9ccd6"
	spI1FullCorpusSHA256     = "219ee26cae52d8b81c6c91f9c517692c544ef4cec1aa9b9314fbc4e8f5ad3c5c"
	spI1TrainingResolvedSHA  = "cc07b55331e15f4e268043d1ed36abf7deec7217771a1b30913db6e738d27f7a"
	spI1FullResolvedSHA      = "16a8756a7c32695f0314b3552c80d2a500226c7a44c57847c916a96e775aa0c5"
)

var spI1CanonicalCases = []struct {
	dataset string
	name    string
	split   string
}{
	{"generated_shortest_paths_v2_d4_o0_r4_fo0_fi16_l2_k0_t0_w0_x4_p0_c0_s0", "GSP-I1-V1-TRAIN-D04-FI016-full", "training"},
	{"generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0", "GSP-I1-V1-TRAIN-D16-FI256-early-d04", "training"},
	{"generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0", "GSP-I1-V1-TRAIN-D16-FI256-full", "training"},
	{"generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0", "GSP-I1-V1-TRAIN-D16-FI256-disconnected", "training"},
	{"generated_shortest_paths_v2_d8_o0_r3_fo0_fi31_l3_k0_t0_w0_x7_p0_c0_s0", "GSP-I1-V1-HOLDOUT-D08-FI031-full", "holdout"},
	{"generated_shortest_paths_v2_d32_o0_r11_fo0_fi191_l21_k0_t0_w0_x13_p0_c0_s0", "GSP-I1-V1-HOLDOUT-D32-FI191-full", "holdout"},
	{"generated_shortest_paths_v2_d32_o0_r11_fo0_fi191_l21_k0_t0_w0_x13_p0_c0_s0", "GSP-I1-V1-HOLDOUT-D32-FI191-disconnected", "holdout"},
}

type spI1CanonicalCohort struct {
	keys                      map[performanceKey]struct{}
	trainingKeys              map[performanceKey]struct{}
	holdoutKeys               map[performanceKey]struct{}
	declarationSHA256         string
	trainingDeclarationSHA256 string
	holdoutDeclarationSHA256  string
	trainingCorpusSHA256      string
	fullCorpusSHA256          string
	trainingResolvedSHA256    string
	fullResolvedSHA256        string
}

type spI1CanonicalDeclaration struct {
	testCase ScaleCase
	fixture  FixtureMetadata
}

func canonicalSPI1Declarations() (map[performanceKey]spI1CanonicalDeclaration, error) {
	repositoryRoot := strings.TrimSpace(commandOutput("git", "rev-parse", "--show-toplevel"))
	if repositoryRoot == "" || repositoryRoot == "unknown" {
		return nil, fmt.Errorf("locate repository root for frozen SP-I1 declarations")
	}
	corpus, err := loadScaleCorpus(filepath.Join(repositoryRoot, "benchmark", "testdata", "scale"))
	if err != nil {
		return nil, fmt.Errorf("load frozen SP-I1 declarations: %w", err)
	}
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return nil, err
	}
	declarations := make(map[performanceKey]spI1CanonicalDeclaration, len(cohort.keys))
	for _, testCase := range corpus.Cases {
		key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
		if _, expected := cohort.keys[key]; !expected {
			continue
		}
		if _, duplicate := declarations[key]; duplicate {
			return nil, fmt.Errorf("frozen SP-I1 corpus duplicates %s/%s", key.dataset, key.name)
		}
		fixture, err := fixtureMetadata("unused", testCase.Dataset)
		if err != nil {
			return nil, fmt.Errorf("derive frozen SP-I1 fixture %s: %w", testCase.Dataset, err)
		}
		declarations[key] = spI1CanonicalDeclaration{testCase: testCase, fixture: fixture}
	}
	if len(declarations) != len(cohort.keys) {
		return nil, fmt.Errorf("frozen SP-I1 corpus omits canonical declarations")
	}
	return declarations, nil
}

func canonicalSPI1Cohort() (spI1CanonicalCohort, error) {
	cohort := spI1CanonicalCohort{
		keys: map[performanceKey]struct{}{}, trainingKeys: map[performanceKey]struct{}{}, holdoutKeys: map[performanceKey]struct{}{},
		trainingCorpusSHA256: spI1TrainingCorpusSHA256, fullCorpusSHA256: spI1FullCorpusSHA256,
		trainingResolvedSHA256: spI1TrainingResolvedSHA, fullResolvedSHA256: spI1FullResolvedSHA,
	}
	var full, training, holdout []DeclaredCaseBackend
	for _, testCase := range spI1CanonicalCases {
		key := performanceKey{dataset: testCase.dataset, name: testCase.name, backend: ModePostgresSQL}
		if _, duplicate := cohort.keys[key]; duplicate || !strings.HasPrefix(testCase.dataset, "generated_shortest_paths_v2_") {
			return spI1CanonicalCohort{}, fmt.Errorf("frozen SP-I1 cohort contains an invalid declaration")
		}
		cohort.keys[key] = struct{}{}
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			item := DeclaredCaseBackend{Dataset: key.dataset, Name: key.name, Backend: backend}
			full = append(full, item)
			if testCase.split == "training" {
				training = append(training, item)
			} else if testCase.split == "holdout" {
				holdout = append(holdout, item)
			} else {
				return spI1CanonicalCohort{}, fmt.Errorf("frozen SP-I1 cohort contains an invalid split")
			}
		}
		if testCase.split == "training" {
			cohort.trainingKeys[key] = struct{}{}
		} else {
			cohort.holdoutKeys[key] = struct{}{}
		}
	}
	if len(cohort.trainingKeys) != 4 || len(cohort.holdoutKeys) != 3 || len(cohort.keys) != 7 {
		return spI1CanonicalCohort{}, fmt.Errorf("frozen SP-I1 cohort must contain exactly 4 training and 3 holdout cases")
	}
	cohort.declarationSHA256 = declarationSHA256(full)
	cohort.trainingDeclarationSHA256 = declarationSHA256(training)
	cohort.holdoutDeclarationSHA256 = declarationSHA256(holdout)
	return cohort, nil
}

func spI1QualificationCaps() map[string]int64 {
	return map[string]int64{
		"state_limit":        100_000,
		"predecessor_limit":  100_000,
		"enumeration_limit":  100_000,
		"output_bytes_limit": 64 * 1024 * 1024,
	}
}

func spI1TelemetryCaps() map[string]int64 {
	return map[string]int64{
		"state_rows":       100_000,
		"predecessor_rows": 100_000,
		"output_rows":      100_000,
		"output_bytes":     64 * 1024 * 1024,
	}
}

type SPI1QualificationOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
	Protocol       string
	// Training evidence paths make confirmation independently recompute the
	// discovery decision instead of trusting only a mutable report and freeze.
	TrainingBaselinePath  string
	TrainingCandidatePath string
	TrainingResourcePath  string
	// SourceArchiveSHA256 binds the report to git archive HEAD. Report-mode
	// callers populate it from the current committed tree; tests may supply a
	// synthetic digest without invoking Git.
	SourceArchiveSHA256 string
	Freeze              *SPI1QualificationFreezeManifest
	Discovery           *SPI1QualificationReport
}

type SPI1QualificationCase struct {
	Dataset            string           `json:"dataset"`
	Name               string           `json:"name"`
	QualificationSplit string           `json:"qualification_split"`
	Rounds             int              `json:"matched_rounds"`
	BaselineSamples    int              `json:"baseline_samples"`
	CandidateSamples   int              `json:"candidate_samples"`
	MedianRatio        RatioInterval    `json:"median_ratio_to_s4"`
	MedianSaving       DurationInterval `json:"median_saving_vs_s4"`
	P95Ratio           RatioInterval    `json:"p95_ratio_to_s4"`
	Material           bool             `json:"material"`
	P95Contained       bool             `json:"p95_contained"`
	ResourcePassed     bool             `json:"resource_passed"`
	RuntimeBranch      string           `json:"runtime_branch"`
	Passed             bool             `json:"passed"`
	Reasons            []string         `json:"reasons,omitempty"`
}

type SPI1QualificationReport struct {
	Version                   int                     `json:"version"`
	Protocol                  string                  `json:"protocol"`
	Baseline                  string                  `json:"baseline"`
	Candidate                 string                  `json:"candidate"`
	Policy                    string                  `json:"policy"`
	QuerySHA256               string                  `json:"query_sha256"`
	Seed                      int64                   `json:"seed"`
	Confidence                float64                 `json:"confidence_level"`
	BootstrapCount            int                     `json:"bootstrap_count"`
	MaterialityRatio          float64                 `json:"materiality_ratio_upper_limit"`
	MaterialityAbsolute       time.Duration           `json:"materiality_absolute_lower_limit"`
	P95RatioLimit             float64                 `json:"p95_ratio_upper_limit"`
	Caps                      map[string]int64        `json:"caps"`
	SourceCommit              string                  `json:"source_commit"`
	SourceArchiveSHA256       string                  `json:"source_archive_sha256"`
	DirtyDiffSHA256           string                  `json:"dirty_diff_sha256"`
	BinarySHA256              string                  `json:"binary_sha256"`
	CorpusSHA256              string                  `json:"corpus_sha256"`
	CohortDeclarationSHA256   string                  `json:"cohort_declaration_sha256"`
	ResolvedSelectionSHA256   string                  `json:"resolved_selection_sha256"`
	TrainingDeclarationSHA256 string                  `json:"training_declaration_sha256"`
	HoldoutDeclarationSHA256  string                  `json:"holdout_declaration_sha256"`
	FullDeclarationSHA256     string                  `json:"full_declaration_sha256"`
	TrainingCorpusSHA256      string                  `json:"training_corpus_sha256"`
	FullCorpusSHA256          string                  `json:"full_corpus_sha256"`
	BaselineArtifactSHA256    string                  `json:"baseline_artifact_sha256,omitempty"`
	CandidateArtifactSHA256   string                  `json:"candidate_artifact_sha256,omitempty"`
	ResourceReportSHA256      string                  `json:"resource_report_sha256,omitempty"`
	FreezeManifestSHA256      string                  `json:"freeze_manifest_sha256,omitempty"`
	EvidencePassed            bool                    `json:"evidence_passed"`
	TrainingCases             int                     `json:"training_cases"`
	HoldoutCases              int                     `json:"holdout_cases"`
	TrainingPassed            bool                    `json:"training_passed"`
	HoldoutPassed             bool                    `json:"holdout_passed"`
	QualificationPassed       bool                    `json:"qualification_passed"`
	Cases                     []SPI1QualificationCase `json:"cases"`
}

type SPI1QualificationFreezeManifest struct {
	Version                   int              `json:"version"`
	Baseline                  string           `json:"baseline"`
	Candidate                 string           `json:"candidate"`
	Policy                    string           `json:"policy"`
	QuerySHA256               string           `json:"query_sha256"`
	Caps                      map[string]int64 `json:"caps"`
	Seed                      int64            `json:"seed"`
	Confidence                float64          `json:"confidence_level"`
	BootstrapCount            int              `json:"bootstrap_count"`
	SourceCommit              string           `json:"source_commit"`
	SourceArchiveSHA256       string           `json:"source_archive_sha256"`
	DirtyDiffSHA256           string           `json:"dirty_diff_sha256"`
	BinarySHA256              string           `json:"binary_sha256"`
	TrainingDeclarationSHA256 string           `json:"training_declaration_sha256"`
	HoldoutDeclarationSHA256  string           `json:"holdout_declaration_sha256"`
	FullDeclarationSHA256     string           `json:"full_declaration_sha256"`
	TrainingCorpusSHA256      string           `json:"training_corpus_sha256"`
	FullCorpusSHA256          string           `json:"full_corpus_sha256"`
	TrainingResolvedSHA256    string           `json:"training_resolved_selection_sha256"`
	FullResolvedSHA256        string           `json:"full_resolved_selection_sha256"`
	BaselineArtifactSHA256    string           `json:"baseline_artifact_sha256"`
	CandidateArtifactSHA256   string           `json:"candidate_artifact_sha256"`
	ResourceReportSHA256      string           `json:"resource_report_sha256"`
	DiscoveryReportSHA256     string           `json:"discovery_report_sha256"`
	TrainingPassed            bool             `json:"training_passed"`
}

type spI1EvidenceIdentity struct {
	sourceCommit      string
	dirtyDiffSHA256   string
	binarySHA256      string
	corpusSHA256      string
	declarationSHA256 string
	resolvedSHA256    string
}

func sourceArchiveSHA256() (string, error) {
	archive, err := exec.Command("git", "archive", "--format=tar", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("archive source commit: %w", err)
	}
	digest := sha256.Sum256(archive)
	return hex.EncodeToString(digest[:]), nil
}

func equalSPI1Caps(left, right map[string]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		if right[name] != value {
			return false
		}
	}
	return true
}

type spI1ProtocolRequirements struct {
	minimumWarmups int
	minimumRounds  int
	maximumRounds  int
	minimumSamples int
	protectedCount int
	protectedSHA   string
	expectedKeys   map[performanceKey]struct{}
	declarationSHA string
	corpusSHA      string
	resolvedSHA    string
}

type spI1QualificationSeries struct {
	baseline       roundSamples
	candidate      roundSamples
	runtimeBranch  string
	resourcePassed bool
}

func spI1Requirements(protocol string, cohort spI1CanonicalCohort) (spI1ProtocolRequirements, error) {
	switch protocol {
	case referencePairProtocolDiscovery:
		return spI1ProtocolRequirements{
			minimumWarmups: 5,
			minimumRounds:  5,
			maximumRounds:  20,
			minimumSamples: 10,
			protectedCount: 2 * len(cohort.holdoutKeys),
			protectedSHA:   cohort.holdoutDeclarationSHA256,
			expectedKeys:   cohort.trainingKeys,
			declarationSHA: cohort.trainingDeclarationSHA256,
			corpusSHA:      cohort.trainingCorpusSHA256,
			resolvedSHA:    cohort.trainingResolvedSHA256,
		}, nil
	case referencePairProtocolConfirmation:
		return spI1ProtocolRequirements{
			minimumWarmups: 20,
			minimumRounds:  10,
			maximumRounds:  20,
			minimumSamples: 50,
			expectedKeys:   cohort.keys,
			declarationSHA: cohort.declarationSHA256,
			corpusSHA:      cohort.fullCorpusSHA256,
			resolvedSHA:    cohort.fullResolvedSHA256,
		}, nil
	default:
		return spI1ProtocolRequirements{}, fmt.Errorf("unsupported SP-I1 qualification protocol %q", protocol)
	}
}

func buildSPI1QualificationReport(
	baseline, candidate []CaseResult,
	resource ResourceGateReport,
	options SPI1QualificationOptions,
) (SPI1QualificationReport, error) {
	if options.Confidence != defaultConfidenceLevel || math.IsNaN(options.Confidence) || math.IsInf(options.Confidence, 0) {
		return SPI1QualificationReport{}, fmt.Errorf("SP-I1 qualification confidence must be the frozen %.4f", defaultConfidenceLevel)
	}
	if options.Seed != 1 {
		return SPI1QualificationReport{}, fmt.Errorf("SP-I1 qualification bootstrap seed must be the frozen value 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount != defaultBootstrapCount {
		return SPI1QualificationReport{}, fmt.Errorf("SP-I1 qualification bootstrap count must be the frozen value %d", defaultBootstrapCount)
	}
	if options.Protocol == "" {
		options.Protocol = referencePairProtocolConfirmation
	}
	if !lowercaseSHA256(options.SourceArchiveSHA256) {
		return SPI1QualificationReport{}, fmt.Errorf("SP-I1 source archive digest is missing or malformed")
	}

	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return SPI1QualificationReport{}, err
	}
	requirements, err := spI1Requirements(options.Protocol, cohort)
	if err != nil {
		return SPI1QualificationReport{}, err
	}
	identity, err := validateSPI1EvidenceIdentity(baseline, candidate, requirements)
	if err != nil {
		return SPI1QualificationReport{}, err
	}
	series, keys, err := collectSPI1QualificationSeries(baseline, candidate, resource, requirements)
	if err != nil {
		return SPI1QualificationReport{}, err
	}

	report := SPI1QualificationReport{
		Version:                   spI1QualificationVersion,
		Protocol:                  options.Protocol,
		Baseline:                  string(optimize.ShortestPathExecutorS4CanonicalWitness),
		Candidate:                 string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Policy:                    optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		QuerySHA256:               spI1QuerySHA256,
		Seed:                      options.Seed,
		Confidence:                options.Confidence,
		BootstrapCount:            options.BootstrapCount,
		MaterialityRatio:          0.95,
		MaterialityAbsolute:       100 * time.Microsecond,
		P95RatioLimit:             1.05,
		Caps:                      spI1QualificationCaps(),
		SourceCommit:              identity.sourceCommit,
		SourceArchiveSHA256:       options.SourceArchiveSHA256,
		DirtyDiffSHA256:           identity.dirtyDiffSHA256,
		BinarySHA256:              identity.binarySHA256,
		CorpusSHA256:              identity.corpusSHA256,
		CohortDeclarationSHA256:   identity.declarationSHA256,
		ResolvedSelectionSHA256:   identity.resolvedSHA256,
		TrainingDeclarationSHA256: cohort.trainingDeclarationSHA256,
		HoldoutDeclarationSHA256:  cohort.holdoutDeclarationSHA256,
		FullDeclarationSHA256:     cohort.declarationSHA256,
		TrainingCorpusSHA256:      cohort.trainingCorpusSHA256,
		FullCorpusSHA256:          cohort.fullCorpusSHA256,
		EvidencePassed:            true,
		TrainingPassed:            true,
		HoldoutPassed:             true,
	}
	if options.Protocol == referencePairProtocolConfirmation {
		if err := validateSPI1Freeze(options.Freeze, options.Discovery, report, cohort); err != nil {
			return SPI1QualificationReport{}, err
		}
	}

	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	for index, key := range keys {
		current := series[key]
		baselineRounds, candidateRounds := matchedRounds(current.baseline, current.candidate)
		if !slices.Equal(sortedRounds(current.baseline), sortedRounds(current.candidate)) ||
			len(baselineRounds) != len(current.baseline) || len(candidateRounds) != len(current.candidate) {
			return SPI1QualificationReport{}, fmt.Errorf("%s/%s SP-I1 arms do not contain identical nonempty round sets", key.dataset, key.name)
		}
		rounds := sortedRounds(baselineRounds)
		if len(rounds) < requirements.minimumRounds || len(rounds) > requirements.maximumRounds {
			return SPI1QualificationReport{}, fmt.Errorf(
				"%s/%s requires %d-%d matched SP-I1 rounds, got %d",
				key.dataset, key.name, requirements.minimumRounds, requirements.maximumRounds, len(rounds),
			)
		}
		for _, round := range rounds {
			if len(baselineRounds[round]) < requirements.minimumSamples || len(candidateRounds[round]) < requirements.minimumSamples {
				return SPI1QualificationReport{}, fmt.Errorf(
					"%s/%s round %d requires at least %d warm samples per SP-I1 arm, got %d/%d",
					key.dataset, key.name, round, requirements.minimumSamples,
					len(baselineRounds[round]), len(candidateRounds[round]),
				)
			}
		}
		if err := validatePairedOrderEvidence(baseline, candidate, key, rounds, requirements.minimumWarmups); err != nil {
			return SPI1QualificationReport{}, fmt.Errorf("invalid SP-I1 paired evidence: %w", err)
		}

		split := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			split = "holdout"
		}
		seed := options.Seed + int64(index)*7919
		gateCase := SPI1QualificationCase{
			Dataset:            key.dataset,
			Name:               key.name,
			QualificationSplit: split,
			Rounds:             len(rounds),
			BaselineSamples:    sampleCount(baselineRounds),
			CandidateSamples:   sampleCount(candidateRounds),
			MedianRatio:        bootstrapRoundMedianRatio(baselineRounds, candidateRounds, seed, gateOptions),
			MedianSaving:       bootstrapRoundMedianSaving(baselineRounds, candidateRounds, seed+1, gateOptions),
			P95Ratio:           bootstrapStratifiedP95Ratio(baselineRounds, candidateRounds, seed+2, gateOptions),
			ResourcePassed:     current.resourcePassed,
			RuntimeBranch:      current.runtimeBranch,
			Passed:             true,
		}
		gateCase.Material = gateCase.MedianRatio.Upper <= report.MaterialityRatio ||
			gateCase.MedianSaving.Lower >= report.MaterialityAbsolute
		gateCase.P95Contained = gateCase.P95Ratio.Upper <= report.P95RatioLimit
		if !gateCase.Material {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
				"median improvement is not material: ratio upper %.4f > %.4f and saving lower %s < %s",
				gateCase.MedianRatio.Upper, report.MaterialityRatio,
				gateCase.MedianSaving.Lower, report.MaterialityAbsolute,
			))
		}
		if !gateCase.P95Contained {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
				"p95 ratio upper %.4f exceeds %.4f", gateCase.P95Ratio.Upper, report.P95RatioLimit,
			))
		}
		if !gateCase.ResourcePassed {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, "candidate resource evidence did not pass")
		}

		switch split {
		case "training":
			report.TrainingCases++
			report.TrainingPassed = report.TrainingPassed && gateCase.Passed
		case "holdout":
			report.HoldoutCases++
			report.HoldoutPassed = report.HoldoutPassed && gateCase.Passed
		}
		report.Cases = append(report.Cases, gateCase)
	}
	report.TrainingPassed = report.TrainingPassed && report.TrainingCases == len(cohort.trainingKeys)
	report.HoldoutPassed = report.HoldoutPassed && report.HoldoutCases == len(cohort.holdoutKeys)
	if options.Protocol == referencePairProtocolDiscovery {
		report.HoldoutPassed = false
	}
	report.QualificationPassed = report.EvidencePassed && report.TrainingPassed && report.HoldoutPassed
	return report, nil
}

func validateSPI1EvidenceIdentity(
	baseline, candidate []CaseResult,
	requirements spI1ProtocolRequirements,
) (spI1EvidenceIdentity, error) {
	if err := validatePerformanceWorkloadIdentity(baseline, candidate); err != nil {
		return spI1EvidenceIdentity{}, err
	}
	baselineHost, err := artifactHostFingerprint(baseline)
	if err != nil {
		return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 baseline host: %w", err)
	}
	candidateHost, err := artifactHostFingerprint(candidate)
	if err != nil {
		return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 candidate host: %w", err)
	}
	if baselineHost != candidateHost {
		return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 baseline and candidate host identities differ")
	}

	identity := spI1EvidenceIdentity{}
	for _, artifact := range []struct {
		name    string
		records []CaseResult
	}{
		{name: "baseline", records: baseline},
		{name: "candidate", records: candidate},
	} {
		selection, err := selectionIdentity(artifact.records)
		if err != nil {
			return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 %s selection: %w", artifact.name, err)
		}
		if err := validateSPI1Selection(selection, requirements); err != nil {
			return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 %s selection: %w", artifact.name, err)
		}
		currentIdentity := spI1EvidenceIdentity{
			declarationSHA256: selection.DeclarationSHA256,
			resolvedSHA256:    resolvedSelectionSHA256(selection.Resolved),
		}
		for _, record := range artifact.records {
			if record.Environment == nil || record.PostgresEnvironment == nil {
				return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm lacks source or PostgreSQL environment identity", record.Dataset, record.Name, artifact.name)
			}
			current := spI1EvidenceIdentity{
				sourceCommit:      strings.TrimSpace(record.Environment.SourceCommit),
				dirtyDiffSHA256:   record.Environment.DirtyDiffSHA256,
				binarySHA256:      record.Environment.BinarySHA256,
				corpusSHA256:      record.Environment.CorpusSHA256,
				declarationSHA256: selection.DeclarationSHA256,
				resolvedSHA256:    currentIdentity.resolvedSHA256,
			}
			if current.sourceCommit == "" || current.sourceCommit == "unknown" ||
				!lowercaseSHA256(current.dirtyDiffSHA256) || !lowercaseSHA256(current.binarySHA256) ||
				!lowercaseSHA256(current.corpusSHA256) {
				return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm lacks frozen source, diff, binary, or corpus identity", record.Dataset, record.Name, artifact.name)
			}
			if current.corpusSHA256 != requirements.corpusSHA {
				return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm corpus digest is not the exact frozen SP-I1 cohort", record.Dataset, record.Name, artifact.name)
			}
			if identity.sourceCommit == "" {
				identity = current
			} else if identity != current {
				return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 artifacts mix source, diff, binary, corpus, declaration, or selection identities")
			}
		}
	}
	if identity.declarationSHA256 != requirements.declarationSHA || identity.resolvedSHA256 != requirements.resolvedSHA {
		return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 artifacts do not bind the exact frozen declaration and resolved selection")
	}
	for key := range requirements.expectedKeys {
		baselinePostgres, err := postgresTimingEnvironmentSHA256ForKey(baseline, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		candidatePostgres, err := postgresTimingEnvironmentSHA256ForKey(candidate, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		baselineFixture, err := fixtureSHA256ForKey(baseline, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		candidateFixture, err := fixtureSHA256ForKey(candidate, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		if !lowercaseSHA256(baselinePostgres) || baselinePostgres != candidatePostgres {
			return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I1 PostgreSQL timing environments differ between arms", key.dataset, key.name)
		}
		if !lowercaseSHA256(baselineFixture) || baselineFixture != candidateFixture {
			return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I1 fixture identities differ between arms", key.dataset, key.name)
		}
		baselineSQL, err := spI1SQLFingerprintForKey(baseline, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		candidateSQL, err := spI1SQLFingerprintForKey(candidate, key)
		if err != nil {
			return spI1EvidenceIdentity{}, err
		}
		if baselineSQL == candidateSQL {
			return spI1EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I1 arms use the same SQL fingerprint", key.dataset, key.name)
		}
		if err := validateOrientationExactObservations(key, baseline, candidate); err != nil {
			return spI1EvidenceIdentity{}, fmt.Errorf("SP-I1 exact observations: %w", err)
		}
	}
	return identity, nil
}

func spI1SQLFingerprintForKey(records []CaseResult, key performanceKey) (string, error) {
	fingerprint := ""
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		if fingerprint != "" && fingerprint != record.SQLFingerprint {
			return "", fmt.Errorf("%s/%s changes SQL fingerprint within one SP-I1 arm", key.dataset, key.name)
		}
		fingerprint = record.SQLFingerprint
	}
	if !lowercaseSHA256(fingerprint) {
		return "", fmt.Errorf("%s/%s lacks one stable SP-I1 SQL fingerprint", key.dataset, key.name)
	}
	return fingerprint, nil
}

func validateSPI1Selection(selection SelectionManifest, requirements spI1ProtocolRequirements) error {
	if selection.Version != selectionManifestVersion || !selection.DiagnosticOnly ||
		selection.SelectedDeclarationCount != 2*len(requirements.expectedKeys) ||
		selection.FullDeclarationCount != selection.SelectedDeclarationCount+selection.OmittedDeclarationCount ||
		selection.ProtectedDeclarationCount != requirements.protectedCount ||
		selection.ProtectedDeclarationSHA256 != requirements.protectedSHA ||
		len(selection.Resolved) != len(requirements.expectedKeys) ||
		selection.DeclarationSHA256 != requirements.declarationSHA ||
		resolvedSelectionSHA256(selection.Resolved) != requirements.resolvedSHA {
		return fmt.Errorf("selection manifest does not bind the exact frozen cohort")
	}
	resolved := make(map[performanceKey]struct{}, len(selection.Resolved))
	for _, item := range selection.Resolved {
		if item.Category != "generated_shortest_path_v2" {
			return fmt.Errorf("selection contains non-SP-I1 category %q", item.Category)
		}
		key := performanceKey{dataset: item.Dataset, name: item.Name, backend: ModePostgresSQL}
		if _, duplicate := resolved[key]; duplicate {
			return fmt.Errorf("selection contains duplicate %s/%s", item.Dataset, item.Name)
		}
		resolved[key] = struct{}{}
	}
	if !orientationV2KeySetsEqual(resolved, requirements.expectedKeys) {
		return fmt.Errorf("selection does not contain the exact frozen SP-I1 cases")
	}
	return nil
}

func collectSPI1QualificationSeries(
	baseline, candidate []CaseResult,
	resource ResourceGateReport,
	requirements spI1ProtocolRequirements,
) (map[performanceKey]*spI1QualificationSeries, []performanceKey, error) {
	if err := validateSPI1GlobalInvocationIDs(baseline, candidate); err != nil {
		return nil, nil, err
	}
	declarations, err := canonicalSPI1Declarations()
	if err != nil {
		return nil, nil, err
	}
	baselineKeys, baselineRounds, err := collectSPI1Artifact("baseline", baseline, requirements, declarations)
	if err != nil {
		return nil, nil, err
	}
	candidateKeys, candidateRounds, err := collectSPI1Artifact("candidate", candidate, requirements, declarations)
	if err != nil {
		return nil, nil, err
	}
	if !orientationV2KeySetsEqual(baselineKeys, requirements.expectedKeys) ||
		!orientationV2KeySetsEqual(candidateKeys, requirements.expectedKeys) {
		return nil, nil, fmt.Errorf("SP-I1 artifacts do not contain the exact protocol cohort")
	}
	if err := validateSPI1RunSchedule(baseline, candidate, requirements); err != nil {
		return nil, nil, err
	}
	resourcePassed, err := validateSPI1ResourceCases(resource, candidate, requirements)
	if err != nil {
		return nil, nil, err
	}

	series := make(map[performanceKey]*spI1QualificationSeries, len(requirements.expectedKeys))
	for key := range requirements.expectedKeys {
		current := &spI1QualificationSeries{
			baseline:       roundSamples{},
			candidate:      roundSamples{},
			resourcePassed: resourcePassed[key],
		}
		series[key] = current
		for round, record := range baselineRounds[key] {
			appendSPI1WarmSamples(current.baseline, round, record)
		}
		for round, record := range candidateRounds[key] {
			appendSPI1WarmSamples(current.candidate, round, record)
			branch := record.TraversalTelemetry.Summary.RuntimeBranch
			if current.runtimeBranch != "" && current.runtimeBranch != branch {
				return nil, nil, fmt.Errorf("%s/%s changes SP-I1 runtime branch across rounds", key.dataset, key.name)
			}
			current.runtimeBranch = branch
		}
		if current.runtimeBranch == "" {
			return nil, nil, fmt.Errorf("%s/%s has no attributable SP-I1 candidate runtime", key.dataset, key.name)
		}
	}
	return series, sortedPerformanceKeys(requirements.expectedKeys), nil
}

// validateSPI1GlobalInvocationIDs prevents one genuine timed receipt from
// being copied into another case, round, or arm. The attestor emits globally
// unique invocation IDs, so the complete paired study must not reuse one.
func validateSPI1GlobalInvocationIDs(artifacts ...[]CaseResult) error {
	seen := map[string]struct{}{}
	for _, records := range artifacts {
		for _, record := range records {
			for _, sample := range record.Stats.Samples {
				if sample.Classification != "warm" {
					continue
				}
				invocationID := strings.TrimSpace(sample.RuntimeInvocationID)
				if invocationID == "" {
					return fmt.Errorf("%s/%s warm sample lacks a global timed invocation identity", record.Dataset, record.Name)
				}
				if _, duplicate := seen[invocationID]; duplicate {
					return fmt.Errorf("SP-I1 evidence reuses timed invocation identity %q across the paired study", invocationID)
				}
				seen[invocationID] = struct{}{}
			}
		}
	}
	return nil
}

type spI1InvocationIdentity struct {
	round, block, order int
	arm, runUUID        string
	startedAt, endedAt  time.Time
}

func validateSPI1RunSchedule(baseline, candidate []CaseResult, requirements spI1ProtocolRequirements) error {
	collect := func(arm string, records []CaseResult) (map[int]spI1InvocationIdentity, error) {
		invocations := map[int]spI1InvocationIdentity{}
		caseCounts := map[int]int{}
		for _, record := range records {
			if record.Environment == nil {
				return nil, fmt.Errorf("%s/%s %s arm lacks invocation chronology", record.Dataset, record.Name, arm)
			}
			environment := record.Environment
			identity := spI1InvocationIdentity{
				round: environment.Round, block: environment.Block, order: environment.ArmOrder,
				arm: environment.Arm, runUUID: environment.RunUUID,
				startedAt: environment.StartedAt, endedAt: environment.EndedAt,
			}
			if identity.startedAt.IsZero() || identity.endedAt.IsZero() || identity.endedAt.Before(identity.startedAt) {
				return nil, fmt.Errorf("SP-I1 %s round %d has malformed invocation timestamps", arm, identity.round)
			}
			if prior, found := invocations[identity.round]; found && prior != identity {
				return nil, fmt.Errorf("SP-I1 %s round %d mixes invocation identities", arm, identity.round)
			}
			invocations[identity.round] = identity
			caseCounts[identity.round]++
		}
		for round, count := range caseCounts {
			if count != len(requirements.expectedKeys) {
				return nil, fmt.Errorf("SP-I1 %s round %d contains %d cases, expected %d", arm, round, count, len(requirements.expectedKeys))
			}
		}
		return invocations, nil
	}
	left, err := collect("baseline", baseline)
	if err != nil {
		return err
	}
	right, err := collect("candidate", candidate)
	if err != nil {
		return err
	}
	if len(left) != len(right) || len(left) < requirements.minimumRounds || len(left) > requirements.maximumRounds {
		return fmt.Errorf("SP-I1 artifacts do not contain one complete paired invocation schedule")
	}
	runUUID := ""
	var priorEnded time.Time
	for round := 1; round <= len(left); round++ {
		baselineInvocation, baselineFound := left[round]
		candidateInvocation, candidateFound := right[round]
		if !baselineFound || !candidateFound {
			return fmt.Errorf("SP-I1 invocation schedule must use contiguous rounds starting at 1")
		}
		expectedBaselineOrder, expectedCandidateOrder := 1, 2
		if round%2 == 0 {
			expectedBaselineOrder, expectedCandidateOrder = 2, 1
		}
		if baselineInvocation.block != round || candidateInvocation.block != round ||
			baselineInvocation.arm != "sp-i1-s4" || candidateInvocation.arm != "sp-i1-candidate" ||
			baselineInvocation.order != expectedBaselineOrder || candidateInvocation.order != expectedCandidateOrder ||
			baselineInvocation.runUUID == "" || baselineInvocation.runUUID != candidateInvocation.runUUID {
			return fmt.Errorf("SP-I1 round %d does not match the frozen alternating two-arm schedule", round)
		}
		if runUUID == "" {
			runUUID = baselineInvocation.runUUID
		} else if runUUID != baselineInvocation.runUUID {
			return fmt.Errorf("SP-I1 artifacts mix run UUIDs across rounds")
		}
		first, second := baselineInvocation, candidateInvocation
		if candidateInvocation.order == 1 {
			first, second = candidateInvocation, baselineInvocation
		}
		if first.endedAt.After(second.startedAt) {
			return fmt.Errorf("SP-I1 round %d arm timestamps contradict the declared execution order", round)
		}
		if !priorEnded.IsZero() && priorEnded.After(first.startedAt) {
			return fmt.Errorf("SP-I1 round %d overlaps or predates the prior round", round)
		}
		priorEnded = second.endedAt
	}
	return nil
}

func collectSPI1Artifact(
	arm string,
	records []CaseResult,
	requirements spI1ProtocolRequirements,
	declarations map[performanceKey]spI1CanonicalDeclaration,
) (map[performanceKey]struct{}, map[performanceKey]map[int]CaseResult, error) {
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("SP-I1 %s artifact is empty", arm)
	}
	keys := map[performanceKey]struct{}{}
	rounds := map[performanceKey]map[int]CaseResult{}
	for _, record := range records {
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		if _, expected := requirements.expectedKeys[key]; !expected {
			return nil, nil, fmt.Errorf("SP-I1 %s artifact contains unexpected case %s/%s", arm, key.dataset, key.name)
		}
		declaration, found := declarations[key]
		if !found {
			return nil, nil, fmt.Errorf("SP-I1 %s artifact has no frozen declaration for %s/%s", arm, key.dataset, key.name)
		}
		if err := validateSPI1Record(record, arm, declaration); err != nil {
			return nil, nil, err
		}
		round, err := orientationV2RecordRound(record)
		if err != nil {
			return nil, nil, err
		}
		if rounds[key] == nil {
			rounds[key] = map[int]CaseResult{}
		}
		if _, duplicate := rounds[key][round]; duplicate {
			return nil, nil, fmt.Errorf("%s/%s %s artifact duplicates round %d", key.dataset, key.name, arm, round)
		}
		rounds[key][round] = record
		keys[key] = struct{}{}
	}
	return keys, rounds, nil
}

func appendSPI1WarmSamples(series roundSamples, round int, record CaseResult) {
	for _, sample := range record.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			series[round] = append(series[round], sample.Duration)
		}
	}
}

func validateSPI1Record(record CaseResult, arm string, declaration spI1CanonicalDeclaration) error {
	if record.ExecutionMode != ModePostgresSQL || record.Status != StatusOK ||
		record.Environment == nil || record.PostgresEnvironment == nil || record.Fixture == nil ||
		record.TraversalTelemetry == nil || record.Optimization == nil || record.PostgresMetrics == nil {
		return fmt.Errorf("%s/%s %s arm lacks a successful telemetry-bearing PostgreSQL record", record.Dataset, record.Name, arm)
	}
	if record.Environment.ArtifactSchemaVersion != 2 || record.Environment.PoolSize != 1 ||
		len(record.Environment.Concurrency) != 0 || record.Environment.ExistingGraph ||
		record.Environment.Protocol != "fixed_confirmation" {
		return fmt.Errorf("%s/%s %s arm lacks the schema-v2 single-session fixed-confirmation contract", record.Dataset, record.Name, arm)
	}
	if record.Fixture.Dataset != record.Dataset || !lowercaseSHA256(record.Fixture.Checksum) ||
		!record.Fixture.PhysicalValidated || record.Fixture.PhysicalNodeCount != int64(record.Fixture.NodeCount) ||
		record.Fixture.PhysicalEdgeCount != int64(record.Fixture.EdgeCount) ||
		record.Fixture.Checksum != declaration.fixture.Checksum ||
		record.Fixture.NodeCount != declaration.fixture.NodeCount || record.Fixture.EdgeCount != declaration.fixture.EdgeCount ||
		record.Fixture.Configuration != declaration.fixture.Configuration ||
		!reflect.DeepEqual(record.Fixture.Shortest, declaration.fixture.Shortest) ||
		record.Fixture.NodeRelationBytes <= 0 || record.Fixture.EdgeRelationBytes <= 0 {
		return fmt.Errorf("%s/%s %s arm lacks one exact physically validated fixture", record.Dataset, record.Name, arm)
	}
	if !strings.EqualFold(strings.TrimSpace(record.PostgresEnvironment.TransactionIsolation), "repeatable read") {
		return fmt.Errorf("%s/%s %s arm was not measured under Repeatable Read", record.Dataset, record.Name, arm)
	}
	testCase := declaration.testCase
	testCase.Source = record.Source
	expectedRecord := newCaseResult(testCase, ModePostgresSQL, nil)
	attachFixtureMetadata(&expectedRecord, *record.Fixture)
	if filepath.Base(record.Source) != "generated_sp_i1_inbound_v1.json" ||
		record.Category != testCase.Category || record.Cypher != testCase.Cypher || sqlFingerprint(record.Cypher) != spI1QuerySHA256 ||
		!lowercaseSHA256(record.WorkloadSHA256) || !lowercaseSHA256(record.SQLFingerprint) ||
		record.WorkloadSHA256 != expectedRecord.WorkloadSHA256 ||
		record.SQL == "" || sqlFingerprint(record.SQL) != record.SQLFingerprint ||
		!reflect.DeepEqual(record.NodeParams, testCase.NodeParams) ||
		!reflect.DeepEqual(record.NodeListParams, testCase.NodeListParams) ||
		!reflect.DeepEqual(record.Shape, testCase.Shape) {
		return fmt.Errorf("%s/%s %s arm lacks the frozen inbound SP-I1 workload identity", record.Dataset, record.Name, arm)
	}
	minimumDepth, maximumDepth := 0, 0
	if record.Shape.MinDepth != nil {
		minimumDepth = *record.Shape.MinDepth
	}
	if record.Shape.MaxDepth != nil {
		maximumDepth = *record.Shape.MaxDepth
	}
	if record.Shape.QualificationSplit != "training" && record.Shape.QualificationSplit != "holdout" ||
		record.Shape.FallbackExpectation != "forbidden" || record.Shape.Direction != "inbound" ||
		record.Shape.RelationshipKindCount != 1 || !slices.Equal(record.Shape.EdgeKinds, []string{"Traverse"}) ||
		minimumDepth != 1 || maximumDepth != 64 || !record.Shape.PathMaterializationRequired {
		return fmt.Errorf("%s/%s %s arm changes the frozen inbound one-path shape", record.Dataset, record.Name, arm)
	}
	expectedSplit := testCase.Shape.QualificationSplit
	if record.Shape.QualificationSplit != expectedSplit {
		return fmt.Errorf("%s/%s %s arm changes the frozen qualification split", record.Dataset, record.Name, arm)
	}
	expectedRows := *testCase.Expected.RowCount
	if !record.StableObservation || record.RowCount != expectedRows || record.ExpectedRowCount == nil ||
		*record.ExpectedRowCount != expectedRows {
		return fmt.Errorf("%s/%s %s arm lacks the exact stable path observation contract", record.Dataset, record.Name, arm)
	}
	if err := validateExpectedObservations(testCase.Expected, record.ObservedRows); err != nil {
		return fmt.Errorf("%s/%s %s arm changes the frozen path observation: %w", record.Dataset, record.Name, arm, err)
	}
	if len(record.Concurrency) != 0 || len(record.PostgresReferences) != 0 || record.ClientWaterfall != nil ||
		record.RawPGXWaterfall != nil || record.RawPGXRoundTrip != nil || record.Baseline != nil {
		return fmt.Errorf("%s/%s %s arm mixes SP-I1 timing with supplemental measurements", record.Dataset, record.Name, arm)
	}
	if err := ValidateTraversalExecutionTelemetry(record.TraversalTelemetry); err != nil {
		return fmt.Errorf("%s/%s %s arm telemetry: %w", record.Dataset, record.Name, arm, err)
	}
	if err := validateSPI1Runtime(record, arm); err != nil {
		return err
	}
	return nil
}

func validateSPI1Runtime(record CaseResult, arm string) error {
	summary := record.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable ||
		summary.Overflow == nil || summary.FallbackExecuted == nil || *summary.Overflow || *summary.FallbackExecuted ||
		summary.WouldSelectIdentity != "" || summary.ObservationMode != string(optimize.ShortestPathObservationOnePath) ||
		summary.SchedulerVersion != string(optimize.ShortestPathSchedulerSingleEndedLevel) {
		return fmt.Errorf("%s/%s %s arm lacks one non-fallback one-path runtime outcome", record.Dataset, record.Name, arm)
	}
	outcome, ok := singleTraversalOutcome(record.Optimization.TargetOutcomes)
	if !ok || outcome.Family != "SP" {
		return fmt.Errorf("%s/%s %s arm lacks one exact SP lowering outcome", record.Dataset, record.Name, arm)
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	candidate := string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
	outcomeDepthsExact := outcome.MinimumDepth != nil && *outcome.MinimumDepth == 1 &&
		outcome.MaximumDepth != nil && *outcome.MaximumDepth == 64
	outcomeShapeExact := outcome.Lowering == optimize.LoweringShortestPathExecutor && outcome.TargetKind == "traversal" &&
		outcome.ObservationMode == string(optimize.ShortestPathObservationOnePath) && outcome.Direction == "inbound" &&
		outcome.PhysicalExpansion == "end_id" && outcome.RelationshipKindCount == 1 && !outcome.UntypedRelationship &&
		outcome.TopologyClassification == "physical_inbound_deep" && outcome.SelectionMode == "forced_tool" &&
		outcome.Scheduler == string(optimize.ShortestPathSchedulerSingleEndedLevel) && outcomeDepthsExact &&
		outcome.Eligible != nil && *outcome.Eligible && outcome.StaticallyEligible != nil && *outcome.StaticallyEligible
	if !outcomeShapeExact {
		return fmt.Errorf("%s/%s %s arm changes the frozen SP-I1 lowering shape", record.Dataset, record.Name, arm)
	}
	switch arm {
	case "baseline":
		if summary.RequestedIdentity != baseline || summary.EmittedIdentity != baseline ||
			summary.RuntimeIdentity != baseline || summary.AppliedIdentity != baseline ||
			!slices.Equal(summary.PlannedIdentities, []string{baseline, "SP-S0"}) ||
			summary.SelectorVersion != "sp-tool-v1" ||
			summary.ExecutionBoundary != optimize.ShortestPathExecutorS4CanonicalWitness.ExecutionBoundary() ||
			summary.RuntimeBranch != "selected" ||
			outcome.Candidate != "" || outcome.Selected != baseline || outcome.Applied != baseline || outcome.Fallback != "SP-S0" ||
			!slices.Equal(outcome.PlannedCandidates, []string{baseline, "SP-S0"}) ||
			outcome.ExecutionBoundary != "stored_helper" || outcome.SelectorVersion != "sp-tool-v1" ||
			outcome.EmittedPolicy != "" || len(outcome.EmittedCandidates) != 0 ||
			outcome.StateLimit != 100_000 || outcome.FrontierLimit != 100_000 || outcome.PredecessorLimit != 100_000 ||
			outcome.EnumerationLimit != 100_000 || outcome.OutputBytesLimit != 64*1024*1024 {
			return fmt.Errorf("%s/%s baseline arm did not execute exact forced S4", record.Dataset, record.Name)
		}
	case "candidate":
		expectedBranch := "inline_canonical_witness"
		if record.RowCount == 0 {
			expectedBranch = "inline_canonical_no_path"
		}
		if summary.RequestedIdentity != candidate || summary.EmittedIdentity != optimize.ShortestPathPolicyI1CanonicalGuardedV1 ||
			summary.RuntimeIdentity != candidate || summary.AppliedIdentity != candidate ||
			!slices.Equal(summary.PlannedIdentities, []string{candidate, baseline}) ||
			summary.SelectorVersion != "sp-i1-canonical-tool-v1" ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
			summary.RuntimeBranch != expectedBranch ||
			!equalSPI1Caps(summary.Caps, spI1TelemetryCaps()) ||
			!slices.Contains(summary.PlannedIdentities, baseline) || !slices.Contains(summary.PlannedIdentities, candidate) ||
			outcome.Candidate != candidate || outcome.Selected != candidate || outcome.Applied != candidate ||
			outcome.Fallback != baseline || outcome.EmittedPolicy != optimize.ShortestPathPolicyI1CanonicalGuardedV1 ||
			!slices.Equal(outcome.PlannedCandidates, []string{candidate, baseline}) ||
			!slices.Equal(outcome.EmittedCandidates, []string{candidate, baseline}) ||
			outcome.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
			outcome.SelectorVersion != "sp-i1-canonical-tool-v1" ||
			outcome.StateLimit != spI1QualificationCaps()["state_limit"] ||
			outcome.PredecessorLimit != spI1QualificationCaps()["predecessor_limit"] ||
			outcome.EnumerationLimit != spI1QualificationCaps()["enumeration_limit"] ||
			outcome.OutputBytesLimit != spI1QualificationCaps()["output_bytes_limit"] || outcome.FrontierLimit != 0 {
			return fmt.Errorf("%s/%s candidate arm did not execute exact guarded canonical I1", record.Dataset, record.Name)
		}
		diagnostic := record.TraversalTelemetry.Diagnostic
		if record.TraversalTelemetry.Level != TraversalTelemetryLevelDiagnostic || diagnostic == nil ||
			diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete || diagnostic.Counters.InlineShortestPath == nil ||
			!slices.Contains(diagnostic.RequiredFamilies, TraversalTelemetryFamilySP) ||
			!slices.Contains(diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) {
			return fmt.Errorf("%s/%s candidate arm lacks complete typed canonical-I1 resource telemetry", record.Dataset, record.Name)
		}
		inline := diagnostic.Counters.InlineShortestPath
		outputRows, outputPresent := int64(0), false
		if diagnostic.PlanReplay != nil {
			outputRows, outputPresent = diagnostic.PlanReplay.Counters["asp_i1_output_rows"]
		}
		if inline.OutputPaths == nil || *inline.OutputPaths != record.RowCount || !outputPresent || outputRows != record.RowCount {
			return fmt.Errorf("%s/%s candidate arm runtime branch does not bind the exact output observation", record.Dataset, record.Name)
		}
	default:
		return fmt.Errorf("unknown SP-I1 arm %q", arm)
	}
	if err := validateSPI1SampleRuntime(record, arm); err != nil {
		return err
	}
	return nil
}

func validateSPI1SampleRuntime(record CaseResult, arm string) error {
	summary := record.TraversalTelemetry.Summary
	if record.Environment == nil || record.Stats.Iterations < 1 || record.Stats.WarmupIterations != record.Environment.WarmupIterations ||
		record.Stats.Median <= 0 || record.Stats.P95 <= 0 {
		return fmt.Errorf("%s/%s %s arm has malformed iteration or warmup evidence", record.Dataset, record.Name, arm)
	}
	expectedArm := "sp-i1-s4"
	if arm == "candidate" {
		expectedArm = "sp-i1-candidate"
	}
	if record.Environment.Arm != expectedArm || record.Environment.Round < 1 || record.Environment.Block != record.Environment.Round ||
		record.Environment.ArmOrder < 1 || record.Environment.ArmOrder > 2 || strings.TrimSpace(record.Environment.RunUUID) == "" {
		return fmt.Errorf("%s/%s %s arm has malformed frozen run metadata", record.Dataset, record.Name, arm)
	}
	warmSamples, coldSamples := 0, 0
	iterations := map[int]struct{}{}
	invocations := map[string]struct{}{}
	for _, sample := range record.Stats.Samples {
		if sample.Duration <= 0 || sample.Dataset != record.Dataset || sample.Case != record.Name || sample.Backend != ModePostgresSQL ||
			sample.Round != record.Environment.Round || sample.Block != record.Environment.Block || sample.Arm != record.Environment.Arm ||
			sample.ArmOrder != record.Environment.ArmOrder || sample.RunUUID != record.Environment.RunUUID || strings.TrimSpace(sample.ConnectionID) == "" {
			return fmt.Errorf("%s/%s %s arm has a sample outside its frozen invocation identity", record.Dataset, record.Name, arm)
		}
		switch sample.Classification {
		case "cold":
			if sample.Iteration != 0 {
				return fmt.Errorf("%s/%s %s arm cold sample has a nonzero iteration", record.Dataset, record.Name, arm)
			}
			coldSamples++
			continue
		case "warm":
		default:
			return fmt.Errorf("%s/%s %s arm contains an unexpected sample classification", record.Dataset, record.Name, arm)
		}
		warmSamples++
		if sample.Iteration < 1 || sample.Iteration > record.Stats.Iterations {
			return fmt.Errorf("%s/%s %s arm has an out-of-range warm iteration", record.Dataset, record.Name, arm)
		}
		if _, duplicate := iterations[sample.Iteration]; duplicate {
			return fmt.Errorf("%s/%s %s arm duplicates warm iteration %d", record.Dataset, record.Name, arm, sample.Iteration)
		}
		iterations[sample.Iteration] = struct{}{}
		if sample.RequestedIdentity != summary.RequestedIdentity || sample.RuntimeIdentity != summary.RuntimeIdentity ||
			sample.FallbackExecuted == nil || *sample.FallbackExecuted != *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s %s arm warm sample contradicts its runtime summary", record.Dataset, record.Name, arm)
		}
		if sample.RuntimeAttestation != "timed_invocation" {
			return fmt.Errorf("%s/%s %s arm warm sample lacks timed-invocation attribution", record.Dataset, record.Name, arm)
		}
		if strings.TrimSpace(sample.RuntimeInvocationID) == "" {
			return fmt.Errorf("%s/%s %s arm warm sample lacks a timed invocation identity", record.Dataset, record.Name, arm)
		}
		if _, duplicate := invocations[sample.RuntimeInvocationID]; duplicate {
			return fmt.Errorf("%s/%s %s arm reuses timed invocation identity %q", record.Dataset, record.Name, arm, sample.RuntimeInvocationID)
		}
		invocations[sample.RuntimeInvocationID] = struct{}{}
		expectedBranch := summary.RuntimeBranch
		if arm == "baseline" {
			expectedBranch = "compact_workspace_witness"
			if record.RowCount == 0 {
				expectedBranch = "compact_no_path"
			}
		}
		if sample.RuntimeBranch != expectedBranch || len(sample.RuntimeReceiptEvents) != 1 ||
			sample.RuntimeReceiptEvents[0].InvocationID != sample.RuntimeInvocationID || sample.RuntimeReceiptEvents[0].FallbackExecuted {
			return fmt.Errorf("%s/%s %s arm warm sample has a non-canonical runtime receipt", record.Dataset, record.Name, arm)
		}
		if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
			return fmt.Errorf("%s/%s %s arm warm sample receipt: %w", record.Dataset, record.Name, arm, err)
		}
	}
	if coldSamples != 1 || warmSamples != record.Stats.Iterations || len(record.Stats.Samples) != record.Stats.Iterations+1 {
		return fmt.Errorf("%s/%s %s arm must contain one cold and exactly %d unique warm samples", record.Dataset, record.Name, arm, record.Stats.Iterations)
	}
	return nil
}

func validateSPI1ResourceCases(
	report ResourceGateReport,
	candidate []CaseResult,
	requirements spI1ProtocolRequirements,
) (map[performanceKey]bool, error) {
	if report.Version != resourceGateVersion {
		return nil, fmt.Errorf("SP-I1 resource report version must be %d", resourceGateVersion)
	}
	type recordKey struct {
		performanceKey
		round, block, order int
		runUUID, arm        string
	}
	expected := map[recordKey]CaseResult{}
	for _, record := range candidate {
		if record.Environment == nil {
			return nil, fmt.Errorf("%s/%s candidate resource record lacks run identity", record.Dataset, record.Name)
		}
		key := recordKey{
			performanceKey: performanceKey{dataset: record.Dataset, name: record.Name, backend: ModePostgresSQL},
			round:          record.Environment.Round, block: record.Environment.Block, order: record.Environment.ArmOrder,
			runUUID: record.Environment.RunUUID, arm: record.Environment.Arm,
		}
		if _, duplicate := expected[key]; duplicate {
			return nil, fmt.Errorf("SP-I1 candidate artifact duplicates a resource record identity")
		}
		expected[key] = record
	}
	actual := map[recordKey]struct{}{}
	passed := map[performanceKey]bool{}
	for key := range requirements.expectedKeys {
		passed[key] = true
	}
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return nil, err
	}
	allPassed := true
	for _, gateCase := range report.Cases {
		key := performanceKey{dataset: gateCase.Dataset, name: gateCase.Name, backend: ModePostgresSQL}
		if _, expected := requirements.expectedKeys[key]; !expected || gateCase.Reference != "" {
			return nil, fmt.Errorf("SP-I1 resource report contains an unexpected production or reference case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		identity := recordKey{
			performanceKey: key, round: gateCase.Round, block: gateCase.Block, order: gateCase.ArmOrder,
			runUUID: gateCase.RunUUID, arm: gateCase.Arm,
		}
		record, found := expected[identity]
		if !found {
			return nil, fmt.Errorf("SP-I1 resource case %s/%s round %d does not bind an exact candidate record", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		if _, duplicate := actual[identity]; duplicate {
			return nil, fmt.Errorf("SP-I1 resource report duplicates %s/%s round %d", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		actual[identity] = struct{}{}
		recomputed := evaluateProductionResourceGateCase(record)
		if !reflect.DeepEqual(gateCase, recomputed) {
			return nil, fmt.Errorf("SP-I1 resource case %s/%s round %d differs from the decision recomputed from its candidate record", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		if gateCase.Architecture != string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness) ||
			gateCase.FallbackArchitecture != "" || gateCase.QualificationSplit != expectedSplit ||
			gateCase.Tier != "normal" || !equalSPI1Caps(gateCase.NumericLimits, spI1TelemetryCaps()) ||
			gateCase.Passed != (len(gateCase.Reasons) == 0) ||
			!reflect.DeepEqual(gateCase.RuntimeReceiptChains, runtimeReceiptChains(record.Stats.Samples)) {
			return nil, fmt.Errorf("SP-I1 resource case %s/%s does not bind exact guarded-I1 limits and split", gateCase.Dataset, gateCase.Name)
		}
		observations := traversalNumericObservations(record.TraversalTelemetry.Diagnostic.Counters)
		if len(gateCase.NumericObserved) != len(spI1TelemetryCaps()) {
			return nil, fmt.Errorf("SP-I1 resource case %s/%s has unexpected numeric observations", gateCase.Dataset, gateCase.Name)
		}
		for name := range spI1TelemetryCaps() {
			observed, found := gateCase.NumericObserved[name]
			expectedObserved, expectedFound := observations[name]
			if !found || !expectedFound || observed != expectedObserved || observed < 0 {
				return nil, fmt.Errorf("SP-I1 resource case %s/%s has invalid %s observation", gateCase.Dataset, gateCase.Name, name)
			}
		}
		passed[key] = passed[key] && gateCase.Passed
		allPassed = allPassed && gateCase.Passed
	}
	if len(actual) != len(expected) {
		return nil, fmt.Errorf("SP-I1 resource report has %d exact record decisions, expected %d", len(actual), len(expected))
	}
	for key := range requirements.expectedKeys {
		if _, found := passed[key]; !found {
			return nil, fmt.Errorf("SP-I1 resource report omits %s/%s", key.dataset, key.name)
		}
	}
	if report.Passed != allPassed {
		return nil, fmt.Errorf("SP-I1 resource report aggregate disposition contradicts its cases")
	}
	return passed, nil
}

func validateSPI1Freeze(
	freeze *SPI1QualificationFreezeManifest,
	discovery *SPI1QualificationReport,
	report SPI1QualificationReport,
	cohort spI1CanonicalCohort,
) error {
	if err := validateSPI1FrozenDiscovery(freeze, discovery, cohort); err != nil {
		return err
	}
	if report.Protocol != referencePairProtocolConfirmation ||
		report.SourceCommit != freeze.SourceCommit || report.SourceArchiveSHA256 != freeze.SourceArchiveSHA256 ||
		report.DirtyDiffSHA256 != freeze.DirtyDiffSHA256 || report.BinarySHA256 != freeze.BinarySHA256 ||
		report.QuerySHA256 != freeze.QuerySHA256 || report.Policy != freeze.Policy ||
		report.Baseline != freeze.Baseline || report.Candidate != freeze.Candidate ||
		report.CohortDeclarationSHA256 != freeze.FullDeclarationSHA256 ||
		report.CorpusSHA256 != freeze.FullCorpusSHA256 || report.ResolvedSelectionSHA256 != freeze.FullResolvedSHA256 ||
		report.Seed != freeze.Seed || report.Confidence != freeze.Confidence || report.BootstrapCount != freeze.BootstrapCount ||
		!equalSPI1Caps(report.Caps, freeze.Caps) {
		return fmt.Errorf("SP-I1 confirmation identity differs from the frozen discovery")
	}
	return nil
}

func validateSPI1FrozenDiscovery(
	freeze *SPI1QualificationFreezeManifest,
	discovery *SPI1QualificationReport,
	cohort spI1CanonicalCohort,
) error {
	if freeze == nil || discovery == nil {
		return fmt.Errorf("SP-I1 confirmation requires a discovery report and freeze manifest")
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	candidate := string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
	if freeze.Version != spI1FreezeVersion || freeze.Baseline != baseline || freeze.Candidate != candidate ||
		freeze.Policy != optimize.ShortestPathPolicyI1CanonicalGuardedV1 || freeze.QuerySHA256 != spI1QuerySHA256 ||
		freeze.Seed != 1 || freeze.Confidence != defaultConfidenceLevel || freeze.BootstrapCount != defaultBootstrapCount ||
		!equalSPI1Caps(freeze.Caps, spI1QualificationCaps()) ||
		freeze.TrainingDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		freeze.HoldoutDeclarationSHA256 != cohort.holdoutDeclarationSHA256 ||
		freeze.FullDeclarationSHA256 != cohort.declarationSHA256 ||
		freeze.TrainingCorpusSHA256 != cohort.trainingCorpusSHA256 || freeze.FullCorpusSHA256 != cohort.fullCorpusSHA256 ||
		freeze.TrainingResolvedSHA256 != cohort.trainingResolvedSHA256 || freeze.FullResolvedSHA256 != cohort.fullResolvedSHA256 ||
		!lowercaseSHA256(freeze.SourceArchiveSHA256) || !lowercaseSHA256(freeze.DirtyDiffSHA256) ||
		!lowercaseSHA256(freeze.BinarySHA256) || !lowercaseSHA256(freeze.BaselineArtifactSHA256) ||
		!lowercaseSHA256(freeze.CandidateArtifactSHA256) || !lowercaseSHA256(freeze.ResourceReportSHA256) ||
		!lowercaseSHA256(freeze.DiscoveryReportSHA256) || strings.TrimSpace(freeze.SourceCommit) == "" {
		return fmt.Errorf("SP-I1 freeze manifest does not bind the exact immutable study identity")
	}
	if freeze.DirtyDiffSHA256 != cleanWorkingTreeSHA256() {
		return fmt.Errorf("SP-I1 freeze manifest was not created from a clean source tree")
	}
	if discovery.Version != spI1QualificationVersion || discovery.Protocol != referencePairProtocolDiscovery ||
		discovery.Baseline != freeze.Baseline || discovery.Candidate != freeze.Candidate ||
		discovery.Policy != freeze.Policy || discovery.QuerySHA256 != freeze.QuerySHA256 ||
		discovery.SourceCommit != freeze.SourceCommit || discovery.SourceArchiveSHA256 != freeze.SourceArchiveSHA256 ||
		discovery.DirtyDiffSHA256 != freeze.DirtyDiffSHA256 || discovery.BinarySHA256 != freeze.BinarySHA256 ||
		discovery.CohortDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		discovery.ResolvedSelectionSHA256 != cohort.trainingResolvedSHA256 ||
		discovery.CorpusSHA256 != cohort.trainingCorpusSHA256 ||
		discovery.TrainingDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		discovery.HoldoutDeclarationSHA256 != cohort.holdoutDeclarationSHA256 ||
		discovery.FullDeclarationSHA256 != cohort.declarationSHA256 ||
		discovery.TrainingCorpusSHA256 != cohort.trainingCorpusSHA256 || discovery.FullCorpusSHA256 != cohort.fullCorpusSHA256 ||
		discovery.BaselineArtifactSHA256 != freeze.BaselineArtifactSHA256 ||
		discovery.CandidateArtifactSHA256 != freeze.CandidateArtifactSHA256 ||
		discovery.ResourceReportSHA256 != freeze.ResourceReportSHA256 ||
		!equalSPI1Caps(discovery.Caps, freeze.Caps) || discovery.Seed != freeze.Seed ||
		discovery.Confidence != freeze.Confidence || discovery.BootstrapCount != freeze.BootstrapCount ||
		discovery.MaterialityRatio != 0.95 || discovery.MaterialityAbsolute != 100*time.Microsecond ||
		discovery.P95RatioLimit != 1.05 || !discovery.EvidencePassed ||
		discovery.TrainingCases != len(cohort.trainingKeys) || discovery.HoldoutCases != 0 ||
		discovery.HoldoutPassed || discovery.QualificationPassed || discovery.TrainingPassed != freeze.TrainingPassed {
		return fmt.Errorf("SP-I1 discovery report does not prove the exact frozen training identity")
	}
	seen := map[performanceKey]struct{}{}
	for _, entry := range discovery.Cases {
		key := performanceKey{dataset: entry.Dataset, name: entry.Name, backend: ModePostgresSQL}
		if entry.QualificationSplit != "training" {
			return fmt.Errorf("SP-I1 discovery report contains non-training timing")
		}
		if _, expected := cohort.trainingKeys[key]; !expected {
			return fmt.Errorf("SP-I1 discovery report contains unexpected case %s/%s", entry.Dataset, entry.Name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("SP-I1 discovery report duplicates case %s/%s", entry.Dataset, entry.Name)
		}
		expectedBranch := "inline_canonical_witness"
		if strings.HasSuffix(entry.Name, "-disconnected") {
			expectedBranch = "inline_canonical_no_path"
		}
		if !validSPI1RatioInterval(entry.MedianRatio) || !validSPI1RatioInterval(entry.P95Ratio) ||
			entry.MedianSaving.Lower > entry.MedianSaving.Estimate || entry.MedianSaving.Estimate > entry.MedianSaving.Upper ||
			entry.Material != (entry.MedianRatio.Upper <= discovery.MaterialityRatio || entry.MedianSaving.Lower >= discovery.MaterialityAbsolute) ||
			entry.P95Contained != (entry.P95Ratio.Upper <= discovery.P95RatioLimit) ||
			!entry.Passed || len(entry.Reasons) != 0 || !entry.Material || !entry.P95Contained || !entry.ResourcePassed ||
			entry.RuntimeBranch != expectedBranch ||
			entry.Rounds < 5 || entry.Rounds > 20 || entry.BaselineSamples < 50 || entry.CandidateSamples < 50 {
			return fmt.Errorf("SP-I1 discovery report case %s/%s did not pass the frozen training gates", entry.Dataset, entry.Name)
		}
		seen[key] = struct{}{}
	}
	if !orientationV2KeySetsEqual(seen, cohort.trainingKeys) {
		return fmt.Errorf("SP-I1 discovery report omits part of the exact training cohort")
	}
	if !freeze.TrainingPassed || !discovery.TrainingPassed {
		return fmt.Errorf("SP-I1 training discovery did not pass")
	}
	return nil
}

func validSPI1RatioInterval(interval RatioInterval) bool {
	return interval.Lower > 0 && interval.Lower <= interval.Estimate && interval.Estimate <= interval.Upper &&
		!math.IsNaN(interval.Lower) && !math.IsNaN(interval.Estimate) && !math.IsNaN(interval.Upper) &&
		!math.IsInf(interval.Lower, 0) && !math.IsInf(interval.Estimate, 0) && !math.IsInf(interval.Upper, 0)
}

// createSPI1QualificationReport loads and evaluates the staged two-arm
// qualification evidence, writes the report even for statistical failures,
// and freezes discovery before any holdout capture is authorized.
func createSPI1QualificationReport(
	baselinePath, candidatePath, resourcePath, freezePath, discoveryPath, freezeOutputPath, outputPath string,
	options SPI1QualificationOptions,
) (bool, error) {
	if err := validateDistinctSPI1Paths(map[string]string{
		"baseline artifact": baselinePath, "candidate artifact": candidatePath, "resource report": resourcePath,
		"freeze manifest": freezePath, "discovery report": discoveryPath, "freeze output": freezeOutputPath, "report output": outputPath,
	}); err != nil {
		return false, err
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return false, fmt.Errorf("read SP-I1 baseline artifact: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return false, fmt.Errorf("read SP-I1 candidate artifact: %w", err)
	}
	resource, err := loadSPI1ResourceReport(resourcePath)
	if err != nil {
		return false, err
	}
	baselineSHA256, err := fileSHA256(baselinePath)
	if err != nil {
		return false, err
	}
	candidateSHA256, err := fileSHA256(candidatePath)
	if err != nil {
		return false, err
	}
	resourceSHA256, err := fileSHA256(resourcePath)
	if err != nil {
		return false, err
	}
	if resource.ArtifactSHA256 != candidateSHA256 {
		return false, fmt.Errorf("SP-I1 resource report is not bound to the exact candidate artifact")
	}

	freezeSHA256 := ""
	if freezePath != "" || discoveryPath != "" {
		if freezePath == "" || discoveryPath == "" {
			return false, fmt.Errorf("SP-I1 confirmation requires both freeze and discovery report paths")
		}
		freeze, digest, err := loadSPI1FreezeManifest(freezePath)
		if err != nil {
			return false, fmt.Errorf("read SP-I1 freeze manifest: %w", err)
		}
		discovery, err := loadSPI1QualificationReport(discoveryPath)
		if err != nil {
			return false, fmt.Errorf("read SP-I1 discovery report: %w", err)
		}
		discoverySHA256, err := fileSHA256(discoveryPath)
		if err != nil {
			return false, err
		}
		if discoverySHA256 != freeze.DiscoveryReportSHA256 {
			return false, fmt.Errorf("SP-I1 discovery report digest does not match freeze manifest")
		}
		options.Freeze, options.Discovery = freeze, discovery
		freezeSHA256 = digest
		if err := validateSPI1FrozenTrainingEvidence(
			freeze, discovery,
			options.TrainingBaselinePath, options.TrainingCandidatePath, options.TrainingResourcePath,
		); err != nil {
			return false, err
		}
	}
	options.SourceArchiveSHA256, err = sourceArchiveSHA256()
	if err != nil {
		return false, err
	}
	report, err := buildSPI1QualificationReport(baseline, candidate, resource, options)
	if err != nil {
		return false, err
	}
	report.BaselineArtifactSHA256 = baselineSHA256
	report.CandidateArtifactSHA256 = candidateSHA256
	report.ResourceReportSHA256 = resourceSHA256
	report.FreezeManifestSHA256 = freezeSHA256
	if err := validateCurrentSPI1Source(report.SourceCommit, report.SourceArchiveSHA256, report.DirtyDiffSHA256, report.BinarySHA256); err != nil {
		return false, err
	}
	if err := writeSPI1QualificationReport(outputPath, report); err != nil {
		return false, err
	}
	if options.Protocol == referencePairProtocolDiscovery {
		if err := writeSPI1FreezeManifest(freezeOutputPath, outputPath, report); err != nil {
			return false, err
		}
		return report.TrainingPassed, nil
	}
	return report.QualificationPassed, nil
}

// validateSPI1HoldoutCapture authorizes the exact frozen cohort before any
// database setup is allowed to begin.
func validateSPI1HoldoutCapture(
	corpus ScaleCorpus,
	freezePath, discoveryPath, trainingBaselinePath, trainingCandidatePath, trainingResourcePath string,
) error {
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return err
	}
	if err := validateSPI1Corpus(corpus, cohort); err != nil {
		return err
	}
	freeze, _, err := loadSPI1FreezeManifest(freezePath)
	if err != nil {
		return fmt.Errorf("read SP-I1 freeze manifest: %w", err)
	}
	discovery, err := loadSPI1QualificationReport(discoveryPath)
	if err != nil {
		return fmt.Errorf("read SP-I1 discovery report: %w", err)
	}
	discoverySHA256, err := fileSHA256(discoveryPath)
	if err != nil {
		return err
	}
	if discoverySHA256 != freeze.DiscoveryReportSHA256 {
		return fmt.Errorf("SP-I1 discovery report digest does not match freeze manifest")
	}
	if err := validateSPI1FrozenTrainingEvidence(
		freeze, discovery, trainingBaselinePath, trainingCandidatePath, trainingResourcePath,
	); err != nil {
		return err
	}
	if err := validateCurrentSPI1Source(freeze.SourceCommit, freeze.SourceArchiveSHA256, freeze.DirtyDiffSHA256, freeze.BinarySHA256); err != nil {
		return err
	}
	return nil
}

// validateSPI1FrozenTrainingEvidence reloads and recomputes the exact training
// closure named by the freeze. This prevents an internally consistent but
// hand-edited report/freeze pair from authorizing protected holdout timing.
func validateSPI1FrozenTrainingEvidence(
	freeze *SPI1QualificationFreezeManifest,
	discovery *SPI1QualificationReport,
	baselinePath, candidatePath, resourcePath string,
) error {
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return err
	}
	if err := validateSPI1FrozenDiscovery(freeze, discovery, cohort); err != nil {
		return err
	}
	if baselinePath == "" || candidatePath == "" || resourcePath == "" {
		return fmt.Errorf("SP-I1 frozen discovery verification requires the three exact training evidence artifacts")
	}
	baselineSHA256, err := fileSHA256(baselinePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I1 training baseline: %w", err)
	}
	candidateSHA256, err := fileSHA256(candidatePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I1 training candidate: %w", err)
	}
	resourceSHA256, err := fileSHA256(resourcePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I1 training resource report: %w", err)
	}
	if baselineSHA256 != freeze.BaselineArtifactSHA256 || candidateSHA256 != freeze.CandidateArtifactSHA256 ||
		resourceSHA256 != freeze.ResourceReportSHA256 {
		return fmt.Errorf("SP-I1 frozen training evidence digests differ from the discovery freeze")
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return fmt.Errorf("read frozen SP-I1 training baseline: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return fmt.Errorf("read frozen SP-I1 training candidate: %w", err)
	}
	resource, err := loadSPI1ResourceReport(resourcePath)
	if err != nil {
		return err
	}
	if resource.ArtifactSHA256 != candidateSHA256 {
		return fmt.Errorf("SP-I1 frozen training resource report is not bound to the candidate artifact")
	}
	recomputed, err := buildSPI1QualificationReport(baseline, candidate, resource, SPI1QualificationOptions{
		Seed: freeze.Seed, Confidence: freeze.Confidence, BootstrapCount: freeze.BootstrapCount,
		Protocol: referencePairProtocolDiscovery, SourceArchiveSHA256: freeze.SourceArchiveSHA256,
	})
	if err != nil {
		return fmt.Errorf("recompute frozen SP-I1 training discovery: %w", err)
	}
	recomputed.BaselineArtifactSHA256 = baselineSHA256
	recomputed.CandidateArtifactSHA256 = candidateSHA256
	recomputed.ResourceReportSHA256 = resourceSHA256
	if !reflect.DeepEqual(recomputed, *discovery) {
		return fmt.Errorf("SP-I1 discovery report differs from its recomputed frozen training evidence")
	}
	return nil
}

func validateSPI1Corpus(corpus ScaleCorpus, cohort spI1CanonicalCohort) error {
	if len(corpus.Cases) != len(cohort.keys) {
		return fmt.Errorf("SP-I1 holdout capture requires exactly the frozen four-training/three-holdout cohort")
	}
	seen := map[performanceKey]struct{}{}
	resolved := make([]ResolvedCaseSelector, 0, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
		if _, expected := cohort.keys[key]; !expected {
			return fmt.Errorf("SP-I1 holdout capture contains unexpected case %s/%s", testCase.Dataset, testCase.Name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("SP-I1 holdout capture duplicates case %s/%s", testCase.Dataset, testCase.Name)
		}
		seen[key] = struct{}{}
		if filepath.Base(testCase.Source) != "generated_sp_i1_inbound_v1.json" ||
			testCase.Category != "generated_shortest_path_v2" || sqlFingerprint(testCase.Cypher) != spI1QuerySHA256 ||
			testCase.Shape.FallbackExpectation != "forbidden" || testCase.Shape.Direction != "inbound" ||
			testCase.Shape.RelationshipKindCount != 1 || !slices.Equal(testCase.Shape.EdgeKinds, []string{"Traverse"}) ||
			testCase.Shape.MinDepth == nil || *testCase.Shape.MinDepth != 1 ||
			testCase.Shape.MaxDepth == nil || *testCase.Shape.MaxDepth != 64 ||
			!testCase.Shape.PathMaterializationRequired ||
			!slices.Equal(testCase.CandidateModes, []ExecutionMode{ModePostgresSQL, ModeNeo4j}) {
			return fmt.Errorf("SP-I1 holdout capture changes frozen declaration %s/%s", testCase.Dataset, testCase.Name)
		}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		if testCase.Shape.QualificationSplit != expectedSplit {
			return fmt.Errorf("SP-I1 holdout capture changes frozen split for %s/%s", testCase.Dataset, testCase.Name)
		}
		resolved = append(resolved, ResolvedCaseSelector{Dataset: testCase.Dataset, Name: testCase.Name, Category: testCase.Category})
	}
	if !orientationV2KeySetsEqual(seen, cohort.keys) ||
		declarationSHA256(corpus.DeclaredBackends()) != cohort.declarationSHA256 ||
		resolvedSelectionSHA256(resolved) != cohort.fullResolvedSHA256 ||
		corpusIdentity(corpus) != cohort.fullCorpusSHA256 {
		return fmt.Errorf("SP-I1 holdout capture does not match the exact frozen declaration, selection, and corpus digests")
	}
	return nil
}

func validateCurrentSPI1Source(sourceCommit, sourceArchive, dirtyDiff, binary string) error {
	currentCommit := strings.TrimSpace(commandOutput("git", "rev-parse", "HEAD"))
	currentArchive, err := sourceArchiveSHA256()
	if err != nil {
		return err
	}
	currentDiff := workingTreeSHA256()
	currentBinary := executableSHA256()
	if currentCommit == "" || currentCommit == "unknown" || sourceCommit != currentCommit ||
		!lowercaseSHA256(sourceArchive) || sourceArchive != currentArchive ||
		dirtyDiff != cleanWorkingTreeSHA256() || currentDiff != cleanWorkingTreeSHA256() ||
		!lowercaseSHA256(binary) || binary != currentBinary {
		return fmt.Errorf("SP-I1 evidence requires the current clean committed source archive and exact running binary")
	}
	return nil
}

func loadSPI1ResourceReport(path string) (ResourceGateReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ResourceGateReport{}, fmt.Errorf("read SP-I1 resource report: %w", err)
	}
	report := ResourceGateReport{}
	if err := json.Unmarshal(raw, &report); err != nil {
		return ResourceGateReport{}, fmt.Errorf("decode SP-I1 resource report: %w", err)
	}
	if report.Version != resourceGateVersion || !lowercaseSHA256(report.ArtifactSHA256) {
		return ResourceGateReport{}, fmt.Errorf("SP-I1 resource report must be checksummed schema v%d", resourceGateVersion)
	}
	return report, nil
}

func loadSPI1QualificationReport(path string) (*SPI1QualificationReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	report := &SPI1QualificationReport{}
	if err := json.Unmarshal(raw, report); err != nil {
		return nil, fmt.Errorf("decode SP-I1 qualification report: %w", err)
	}
	return report, nil
}

func loadSPI1FreezeManifest(path string) (*SPI1QualificationFreezeManifest, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	manifest := &SPI1QualificationFreezeManifest{}
	if err := json.Unmarshal(raw, manifest); err != nil {
		return nil, "", fmt.Errorf("decode SP-I1 freeze manifest: %w", err)
	}
	digest := sha256.Sum256(raw)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func writeSPI1QualificationReport(path string, report SPI1QualificationReport) (err error) {
	if path == "" {
		return fmt.Errorf("SP-I1 qualification requires an explicit report output path")
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func writeSPI1FreezeManifest(path, discoveryReportPath string, report SPI1QualificationReport) (err error) {
	if path == "" || discoveryReportPath == "" {
		return fmt.Errorf("SP-I1 discovery freeze requires report and manifest output paths")
	}
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return err
	}
	if report.Protocol != referencePairProtocolDiscovery || report.CohortDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		report.ResolvedSelectionSHA256 != cohort.trainingResolvedSHA256 || report.CorpusSHA256 != cohort.trainingCorpusSHA256 ||
		report.TrainingCases != len(cohort.trainingKeys) || report.HoldoutCases != 0 ||
		report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.BootstrapCount != defaultBootstrapCount ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || !equalSPI1Caps(report.Caps, spI1QualificationCaps()) ||
		!lowercaseSHA256(report.BaselineArtifactSHA256) || !lowercaseSHA256(report.CandidateArtifactSHA256) ||
		!lowercaseSHA256(report.ResourceReportSHA256) {
		return fmt.Errorf("SP-I1 discovery freeze requires the exact clean training-only report")
	}
	discoveryReportSHA256, err := fileSHA256(discoveryReportPath)
	if err != nil {
		return err
	}
	manifest := SPI1QualificationFreezeManifest{
		Version:                   spI1FreezeVersion,
		Baseline:                  report.Baseline,
		Candidate:                 report.Candidate,
		Policy:                    report.Policy,
		QuerySHA256:               report.QuerySHA256,
		Caps:                      report.Caps,
		Seed:                      report.Seed,
		Confidence:                report.Confidence,
		BootstrapCount:            report.BootstrapCount,
		SourceCommit:              report.SourceCommit,
		SourceArchiveSHA256:       report.SourceArchiveSHA256,
		DirtyDiffSHA256:           report.DirtyDiffSHA256,
		BinarySHA256:              report.BinarySHA256,
		TrainingDeclarationSHA256: cohort.trainingDeclarationSHA256,
		HoldoutDeclarationSHA256:  cohort.holdoutDeclarationSHA256,
		FullDeclarationSHA256:     cohort.declarationSHA256,
		TrainingCorpusSHA256:      cohort.trainingCorpusSHA256,
		FullCorpusSHA256:          cohort.fullCorpusSHA256,
		TrainingResolvedSHA256:    cohort.trainingResolvedSHA256,
		FullResolvedSHA256:        cohort.fullResolvedSHA256,
		BaselineArtifactSHA256:    report.BaselineArtifactSHA256,
		CandidateArtifactSHA256:   report.CandidateArtifactSHA256,
		ResourceReportSHA256:      report.ResourceReportSHA256,
		DiscoveryReportSHA256:     discoveryReportSHA256,
		TrainingPassed:            report.TrainingPassed,
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(manifest)
	closeErr := output.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}

func validateDistinctSPI1Paths(paths map[string]string) error {
	names := make([]string, 0, len(paths))
	for name, path := range paths {
		if path != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	type resolvedPath struct {
		name string
		info os.FileInfo
	}
	resolved := map[string]resolvedPath{}
	var existing []resolvedPath
	for _, name := range names {
		absolute, err := filepath.Abs(filepath.Clean(paths[name]))
		if err != nil {
			return fmt.Errorf("resolve SP-I1 %s: %w", name, err)
		}
		if evaluated, err := filepath.EvalSymlinks(absolute); err == nil {
			absolute = evaluated
		} else if evaluatedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(absolute)); parentErr == nil {
			absolute = filepath.Join(evaluatedParent, filepath.Base(absolute))
		}
		if prior, duplicate := resolved[absolute]; duplicate {
			return fmt.Errorf("SP-I1 %s and %s must use distinct paths", prior.name, name)
		}
		current := resolvedPath{name: name}
		if info, err := os.Stat(paths[name]); err == nil {
			current.info = info
			for _, prior := range existing {
				if prior.info != nil && os.SameFile(prior.info, info) {
					return fmt.Errorf("SP-I1 %s and %s must not alias the same file", prior.name, name)
				}
			}
			existing = append(existing, current)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect SP-I1 %s path: %w", name, err)
		}
		resolved[absolute] = current
	}
	return nil
}

func selectedCorpusContainsSPI1Holdout(corpus ScaleCorpus) bool {
	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return true
	}
	for _, testCase := range corpus.Cases {
		key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			return true
		}
	}
	return false
}

// selectRunnableScaleCorpus keeps the protected SP-I1 holdout out of ordinary
// GraphBench selection. The holdout becomes selectable only through its exact
// protocol tag or an exact case name; database capture then passes through the
// freeze checks in main before any target is opened.
func selectRunnableScaleCorpus(corpus ScaleCorpus, selectors CorpusSelectors) (ScaleCorpus, SelectionManifest, error) {
	if err := validateCorpusSelectors(corpus, selectors); err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	includeProtected := slices.Contains(selectors.Tags, spI1HoldoutTag)
	if !includeProtected && len(selectors.Cases) > 0 {
		protectedNames := make(map[string]struct{}, len(spI1CanonicalCases))
		for _, testCase := range spI1CanonicalCases {
			if testCase.split == "holdout" {
				protectedNames[testCase.name] = struct{}{}
			}
		}
		for _, name := range selectors.Cases {
			if _, protected := protectedNames[name]; protected {
				includeProtected = true
				break
			}
		}
	}
	if includeProtected {
		return selectScaleCorpusValidated(corpus, selectors)
	}

	cohort, err := canonicalSPI1Cohort()
	if err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	filtered := ScaleCorpus{Cases: make([]ScaleCase, 0, len(corpus.Cases))}
	protected := ScaleCorpus{Cases: make([]ScaleCase, 0, len(cohort.holdoutKeys))}
	for _, testCase := range corpus.Cases {
		key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
		if _, isProtected := cohort.holdoutKeys[key]; isProtected {
			protected.Cases = append(protected.Cases, testCase)
			continue
		}
		filtered.Cases = append(filtered.Cases, testCase)
	}
	selected, manifest, err := selectScaleCorpusValidated(filtered, selectors)
	if err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	manifest.FullDeclarationCount = len(corpus.DeclaredBackends())
	manifest.OmittedDeclarationCount = manifest.FullDeclarationCount - manifest.SelectedDeclarationCount
	manifest.ProtectedDeclarationCount = len(protected.DeclaredBackends())
	manifest.ProtectedDeclarationSHA256 = declarationSHA256(protected.DeclaredBackends())
	return selected, manifest, nil
}

func validateSPI1HoldoutCaptureConfig(cfg config) error {
	if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL || cfg.ExistingGraph || cfg.Discovery {
		return fmt.Errorf("SP-I1 holdout capture requires one managed PostgreSQL fixed-confirmation mode")
	}
	if cfg.Iterations < 50 || cfg.WarmupIterations < 20 || cfg.PoolSize != 1 || len(cfg.Concurrency) != 0 {
		return fmt.Errorf("SP-I1 holdout capture requires at least 50 samples, 20 warmups, pool size 1, and no concurrency block")
	}
	if cfg.Round < 1 || cfg.Round > 20 || cfg.Block != cfg.Round || cfg.ArmOrder < 1 || cfg.ArmOrder > 2 ||
		strings.TrimSpace(cfg.RunUUID) == "" {
		return fmt.Errorf("SP-I1 holdout capture requires rounds 1-20, block equal to round, a two-arm order, and an explicit shared run UUID")
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	candidate := string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
	expectedArm, expectedOrder := "", 0
	switch cfg.PostgresForceShortest {
	case baseline:
		expectedArm = "sp-i1-s4"
		expectedOrder = 1
		if cfg.Round%2 == 0 {
			expectedOrder = 2
		}
	case candidate:
		expectedArm = "sp-i1-candidate"
		expectedOrder = 2
		if cfg.Round%2 == 0 {
			expectedOrder = 1
		}
	default:
		return fmt.Errorf("SP-I1 holdout capture must force exact S4 or guarded canonical I1")
	}
	if cfg.Arm != expectedArm || cfg.ArmOrder != expectedOrder {
		return fmt.Errorf("SP-I1 holdout capture round %d requires arm %q at order %d", cfg.Round, expectedArm, expectedOrder)
	}
	if !cfg.PostgresRepeatableRead || cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic ||
		cfg.PostgresProductionManifest != "" || cfg.PostgresForceExpansion != "" ||
		cfg.PostgresExpansionOrientationShadow || cfg.PostgresExpansionOrientationTournament ||
		cfg.PostgresReferences || len(cfg.PostgresReferenceArms) != 0 || cfg.Baseline != "" ||
		cfg.BundleDir != "" || len(cfg.BundleEvidence) != 0 {
		return fmt.Errorf("SP-I1 holdout capture requires forced Repeatable Read with diagnostic telemetry and no supplemental PostgreSQL arms")
	}
	if cfg.OutputJSONL == "" || cfg.Round > 1 && !cfg.AppendJSONL {
		return fmt.Errorf("SP-I1 holdout capture requires a JSONL output and append mode after round 1")
	}
	return validateDistinctSPI1Paths(map[string]string{
		"freeze manifest": cfg.SPI1Freeze, "discovery report": cfg.SPI1DiscoveryReport,
		"training baseline artifact":  cfg.SPI1TrainingBaseline,
		"training candidate artifact": cfg.SPI1TrainingCandidate,
		"training resource report":    cfg.SPI1TrainingResource,
		"capture JSONL":               cfg.OutputJSONL, "capture summary": cfg.Summary, "capture JSON summary": cfg.SummaryJSON,
	})
}
