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
	// spI2QualificationVersion reserves the stable protocol value used to recognize sp i2 qualification version across artifacts and executions.
	spI2QualificationVersion = 1

	// spI2FreezeVersion reserves the stable protocol value used to recognize sp i2 freeze version across artifacts and executions.
	spI2FreezeVersion = 1

	// spI2TrainingTag reserves the stable protocol value used to recognize sp i2 training tag across artifacts and executions.
	spI2TrainingTag = "sp-i2-distance-v1-training"

	// spI2HoldoutTag reserves the stable protocol value used to recognize sp i2 holdout tag across artifacts and executions.
	spI2HoldoutTag = "sp-i2-distance-v1-holdout"

	// spI2QuerySHA256 reserves the stable protocol value used to recognize sp i2 query sha256 across artifacts and executions.
	spI2QuerySHA256 = "69c1d7778963a742dbac8adeff01213850b60d1ed61858832a8315aa5184b3db"

	// spI2TrainingCorpusSHA256 reserves the stable protocol value used to recognize sp i2 training corpus sha256 across artifacts and executions.
	spI2TrainingCorpusSHA256 = "33294507fdf87e5fed07e702f7c8c00d5abc7c0d19b9b9472b740b244531e9f9"

	// spI2FullCorpusSHA256 reserves the stable protocol value used to recognize sp i2 full corpus sha256 across artifacts and executions.
	spI2FullCorpusSHA256 = "eca42bb762acc379673edffac130e88432dcf089261f1295451e03ea7f1fa35a"

	// spI2TrainingResolvedSHA reserves the stable protocol value used to recognize sp i2 training resolved sha across artifacts and executions.
	spI2TrainingResolvedSHA = "3c05a0f65efed8d08d79953d8398c013f6ea52b2a53f9900af7d7f68626cf4fb"

	// spI2FullResolvedSHA reserves the stable protocol value used to recognize sp i2 full resolved sha across artifacts and executions.
	spI2FullResolvedSHA = "2d7acf8e3d9904b9cf5fefb5c83f2740a5b184c6dfcd682d5115250cc5b19fe5"
)

// spI2CanonicalCases freezes the training and holdout workloads admitted to SP-I2 qualification.
var spI2CanonicalCases = []struct {
	// dataset identifies the generated fixture containing the workload.
	dataset string

	// name identifies the workload within the fixture dataset.
	name string

	// split assigns the workload to training or unopened holdout evidence.
	split string
}{
	{
		dataset: "generated_shortest_paths_v2_d3_o0_r64_fo0_fi32_l2_k0_t0_w0_x3_p0_c0_s0",
		name:    "GSP-I2-V1-TRAIN-D03-RI064-FI032-full",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d8_o0_r128_fo0_fi64_l4_k0_t0_w0_x8_p0_c0_s0",
		name:    "GSP-I2-V1-TRAIN-D08-RI128-FI064-early-d02",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d8_o0_r128_fo0_fi64_l4_k0_t0_w0_x8_p0_c0_s0",
		name:    "GSP-I2-V1-TRAIN-D08-RI128-FI064-full",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d16_o0_r256_fo0_fi512_l8_k0_t0_w0_x16_p0_c0_s0",
		name:    "GSP-I2-V1-TRAIN-D16-RI256-FI512-full",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d16_o0_r256_fo0_fi512_l8_k0_t0_w0_x16_p0_c0_s0",
		name:    "GSP-I2-V1-TRAIN-D16-RI256-FI512-disconnected",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d6_o0_r0_fo0_fi0_l0_k0_t0_w0_x6_p0_c1_s0",
		name:    "GSP-I2-V1-TRAIN-cycle-control",
		split:   "training",
	},
	{
		dataset: "generated_shortest_paths_v2_d5_o0_r47_fo0_fi23_l2_k0_t0_w0_x5_p0_c0_s0",
		name:    "GSP-I2-V1-HOLDOUT-D05-RI047-FI023-full",
		split:   "holdout",
	},
	{
		dataset: "generated_shortest_paths_v2_d13_o0_r191_fo0_fi383_l7_k0_t0_w0_x13_p0_c0_s0",
		name:    "GSP-I2-V1-HOLDOUT-D13-RI191-FI383-full",
		split:   "holdout",
	},
	{
		dataset: "generated_shortest_paths_v2_d13_o0_r191_fo0_fi383_l7_k0_t0_w0_x13_p0_c0_s0",
		name:    "GSP-I2-V1-HOLDOUT-D13-RI191-FI383-early-d03",
		split:   "holdout",
	},
	{
		dataset: "generated_shortest_paths_v2_d21_o0_r127_fo0_fi255_l11_k0_t0_w0_x21_p0_c0_s0",
		name:    "GSP-I2-V1-HOLDOUT-D21-RI127-FI255-disconnected",
		split:   "holdout",
	},
}

// spI2CanonicalCohort groups state that must remain consistent while processing sp i2 canonical cohort.
type spI2CanonicalCohort struct {
	// keys retains the keys while spI2CanonicalCohort is assembled or evaluated.
	keys map[performanceKey]struct{}
	// trainingKeys retains the training keys while spI2CanonicalCohort is assembled or evaluated.
	trainingKeys map[performanceKey]struct{}
	// holdoutKeys retains the holdout keys while spI2CanonicalCohort is assembled or evaluated.
	holdoutKeys map[performanceKey]struct{}
	// declarationSHA256 binds the referenced declaration content by SHA-256 digest.
	declarationSHA256 string
	// trainingDeclarationSHA256 binds the referenced training declaration content by SHA-256 digest.
	trainingDeclarationSHA256 string
	// holdoutDeclarationSHA256 binds the referenced holdout declaration content by SHA-256 digest.
	holdoutDeclarationSHA256 string
	// trainingCorpusSHA256 binds the referenced training corpus content by SHA-256 digest.
	trainingCorpusSHA256 string
	// fullCorpusSHA256 binds the referenced full corpus content by SHA-256 digest.
	fullCorpusSHA256 string
	// trainingResolvedSHA256 binds the referenced training resolved content by SHA-256 digest.
	trainingResolvedSHA256 string
	// fullResolvedSHA256 binds the referenced full resolved content by SHA-256 digest.
	fullResolvedSHA256 string
}

// spI2CanonicalDeclaration groups state that must remain consistent while processing sp i2 canonical declaration.
type spI2CanonicalDeclaration struct {
	// testCase retains the test case while spI2CanonicalDeclaration is assembled or evaluated.
	testCase ScaleCase
	// fixture retains the fixture while spI2CanonicalDeclaration is assembled or evaluated.
	fixture FixtureMetadata
}

// canonicalSPI2Declarations resolves the frozen SP-I2 workload declarations and fixture metadata.
func canonicalSPI2Declarations() (map[performanceKey]spI2CanonicalDeclaration, error) {
	repositoryRoot := strings.TrimSpace(commandOutput("git", "rev-parse", "--show-toplevel"))
	if repositoryRoot == "" || repositoryRoot == "unknown" {
		return nil, fmt.Errorf("locate repository root for frozen SP-I2 declarations")
	}

	if corpus, err := loadScaleCorpus(filepath.Join(repositoryRoot, "benchmark", "testdata", "scale")); err != nil {
		return nil, fmt.Errorf("load frozen SP-I2 declarations: %w", err)
	} else if cohort, err := canonicalSPI2Cohort(); err != nil {
		return nil, err
	} else {
		declarations := make(map[performanceKey]spI2CanonicalDeclaration, len(cohort.keys))
		for _, testCase := range corpus.Cases {
			key := performanceKey{
				dataset: testCase.Dataset,
				name:    testCase.Name,
				backend: ModePostgresSQL,
			}
			if _, expected := cohort.keys[key]; !expected {
				continue
			}
			if _, duplicate := declarations[key]; duplicate {
				return nil, fmt.Errorf("frozen SP-I2 corpus duplicates %s/%s", key.dataset, key.name)
			}
			if fixture, err := fixtureMetadata("unused", testCase.Dataset); err != nil {
				return nil, fmt.Errorf("derive frozen SP-I2 fixture %s: %w", testCase.Dataset, err)
			} else {
				declarations[key] = spI2CanonicalDeclaration{
					testCase: testCase,
					fixture:  fixture,
				}
			}
		}
		if len(declarations) != len(cohort.keys) {
			return nil, fmt.Errorf("frozen SP-I2 corpus omits canonical declarations")
		}

		return declarations, nil
	}
}

// canonicalSPI2Cohort builds the immutable training and holdout membership used by qualification.
func canonicalSPI2Cohort() (spI2CanonicalCohort, error) {
	cohort := spI2CanonicalCohort{
		keys:                   map[performanceKey]struct{}{},
		trainingKeys:           map[performanceKey]struct{}{},
		holdoutKeys:            map[performanceKey]struct{}{},
		trainingCorpusSHA256:   spI2TrainingCorpusSHA256,
		fullCorpusSHA256:       spI2FullCorpusSHA256,
		trainingResolvedSHA256: spI2TrainingResolvedSHA,
		fullResolvedSHA256:     spI2FullResolvedSHA,
	}
	var full, training, holdout []DeclaredCaseBackend
	for _, testCase := range spI2CanonicalCases {
		key := performanceKey{
			dataset: testCase.dataset,
			name:    testCase.name,
			backend: ModePostgresSQL,
		}
		if _, duplicate := cohort.keys[key]; duplicate || !strings.HasPrefix(testCase.dataset, "generated_shortest_paths_v2_") {
			return spI2CanonicalCohort{}, fmt.Errorf("frozen SP-I2 cohort contains an invalid declaration")
		}
		cohort.keys[key] = struct{}{}
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			item := DeclaredCaseBackend{
				Dataset: key.dataset,
				Name:    key.name,
				Backend: backend,
			}
			full = append(full, item)
			if testCase.split == "training" {
				training = append(training, item)
			} else if testCase.split == "holdout" {
				holdout = append(holdout, item)
			} else {
				return spI2CanonicalCohort{}, fmt.Errorf("frozen SP-I2 cohort contains an invalid split")
			}
		}
		if testCase.split == "training" {
			cohort.trainingKeys[key] = struct{}{}
		} else {
			cohort.holdoutKeys[key] = struct{}{}
		}
	}
	if len(cohort.trainingKeys) != 6 || len(cohort.holdoutKeys) != 4 || len(cohort.keys) != 10 {
		return spI2CanonicalCohort{}, fmt.Errorf("frozen SP-I2 cohort must contain exactly 6 training and 4 holdout cases")
	}
	cohort.declarationSHA256 = declarationSHA256(full)
	cohort.trainingDeclarationSHA256 = declarationSHA256(training)
	cohort.holdoutDeclarationSHA256 = declarationSHA256(holdout)
	return cohort, nil
}

// spI2QualificationCaps returns the resource limits enforced for sp i2 qualification.
func spI2QualificationCaps() map[string]int64 {
	return spI2PromotionCaps()
}

// spI2TelemetryCaps returns the resource limits enforced for sp i2 telemetry.
func spI2TelemetryCaps() map[string]int64 {
	return map[string]int64{
		"state_rows":    optimize.ShortestPathI2QualifiedStateLimit,
		"frontier_rows": optimize.ShortestPathI2QualifiedFrontierLimit,
		"queue_rows":    optimize.ShortestPathI2QualifiedFrontierLimit,
	}
}

// spI2SummaryCaps freezes the translator's conservative queue alias as part
// of the emitted runtime identity while the resource report keeps only the
// independently enforced state and frontier dimensions.
func spI2SummaryCaps() map[string]int64 {
	return map[string]int64{
		"state_rows":    optimize.ShortestPathI2QualifiedStateLimit,
		"frontier_rows": optimize.ShortestPathI2QualifiedFrontierLimit,
		"queue_rows":    optimize.ShortestPathI2QualifiedFrontierLimit,
	}
}

// SPI2QualificationOptions configures spi2 qualification.
type SPI2QualificationOptions struct {
	// Seed makes randomized statistical procedures reproducible.
	Seed int64
	// Confidence sets the requested statistical confidence level.
	Confidence float64
	// BootstrapCount records the number of bootstrap count.
	BootstrapCount int
	// Protocol identifies the protocol.
	Protocol string
	// Training evidence paths make confirmation independently recompute the
	// discovery decision instead of trusting only a mutable report and freeze.
	TrainingBaselinePath string
	// TrainingCandidatePath identifies the filesystem training candidate path.
	TrainingCandidatePath string
	// TrainingResourcePath identifies the filesystem training resource path.
	TrainingResourcePath string
	// SourceArchiveSHA256 binds the report to git archive HEAD. Report-mode
	// callers populate it from the current committed tree; tests may supply a
	// synthetic digest without invoking Git.
	SourceArchiveSHA256 string
	// Freeze supplies the freeze input to the SPI2QualificationOptions contract.
	Freeze *SPI2QualificationFreezeManifest
	// Discovery supplies the discovery input to the SPI2QualificationOptions contract.
	Discovery *SPI2QualificationReport
}

// SPI2QualificationCase records the evidence and decision for one spi2 qualification workload.
type SPI2QualificationCase struct {
	// Dataset identifies the fixture dataset that supplies the workload graph.
	Dataset string `json:"dataset"`
	// Name identifies the name.
	Name string `json:"name"`
	// QualificationSplit assigns the workload to training, holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// QualificationRole distinguishes improvement targets from preregistered adverse controls.
	QualificationRole string `json:"qualification_role"`
	// Rounds records the number of rounds.
	Rounds int `json:"matched_rounds"`
	// BaselineSamples supplies the baseline samples input to the SPI2QualificationCase contract.
	BaselineSamples int `json:"baseline_samples"`
	// CandidateSamples supplies the candidate samples input to the SPI2QualificationCase contract.
	CandidateSamples int `json:"candidate_samples"`
	// MedianRatio supplies the median ratio input to the SPI2QualificationCase contract.
	MedianRatio RatioInterval `json:"median_ratio_to_s4"`
	// MedianSaving supplies the median saving input to the SPI2QualificationCase contract.
	MedianSaving DurationInterval `json:"median_saving_vs_s4"`
	// P95Ratio supplies the p95 ratio input to the SPI2QualificationCase contract.
	P95Ratio RatioInterval `json:"p95_ratio_to_s4"`
	// Material indicates whether material applies.
	Material bool `json:"material"`
	// P95Contained indicates whether p95 contained applies.
	P95Contained bool `json:"p95_contained"`
	// ResourcePassed indicates whether resource passed applies.
	ResourcePassed bool `json:"resource_passed"`
	// RuntimeBranch supplies the runtime branch input to the SPI2QualificationCase contract.
	RuntimeBranch string `json:"runtime_branch"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// Reasons explains each failed or inapplicable validation gate.
	Reasons []string `json:"reasons,omitempty"`
}

// SPI2QualificationReport records the evidence and outcome produced by spi2 qualification.
type SPI2QualificationReport struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// Protocol identifies the protocol.
	Protocol string `json:"protocol"`
	// Baseline identifies the incumbent execution strategy used for comparison.
	Baseline string `json:"baseline"`
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate"`
	// Policy identifies the policy.
	Policy string `json:"policy"`
	// QuerySHA256 binds the referenced query content by SHA-256 digest.
	QuerySHA256 string `json:"query_sha256"`
	// Seed makes randomized statistical procedures reproducible.
	Seed int64 `json:"seed"`
	// Confidence sets the requested statistical confidence level.
	Confidence float64 `json:"confidence_level"`
	// BootstrapCount records the number of bootstrap count.
	BootstrapCount int `json:"bootstrap_count"`
	// MaterialityRatio supplies the materiality ratio input to the SPI2QualificationReport contract.
	MaterialityRatio float64 `json:"materiality_ratio_upper_limit"`
	// MaterialityAbsolute supplies the materiality absolute input to the SPI2QualificationReport contract.
	MaterialityAbsolute time.Duration `json:"materiality_absolute_lower_limit"`
	// P95RatioLimit supplies the p95 ratio limit input to the SPI2QualificationReport contract.
	P95RatioLimit float64 `json:"p95_ratio_upper_limit"`
	// AdverseRatioLimit caps relative overhead for preregistered adverse controls.
	AdverseRatioLimit float64 `json:"adverse_ratio_upper_limit"`
	// AdverseAbsoluteLimit caps absolute overhead for preregistered adverse controls.
	AdverseAbsoluteLimit time.Duration `json:"adverse_absolute_upper_limit"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// SourceCommit supplies the source commit input to the SPI2QualificationReport contract.
	SourceCommit string `json:"source_commit"`
	// SourceArchiveSHA256 binds the referenced source archive content by SHA-256 digest.
	SourceArchiveSHA256 string `json:"source_archive_sha256"`
	// DirtyDiffSHA256 binds the referenced dirty diff content by SHA-256 digest.
	DirtyDiffSHA256 string `json:"dirty_diff_sha256"`
	// BinarySHA256 binds the referenced binary content by SHA-256 digest.
	BinarySHA256 string `json:"binary_sha256"`
	// CorpusSHA256 binds the referenced corpus content by SHA-256 digest.
	CorpusSHA256 string `json:"corpus_sha256"`
	// CohortDeclarationSHA256 binds the referenced cohort declaration content by SHA-256 digest.
	CohortDeclarationSHA256 string `json:"cohort_declaration_sha256"`
	// ResolvedSelectionSHA256 binds the referenced resolved selection content by SHA-256 digest.
	ResolvedSelectionSHA256 string `json:"resolved_selection_sha256"`
	// TrainingDeclarationSHA256 binds the referenced training declaration content by SHA-256 digest.
	TrainingDeclarationSHA256 string `json:"training_declaration_sha256"`
	// HoldoutDeclarationSHA256 binds the referenced holdout declaration content by SHA-256 digest.
	HoldoutDeclarationSHA256 string `json:"holdout_declaration_sha256"`
	// FullDeclarationSHA256 binds the referenced full declaration content by SHA-256 digest.
	FullDeclarationSHA256 string `json:"full_declaration_sha256"`
	// TrainingCorpusSHA256 binds the referenced training corpus content by SHA-256 digest.
	TrainingCorpusSHA256 string `json:"training_corpus_sha256"`
	// FullCorpusSHA256 binds the referenced full corpus content by SHA-256 digest.
	FullCorpusSHA256 string `json:"full_corpus_sha256"`
	// BaselineArtifactSHA256 binds the referenced baseline artifact content by SHA-256 digest.
	BaselineArtifactSHA256 string `json:"baseline_artifact_sha256,omitempty"`
	// CandidateArtifactSHA256 binds the referenced candidate artifact content by SHA-256 digest.
	CandidateArtifactSHA256 string `json:"candidate_artifact_sha256,omitempty"`
	// ResourceReportSHA256 binds the referenced resource report content by SHA-256 digest.
	ResourceReportSHA256 string `json:"resource_report_sha256,omitempty"`
	// FreezeManifestSHA256 binds the referenced freeze manifest content by SHA-256 digest.
	FreezeManifestSHA256 string `json:"freeze_manifest_sha256,omitempty"`
	// EvidencePassed indicates whether evidence passed applies.
	EvidencePassed bool `json:"evidence_passed"`
	// TrainingCases supplies the training cases input to the SPI2QualificationReport contract.
	TrainingCases int `json:"training_cases"`
	// HoldoutCases supplies the holdout cases input to the SPI2QualificationReport contract.
	HoldoutCases int `json:"holdout_cases"`
	// TrainingPassed indicates whether training passed applies.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed indicates whether holdout passed applies.
	HoldoutPassed bool `json:"holdout_passed"`
	// QualificationPassed indicates whether qualification passed applies.
	QualificationPassed bool `json:"qualification_passed"`
	// Cases contains the per-workload evidence underlying the aggregate decision.
	Cases []SPI2QualificationCase `json:"cases"`
}

// SPI2QualificationFreezeManifest binds the immutable inputs authorized for spi2 qualification freeze.
type SPI2QualificationFreezeManifest struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// Baseline identifies the incumbent execution strategy used for comparison.
	Baseline string `json:"baseline"`
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate"`
	// Policy identifies the policy.
	Policy string `json:"policy"`
	// QuerySHA256 binds the referenced query content by SHA-256 digest.
	QuerySHA256 string `json:"query_sha256"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// Seed makes randomized statistical procedures reproducible.
	Seed int64 `json:"seed"`
	// Confidence sets the requested statistical confidence level.
	Confidence float64 `json:"confidence_level"`
	// BootstrapCount records the number of bootstrap count.
	BootstrapCount int `json:"bootstrap_count"`
	// SourceCommit supplies the source commit input to the SPI2QualificationFreezeManifest contract.
	SourceCommit string `json:"source_commit"`
	// SourceArchiveSHA256 binds the referenced source archive content by SHA-256 digest.
	SourceArchiveSHA256 string `json:"source_archive_sha256"`
	// DirtyDiffSHA256 binds the referenced dirty diff content by SHA-256 digest.
	DirtyDiffSHA256 string `json:"dirty_diff_sha256"`
	// BinarySHA256 binds the referenced binary content by SHA-256 digest.
	BinarySHA256 string `json:"binary_sha256"`
	// TrainingDeclarationSHA256 binds the referenced training declaration content by SHA-256 digest.
	TrainingDeclarationSHA256 string `json:"training_declaration_sha256"`
	// HoldoutDeclarationSHA256 binds the referenced holdout declaration content by SHA-256 digest.
	HoldoutDeclarationSHA256 string `json:"holdout_declaration_sha256"`
	// FullDeclarationSHA256 binds the referenced full declaration content by SHA-256 digest.
	FullDeclarationSHA256 string `json:"full_declaration_sha256"`
	// TrainingCorpusSHA256 binds the referenced training corpus content by SHA-256 digest.
	TrainingCorpusSHA256 string `json:"training_corpus_sha256"`
	// FullCorpusSHA256 binds the referenced full corpus content by SHA-256 digest.
	FullCorpusSHA256 string `json:"full_corpus_sha256"`
	// TrainingResolvedSHA256 binds the referenced training resolved content by SHA-256 digest.
	TrainingResolvedSHA256 string `json:"training_resolved_selection_sha256"`
	// FullResolvedSHA256 binds the referenced full resolved content by SHA-256 digest.
	FullResolvedSHA256 string `json:"full_resolved_selection_sha256"`
	// BaselineArtifactSHA256 binds the referenced baseline artifact content by SHA-256 digest.
	BaselineArtifactSHA256 string `json:"baseline_artifact_sha256"`
	// CandidateArtifactSHA256 binds the referenced candidate artifact content by SHA-256 digest.
	CandidateArtifactSHA256 string `json:"candidate_artifact_sha256"`
	// ResourceReportSHA256 binds the referenced resource report content by SHA-256 digest.
	ResourceReportSHA256 string `json:"resource_report_sha256"`
	// DiscoveryReportSHA256 binds the referenced discovery report content by SHA-256 digest.
	DiscoveryReportSHA256 string `json:"discovery_report_sha256"`
	// TrainingPassed indicates whether training passed applies.
	TrainingPassed bool `json:"training_passed"`
}

// spI2EvidenceIdentity groups state that must remain consistent while processing sp i2 evidence identity.
type spI2EvidenceIdentity struct {
	// sourceCommit retains the source commit while spI2EvidenceIdentity is assembled or evaluated.
	sourceCommit string
	// dirtyDiffSHA256 binds the referenced dirty diff content by SHA-256 digest.
	dirtyDiffSHA256 string
	// binarySHA256 binds the referenced binary content by SHA-256 digest.
	binarySHA256 string
	// corpusSHA256 binds the referenced corpus content by SHA-256 digest.
	corpusSHA256 string
	// declarationSHA256 binds the referenced declaration content by SHA-256 digest.
	declarationSHA256 string
	// resolvedSHA256 binds the referenced resolved content by SHA-256 digest.
	resolvedSHA256 string
}

// sourceArchiveSHA256 supports benchmark evidence processing for source archive sha256.
func spI2SourceArchiveSHA256() (string, error) {
	archive, err := exec.Command("git", "archive", "--format=tar", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("archive source commit: %w", err)
	}
	digest := sha256.Sum256(archive)
	return hex.EncodeToString(digest[:]), nil
}

// equalSPI2Caps returns the resource limits enforced for equal spi2.
func equalSPI2Caps(left, right map[string]int64) bool {
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

// spI2ProtocolRequirements groups state that must remain consistent while processing sp i2 protocol requirements.
type spI2ProtocolRequirements struct {
	// minimumWarmups retains the minimum warmups while spI2ProtocolRequirements is assembled or evaluated.
	minimumWarmups int
	// minimumRounds records the number of minimum rounds.
	minimumRounds int
	// maximumRounds records the number of maximum rounds.
	maximumRounds int
	// minimumSamples retains the minimum samples while spI2ProtocolRequirements is assembled or evaluated.
	minimumSamples int
	// protectedCount records the number of protected count.
	protectedCount int
	// protectedSHA retains the protected sha while spI2ProtocolRequirements is assembled or evaluated.
	protectedSHA string
	// expectedKeys retains the expected keys while spI2ProtocolRequirements is assembled or evaluated.
	expectedKeys map[performanceKey]struct{}
	// declarationSHA retains the declaration sha while spI2ProtocolRequirements is assembled or evaluated.
	declarationSHA string
	// corpusSHA retains the corpus sha while spI2ProtocolRequirements is assembled or evaluated.
	corpusSHA string
	// resolvedSHA retains the resolved sha while spI2ProtocolRequirements is assembled or evaluated.
	resolvedSHA string
}

// spI2QualificationSeries accumulates matched observations used to evaluate sp i2 qualification.
type spI2QualificationSeries struct {
	// baseline retains the baseline while spI2QualificationSeries is assembled or evaluated.
	baseline roundSamples
	// candidate retains the candidate while spI2QualificationSeries is assembled or evaluated.
	candidate roundSamples
	// runtimeBranch retains the runtime branch while spI2QualificationSeries is assembled or evaluated.
	runtimeBranch string
	// resourcePassed indicates whether resource passed applies.
	resourcePassed bool
}

// spI2Requirements supports benchmark evidence processing for sp i2 requirements.
func spI2Requirements(protocol string, cohort spI2CanonicalCohort) (spI2ProtocolRequirements, error) {
	switch protocol {
	case referencePairProtocolDiscovery:
		return spI2ProtocolRequirements{
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
		return spI2ProtocolRequirements{
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
		return spI2ProtocolRequirements{}, fmt.Errorf("unsupported SP-I2 qualification protocol %q", protocol)
	}
}

// buildSPI2QualificationReport builds spi2 qualification report.
func buildSPI2QualificationReport(
	baseline, candidate []CaseResult,
	resource ResourceGateReport,
	options SPI2QualificationOptions,
) (SPI2QualificationReport, error) {
	if options.Confidence != defaultConfidenceLevel || math.IsNaN(options.Confidence) || math.IsInf(options.Confidence, 0) {
		return SPI2QualificationReport{}, fmt.Errorf("SP-I2 qualification confidence must be the frozen %.4f", defaultConfidenceLevel)
	}
	if options.Seed != 1 {
		return SPI2QualificationReport{}, fmt.Errorf("SP-I2 qualification bootstrap seed must be the frozen value 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount != defaultBootstrapCount {
		return SPI2QualificationReport{}, fmt.Errorf("SP-I2 qualification bootstrap count must be the frozen value %d", defaultBootstrapCount)
	}
	if options.Protocol == "" {
		options.Protocol = referencePairProtocolConfirmation
	}
	if !lowercaseSHA256(options.SourceArchiveSHA256) {
		return SPI2QualificationReport{}, fmt.Errorf("SP-I2 source archive digest is missing or malformed")
	}

	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return SPI2QualificationReport{}, err
	}
	requirements, err := spI2Requirements(options.Protocol, cohort)
	if err != nil {
		return SPI2QualificationReport{}, err
	}
	identity, err := validateSPI2EvidenceIdentity(baseline, candidate, requirements)
	if err != nil {
		return SPI2QualificationReport{}, err
	}
	series, keys, err := collectSPI2QualificationSeries(baseline, candidate, resource, requirements)
	if err != nil {
		return SPI2QualificationReport{}, err
	}

	report := SPI2QualificationReport{
		Version:                   spI2QualificationVersion,
		Protocol:                  options.Protocol,
		Baseline:                  string(optimize.ShortestPathExecutorS4CanonicalDistance),
		Candidate:                 string(optimize.ShortestPathExecutorI2GuardedDistance),
		Policy:                    optimize.ShortestPathPolicyI2DistanceGuardedV1,
		QuerySHA256:               spI2QuerySHA256,
		Seed:                      options.Seed,
		Confidence:                options.Confidence,
		BootstrapCount:            options.BootstrapCount,
		MaterialityRatio:          0.95,
		MaterialityAbsolute:       100 * time.Microsecond,
		P95RatioLimit:             1.05,
		AdverseRatioLimit:         1.10,
		AdverseAbsoluteLimit:      100 * time.Microsecond,
		Caps:                      spI2QualificationCaps(),
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
		if err := validateSPI2Freeze(options.Freeze, options.Discovery, report, cohort); err != nil {
			return SPI2QualificationReport{}, err
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
			return SPI2QualificationReport{}, fmt.Errorf("%s/%s SP-I2 arms do not contain identical nonempty round sets", key.dataset, key.name)
		}
		rounds := sortedRounds(baselineRounds)
		if len(rounds) < requirements.minimumRounds || len(rounds) > requirements.maximumRounds {
			return SPI2QualificationReport{}, fmt.Errorf(
				"%s/%s requires %d-%d matched SP-I2 rounds, got %d",
				key.dataset, key.name, requirements.minimumRounds, requirements.maximumRounds, len(rounds),
			)
		}
		for _, round := range rounds {
			if len(baselineRounds[round]) < requirements.minimumSamples || len(candidateRounds[round]) < requirements.minimumSamples {
				return SPI2QualificationReport{}, fmt.Errorf(
					"%s/%s round %d requires at least %d warm samples per SP-I2 arm, got %d/%d",
					key.dataset, key.name, round, requirements.minimumSamples,
					len(baselineRounds[round]), len(candidateRounds[round]),
				)
			}
		}
		if err := validatePairedOrderEvidence(baseline, candidate, key, rounds, requirements.minimumWarmups); err != nil {
			return SPI2QualificationReport{}, fmt.Errorf("invalid SP-I2 paired evidence: %w", err)
		}

		split := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			split = "holdout"
		}
		seed := options.Seed + int64(index)*7919
		gateCase := SPI2QualificationCase{
			Dataset:            key.dataset,
			Name:               key.name,
			QualificationSplit: split,
			QualificationRole:  "target",
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
		if strings.Contains(gateCase.Name, "cycle-control") {
			gateCase.QualificationRole = "adverse_control"
			gateCase.Material = gateCase.MedianRatio.Upper <= report.AdverseRatioLimit ||
				gateCase.MedianSaving.Lower >= -report.AdverseAbsoluteLimit
		} else {
			gateCase.Material = gateCase.MedianRatio.Upper <= report.MaterialityRatio ||
				gateCase.MedianSaving.Lower >= report.MaterialityAbsolute
		}
		gateCase.P95Contained = gateCase.P95Ratio.Upper <= report.P95RatioLimit
		if !gateCase.Material {
			gateCase.Passed = false
			if gateCase.QualificationRole == "adverse_control" {
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
					"adverse-control overhead is not contained: ratio upper %.4f > %.4f and overhead upper %s > %s",
					gateCase.MedianRatio.Upper, report.AdverseRatioLimit,
					-gateCase.MedianSaving.Lower, report.AdverseAbsoluteLimit,
				))
			} else {
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
					"median improvement is not material: ratio upper %.4f > %.4f and saving lower %s < %s",
					gateCase.MedianRatio.Upper, report.MaterialityRatio,
					gateCase.MedianSaving.Lower, report.MaterialityAbsolute,
				))
			}
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

// validateSPI2EvidenceIdentity validates spi2 evidence identity.
func validateSPI2EvidenceIdentity(
	baseline, candidate []CaseResult,
	requirements spI2ProtocolRequirements,
) (spI2EvidenceIdentity, error) {
	if err := validatePerformanceWorkloadIdentity(baseline, candidate); err != nil {
		return spI2EvidenceIdentity{}, err
	}
	baselineHost, err := artifactHostFingerprint(baseline)
	if err != nil {
		return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 baseline host: %w", err)
	}
	candidateHost, err := artifactHostFingerprint(candidate)
	if err != nil {
		return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 candidate host: %w", err)
	}
	if baselineHost != candidateHost {
		return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 baseline and candidate host identities differ")
	}

	identity := spI2EvidenceIdentity{}
	for _, artifact := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// records retains the records while anonymous record is assembled or evaluated.
		records []CaseResult
	}{
		{
			name:    "baseline",
			records: baseline,
		},
		{
			name:    "candidate",
			records: candidate,
		},
	} {
		selection, err := selectionIdentity(artifact.records)
		if err != nil {
			return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 %s selection: %w", artifact.name, err)
		}
		if err := validateSPI2Selection(selection, requirements); err != nil {
			return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 %s selection: %w", artifact.name, err)
		}
		currentIdentity := spI2EvidenceIdentity{
			declarationSHA256: selection.DeclarationSHA256,
			resolvedSHA256:    resolvedSelectionSHA256(selection.Resolved),
		}
		for _, record := range artifact.records {
			if record.Environment == nil || record.PostgresEnvironment == nil {
				return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm lacks source or PostgreSQL environment identity", record.Dataset, record.Name, artifact.name)
			}
			current := spI2EvidenceIdentity{
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
				return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm lacks frozen source, diff, binary, or corpus identity", record.Dataset, record.Name, artifact.name)
			}
			if current.corpusSHA256 != requirements.corpusSHA {
				return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s %s arm corpus digest is not the exact frozen SP-I2 cohort", record.Dataset, record.Name, artifact.name)
			}
			if identity.sourceCommit == "" {
				identity = current
			} else if identity != current {
				return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 artifacts mix source, diff, binary, corpus, declaration, or selection identities")
			}
		}
	}
	if identity.declarationSHA256 != requirements.declarationSHA || identity.resolvedSHA256 != requirements.resolvedSHA {
		return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 artifacts do not bind the exact frozen declaration and resolved selection")
	}
	for key := range requirements.expectedKeys {
		baselinePostgres, err := postgresTimingEnvironmentSHA256ForKey(baseline, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		candidatePostgres, err := postgresTimingEnvironmentSHA256ForKey(candidate, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		baselineFixture, err := fixtureSHA256ForKey(baseline, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		candidateFixture, err := fixtureSHA256ForKey(candidate, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		if !lowercaseSHA256(baselinePostgres) || baselinePostgres != candidatePostgres {
			return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I2 PostgreSQL timing environments differ between arms", key.dataset, key.name)
		}
		if !lowercaseSHA256(baselineFixture) || baselineFixture != candidateFixture {
			return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I2 fixture identities differ between arms", key.dataset, key.name)
		}
		baselineSQL, err := spI2SQLFingerprintForKey(baseline, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		candidateSQL, err := spI2SQLFingerprintForKey(candidate, key)
		if err != nil {
			return spI2EvidenceIdentity{}, err
		}
		if baselineSQL == candidateSQL {
			return spI2EvidenceIdentity{}, fmt.Errorf("%s/%s SP-I2 arms use the same SQL fingerprint", key.dataset, key.name)
		}
		if err := validateOrientationExactObservations(key, baseline, candidate); err != nil {
			return spI2EvidenceIdentity{}, fmt.Errorf("SP-I2 exact observations: %w", err)
		}
	}
	return identity, nil
}

// spI2SQLFingerprintForKey derives the lookup key used for sp i2sql fingerprint for.
func spI2SQLFingerprintForKey(records []CaseResult, key performanceKey) (string, error) {
	fingerprint := ""
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		if fingerprint != "" && fingerprint != record.SQLFingerprint {
			return "", fmt.Errorf("%s/%s changes SQL fingerprint within one SP-I2 arm", key.dataset, key.name)
		}
		fingerprint = record.SQLFingerprint
	}
	if !lowercaseSHA256(fingerprint) {
		return "", fmt.Errorf("%s/%s lacks one stable SP-I2 SQL fingerprint", key.dataset, key.name)
	}
	return fingerprint, nil
}

// validateSPI2Selection validates spi2 selection.
func validateSPI2Selection(selection SelectionManifest, requirements spI2ProtocolRequirements) error {
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
			return fmt.Errorf("selection contains non-SP-I2 category %q", item.Category)
		}
		key := performanceKey{
			dataset: item.Dataset,
			name:    item.Name,
			backend: ModePostgresSQL,
		}
		if _, duplicate := resolved[key]; duplicate {
			return fmt.Errorf("selection contains duplicate %s/%s", item.Dataset, item.Name)
		}
		resolved[key] = struct{}{}
	}
	if !orientationV2KeySetsEqual(resolved, requirements.expectedKeys) {
		return fmt.Errorf("selection does not contain the exact frozen SP-I2 cases")
	}
	return nil
}

// collectSPI2QualificationSeries collects spi2 qualification series.
func collectSPI2QualificationSeries(
	baseline, candidate []CaseResult,
	resource ResourceGateReport,
	requirements spI2ProtocolRequirements,
) (map[performanceKey]*spI2QualificationSeries, []performanceKey, error) {
	if err := validateSPI2GlobalInvocationIDs(baseline, candidate); err != nil {
		return nil, nil, err
	}
	declarations, err := canonicalSPI2Declarations()
	if err != nil {
		return nil, nil, err
	}
	baselineKeys, baselineRounds, err := collectSPI2Artifact("baseline", baseline, requirements, declarations)
	if err != nil {
		return nil, nil, err
	}
	candidateKeys, candidateRounds, err := collectSPI2Artifact("candidate", candidate, requirements, declarations)
	if err != nil {
		return nil, nil, err
	}
	if !orientationV2KeySetsEqual(baselineKeys, requirements.expectedKeys) ||
		!orientationV2KeySetsEqual(candidateKeys, requirements.expectedKeys) {
		return nil, nil, fmt.Errorf("SP-I2 artifacts do not contain the exact protocol cohort")
	}
	if err := validateSPI2RunSchedule(baseline, candidate, requirements); err != nil {
		return nil, nil, err
	}
	resourcePassed, err := validateSPI2ResourceCases(resource, candidate, requirements)
	if err != nil {
		return nil, nil, err
	}

	series := make(map[performanceKey]*spI2QualificationSeries, len(requirements.expectedKeys))
	for key := range requirements.expectedKeys {
		current := &spI2QualificationSeries{
			baseline:       roundSamples{},
			candidate:      roundSamples{},
			resourcePassed: resourcePassed[key],
		}
		series[key] = current
		for round, record := range baselineRounds[key] {
			appendSPI2WarmSamples(current.baseline, round, record)
		}
		for round, record := range candidateRounds[key] {
			appendSPI2WarmSamples(current.candidate, round, record)
			branch := record.TraversalTelemetry.Summary.RuntimeBranch
			if current.runtimeBranch != "" && current.runtimeBranch != branch {
				return nil, nil, fmt.Errorf("%s/%s changes SP-I2 runtime branch across rounds", key.dataset, key.name)
			}
			current.runtimeBranch = branch
		}
		if current.runtimeBranch == "" {
			return nil, nil, fmt.Errorf("%s/%s has no attributable SP-I2 candidate runtime", key.dataset, key.name)
		}
	}
	return series, sortedPerformanceKeys(requirements.expectedKeys), nil
}

// validateSPI2GlobalInvocationIDs prevents one genuine timed receipt from
// being copied into another case, round, or arm. The attestor emits globally
// unique invocation IDs, so the complete paired study must not reuse one.
func validateSPI2GlobalInvocationIDs(artifacts ...[]CaseResult) error {
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
					return fmt.Errorf("SP-I2 evidence reuses timed invocation identity %q across the paired study", invocationID)
				}
				seen[invocationID] = struct{}{}
			}
		}
	}
	return nil
}

// spI2InvocationIdentity binds one timed sample to its scheduled run and arm.
type spI2InvocationIdentity struct {
	// round identifies the paired benchmark round.
	round int

	// block identifies the order-balancing block containing the round.
	block int

	// order retains the order while spI2InvocationIdentity is assembled or evaluated.
	order int

	// arm identifies the baseline or candidate treatment.
	arm string

	// runUUID binds the sample to one benchmark process invocation.
	runUUID string

	// startedAt records when timed execution began.
	startedAt time.Time

	// endedAt records when timed execution completed.
	endedAt time.Time
}

// validateSPI2RunSchedule validates spi2 run schedule.
func validateSPI2RunSchedule(baseline, candidate []CaseResult, requirements spI2ProtocolRequirements) error {
	collect := func(arm string, records []CaseResult) (map[int]spI2InvocationIdentity, error) {
		invocations := map[int]spI2InvocationIdentity{}
		caseCounts := map[int]int{}
		for _, record := range records {
			if record.Environment == nil {
				return nil, fmt.Errorf("%s/%s %s arm lacks invocation chronology", record.Dataset, record.Name, arm)
			}
			environment := record.Environment
			identity := spI2InvocationIdentity{
				round:     environment.Round,
				block:     environment.Block,
				order:     environment.ArmOrder,
				arm:       environment.Arm,
				runUUID:   environment.RunUUID,
				startedAt: environment.StartedAt,
				endedAt:   environment.EndedAt,
			}
			if identity.startedAt.IsZero() || identity.endedAt.IsZero() || identity.endedAt.Before(identity.startedAt) {
				return nil, fmt.Errorf("SP-I2 %s round %d has malformed invocation timestamps", arm, identity.round)
			}
			if prior, found := invocations[identity.round]; found && prior != identity {
				return nil, fmt.Errorf("SP-I2 %s round %d mixes invocation identities", arm, identity.round)
			}
			invocations[identity.round] = identity
			caseCounts[identity.round]++
		}
		for round, count := range caseCounts {
			if count != len(requirements.expectedKeys) {
				return nil, fmt.Errorf("SP-I2 %s round %d contains %d cases, expected %d", arm, round, count, len(requirements.expectedKeys))
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
		return fmt.Errorf("SP-I2 artifacts do not contain one complete paired invocation schedule")
	}
	runUUID := ""
	var priorEnded time.Time
	for round := 1; round <= len(left); round++ {
		baselineInvocation, baselineFound := left[round]
		candidateInvocation, candidateFound := right[round]
		if !baselineFound || !candidateFound {
			return fmt.Errorf("SP-I2 invocation schedule must use contiguous rounds starting at 1")
		}
		expectedBaselineOrder, expectedCandidateOrder := 1, 2
		if round%2 == 0 {
			expectedBaselineOrder, expectedCandidateOrder = 2, 1
		}
		if baselineInvocation.block != round || candidateInvocation.block != round ||
			baselineInvocation.arm != "sp-i2-s4" || candidateInvocation.arm != "sp-i2-candidate" ||
			baselineInvocation.order != expectedBaselineOrder || candidateInvocation.order != expectedCandidateOrder ||
			baselineInvocation.runUUID == "" || baselineInvocation.runUUID != candidateInvocation.runUUID {
			return fmt.Errorf("SP-I2 round %d does not match the frozen alternating two-arm schedule", round)
		}
		if runUUID == "" {
			runUUID = baselineInvocation.runUUID
		} else if runUUID != baselineInvocation.runUUID {
			return fmt.Errorf("SP-I2 artifacts mix run UUIDs across rounds")
		}
		first, second := baselineInvocation, candidateInvocation
		if candidateInvocation.order == 1 {
			first, second = candidateInvocation, baselineInvocation
		}
		if first.endedAt.After(second.startedAt) {
			return fmt.Errorf("SP-I2 round %d arm timestamps contradict the declared execution order", round)
		}
		if !priorEnded.IsZero() && priorEnded.After(first.startedAt) {
			return fmt.Errorf("SP-I2 round %d overlaps or predates the prior round", round)
		}
		priorEnded = second.endedAt
	}
	return nil
}

// collectSPI2Artifact collects spi2 artifact.
func collectSPI2Artifact(
	arm string,
	records []CaseResult,
	requirements spI2ProtocolRequirements,
	declarations map[performanceKey]spI2CanonicalDeclaration,
) (map[performanceKey]struct{}, map[performanceKey]map[int]CaseResult, error) {
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("SP-I2 %s artifact is empty", arm)
	}
	keys := map[performanceKey]struct{}{}
	rounds := map[performanceKey]map[int]CaseResult{}
	for _, record := range records {
		key := performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: record.ExecutionMode,
		}
		if _, expected := requirements.expectedKeys[key]; !expected {
			return nil, nil, fmt.Errorf("SP-I2 %s artifact contains unexpected case %s/%s", arm, key.dataset, key.name)
		}
		declaration, found := declarations[key]
		if !found {
			return nil, nil, fmt.Errorf("SP-I2 %s artifact has no frozen declaration for %s/%s", arm, key.dataset, key.name)
		}
		if err := validateSPI2Record(record, arm, declaration); err != nil {
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

// appendSPI2WarmSamples appends spi2 warm samples.
func appendSPI2WarmSamples(series roundSamples, round int, record CaseResult) {
	for _, sample := range record.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			series[round] = append(series[round], sample.Duration)
		}
	}
}

// validateSPI2Record validates spi2 record.
func validateSPI2Record(record CaseResult, arm string, declaration spI2CanonicalDeclaration) error {
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
	if filepath.Base(record.Source) != "generated_sp_i2_distance_v1.json" ||
		record.Category != testCase.Category || record.Cypher != testCase.Cypher || sqlFingerprint(record.Cypher) != spI2QuerySHA256 ||
		!lowercaseSHA256(record.WorkloadSHA256) || !lowercaseSHA256(record.SQLFingerprint) ||
		record.WorkloadSHA256 != expectedRecord.WorkloadSHA256 ||
		record.SQL == "" || sqlFingerprint(record.SQL) != record.SQLFingerprint ||
		!reflect.DeepEqual(record.NodeParams, testCase.NodeParams) ||
		!reflect.DeepEqual(record.NodeListParams, testCase.NodeListParams) ||
		!reflect.DeepEqual(record.Shape, testCase.Shape) {
		return fmt.Errorf("%s/%s %s arm lacks the frozen inbound SP-I2 workload identity", record.Dataset, record.Name, arm)
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
		minimumDepth != 1 || maximumDepth != 64 || record.Shape.PathMaterializationRequired {
		return fmt.Errorf("%s/%s %s arm changes the frozen inbound distance shape", record.Dataset, record.Name, arm)
	}
	expectedSplit := testCase.Shape.QualificationSplit
	if record.Shape.QualificationSplit != expectedSplit {
		return fmt.Errorf("%s/%s %s arm changes the frozen qualification split", record.Dataset, record.Name, arm)
	}
	expectedRows := *testCase.Expected.RowCount
	if !record.StableObservation || record.RowCount != expectedRows || record.ExpectedRowCount == nil ||
		*record.ExpectedRowCount != expectedRows {
		return fmt.Errorf("%s/%s %s arm lacks the exact stable distance observation contract", record.Dataset, record.Name, arm)
	}
	if err := validateExpectedObservations(testCase.Expected, record.ObservedRows); err != nil {
		return fmt.Errorf("%s/%s %s arm changes the frozen distance observation: %w", record.Dataset, record.Name, arm, err)
	}
	if len(record.Concurrency) != 0 || len(record.PostgresReferences) != 0 || record.ClientWaterfall != nil ||
		record.RawPGXWaterfall != nil || record.RawPGXRoundTrip != nil || record.Baseline != nil {
		return fmt.Errorf("%s/%s %s arm mixes SP-I2 timing with supplemental measurements", record.Dataset, record.Name, arm)
	}
	if err := ValidateTraversalExecutionTelemetry(record.TraversalTelemetry); err != nil {
		return fmt.Errorf("%s/%s %s arm telemetry: %w", record.Dataset, record.Name, arm, err)
	}
	if err := validateSPI2Runtime(record, arm); err != nil {
		return err
	}
	return nil
}

// validateSPI2Runtime validates spi2 runtime.
func validateSPI2Runtime(record CaseResult, arm string) error {
	summary := record.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable ||
		summary.Overflow == nil || summary.FallbackExecuted == nil || *summary.Overflow || *summary.FallbackExecuted ||
		summary.WouldSelectIdentity != "" || summary.ObservationMode != string(optimize.ShortestPathObservationDistance) ||
		summary.SchedulerVersion != string(optimize.ShortestPathSchedulerSingleEndedLevel) {
		return fmt.Errorf("%s/%s %s arm lacks one non-fallback distance runtime outcome", record.Dataset, record.Name, arm)
	}
	outcome, ok := singleTraversalOutcome(record.Optimization.TargetOutcomes)
	if !ok || outcome.Family != "SP" {
		return fmt.Errorf("%s/%s %s arm lacks one exact SP lowering outcome", record.Dataset, record.Name, arm)
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	candidate := string(optimize.ShortestPathExecutorI2GuardedDistance)
	plannedIdentities := spI2ShortestPathPlannedIdentities()
	outcomeDepthsExact := outcome.MinimumDepth != nil && *outcome.MinimumDepth == 1 &&
		outcome.MaximumDepth != nil && *outcome.MaximumDepth == 64
	outcomeShapeExact := outcome.Lowering == optimize.LoweringShortestPathExecutor && outcome.TargetKind == "traversal" &&
		outcome.ObservationMode == string(optimize.ShortestPathObservationDistance) && outcome.Direction == "inbound" &&
		outcome.PhysicalExpansion == "end_id" && outcome.RelationshipKindCount == 1 && !outcome.UntypedRelationship &&
		outcome.TopologyClassification == "physical_inbound_deep" && outcome.SelectionMode == "forced_tool" &&
		outcome.Scheduler == string(optimize.ShortestPathSchedulerSingleEndedLevel) && outcomeDepthsExact &&
		outcome.Eligible != nil && *outcome.Eligible && outcome.StaticallyEligible != nil && *outcome.StaticallyEligible
	if !outcomeShapeExact {
		return fmt.Errorf("%s/%s %s arm changes the frozen SP-I2 lowering shape", record.Dataset, record.Name, arm)
	}
	switch arm {
	case "baseline":
		if summary.RequestedIdentity != baseline || summary.EmittedIdentity != baseline ||
			summary.RuntimeIdentity != baseline || summary.AppliedIdentity != baseline ||
			!slices.Equal(summary.PlannedIdentities, plannedIdentities) ||
			summary.SelectorVersion != "sp-tool-v1" ||
			summary.ExecutionBoundary != optimize.ShortestPathExecutorS4CanonicalDistance.ExecutionBoundary() ||
			summary.RuntimeBranch != "selected" ||
			outcome.Candidate != "" || outcome.Selected != baseline || outcome.Applied != baseline || outcome.Fallback != "SP-S0" ||
			!slices.Equal(outcome.PlannedCandidates, plannedIdentities) ||
			outcome.ExecutionBoundary != "stored_helper" || outcome.SelectorVersion != "sp-tool-v1" ||
			outcome.EmittedPolicy != "" || len(outcome.EmittedCandidates) != 0 ||
			outcome.StateLimit != 100_000 || outcome.FrontierLimit != 100_000 || outcome.PredecessorLimit != 100_000 ||
			outcome.EnumerationLimit != 100_000 || outcome.OutputBytesLimit != 64*1024*1024 {
			return fmt.Errorf("%s/%s baseline arm did not execute exact forced S4", record.Dataset, record.Name)
		}
	case "candidate":
		expectedBranch := "inline_canonical_distance"
		if record.RowCount == 0 {
			expectedBranch = "inline_canonical_distance_no_path"
		}
		if summary.RequestedIdentity != candidate || summary.EmittedIdentity != optimize.ShortestPathPolicyI2DistanceGuardedV1 ||
			summary.RuntimeIdentity != candidate || summary.AppliedIdentity != candidate ||
			!slices.Equal(summary.PlannedIdentities, plannedIdentities) ||
			summary.SelectorVersion != optimize.ShortestPathSelectorStaticV8HiddenFanIn ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
			summary.RuntimeBranch != expectedBranch ||
			!equalSPI2Caps(summary.Caps, spI2SummaryCaps()) ||
			!slices.Contains(summary.PlannedIdentities, baseline) || !slices.Contains(summary.PlannedIdentities, candidate) ||
			outcome.Candidate != candidate || outcome.Selected != candidate || outcome.Applied != candidate ||
			outcome.Fallback != baseline || outcome.EmittedPolicy != optimize.ShortestPathPolicyI2DistanceGuardedV1 ||
			!slices.Equal(outcome.PlannedCandidates, plannedIdentities) ||
			!slices.Equal(outcome.EmittedCandidates, []string{candidate, baseline}) ||
			outcome.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
			outcome.SelectorVersion != optimize.ShortestPathSelectorStaticV8HiddenFanIn ||
			outcome.StateLimit != spI2QualificationCaps()["state_limit"] ||
			outcome.FrontierLimit != spI2QualificationCaps()["frontier_limit"] ||
			outcome.PredecessorLimit != 0 || outcome.EnumerationLimit != 0 || outcome.OutputBytesLimit != 0 {
			return fmt.Errorf("%s/%s candidate arm did not execute exact guarded SP-I2 distance", record.Dataset, record.Name)
		}
		diagnostic := record.TraversalTelemetry.Diagnostic
		if record.TraversalTelemetry.Level != TraversalTelemetryLevelDiagnostic || diagnostic == nil ||
			diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete || diagnostic.Counters.InlineShortestDistance == nil ||
			!slices.Contains(diagnostic.RequiredFamilies, TraversalTelemetryFamilySP) {
			return fmt.Errorf("%s/%s candidate arm lacks complete typed SP-I2 distance resource telemetry", record.Dataset, record.Name)
		}
		inline := diagnostic.Counters.InlineShortestDistance
		outputRows, outputPresent := int64(0), false
		if diagnostic.PlanReplay != nil {
			outputRows, outputPresent = diagnostic.PlanReplay.Counters["sp_i2_output_rows"]
		}
		if inline.OutputRows == nil || *inline.OutputRows != record.RowCount || !outputPresent || outputRows != record.RowCount {
			return fmt.Errorf("%s/%s candidate arm runtime branch does not bind the exact output observation", record.Dataset, record.Name)
		}
	default:
		return fmt.Errorf("unknown SP-I2 arm %q", arm)
	}
	if err := validateSPI2SampleRuntime(record, arm); err != nil {
		return err
	}
	return nil
}

// spI2ShortestPathPlannedIdentities mirrors the optimizer's complete SP search
// space. Planned candidates describe every executor considered by lowering;
// emitted candidates and the runtime receipt separately attest the exact
// guarded two-arm statement that executed.
func spI2ShortestPathPlannedIdentities() []string {
	return []string{
		string(optimize.ShortestPathExecutorIncumbentWorkspace),
		string(optimize.ShortestPathExecutorS0Direct),
		string(optimize.ShortestPathExecutorS1ArrayBFS),
		string(optimize.ShortestPathExecutorS2TraceRelation),
		string(optimize.ShortestPathExecutorS3Unidirectional),
		string(optimize.ShortestPathExecutorS3EdgeM0),
		string(optimize.ShortestPathExecutorS4CanonicalDistance),
		string(optimize.ShortestPathExecutorS4CanonicalWitness),
		string(optimize.ShortestPathExecutorI1CanonicalDistance),
		string(optimize.ShortestPathExecutorI2GuardedDistance),
		string(optimize.ShortestPathExecutorI1CanonicalWitness),
		string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		string(optimize.ShortestPathExecutorB1AlternatingNodeDistance),
		string(optimize.ShortestPathExecutorB1AlternatingNodeWitness),
		string(optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance),
		string(optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness),
	}
}

// validateSPI2SampleRuntime validates spi2 sample runtime.
func validateSPI2SampleRuntime(record CaseResult, arm string) error {
	summary := record.TraversalTelemetry.Summary
	if record.Environment == nil || record.Stats.Iterations < 1 || record.Stats.WarmupIterations != record.Environment.WarmupIterations ||
		record.Stats.Median <= 0 || record.Stats.P95 <= 0 {
		return fmt.Errorf("%s/%s %s arm has malformed iteration or warmup evidence", record.Dataset, record.Name, arm)
	}
	expectedArm := "sp-i2-s4"
	if arm == "candidate" {
		expectedArm = "sp-i2-candidate"
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
			} else if strings.Contains(record.Name, "cycle-control") {
				expectedBranch = "one_hop_preflight"
			} else if strings.Contains(record.Name, "early-d02") {
				expectedBranch = "two_hop_preflight"
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

// validateSPI2ResourceCases validates spi2 resource cases.
func validateSPI2ResourceCases(
	report ResourceGateReport,
	candidate []CaseResult,
	requirements spI2ProtocolRequirements,
) (map[performanceKey]bool, error) {
	if report.Version != resourceGateVersion {
		return nil, fmt.Errorf("SP-I2 resource report version must be %d", resourceGateVersion)
	}

	// recordKey binds resource evidence to an exact scheduled candidate invocation.
	type recordKey struct {
		// performanceKey identifies the workload and backend.
		performanceKey

		// round identifies the paired benchmark round.
		round int

		// block identifies the order-balancing block containing the round.
		block int

		// order retains the order while recordKey is assembled or evaluated.
		order int

		// runUUID binds the record to one benchmark process invocation.
		runUUID string

		// arm identifies the treatment that produced the record.
		arm string
	}
	expected := map[recordKey]CaseResult{}
	for _, record := range candidate {
		if record.Environment == nil {
			return nil, fmt.Errorf("%s/%s candidate resource record lacks run identity", record.Dataset, record.Name)
		}
		key := recordKey{
			performanceKey: performanceKey{
				dataset: record.Dataset,
				name:    record.Name,
				backend: ModePostgresSQL,
			},
			round:   record.Environment.Round,
			block:   record.Environment.Block,
			order:   record.Environment.ArmOrder,
			runUUID: record.Environment.RunUUID,
			arm:     record.Environment.Arm,
		}
		if _, duplicate := expected[key]; duplicate {
			return nil, fmt.Errorf("SP-I2 candidate artifact duplicates a resource record identity")
		}
		expected[key] = record
	}
	actual := map[recordKey]struct{}{}
	passed := map[performanceKey]bool{}
	for key := range requirements.expectedKeys {
		passed[key] = true
	}
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return nil, err
	}
	allPassed := true
	for _, gateCase := range report.Cases {
		key := performanceKey{
			dataset: gateCase.Dataset,
			name:    gateCase.Name,
			backend: ModePostgresSQL,
		}
		if _, expected := requirements.expectedKeys[key]; !expected || gateCase.Reference != "" {
			return nil, fmt.Errorf("SP-I2 resource report contains an unexpected production or reference case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		identity := recordKey{
			performanceKey: key,
			round:          gateCase.Round,
			block:          gateCase.Block,
			order:          gateCase.ArmOrder,
			runUUID:        gateCase.RunUUID,
			arm:            gateCase.Arm,
		}
		record, found := expected[identity]
		if !found {
			return nil, fmt.Errorf("SP-I2 resource case %s/%s round %d does not bind an exact candidate record", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		if _, duplicate := actual[identity]; duplicate {
			return nil, fmt.Errorf("SP-I2 resource report duplicates %s/%s round %d", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		actual[identity] = struct{}{}
		recomputed := evaluateProductionResourceGateCase(record)
		if !reflect.DeepEqual(gateCase, recomputed) {
			return nil, fmt.Errorf("SP-I2 resource case %s/%s round %d differs from the decision recomputed from its candidate record", gateCase.Dataset, gateCase.Name, gateCase.Round)
		}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		if gateCase.Architecture != string(optimize.ShortestPathExecutorI2GuardedDistance) ||
			gateCase.FallbackArchitecture != "" || gateCase.QualificationSplit != expectedSplit ||
			gateCase.Tier != "normal" || !equalSPI2Caps(gateCase.NumericLimits, spI2TelemetryCaps()) ||
			gateCase.Passed != (len(gateCase.Reasons) == 0) ||
			!reflect.DeepEqual(gateCase.RuntimeReceiptChains, runtimeReceiptChains(record.Stats.Samples)) {
			return nil, fmt.Errorf("SP-I2 resource case %s/%s does not bind exact guarded-distance limits and split", gateCase.Dataset, gateCase.Name)
		}
		observations := traversalNumericObservations(record.TraversalTelemetry.Diagnostic.Counters)
		if len(gateCase.NumericObserved) != len(spI2TelemetryCaps()) {
			return nil, fmt.Errorf("SP-I2 resource case %s/%s has unexpected numeric observations", gateCase.Dataset, gateCase.Name)
		}
		for name := range spI2TelemetryCaps() {
			observed, found := gateCase.NumericObserved[name]
			expectedObserved, expectedFound := observations[name]
			if !found || !expectedFound || observed != expectedObserved || observed < 0 {
				return nil, fmt.Errorf("SP-I2 resource case %s/%s has invalid %s observation", gateCase.Dataset, gateCase.Name, name)
			}
		}
		passed[key] = passed[key] && gateCase.Passed
		allPassed = allPassed && gateCase.Passed
	}
	if len(actual) != len(expected) {
		return nil, fmt.Errorf("SP-I2 resource report has %d exact record decisions, expected %d", len(actual), len(expected))
	}
	for key := range requirements.expectedKeys {
		if _, found := passed[key]; !found {
			return nil, fmt.Errorf("SP-I2 resource report omits %s/%s", key.dataset, key.name)
		}
	}
	if report.Passed != allPassed {
		return nil, fmt.Errorf("SP-I2 resource report aggregate disposition contradicts its cases")
	}
	return passed, nil
}

// validateSPI2Freeze validates spi2 freeze.
func validateSPI2Freeze(
	freeze *SPI2QualificationFreezeManifest,
	discovery *SPI2QualificationReport,
	report SPI2QualificationReport,
	cohort spI2CanonicalCohort,
) error {
	if err := validateSPI2FrozenDiscovery(freeze, discovery, cohort); err != nil {
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
		!equalSPI2Caps(report.Caps, freeze.Caps) {
		return fmt.Errorf("SP-I2 confirmation identity differs from the frozen discovery")
	}
	return nil
}

// validateSPI2FrozenDiscovery validates spi2 frozen discovery.
func validateSPI2FrozenDiscovery(
	freeze *SPI2QualificationFreezeManifest,
	discovery *SPI2QualificationReport,
	cohort spI2CanonicalCohort,
) error {
	if freeze == nil || discovery == nil {
		return fmt.Errorf("SP-I2 confirmation requires a discovery report and freeze manifest")
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	candidate := string(optimize.ShortestPathExecutorI2GuardedDistance)
	if freeze.Version != spI2FreezeVersion || freeze.Baseline != baseline || freeze.Candidate != candidate ||
		freeze.Policy != optimize.ShortestPathPolicyI2DistanceGuardedV1 || freeze.QuerySHA256 != spI2QuerySHA256 ||
		freeze.Seed != 1 || freeze.Confidence != defaultConfidenceLevel || freeze.BootstrapCount != defaultBootstrapCount ||
		!equalSPI2Caps(freeze.Caps, spI2QualificationCaps()) ||
		freeze.TrainingDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		freeze.HoldoutDeclarationSHA256 != cohort.holdoutDeclarationSHA256 ||
		freeze.FullDeclarationSHA256 != cohort.declarationSHA256 ||
		freeze.TrainingCorpusSHA256 != cohort.trainingCorpusSHA256 || freeze.FullCorpusSHA256 != cohort.fullCorpusSHA256 ||
		freeze.TrainingResolvedSHA256 != cohort.trainingResolvedSHA256 || freeze.FullResolvedSHA256 != cohort.fullResolvedSHA256 ||
		!lowercaseSHA256(freeze.SourceArchiveSHA256) || !lowercaseSHA256(freeze.DirtyDiffSHA256) ||
		!lowercaseSHA256(freeze.BinarySHA256) || !lowercaseSHA256(freeze.BaselineArtifactSHA256) ||
		!lowercaseSHA256(freeze.CandidateArtifactSHA256) || !lowercaseSHA256(freeze.ResourceReportSHA256) ||
		!lowercaseSHA256(freeze.DiscoveryReportSHA256) || strings.TrimSpace(freeze.SourceCommit) == "" {
		return fmt.Errorf("SP-I2 freeze manifest does not bind the exact immutable study identity")
	}
	if freeze.DirtyDiffSHA256 != cleanWorkingTreeSHA256() {
		return fmt.Errorf("SP-I2 freeze manifest was not created from a clean source tree")
	}
	if discovery.Version != spI2QualificationVersion || discovery.Protocol != referencePairProtocolDiscovery ||
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
		!equalSPI2Caps(discovery.Caps, freeze.Caps) || discovery.Seed != freeze.Seed ||
		discovery.Confidence != freeze.Confidence || discovery.BootstrapCount != freeze.BootstrapCount ||
		discovery.MaterialityRatio != 0.95 || discovery.MaterialityAbsolute != 100*time.Microsecond ||
		discovery.P95RatioLimit != 1.05 || discovery.AdverseRatioLimit != 1.10 ||
		discovery.AdverseAbsoluteLimit != 100*time.Microsecond || !discovery.EvidencePassed ||
		discovery.TrainingCases != len(cohort.trainingKeys) || discovery.HoldoutCases != 0 ||
		discovery.HoldoutPassed || discovery.QualificationPassed || discovery.TrainingPassed != freeze.TrainingPassed {
		return fmt.Errorf("SP-I2 discovery report does not prove the exact frozen training identity")
	}
	seen := map[performanceKey]struct{}{}
	for _, entry := range discovery.Cases {
		key := performanceKey{
			dataset: entry.Dataset,
			name:    entry.Name,
			backend: ModePostgresSQL,
		}
		if entry.QualificationSplit != "training" {
			return fmt.Errorf("SP-I2 discovery report contains non-training timing")
		}
		if _, expected := cohort.trainingKeys[key]; !expected {
			return fmt.Errorf("SP-I2 discovery report contains unexpected case %s/%s", entry.Dataset, entry.Name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("SP-I2 discovery report duplicates case %s/%s", entry.Dataset, entry.Name)
		}
		expectedBranch := "inline_canonical_distance"
		if strings.HasSuffix(entry.Name, "-disconnected") {
			expectedBranch = "inline_canonical_distance_no_path"
		}
		expectedRole := "target"
		expectedPerformanceGate := entry.MedianRatio.Upper <= discovery.MaterialityRatio || entry.MedianSaving.Lower >= discovery.MaterialityAbsolute
		if strings.Contains(entry.Name, "cycle-control") {
			expectedRole = "adverse_control"
			expectedPerformanceGate = entry.MedianRatio.Upper <= discovery.AdverseRatioLimit || entry.MedianSaving.Lower >= -discovery.AdverseAbsoluteLimit
		}
		if !validSPI2RatioInterval(entry.MedianRatio) || !validSPI2RatioInterval(entry.P95Ratio) ||
			entry.MedianSaving.Lower > entry.MedianSaving.Estimate || entry.MedianSaving.Estimate > entry.MedianSaving.Upper ||
			entry.QualificationRole != expectedRole || entry.Material != expectedPerformanceGate ||
			entry.P95Contained != (entry.P95Ratio.Upper <= discovery.P95RatioLimit) ||
			!entry.Passed || len(entry.Reasons) != 0 || !entry.Material || !entry.P95Contained || !entry.ResourcePassed ||
			entry.RuntimeBranch != expectedBranch ||
			entry.Rounds < 5 || entry.Rounds > 20 || entry.BaselineSamples < 50 || entry.CandidateSamples < 50 {
			return fmt.Errorf("SP-I2 discovery report case %s/%s did not pass the frozen training gates", entry.Dataset, entry.Name)
		}
		seen[key] = struct{}{}
	}
	if !orientationV2KeySetsEqual(seen, cohort.trainingKeys) {
		return fmt.Errorf("SP-I2 discovery report omits part of the exact training cohort")
	}
	if !freeze.TrainingPassed || !discovery.TrainingPassed {
		return fmt.Errorf("SP-I2 training discovery did not pass")
	}
	return nil
}

// validSPI2RatioInterval reports whether a confidence interval contains finite ordered bounds.
func validSPI2RatioInterval(interval RatioInterval) bool {
	return interval.Lower > 0 && interval.Lower <= interval.Estimate && interval.Estimate <= interval.Upper &&
		!math.IsNaN(interval.Lower) && !math.IsNaN(interval.Estimate) && !math.IsNaN(interval.Upper) &&
		!math.IsInf(interval.Lower, 0) && !math.IsInf(interval.Estimate, 0) && !math.IsInf(interval.Upper, 0)
}

// createSPI2QualificationReport loads and evaluates the staged two-arm
// qualification evidence, writes the report even for statistical failures,
// and freezes discovery before any holdout capture is authorized.
func createSPI2QualificationReport(
	baselinePath, candidatePath, resourcePath, freezePath, discoveryPath, freezeOutputPath, outputPath string,
	options SPI2QualificationOptions,
) (bool, error) {
	if err := validateDistinctSPI2Paths(map[string]string{
		"baseline artifact": baselinePath, "candidate artifact": candidatePath, "resource report": resourcePath,
		"freeze manifest": freezePath, "discovery report": discoveryPath, "freeze output": freezeOutputPath, "report output": outputPath,
	}); err != nil {
		return false, err
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return false, fmt.Errorf("read SP-I2 baseline artifact: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return false, fmt.Errorf("read SP-I2 candidate artifact: %w", err)
	}
	resource, err := loadSPI2ResourceReport(resourcePath)
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
		return false, fmt.Errorf("SP-I2 resource report is not bound to the exact candidate artifact")
	}

	freezeSHA256 := ""
	if freezePath != "" || discoveryPath != "" {
		if freezePath == "" || discoveryPath == "" {
			return false, fmt.Errorf("SP-I2 confirmation requires both freeze and discovery report paths")
		}
		freeze, digest, err := loadSPI2FreezeManifest(freezePath)
		if err != nil {
			return false, fmt.Errorf("read SP-I2 freeze manifest: %w", err)
		}
		discovery, err := loadSPI2QualificationReport(discoveryPath)
		if err != nil {
			return false, fmt.Errorf("read SP-I2 discovery report: %w", err)
		}
		discoverySHA256, err := fileSHA256(discoveryPath)
		if err != nil {
			return false, err
		}
		if discoverySHA256 != freeze.DiscoveryReportSHA256 {
			return false, fmt.Errorf("SP-I2 discovery report digest does not match freeze manifest")
		}
		options.Freeze, options.Discovery = freeze, discovery
		freezeSHA256 = digest
		if err := validateSPI2FrozenTrainingEvidence(
			freeze, discovery,
			options.TrainingBaselinePath, options.TrainingCandidatePath, options.TrainingResourcePath,
		); err != nil {
			return false, err
		}
	}
	options.SourceArchiveSHA256, err = spI2SourceArchiveSHA256()
	if err != nil {
		return false, err
	}
	report, err := buildSPI2QualificationReport(baseline, candidate, resource, options)
	if err != nil {
		return false, err
	}
	report.BaselineArtifactSHA256 = baselineSHA256
	report.CandidateArtifactSHA256 = candidateSHA256
	report.ResourceReportSHA256 = resourceSHA256
	report.FreezeManifestSHA256 = freezeSHA256
	if err := validateCurrentSPI2Source(report.SourceCommit, report.SourceArchiveSHA256, report.DirtyDiffSHA256, report.BinarySHA256); err != nil {
		return false, err
	}
	if err := writeSPI2QualificationReport(outputPath, report); err != nil {
		return false, err
	}
	if options.Protocol == referencePairProtocolDiscovery {
		if err := writeSPI2FreezeManifest(freezeOutputPath, outputPath, report); err != nil {
			return false, err
		}
		return report.TrainingPassed, nil
	}
	return report.QualificationPassed, nil
}

// validateSPI2HoldoutCapture authorizes the exact frozen cohort before any
// database setup is allowed to begin.
func validateSPI2HoldoutCapture(
	corpus ScaleCorpus,
	freezePath, discoveryPath, trainingBaselinePath, trainingCandidatePath, trainingResourcePath string,
) error {
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return err
	}
	if err := validateSPI2Corpus(corpus, cohort); err != nil {
		return err
	}
	freeze, _, err := loadSPI2FreezeManifest(freezePath)
	if err != nil {
		return fmt.Errorf("read SP-I2 freeze manifest: %w", err)
	}
	discovery, err := loadSPI2QualificationReport(discoveryPath)
	if err != nil {
		return fmt.Errorf("read SP-I2 discovery report: %w", err)
	}
	discoverySHA256, err := fileSHA256(discoveryPath)
	if err != nil {
		return err
	}
	if discoverySHA256 != freeze.DiscoveryReportSHA256 {
		return fmt.Errorf("SP-I2 discovery report digest does not match freeze manifest")
	}
	if err := validateSPI2FrozenTrainingEvidence(
		freeze, discovery, trainingBaselinePath, trainingCandidatePath, trainingResourcePath,
	); err != nil {
		return err
	}
	if err := validateCurrentSPI2Source(freeze.SourceCommit, freeze.SourceArchiveSHA256, freeze.DirtyDiffSHA256, freeze.BinarySHA256); err != nil {
		return err
	}
	return nil
}

// validateSPI2FrozenTrainingEvidence reloads and recomputes the exact training
// closure named by the freeze. This prevents an internally consistent but
// hand-edited report/freeze pair from authorizing protected holdout timing.
func validateSPI2FrozenTrainingEvidence(
	freeze *SPI2QualificationFreezeManifest,
	discovery *SPI2QualificationReport,
	baselinePath, candidatePath, resourcePath string,
) error {
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return err
	}
	if err := validateSPI2FrozenDiscovery(freeze, discovery, cohort); err != nil {
		return err
	}
	if baselinePath == "" || candidatePath == "" || resourcePath == "" {
		return fmt.Errorf("SP-I2 frozen discovery verification requires the three exact training evidence artifacts")
	}
	baselineSHA256, err := fileSHA256(baselinePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I2 training baseline: %w", err)
	}
	candidateSHA256, err := fileSHA256(candidatePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I2 training candidate: %w", err)
	}
	resourceSHA256, err := fileSHA256(resourcePath)
	if err != nil {
		return fmt.Errorf("hash frozen SP-I2 training resource report: %w", err)
	}
	if baselineSHA256 != freeze.BaselineArtifactSHA256 || candidateSHA256 != freeze.CandidateArtifactSHA256 ||
		resourceSHA256 != freeze.ResourceReportSHA256 {
		return fmt.Errorf("SP-I2 frozen training evidence digests differ from the discovery freeze")
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return fmt.Errorf("read frozen SP-I2 training baseline: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return fmt.Errorf("read frozen SP-I2 training candidate: %w", err)
	}
	resource, err := loadSPI2ResourceReport(resourcePath)
	if err != nil {
		return err
	}
	if resource.ArtifactSHA256 != candidateSHA256 {
		return fmt.Errorf("SP-I2 frozen training resource report is not bound to the candidate artifact")
	}
	recomputed, err := buildSPI2QualificationReport(baseline, candidate, resource, SPI2QualificationOptions{
		Seed:                freeze.Seed,
		Confidence:          freeze.Confidence,
		BootstrapCount:      freeze.BootstrapCount,
		Protocol:            referencePairProtocolDiscovery,
		SourceArchiveSHA256: freeze.SourceArchiveSHA256,
	})
	if err != nil {
		return fmt.Errorf("recompute frozen SP-I2 training discovery: %w", err)
	}
	recomputed.BaselineArtifactSHA256 = baselineSHA256
	recomputed.CandidateArtifactSHA256 = candidateSHA256
	recomputed.ResourceReportSHA256 = resourceSHA256
	if !reflect.DeepEqual(recomputed, *discovery) {
		return fmt.Errorf("SP-I2 discovery report differs from its recomputed frozen training evidence")
	}
	return nil
}

// validateSPI2Corpus validates spi2 corpus.
func validateSPI2Corpus(corpus ScaleCorpus, cohort spI2CanonicalCohort) error {
	if len(corpus.Cases) != len(cohort.keys) {
		return fmt.Errorf("SP-I2 holdout capture requires exactly the frozen six-training/four-holdout cohort")
	}
	seen := map[performanceKey]struct{}{}
	resolved := make([]ResolvedCaseSelector, 0, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		key := performanceKey{
			dataset: testCase.Dataset,
			name:    testCase.Name,
			backend: ModePostgresSQL,
		}
		if _, expected := cohort.keys[key]; !expected {
			return fmt.Errorf("SP-I2 holdout capture contains unexpected case %s/%s", testCase.Dataset, testCase.Name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("SP-I2 holdout capture duplicates case %s/%s", testCase.Dataset, testCase.Name)
		}
		seen[key] = struct{}{}
		if filepath.Base(testCase.Source) != "generated_sp_i2_distance_v1.json" ||
			testCase.Category != "generated_shortest_path_v2" || sqlFingerprint(testCase.Cypher) != spI2QuerySHA256 ||
			testCase.Shape.FallbackExpectation != "forbidden" || testCase.Shape.Direction != "inbound" ||
			testCase.Shape.RelationshipKindCount != 1 || !slices.Equal(testCase.Shape.EdgeKinds, []string{"Traverse"}) ||
			testCase.Shape.MinDepth == nil || *testCase.Shape.MinDepth != 1 ||
			testCase.Shape.MaxDepth == nil || *testCase.Shape.MaxDepth != 64 ||
			testCase.Shape.PathMaterializationRequired ||
			!slices.Equal(testCase.CandidateModes, []ExecutionMode{ModePostgresSQL, ModeNeo4j}) {
			return fmt.Errorf("SP-I2 holdout capture changes frozen declaration %s/%s", testCase.Dataset, testCase.Name)
		}
		expectedSplit := "training"
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			expectedSplit = "holdout"
		}
		if testCase.Shape.QualificationSplit != expectedSplit {
			return fmt.Errorf("SP-I2 holdout capture changes frozen split for %s/%s", testCase.Dataset, testCase.Name)
		}
		resolved = append(resolved, ResolvedCaseSelector{
			Dataset:  testCase.Dataset,
			Name:     testCase.Name,
			Category: testCase.Category,
		})
	}
	if !orientationV2KeySetsEqual(seen, cohort.keys) ||
		declarationSHA256(corpus.DeclaredBackends()) != cohort.declarationSHA256 ||
		resolvedSelectionSHA256(resolved) != cohort.fullResolvedSHA256 ||
		corpusIdentity(corpus) != cohort.fullCorpusSHA256 {
		return fmt.Errorf("SP-I2 holdout capture does not match the exact frozen declaration, selection, and corpus digests")
	}
	return nil
}

// validateCurrentSPI2Source validates current spi2 source.
func validateCurrentSPI2Source(sourceCommit, sourceArchive, dirtyDiff, binary string) error {
	currentCommit := strings.TrimSpace(commandOutput("git", "rev-parse", "HEAD"))
	currentArchive, err := spI2SourceArchiveSHA256()
	if err != nil {
		return err
	}
	currentDiff := workingTreeSHA256()
	currentBinary := executableSHA256()
	if currentCommit == "" || currentCommit == "unknown" || sourceCommit != currentCommit ||
		!lowercaseSHA256(sourceArchive) || sourceArchive != currentArchive ||
		dirtyDiff != cleanWorkingTreeSHA256() || currentDiff != cleanWorkingTreeSHA256() ||
		!lowercaseSHA256(binary) || binary != currentBinary {
		return fmt.Errorf("SP-I2 evidence requires the current clean committed source archive and exact running binary")
	}
	return nil
}

// loadSPI2ResourceReport loads spi2 resource report.
func loadSPI2ResourceReport(path string) (ResourceGateReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ResourceGateReport{}, fmt.Errorf("read SP-I2 resource report: %w", err)
	}
	report := ResourceGateReport{}
	if err := json.Unmarshal(raw, &report); err != nil {
		return ResourceGateReport{}, fmt.Errorf("decode SP-I2 resource report: %w", err)
	}
	if report.Version != resourceGateVersion || !lowercaseSHA256(report.ArtifactSHA256) {
		return ResourceGateReport{}, fmt.Errorf("SP-I2 resource report must be checksummed schema v%d", resourceGateVersion)
	}
	return report, nil
}

// loadSPI2QualificationReport loads spi2 qualification report.
func loadSPI2QualificationReport(path string) (*SPI2QualificationReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	report := &SPI2QualificationReport{}
	if err := json.Unmarshal(raw, report); err != nil {
		return nil, fmt.Errorf("decode SP-I2 qualification report: %w", err)
	}
	return report, nil
}

// loadSPI2FreezeManifest loads spi2 freeze manifest.
func loadSPI2FreezeManifest(path string) (*SPI2QualificationFreezeManifest, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	manifest := &SPI2QualificationFreezeManifest{}
	if err := json.Unmarshal(raw, manifest); err != nil {
		return nil, "", fmt.Errorf("decode SP-I2 freeze manifest: %w", err)
	}
	digest := sha256.Sum256(raw)
	return manifest, hex.EncodeToString(digest[:]), nil
}

// writeSPI2QualificationReport writes spi2 qualification report.
func writeSPI2QualificationReport(path string, report SPI2QualificationReport) (err error) {
	if path == "" {
		return fmt.Errorf("SP-I2 qualification requires an explicit report output path")
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

// writeSPI2FreezeManifest writes spi2 freeze manifest.
func writeSPI2FreezeManifest(path, discoveryReportPath string, report SPI2QualificationReport) (err error) {
	if path == "" || discoveryReportPath == "" {
		return fmt.Errorf("SP-I2 discovery freeze requires report and manifest output paths")
	}
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return err
	}
	if report.Protocol != referencePairProtocolDiscovery || report.CohortDeclarationSHA256 != cohort.trainingDeclarationSHA256 ||
		report.ResolvedSelectionSHA256 != cohort.trainingResolvedSHA256 || report.CorpusSHA256 != cohort.trainingCorpusSHA256 ||
		report.TrainingCases != len(cohort.trainingKeys) || report.HoldoutCases != 0 ||
		!report.EvidencePassed || !report.TrainingPassed ||
		report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.BootstrapCount != defaultBootstrapCount ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || !equalSPI2Caps(report.Caps, spI2QualificationCaps()) ||
		!lowercaseSHA256(report.BaselineArtifactSHA256) || !lowercaseSHA256(report.CandidateArtifactSHA256) ||
		!lowercaseSHA256(report.ResourceReportSHA256) {
		return fmt.Errorf("SP-I2 discovery freeze requires the exact passing clean training-only report")
	}
	discoveryReportSHA256, err := fileSHA256(discoveryReportPath)
	if err != nil {
		return err
	}
	manifest := SPI2QualificationFreezeManifest{
		Version:                   spI2FreezeVersion,
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

// validateDistinctSPI2Paths validates distinct spi2 paths.
func validateDistinctSPI2Paths(paths map[string]string) error {
	names := make([]string, 0, len(paths))
	for name, path := range paths {
		if path != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	// resolvedPath records the canonical filesystem identity of one evidence input.
	type resolvedPath struct {
		// name retains the name while resolvedPath is assembled or evaluated.
		name string
		// info retains the info while resolvedPath is assembled or evaluated.
		info os.FileInfo
	}
	resolved := map[string]resolvedPath{}
	var existing []resolvedPath
	for _, name := range names {
		absolute, err := filepath.Abs(filepath.Clean(paths[name]))
		if err != nil {
			return fmt.Errorf("resolve SP-I2 %s: %w", name, err)
		}
		if evaluated, err := filepath.EvalSymlinks(absolute); err == nil {
			absolute = evaluated
		} else if evaluatedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(absolute)); parentErr == nil {
			absolute = filepath.Join(evaluatedParent, filepath.Base(absolute))
		}
		if prior, duplicate := resolved[absolute]; duplicate {
			return fmt.Errorf("SP-I2 %s and %s must use distinct paths", prior.name, name)
		}
		current := resolvedPath{name: name}
		if info, err := os.Stat(paths[name]); err == nil {
			current.info = info
			for _, prior := range existing {
				if prior.info != nil && os.SameFile(prior.info, info) {
					return fmt.Errorf("SP-I2 %s and %s must not alias the same file", prior.name, name)
				}
			}
			existing = append(existing, current)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect SP-I2 %s path: %w", name, err)
		}
		resolved[absolute] = current
	}
	return nil
}

// selectedCorpusContainsSPI2Holdout selects ed corpus contains spi2 holdout.
func selectedCorpusContainsSPI2Holdout(corpus ScaleCorpus) bool {
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return true
	}
	for _, testCase := range corpus.Cases {
		key := performanceKey{
			dataset: testCase.Dataset,
			name:    testCase.Name,
			backend: ModePostgresSQL,
		}
		if _, holdout := cohort.holdoutKeys[key]; holdout {
			return true
		}
	}
	return false
}

// selectRunnableScaleCorpus keeps the protected SP-I2 holdout out of ordinary
// GraphBench selection. The holdout becomes selectable only through its exact
// protocol tag or an exact case name; database capture then passes through the
// freeze checks in main before any target is opened.
func selectRunnableScaleCorpusWithSPI2Protection(corpus ScaleCorpus, selectors CorpusSelectors) (ScaleCorpus, SelectionManifest, error) {
	if err := validateCorpusSelectors(corpus, selectors); err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	// Preserve SP-I1's independently frozen selection identity when its exact
	// protocol selectors are in use. The two qualification studies must never
	// change one another's protected-declaration digest.
	spI1ProtocolSelection := slices.Contains(selectors.Tags, spI1TrainingTag) || slices.Contains(selectors.Tags, spI1HoldoutTag)
	if !spI1ProtocolSelection {
		for _, selectedName := range selectors.Cases {
			for _, testCase := range spI1CanonicalCases {
				if selectedName == testCase.name {
					spI1ProtocolSelection = true
					break
				}
			}
		}
	}
	if spI1ProtocolSelection {
		return selectRunnableScaleCorpus(corpus, selectors)
	}
	spI2ProtocolSelection := slices.Contains(selectors.Tags, spI2TrainingTag) || slices.Contains(selectors.Tags, spI2HoldoutTag)
	if !spI2ProtocolSelection {
		for _, selectedName := range selectors.Cases {
			for _, testCase := range spI2CanonicalCases {
				if selectedName == testCase.name {
					spI2ProtocolSelection = true
					break
				}
			}
		}
	}
	includeProtected := slices.Contains(selectors.Tags, spI2HoldoutTag)
	if !includeProtected && len(selectors.Cases) > 0 {
		protectedNames := make(map[string]struct{}, len(spI2CanonicalCases))
		for _, testCase := range spI2CanonicalCases {
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

	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	filtered := ScaleCorpus{Cases: make([]ScaleCase, 0, len(corpus.Cases))}
	protected := ScaleCorpus{Cases: make([]ScaleCase, 0, len(cohort.holdoutKeys))}
	for _, testCase := range corpus.Cases {
		key := performanceKey{
			dataset: testCase.Dataset,
			name:    testCase.Name,
			backend: ModePostgresSQL,
		}
		if _, isProtected := cohort.holdoutKeys[key]; isProtected {
			protected.Cases = append(protected.Cases, testCase)
			continue
		}
		filtered.Cases = append(filtered.Cases, testCase)
	}
	var selected ScaleCorpus
	var manifest SelectionManifest
	if spI2ProtocolSelection {
		selected, manifest, err = selectScaleCorpusValidated(filtered, selectors)
	} else {
		selected, manifest, err = selectRunnableScaleCorpus(filtered, selectors)
	}
	if err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}
	manifest.FullDeclarationCount = len(corpus.DeclaredBackends())
	manifest.OmittedDeclarationCount = manifest.FullDeclarationCount - manifest.SelectedDeclarationCount
	if !spI2ProtocolSelection && manifest.ProtectedDeclarationCount > 0 {
		spI1Cohort, err := canonicalSPI1Cohort()
		if err != nil {
			return ScaleCorpus{}, SelectionManifest{}, err
		}
		for _, testCase := range corpus.Cases {
			key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
			if _, isProtected := spI1Cohort.holdoutKeys[key]; isProtected {
				protected.Cases = append(protected.Cases, testCase)
			}
		}
	}
	manifest.ProtectedDeclarationCount = len(protected.DeclaredBackends())
	manifest.ProtectedDeclarationSHA256 = declarationSHA256(protected.DeclaredBackends())
	return selected, manifest, nil
}

// validateSPI2HoldoutCaptureConfig validates spi2 holdout capture config.
func validateSPI2HoldoutCaptureConfig(cfg config) error {
	if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL || cfg.ExistingGraph || cfg.Discovery {
		return fmt.Errorf("SP-I2 holdout capture requires one managed PostgreSQL fixed-confirmation mode")
	}
	if cfg.Iterations < 50 || cfg.WarmupIterations < 20 || cfg.PoolSize != 1 || len(cfg.Concurrency) != 0 {
		return fmt.Errorf("SP-I2 holdout capture requires at least 50 samples, 20 warmups, pool size 1, and no concurrency block")
	}
	if cfg.Round < 1 || cfg.Round > 20 || cfg.Block != cfg.Round || cfg.ArmOrder < 1 || cfg.ArmOrder > 2 ||
		strings.TrimSpace(cfg.RunUUID) == "" {
		return fmt.Errorf("SP-I2 holdout capture requires rounds 1-20, block equal to round, a two-arm order, and an explicit shared run UUID")
	}
	baseline := string(optimize.ShortestPathExecutorS4CanonicalDistance)
	candidate := string(optimize.ShortestPathExecutorI2GuardedDistance)
	expectedArm, expectedOrder := "", 0
	switch cfg.PostgresForceShortest {
	case baseline:
		expectedArm = "sp-i2-s4"
		expectedOrder = 1
		if cfg.Round%2 == 0 {
			expectedOrder = 2
		}
	case candidate:
		expectedArm = "sp-i2-candidate"
		expectedOrder = 2
		if cfg.Round%2 == 0 {
			expectedOrder = 1
		}
	default:
		return fmt.Errorf("SP-I2 holdout capture must force exact S4 distance or guarded SP-I2 distance")
	}
	if cfg.Arm != expectedArm || cfg.ArmOrder != expectedOrder {
		return fmt.Errorf("SP-I2 holdout capture round %d requires arm %q at order %d", cfg.Round, expectedArm, expectedOrder)
	}
	if !cfg.PostgresRepeatableRead || cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic ||
		cfg.PostgresProductionManifest != "" || cfg.PostgresForceExpansion != "" ||
		cfg.PostgresExpansionOrientationShadow || cfg.PostgresExpansionOrientationTournament ||
		cfg.PostgresReferences || len(cfg.PostgresReferenceArms) != 0 || cfg.Baseline != "" ||
		cfg.BundleDir != "" || len(cfg.BundleEvidence) != 0 {
		return fmt.Errorf("SP-I2 holdout capture requires forced Repeatable Read with diagnostic telemetry and no supplemental PostgreSQL arms")
	}
	if cfg.OutputJSONL == "" || cfg.Round > 1 && !cfg.AppendJSONL {
		return fmt.Errorf("SP-I2 holdout capture requires a JSONL output and append mode after round 1")
	}
	return validateDistinctSPI2Paths(map[string]string{
		"freeze manifest": cfg.SPI2Freeze, "discovery report": cfg.SPI2DiscoveryReport,
		"training baseline artifact":  cfg.SPI2TrainingBaseline,
		"training candidate artifact": cfg.SPI2TrainingCandidate,
		"training resource report":    cfg.SPI2TrainingResource,
		"capture JSONL":               cfg.OutputJSONL, "capture summary": cfg.Summary, "capture JSON summary": cfg.SummaryJSON,
	})
}
