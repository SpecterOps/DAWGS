// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const (
	// defaultConfidenceLevel is the default confidence used by qualification reports.
	defaultConfidenceLevel = 0.975
	// minimumTimingNoiseRatio is the smallest relative timing floor accepted for promotion decisions.
	minimumTimingNoiseRatio = 0.05
	// minimumTimingNoiseAbsolute is the smallest absolute timing floor accepted for promotion decisions.
	minimumTimingNoiseAbsolute = 100 * time.Microsecond
)

// benchmarkHostIdentity contains stable host properties that must match an A/A calibration.
type benchmarkHostIdentity struct {
	// GOOS supplies the goos input to the benchmarkHostIdentity contract.
	GOOS string `json:"goos"`
	// GOARCH supplies the goarch input to the benchmarkHostIdentity contract.
	GOARCH string `json:"goarch"`
	// CPUCount records the number of cpu count.
	CPUCount int `json:"cpu_count"`
	// CPUModel supplies the cpu model input to the benchmarkHostIdentity contract.
	CPUModel string `json:"cpu_model"`
	// Kernel supplies the kernel input to the benchmarkHostIdentity contract.
	Kernel string `json:"kernel"`
	// CgroupCPU supplies the cgroup cpu input to the benchmarkHostIdentity contract.
	CgroupCPU string `json:"cgroup_cpu,omitempty"`
	// CgroupMemory supplies the cgroup memory input to the benchmarkHostIdentity contract.
	CgroupMemory string `json:"cgroup_memory,omitempty"`
	// CPUGovernor supplies the cpu governor input to the benchmarkHostIdentity contract.
	CPUGovernor string `json:"cpu_governor,omitempty"`
}

// artifactHostFingerprint returns one stable host fingerprint for all PostgreSQL timing records.
func artifactHostFingerprint(records []CaseResult) (string, error) {
	fingerprint := ""
	found := false
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL || !hasWarmLatencySample(record) {
			continue
		}
		if record.Environment == nil {
			return "", fmt.Errorf("%s/%s has no run environment for host calibration", record.Dataset, record.Name)
		}
		identity := benchmarkHostIdentity{
			GOOS:         strings.TrimSpace(record.Environment.GOOS),
			GOARCH:       strings.TrimSpace(record.Environment.GOARCH),
			CPUCount:     record.Environment.CPUCount,
			CPUModel:     strings.TrimSpace(record.Environment.CPUModel),
			Kernel:       strings.TrimSpace(record.Environment.Kernel),
			CgroupCPU:    strings.TrimSpace(record.Environment.CgroupCPU),
			CgroupMemory: strings.TrimSpace(record.Environment.CgroupMemory),
			CPUGovernor:  strings.TrimSpace(record.Environment.CPUGovernor),
		}
		if identity.GOOS == "" || identity.GOARCH == "" || identity.CPUCount < 1 || identity.CPUModel == "" || identity.Kernel == "" {
			return "", fmt.Errorf("%s/%s has incomplete host identity", record.Dataset, record.Name)
		}
		raw, err := json.Marshal(identity)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(raw)
		current := hex.EncodeToString(digest[:])
		if fingerprint != "" && current != fingerprint {
			return "", fmt.Errorf("PostgreSQL timing artifact mixes host identities")
		}
		fingerprint = current
		found = true
	}
	if !found {
		return "", fmt.Errorf("artifact has no PostgreSQL warm timing records for host calibration")
	}

	return fingerprint, nil
}

// hasWarmLatencySample reports whether has warm latency sample.
func hasWarmLatencySample(record CaseResult) bool {
	for _, sample := range record.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			return true
		}
	}
	return false
}

// validateAAResolutionEvidence verifies schema, checksum, confidence, host, and per-case metric integrity.
func validateAAResolutionEvidence(report *AAResolutionReport, records []CaseResult, confidence float64) error {
	if report == nil {
		return fmt.Errorf("host A/A resolution report is required")
	}
	if report.Version != aaReportVersion {
		return fmt.Errorf("A/A report version must be %d", aaReportVersion)
	}
	if report.Confidence <= 0 || report.Confidence >= 1 || math.IsNaN(report.Confidence) || report.Confidence < confidence {
		return fmt.Errorf("A/A confidence %.4f is below requested confidence %.4f", report.Confidence, confidence)
	}
	if !validSHA256(report.ArtifactSHA256) {
		return fmt.Errorf("A/A artifact SHA-256 is missing or malformed")
	}
	chronology := report.PhysicalChronology
	if chronology == nil || chronology.Version != aaPhysicalChronologyVersion || !chronology.Validated ||
		chronology.ArtifactSHA256 != report.ArtifactSHA256 || !validSHA256(chronology.ArtifactSHA256) ||
		chronology.Rounds < report.MinimumRounds || len(chronology.Arms) != 2 ||
		strings.TrimSpace(chronology.Arms[0]) == "" || strings.TrimSpace(chronology.Arms[1]) == "" || chronology.Arms[0] == chronology.Arms[1] {
		return fmt.Errorf("A/A report lacks artifact-bound physical chronology provenance")
	}
	hostFingerprint, err := artifactHostFingerprint(records)
	if err != nil {
		return err
	}
	if !validSHA256(report.HostFingerprint) || report.HostFingerprint != hostFingerprint {
		return fmt.Errorf("A/A host fingerprint does not match timing artifact host")
	}
	if report.MinimumRounds < minimumGateRounds || report.MinimumSamplesPerArmPerRound < 10 || !report.OrderBalanced {
		return fmt.Errorf("A/A report lacks the balanced discovery evidence protocol")
	}
	if len(report.Cases) == 0 {
		return fmt.Errorf("A/A report contains no case resolution evidence")
	}

	seen := map[performanceKey]struct{}{}
	for _, entry := range report.Cases {
		key := performanceKey{
			dataset: entry.Dataset,
			name:    entry.Name,
			backend: entry.Backend,
		}
		if entry.Dataset == "" || entry.Name == "" || entry.Backend != ModePostgresSQL {
			return fmt.Errorf("A/A report contains malformed case identity")
		}
		if strings.TrimSpace(entry.WorkloadSHA256) == "" {
			return fmt.Errorf("A/A case %s/%s has no workload identity", key.dataset, key.name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("A/A report contains duplicate case %s/%s/%s", key.dataset, key.name, key.backend)
		}
		seen[key] = struct{}{}
		if entry.Rounds < minimumGateRounds || entry.SamplesPerArm < entry.Rounds*report.MinimumSamplesPerArmPerRound {
			return fmt.Errorf("A/A case %s/%s lacks discovery-grade rounds or samples", key.dataset, key.name)
		}
		if err := validateAAMetric(entry.P50); err != nil {
			return fmt.Errorf("A/A case %s/%s p50: %w", key.dataset, key.name, err)
		}
		if err := validateAAMetric(entry.P95); err != nil {
			return fmt.Errorf("A/A case %s/%s p95: %w", key.dataset, key.name, err)
		}
		for _, record := range records {
			if record.Dataset == key.dataset && record.Name == key.name && record.ExecutionMode == key.backend && record.WorkloadSHA256 != entry.WorkloadSHA256 {
				return fmt.Errorf("A/A workload identity does not match %s/%s/%s", key.dataset, key.name, key.backend)
			}
		}
	}

	return nil
}

// workloadSHA256ForKey derives the lookup key used for workload sha256 for.
func workloadSHA256ForKey(records []CaseResult, key performanceKey) (string, error) {
	identity := ""
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		if record.WorkloadSHA256 == "" {
			return "", fmt.Errorf("%s/%s/%s has no workload identity", key.dataset, key.name, key.backend)
		}
		if identity != "" && identity != record.WorkloadSHA256 {
			return "", fmt.Errorf("%s/%s/%s mixes workload identities", key.dataset, key.name, key.backend)
		}
		identity = record.WorkloadSHA256
	}
	if identity == "" {
		return "", fmt.Errorf("%s/%s/%s has no workload record", key.dataset, key.name, key.backend)
	}
	return identity, nil
}

// postgresTimingEnvironmentSHA256ForKey derives the lookup key used for postgres timing environment sha256 for.
func postgresTimingEnvironmentSHA256ForKey(records []CaseResult, key performanceKey) (string, error) {
	identity := ""
	found, missing := false, false
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		found = true
		if record.PostgresEnvironment == nil {
			missing = true
			continue
		}
		value := *record.PostgresEnvironment
		value.AnalyzeState = normalizedAnalyzeState(value.AnalyzeState)
		raw, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("encode %s/%s/%s PostgreSQL timing environment: %w", key.dataset, key.name, key.backend, err)
		}
		digest := sha256.Sum256(raw)
		current := hex.EncodeToString(digest[:])
		if identity != "" && identity != current {
			return "", fmt.Errorf("%s/%s/%s mixes PostgreSQL timing environments", key.dataset, key.name, key.backend)
		}
		identity = current
	}
	if !found {
		return "", fmt.Errorf("%s/%s/%s has no workload record", key.dataset, key.name, key.backend)
	}
	if missing && identity != "" {
		return "", fmt.Errorf("%s/%s/%s has partially missing PostgreSQL timing environment", key.dataset, key.name, key.backend)
	}
	return identity, nil
}

// fixtureSHA256ForKey derives the lookup key used for fixture sha256 for.
func fixtureSHA256ForKey(records []CaseResult, key performanceKey) (string, error) {
	identity := ""
	found, missing := false, false
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		found = true
		if record.Fixture == nil {
			missing = true
			continue
		}
		raw, err := json.Marshal(record.Fixture)
		if err != nil {
			return "", fmt.Errorf("encode %s/%s/%s fixture: %w", key.dataset, key.name, key.backend, err)
		}
		digest := sha256.Sum256(raw)
		current := hex.EncodeToString(digest[:])
		if identity != "" && identity != current {
			return "", fmt.Errorf("%s/%s/%s mixes fixture identities", key.dataset, key.name, key.backend)
		}
		identity = current
	}
	if !found {
		return "", fmt.Errorf("%s/%s/%s has no workload record", key.dataset, key.name, key.backend)
	}
	if missing && identity != "" {
		return "", fmt.Errorf("%s/%s/%s has partially missing fixture identity", key.dataset, key.name, key.backend)
	}
	return identity, nil
}

// normalizedAnalyzeState normalizes d analyze state.
func normalizedAnalyzeState(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	entries := strings.Split(value, ",")
	for index, entry := range entries {
		relation, state, found := strings.Cut(strings.TrimSpace(entry), ":")
		if !found {
			entries[index] = relation
			continue
		}
		state = strings.TrimSpace(state)
		if state != "" && state != "never" {
			state = "analyzed"
		}
		entries[index] = relation + ":" + state
	}
	sort.Strings(entries)
	return strings.Join(entries, ",")
}

// validateAAMetric validates aa metric.
func validateAAMetric(metric AAMetricResolution) error {
	if metric.Ratio.Estimate <= 0 || metric.Ratio.Lower <= 0 || metric.Ratio.Upper <= 0 ||
		metric.Ratio.Lower > metric.Ratio.Estimate || metric.Ratio.Estimate > metric.Ratio.Upper ||
		math.IsNaN(metric.Ratio.Estimate) || math.IsNaN(metric.Ratio.Lower) || math.IsNaN(metric.Ratio.Upper) ||
		math.IsInf(metric.Ratio.Estimate, 0) || math.IsInf(metric.Ratio.Lower, 0) || math.IsInf(metric.Ratio.Upper, 0) {
		return fmt.Errorf("ratio interval is malformed")
	}
	if metric.RatioResolution < 0 || math.IsNaN(metric.RatioResolution) || math.IsInf(metric.RatioResolution, 0) || metric.AbsoluteResolution < 0 {
		return fmt.Errorf("resolution is malformed")
	}
	if metric.AbsoluteChange.Lower > metric.AbsoluteChange.Estimate || metric.AbsoluteChange.Estimate > metric.AbsoluteChange.Upper ||
		metric.AbsoluteResolution < max(absDuration(metric.AbsoluteChange.Lower), absDuration(metric.AbsoluteChange.Upper)) {
		return fmt.Errorf("absolute-change interval is malformed")
	}
	return nil
}

// aaTimingFloor returns host-derived per-case noise with the mandatory relative and absolute minimums.
func aaTimingFloor(report *AAResolutionReport, key performanceKey, p95 bool, configuredRatio float64) (float64, time.Duration, error) {
	for _, entry := range report.Cases {
		if entry.Dataset != key.dataset || entry.Name != key.name || entry.Backend != key.backend {
			continue
		}
		metric := entry.P50
		if p95 {
			metric = entry.P95
		}
		return max(minimumTimingNoiseRatio, configuredRatio, metric.RatioResolution),
			max(minimumTimingNoiseAbsolute, metric.AbsoluteResolution), nil
	}

	return 0, 0, fmt.Errorf("A/A report has no resolution evidence for %s/%s/%s", key.dataset, key.name, key.backend)
}

// validSHA256 reports whether a value is a canonical lowercase SHA-256 digest.
func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// timingTier requires a stable, explicit normal, envelope, or stress classification across artifacts.
func timingTier(key performanceKey, artifacts ...[]CaseResult) (string, error) {
	tier := ""
	found := false
	for _, records := range artifacts {
		for _, record := range records {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
				continue
			}
			current := record.Shape.FixtureTier
			if current != "normal" && current != "envelope" && current != "stress" {
				return "", fmt.Errorf("%s/%s/%s has missing or unsupported fixture tier %q", key.dataset, key.name, key.backend, current)
			}
			if tier != "" && tier != current {
				return "", fmt.Errorf("%s/%s/%s changes fixture tier across artifacts", key.dataset, key.name, key.backend)
			}
			tier = current
			found = true
		}
	}
	if !found {
		return "unknown", nil
	}
	return tier, nil
}

// qualificationSplit requires one stable training, holdout, or diagnostic
// partition for prioritized traversal records. The split is part of the
// workload declaration and may not drift between benchmark arms or rounds.
// Legacy non-traversal records may omit it.
func qualificationSplit(key performanceKey, artifacts ...[]CaseResult) (string, error) {
	split := ""
	found := false
	for _, records := range artifacts {
		for _, record := range records {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
				continue
			}
			current := record.Shape.QualificationSplit
			if current == "" {
				if prioritizedTraversalRecord(record) {
					return "", fmt.Errorf("%s/%s/%s has no frozen qualification split", key.dataset, key.name, key.backend)
				}
				continue
			}
			if current != "training" && current != "holdout" && current != "diagnostic" {
				return "", fmt.Errorf("%s/%s/%s has unsupported qualification split %q", key.dataset, key.name, key.backend, current)
			}
			if split != "" && split != current {
				return "", fmt.Errorf("%s/%s/%s changes qualification split across artifacts", key.dataset, key.name, key.backend)
			}
			split = current
			found = true
		}
	}
	if !found {
		return "legacy", nil
	}
	return split, nil
}

// prioritizedTraversalCategory identifies result families introduced by the
// traversal-priority qualification program. Their split remains mandatory
// even when an artifact was assembled outside the scale-corpus loader.
func prioritizedTraversalCategory(category string) bool {
	switch category {
	case "generated_shortest_path_v2", "generated_all_shortest_path_v2", "expand_into_one_hop", "generated_endpoint_seeded_expansion", "generated_fixed_suffix_expansion_v2", "orientation_shadow":
		return true
	default:
		return false
	}
}

// prioritizedTraversalRecord also recognizes the fixed-suffix v2 and
// boundary datasets whose category intentionally remains compatible with the
// original corpus. Artifact consumers must not mistake that shared category
// for permission to omit the frozen qualification split.
func prioritizedTraversalRecord(record CaseResult) bool {
	if prioritizedTraversalCategory(record.Category) {
		return true
	}

	return record.Category == "generated_fixed_suffix_expansion" &&
		(strings.HasPrefix(record.Dataset, "generated_fixed_suffix_expansion_v2_") ||
			strings.HasPrefix(record.Dataset, "generated_fixed_suffix_expansion_v3_") ||
			strings.HasPrefix(record.Name, "GFSE-V2-") ||
			strings.HasPrefix(record.Name, "GFSE-V3-") ||
			strings.HasPrefix(record.Name, "GFSE-BOUNDARY-"))
}

// prioritizedTraversalKey reports whether either artifact identifies a
// matched performance key as part of the traversal qualification program.
// Looking at both artifacts makes the gate fail closed if one side drops or
// changes the category while preserving the logical case identity.
func prioritizedTraversalKey(key performanceKey, artifacts ...[]CaseResult) bool {
	for _, records := range artifacts {
		for _, record := range records {
			if record.Dataset == key.dataset && record.Name == key.name && record.ExecutionMode == key.backend && prioritizedTraversalRecord(record) {
				return true
			}
		}
	}

	return false
}

// TraversalQualificationStatus reports independent selector-training and
// frozen-holdout coverage for one concrete traversal candidate family.
type TraversalQualificationStatus struct {
	// Family supplies the family input to the TraversalQualificationStatus contract.
	Family string `json:"family"`
	// TrainingCases supplies the training cases input to the TraversalQualificationStatus contract.
	TrainingCases int `json:"training_cases"`
	// HoldoutCases supplies the holdout cases input to the TraversalQualificationStatus contract.
	HoldoutCases int `json:"holdout_cases"`
	// TrainingPassed indicates whether training passed applies.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed indicates whether holdout passed applies.
	HoldoutPassed bool `json:"holdout_passed"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
}

// traversalQualificationFamily returns the most specific stable candidate
// identity available for a matched key. Candidate/right artifacts take
// precedence over incumbent/left artifacts. A conservative semantic family
// remains available for externally assembled artifacts without optimizer or
// runtime telemetry.
func traversalQualificationFamily(key performanceKey, artifacts ...[]CaseResult) string {
	for artifactIdx := len(artifacts) - 1; artifactIdx >= 0; artifactIdx-- {
		for _, record := range artifacts[artifactIdx] {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
				continue
			}
			if record.TraversalTelemetry != nil {
				summary := record.TraversalTelemetry.Summary
				for _, identity := range []string{summary.EmittedIdentity, summary.SelectorVersion} {
					if isOrientationProbePolicy(identity) {
						return identity
					}
				}
				if identity := summary.RequestedIdentity; prioritizedTraversalIdentity(identity) {
					branch := summary.RuntimeBranch
					if branch != "" && branch != "runtime_outcome_unavailable" && branch != "mixed" {
						return identity + "@" + branch
					}
					return identity
				}
			}
			if record.Optimization != nil {
				for outcomeIdx := len(record.Optimization.TargetOutcomes) - 1; outcomeIdx >= 0; outcomeIdx-- {
					outcome := record.Optimization.TargetOutcomes[outcomeIdx]
					for _, identity := range []string{outcome.Candidate, outcome.EmittedPolicy, outcome.PlannedPolicy, outcome.Applied, outcome.Selected} {
						if prioritizedTraversalIdentity(identity) {
							return identity
						}
					}
				}
			}
		}
	}
	for artifactIdx := len(artifacts) - 1; artifactIdx >= 0; artifactIdx-- {
		for _, record := range artifacts[artifactIdx] {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend || record.Optimization == nil {
				continue
			}
			for _, outcome := range record.Optimization.TargetOutcomes {
				if outcome.TargetKind != "" && outcome.TargetKind != "traversal" {
					continue
				}
				if outcome.Family == "SP" || outcome.Family == "ASP" || strings.Contains(outcome.Family, "expansion") {
					return outcome.Family
				}
			}
		}
	}

	for _, records := range artifacts {
		for _, record := range records {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
				continue
			}
			if strings.HasPrefix(record.Dataset, "generated_fixed_suffix_expansion_v3_") || strings.HasPrefix(record.Name, "GFSE-V3-") {
				return string(optimize.ExpansionSearchPolicyOrientationProbeV2)
			}
			switch record.Category {
			case "generated_shortest_path_v2", "generated_all_shortest_path_v2":
				if strings.Contains(strings.ToLower(record.Cypher), "allshortestpaths") || strings.Contains(strings.ToLower(record.Name), "all-shortest") {
					return "ASP"
				}
				return "SP"
			case "generated_endpoint_seeded_expansion":
				return "fixed_prefix_terminal_expansion"
			case "generated_fixed_suffix_expansion", "generated_fixed_suffix_expansion_v2", "orientation_shadow":
				return "orientation-probe-v1"
			case "expand_into_one_hop":
				return "expand-into-study-v1"
			}
		}
	}

	return "prioritized_traversal"
}

// validateCandidateRuntimeEvidence rejects performance attribution to an
// experimental traversal arm unless every warm sample is bound to one
// singular, non-fallback runtime outcome for that measured invocation. A
// same-case diagnostic replay is useful resource evidence but is not allowed
// to attest latency samples because concurrent graph changes or cap outcomes
// could select a different branch.
func validateCandidateRuntimeEvidence(records []CaseResult, key performanceKey) error {
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend || !requiresCandidateRuntimeEvidence(record) {
			continue
		}
		if record.TraversalTelemetry == nil {
			return fmt.Errorf("candidate traversal has no runtime telemetry")
		}
		summary := record.TraversalTelemetry.Summary
		if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable || summary.RuntimeIdentity == "" || summary.RuntimeBranch == "" || summary.RuntimeBranch == "mixed" || summary.RuntimeBranch == "runtime_outcome_unavailable" {
			return fmt.Errorf("candidate traversal runtime outcome is unavailable or mixed")
		}
		if summary.FallbackExecuted == nil {
			return fmt.Errorf("candidate traversal fallback outcome is unavailable")
		}
		if *summary.FallbackExecuted {
			return fmt.Errorf("candidate traversal executed exact fallback %q", summary.FallbackIdentity)
		}
		for _, sample := range record.Stats.Samples {
			if sample.Classification != "warm" || sample.Duration <= 0 {
				continue
			}
			if sample.RequestedIdentity != summary.RequestedIdentity || sample.RuntimeIdentity != summary.RuntimeIdentity || sample.RuntimeBranch != summary.RuntimeBranch || sample.FallbackExecuted == nil || *sample.FallbackExecuted || sample.RuntimeAttestation != "timed_invocation" {
				return fmt.Errorf("warm sample lacks matching singular runtime attribution")
			}
			if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
				return fmt.Errorf("warm sample runtime receipt chain: %w", err)
			}
		}
	}
	return nil
}

// validateRuntimeReceiptEvents validates runtime receipt events.
func validateRuntimeReceiptEvents(events []RuntimeReceiptEvent, runtimeIdentity, runtimeBranch string, fallbackExecuted *bool) error {
	if len(events) == 0 {
		return fmt.Errorf("event chain is missing")
	}
	for idx, event := range events {
		if event.Ordinal != idx+1 || event.RuntimeIdentity == "" || event.RuntimeBranch == "" {
			return fmt.Errorf("event chain is not contiguous")
		}
	}
	terminal := events[len(events)-1]
	if terminal.RuntimeIdentity != runtimeIdentity || terminal.RuntimeBranch != runtimeBranch {
		return fmt.Errorf("terminal event does not match runtime outcome")
	}
	if fallbackExecuted == nil || terminal.FallbackExecuted != *fallbackExecuted {
		return fmt.Errorf("terminal event does not match fallback outcome")
	}
	return nil
}

// runtimeReceiptChains supports benchmark evidence processing for runtime receipt chains.
func runtimeReceiptChains(samples []LatencySample) [][]RuntimeReceiptEvent {
	chains := make([][]RuntimeReceiptEvent, 0)
	for _, sample := range samples {
		if len(sample.RuntimeReceiptEvents) == 0 {
			continue
		}
		chains = append(chains, append([]RuntimeReceiptEvent(nil), sample.RuntimeReceiptEvents...))
	}
	return chains
}

// caseRuntimeReceiptChains supports benchmark evidence processing for case runtime receipt chains.
func caseRuntimeReceiptChains(records []CaseResult, key performanceKey) [][]RuntimeReceiptEvent {
	chains := make([][]RuntimeReceiptEvent, 0)
	for _, record := range records {
		if record.Dataset == key.dataset && record.Name == key.name && record.ExecutionMode == key.backend {
			chains = append(chains, runtimeReceiptChains(record.Stats.Samples)...)
		}
	}
	return chains
}

// requiresCandidateRuntimeEvidence reports whether requires candidate runtime evidence.
func requiresCandidateRuntimeEvidence(record CaseResult) bool {
	if record.TraversalTelemetry != nil {
		summary := record.TraversalTelemetry.Summary
		if isOrientationProbePolicy(summary.EmittedIdentity) || isOrientationProbePolicy(summary.SelectorVersion) {
			return true
		}
		requested := summary.RequestedIdentity
		if strings.HasPrefix(requested, "SP-B") || strings.HasPrefix(requested, "ASP-B") || isOrientationProbePolicy(requested) {
			return true
		}
	}
	if record.Optimization == nil {
		return false
	}
	for _, outcome := range record.Optimization.TargetOutcomes {
		for _, identity := range []string{outcome.Candidate, outcome.EmittedPolicy, outcome.Selected} {
			if strings.HasPrefix(identity, "SP-B") || strings.HasPrefix(identity, "ASP-B") || isOrientationProbePolicy(identity) {
				return true
			}
		}
	}
	return false
}

// prioritizedTraversalIdentity derives the stable identity used to compare prioritized traversal.
func prioritizedTraversalIdentity(identity string) bool {
	return strings.HasPrefix(identity, "SP-") ||
		strings.HasPrefix(identity, "ASP-") ||
		strings.HasPrefix(identity, "EXPANSION-") ||
		isOrientationProbePolicy(identity)
}

// promotionTimingSplit reports whether a frozen qualification partition may
// contribute timing evidence to a promotion decision. Diagnostic records are
// still checked for correctness and resource behavior, but never tune or
// qualify a production selector.
func promotionTimingSplit(split string) bool {
	return split != "diagnostic"
}

// pairedRoundEvidence records independently verifiable observations for paired round.
type pairedRoundEvidence struct {
	// Block supplies the block input to the pairedRoundEvidence contract.
	Block int
	// ArmOrder supplies the arm order input to the pairedRoundEvidence contract.
	ArmOrder int
	// RunUUID identifies the run uuid.
	RunUUID string
	// Arm supplies the arm input to the pairedRoundEvidence contract.
	Arm string
	// Warmups supplies the warmups input to the pairedRoundEvidence contract.
	Warmups int
}

// validatePairedOrderEvidence verifies matched block identity and balanced two-arm ordering for the requested rounds.
func validatePairedOrderEvidence(left, right []CaseResult, key performanceKey, rounds []int, minimumWarmups int) error {
	leftEvidence, err := collectPairedRoundEvidence(left, key)
	if err != nil {
		return err
	}
	rightEvidence, err := collectPairedRoundEvidence(right, key)
	if err != nil {
		return err
	}
	leftFirst := 0
	for _, round := range rounds {
		leftRound, leftOK := leftEvidence[round]
		rightRound, rightOK := rightEvidence[round]
		if !leftOK || !rightOK {
			return fmt.Errorf("%s/%s round %d lacks paired order evidence", key.dataset, key.name, round)
		}
		if leftRound.Warmups < minimumWarmups || rightRound.Warmups < minimumWarmups {
			return fmt.Errorf("%s/%s round %d requires at least %d warmups per arm, got %d/%d", key.dataset, key.name, round, minimumWarmups, leftRound.Warmups, rightRound.Warmups)
		}
		if leftRound.Block < 1 || leftRound.Block != rightRound.Block {
			return fmt.Errorf("%s/%s round %d has missing or mismatched paired block", key.dataset, key.name, round)
		}
		if leftRound.RunUUID == "" || leftRound.RunUUID != rightRound.RunUUID {
			return fmt.Errorf("%s/%s round %d has missing or mismatched paired run UUID", key.dataset, key.name, round)
		}
		if leftRound.Arm == "" || rightRound.Arm == "" || leftRound.Arm == "unlabeled" || rightRound.Arm == "unlabeled" || leftRound.Arm == rightRound.Arm {
			return fmt.Errorf("%s/%s round %d has missing or indistinct arm identity", key.dataset, key.name, round)
		}
		if !((leftRound.ArmOrder == 1 && rightRound.ArmOrder == 2) || (leftRound.ArmOrder == 2 && rightRound.ArmOrder == 1)) {
			return fmt.Errorf("%s/%s round %d lacks a complete two-arm order", key.dataset, key.name, round)
		}
		if leftRound.ArmOrder == 1 {
			leftFirst++
		}
	}
	rightFirst := len(rounds) - leftFirst
	if leftFirst-rightFirst > 1 || rightFirst-leftFirst > 1 {
		return fmt.Errorf("%s/%s paired arm order is not balanced: %d/%d", key.dataset, key.name, leftFirst, rightFirst)
	}

	return nil
}

// collectPairedRoundEvidence collects paired round evidence.
func collectPairedRoundEvidence(records []CaseResult, key performanceKey) (map[int]pairedRoundEvidence, error) {
	evidence := map[int]pairedRoundEvidence{}
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		warmups := record.Stats.WarmupIterations
		if record.Environment != nil {
			if warmups != 0 && record.Environment.WarmupIterations != 0 && warmups != record.Environment.WarmupIterations {
				return nil, fmt.Errorf("%s/%s has inconsistent warmup evidence", key.dataset, key.name)
			}
			if warmups == 0 {
				warmups = record.Environment.WarmupIterations
			}
		}
		for _, sample := range record.Stats.Samples {
			if sample.Classification != "warm" || sample.Duration <= 0 {
				continue
			}
			round := sample.Round
			current := pairedRoundEvidence{
				Block:    sample.Block,
				ArmOrder: sample.ArmOrder,
				RunUUID:  sample.RunUUID,
				Arm:      sample.Arm,
				Warmups:  warmups,
			}
			if record.Environment != nil {
				if round == 0 {
					round = record.Environment.Round
				}
				if current.Block == 0 {
					current.Block = record.Environment.Block
				}
				if current.ArmOrder == 0 {
					current.ArmOrder = record.Environment.ArmOrder
				}
				if current.RunUUID == "" {
					current.RunUUID = record.Environment.RunUUID
				}
				if current.Arm == "" {
					current.Arm = record.Environment.Arm
				}
			}
			if round < 1 {
				return nil, fmt.Errorf("%s/%s has warm sample without a round", key.dataset, key.name)
			}
			if prior, found := evidence[round]; found && prior != current {
				return nil, fmt.Errorf("%s/%s round %d has inconsistent paired order metadata", key.dataset, key.name, round)
			}
			evidence[round] = current
		}
	}
	return evidence, nil
}

// sortedPerformanceKeys returns stable keys from a set.
func sortedPerformanceKeys(values map[performanceKey]struct{}) []performanceKey {
	keys := make([]performanceKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].backend < keys[j].backend
	})
	return keys
}
