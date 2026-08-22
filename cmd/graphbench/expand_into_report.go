// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// expandIntoStudyReportVersion reserves the stable protocol value used to recognize expand into study report version across artifacts and executions.
const expandIntoStudyReportVersion = 2

// expandIntoStudyArms contains the frozen expand into study arms declaration consulted by package validation.
var expandIntoStudyArms = []string{
	"expand_into_pair_join",
	"expand_into_lower_degree_scan",
	"expand_into_pair_cache",
}

// ExpandIntoStudyOptions selects the discovery or confirmation evidence protocol.
type ExpandIntoStudyOptions struct {
	// Seed makes randomized statistical procedures reproducible.
	Seed int64
	// Confidence sets the requested statistical confidence level.
	Confidence float64
	// BootstrapCount records the number of bootstrap count.
	BootstrapCount int
	// Protocol identifies the protocol.
	Protocol string
	// MaterialityRatio supplies the materiality ratio input to the ExpandIntoStudyOptions contract.
	MaterialityRatio float64
	// MaterialityAbsolute supplies the materiality absolute input to the ExpandIntoStudyOptions contract.
	MaterialityAbsolute time.Duration
	// P95RatioLimit supplies the p95 ratio limit input to the ExpandIntoStudyOptions contract.
	P95RatioLimit float64
}

// ExpandIntoStudyReport contains exact three-arm fixed-one-hop evidence.
type ExpandIntoStudyReport struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// ArtifactSHA256 binds the referenced artifact content by SHA-256 digest.
	ArtifactSHA256 string `json:"artifact_sha256"`
	// Protocol identifies the protocol.
	Protocol string `json:"protocol"`
	// Confidence sets the requested statistical confidence level.
	Confidence float64 `json:"confidence_level"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// TrainingCases supplies the training cases input to the ExpandIntoStudyReport contract.
	TrainingCases int `json:"training_cases"`
	// HoldoutCases supplies the holdout cases input to the ExpandIntoStudyReport contract.
	HoldoutCases int `json:"holdout_cases"`
	// TrainingPassed indicates whether training passed applies.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed indicates whether holdout passed applies.
	HoldoutPassed bool `json:"holdout_passed"`
	// QualificationPassed indicates whether qualification passed applies.
	QualificationPassed bool `json:"qualification_passed"`
	// PromotionEligible indicates whether promotion eligible applies.
	PromotionEligible bool `json:"promotion_eligible"`
	// Winner supplies the winner input to the ExpandIntoStudyReport contract.
	Winner string `json:"winner,omitempty"`
	// Cases contains the per-workload evidence underlying the aggregate decision.
	Cases []ExpandIntoStudyCase `json:"cases"`
}

// ExpandIntoStudyCase reports exactness, order balance, plan shape, and latency for one pair workload.
type ExpandIntoStudyCase struct {
	// Dataset identifies the fixture dataset that supplies the workload graph.
	Dataset string `json:"dataset"`
	// Name identifies the name.
	Name string `json:"name"`
	// Tier supplies the tier input to the ExpandIntoStudyCase contract.
	Tier string `json:"tier,omitempty"`
	// QualificationSplit assigns the workload to training, holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// Rounds records the number of rounds.
	Rounds int `json:"rounds"`
	// Winner supplies the winner input to the ExpandIntoStudyCase contract.
	Winner string `json:"descriptive_median_winner,omitempty"`
	// QualifiedWinner supplies the qualified winner input to the ExpandIntoStudyCase contract.
	QualifiedWinner string `json:"qualified_winner,omitempty"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// Reasons explains each failed or inapplicable validation gate.
	Reasons []string `json:"reasons,omitempty"`
	// ArmResults supplies the arm results input to the ExpandIntoStudyCase contract.
	ArmResults []ExpandIntoStudyArmEvidence `json:"arms"`
}

// ExpandIntoStudyArmEvidence records one exact plan-study arm and its ratio to the direct pair join.
type ExpandIntoStudyArmEvidence struct {
	// Name identifies the name.
	Name string `json:"name"`
	// Architecture supplies the architecture input to the ExpandIntoStudyArmEvidence contract.
	Architecture string `json:"architecture"`
	// ImplementationID identifies the implementation id.
	ImplementationID string `json:"implementation_id"`
	// SQLFingerprint supplies the sql fingerprint input to the ExpandIntoStudyArmEvidence contract.
	SQLFingerprint string `json:"sql_fingerprint"`
	// Samples supplies the samples input to the ExpandIntoStudyArmEvidence contract.
	Samples int `json:"samples"`
	// Median supplies the median input to the ExpandIntoStudyArmEvidence contract.
	Median time.Duration `json:"median"`
	// P95 supplies the p95 input to the ExpandIntoStudyArmEvidence contract.
	P95 time.Duration `json:"p95"`
	// MedianRatioToDirect supplies the median ratio to direct input to the ExpandIntoStudyArmEvidence contract.
	MedianRatioToDirect *RatioInterval `json:"median_ratio_to_direct,omitempty"`
	// MedianSavingToDirect supplies the median saving to direct input to the ExpandIntoStudyArmEvidence contract.
	MedianSavingToDirect *DurationInterval `json:"median_saving_to_direct,omitempty"`
	// P95RatioToDirect supplies the p95 ratio to direct input to the ExpandIntoStudyArmEvidence contract.
	P95RatioToDirect *RatioInterval `json:"p95_ratio_to_direct,omitempty"`
	// Material indicates whether material applies.
	Material bool `json:"material"`
	// P95Contained indicates whether p95 contained applies.
	P95Contained bool `json:"p95_contained"`
	// QualifiedWinner indicates whether qualified winner applies.
	QualifiedWinner bool `json:"qualified_winner"`
	// PlanModes supplies the plan modes input to the ExpandIntoStudyArmEvidence contract.
	PlanModes []ExpandIntoPlanMode `json:"plan_modes"`
}

// ExpandIntoPlanMode summarizes the PostgreSQL shapes observed under one plan-cache mode.
type ExpandIntoPlanMode struct {
	// PlanCacheMode identifies the plan cache mode.
	PlanCacheMode string `json:"plan_cache_mode"`
	// Fingerprints supplies the fingerprints input to the ExpandIntoPlanMode contract.
	Fingerprints []string `json:"plan_fingerprints"`
	// OperatorFamilies supplies the operator families input to the ExpandIntoPlanMode contract.
	OperatorFamilies []string `json:"operator_families"`
	// ParameterizedIndex indicates whether parameterized index applies.
	ParameterizedIndex bool `json:"parameterized_index"`
	// Memoize indicates whether memoize applies.
	Memoize bool `json:"memoize"`
	// HashJoin indicates whether hash join applies.
	HashJoin bool `json:"hash_join"`
}

// expandIntoArmSeries accumulates matched observations used to evaluate expand into arm.
type expandIntoArmSeries struct {
	// identity retains the identity while expandIntoArmSeries is assembled or evaluated.
	identity postgresReferenceSpec
	// samples retains the samples while expandIntoArmSeries is assembled or evaluated.
	samples roundSamples
	// plans retains the plans while expandIntoArmSeries is assembled or evaluated.
	plans map[string]map[string][]string
}

// buildExpandIntoStudyReport validates all three exact arms and constructs descriptive crossover evidence.
func buildExpandIntoStudyReport(records []CaseResult, options ExpandIntoStudyOptions) (ExpandIntoStudyReport, error) {
	protocol := options.Protocol
	if protocol == "" {
		protocol = referencePairProtocolDiscovery
	}
	minimumWarmups, minimumRounds, maximumRounds, minimumSamples := 5, 5, 20, 10
	if protocol == referencePairProtocolConfirmation {
		minimumWarmups, minimumRounds, maximumRounds, minimumSamples = 20, 10, 20, 50
	} else if protocol != referencePairProtocolDiscovery {
		return ExpandIntoStudyReport{}, fmt.Errorf("unsupported ExpandInto study protocol %q", protocol)
	}
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return ExpandIntoStudyReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.MaterialityRatio == 0 {
		options.MaterialityRatio = .95
	}
	if options.MaterialityRatio <= 0 || options.MaterialityRatio >= 1 {
		return ExpandIntoStudyReport{}, fmt.Errorf("materiality ratio must be between 0 and 1")
	}
	if options.MaterialityAbsolute == 0 {
		options.MaterialityAbsolute = 100 * time.Microsecond
	}
	if options.MaterialityAbsolute < 0 {
		return ExpandIntoStudyReport{}, fmt.Errorf("materiality absolute must not be negative")
	}
	if options.P95RatioLimit == 0 {
		options.P95RatioLimit = 1.05
	}
	if options.P95RatioLimit <= 0 {
		return ExpandIntoStudyReport{}, fmt.Errorf("p95 ratio limit must be positive")
	}

	// key identifies a dataset and workload pair in the collected evidence.
	type key struct {
		// dataset identifies the fixture dataset containing the workload.
		dataset string

		// name identifies the workload within the dataset.
		name string
	}

	// caseSeries accumulates the evidence arms and rounds for one workload.
	type caseSeries struct {
		// tier retains the tier while caseSeries is assembled or evaluated.
		tier string
		// split retains the split while caseSeries is assembled or evaluated.
		split string
		// arms retains the arms while caseSeries is assembled or evaluated.
		arms map[string]*expandIntoArmSeries
		// rounds retains the rounds while caseSeries is assembled or evaluated.
		rounds map[int]struct{}
		// planModes retains the plan modes while caseSeries is assembled or evaluated.
		planModes map[string]struct{}
		// problems retains the problems while caseSeries is assembled or evaluated.
		problems map[string]struct{}
	}
	series := map[key]*caseSeries{}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL || record.Category != "expand_into_one_hop" {
			continue
		}
		if record.Status != StatusOK || record.Environment == nil || record.Environment.WarmupIterations < minimumWarmups {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s lacks a successful %d-warmup PostgreSQL record", record.Dataset, record.Name, minimumWarmups)
		}
		caseKey := key{record.Dataset, record.Name}
		current := series[caseKey]
		if current == nil {
			current = &caseSeries{
				tier:      record.Shape.FixtureTier,
				split:     record.Shape.QualificationSplit,
				arms:      map[string]*expandIntoArmSeries{},
				rounds:    map[int]struct{}{},
				planModes: map[string]struct{}{},
				problems:  map[string]struct{}{},
			}
			series[caseKey] = current
		} else if current.tier != record.Shape.FixtureTier {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s changes fixture tier across rounds", record.Dataset, record.Name)
		} else if current.split != record.Shape.QualificationSplit {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s changes qualification split across rounds", record.Dataset, record.Name)
		}
		if current.split != "training" && current.split != "holdout" {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s requires a training or holdout qualification split", record.Dataset, record.Name)
		}
		if record.Environment.Round < 1 {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s has an invalid measurement round %d", record.Dataset, record.Name, record.Environment.Round)
		}
		if _, duplicate := current.rounds[record.Environment.Round]; duplicate {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s has duplicate round %d", record.Dataset, record.Name, record.Environment.Round)
		}
		current.rounds[record.Environment.Round] = struct{}{}
		cacheMode := ""
		if record.PostgresEnvironment != nil {
			cacheMode = record.PostgresEnvironment.PlanCacheMode
		}
		if cacheMode != "auto" && cacheMode != "force_custom_plan" && cacheMode != "force_generic_plan" {
			current.problems[fmt.Sprintf("round %d has missing or unsupported plan_cache_mode %q", record.Environment.Round, cacheMode)] = struct{}{}
		} else {
			current.planModes[cacheMode] = struct{}{}
		}
		for _, armName := range expandIntoStudyArms {
			reference := findReference(record.PostgresReferences, armName)
			if reference == nil {
				return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s round %d lacks ExpandInto arm %s", record.Dataset, record.Name, record.Environment.Round, armName)
			}
			if !reference.FullComparator || reference.SemanticValidation != "exact_public_observation" || reference.RowCount != record.RowCount || !equalStrings(reference.ObservedRows, record.ObservedRows) {
				return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s arm %s is not an exact public comparator", record.Dataset, record.Name, armName)
			}
			if reference.Stats.WarmupIterations < minimumWarmups {
				return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s arm %s has fewer than %d warmups", record.Dataset, record.Name, armName, minimumWarmups)
			}
			arm := current.arms[armName]
			identity := normalizedReferenceSpec(postgresReferenceSpec{
				name:               reference.Name,
				architecture:       reference.Architecture,
				implementationID:   reference.ImplementationID,
				stateShape:         reference.StateShape,
				observationShape:   reference.ObservationShape,
				semanticValidation: reference.SemanticValidation,
				boundary:           reference.Boundary,
				fullComparator:     reference.FullComparator,
				timingBoundary:     reference.TimingBoundary,
				sql:                reference.SQL,
			})
			if arm == nil {
				arm = &expandIntoArmSeries{
					identity: identity,
					samples:  roundSamples{},
					plans:    map[string]map[string][]string{},
				}
				current.arms[armName] = arm
			} else if arm.identity.architecture != identity.architecture || arm.identity.implementationID != identity.implementationID || normalizedSQLFingerprint(arm.identity.sql) != normalizedSQLFingerprint(identity.sql) {
				return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s arm %s identity changed across rounds", record.Dataset, record.Name, armName)
			}
			for _, sample := range reference.Stats.Samples {
				if sample.Classification == "warm" && sample.Duration > 0 {
					arm.samples[record.Environment.Round] = append(arm.samples[record.Environment.Round], sample.Duration)
				}
			}
			if len(reference.PostgresPlan) == 0 {
				current.problems[fmt.Sprintf("%s round %d has no persisted PostgreSQL plan", armName, record.Environment.Round)] = struct{}{}
			}
			planModeKey := cacheMode
			if planModeKey == "" {
				planModeKey = "unknown"
			}
			fingerprint := normalizedSQLFingerprint(strings.Join(reference.PostgresPlan, "\n"))
			if arm.plans[planModeKey] == nil {
				arm.plans[planModeKey] = map[string][]string{}
			}
			arm.plans[planModeKey][fingerprint] = append([]string(nil), reference.PostgresPlan...)
		}
		if err := validateExpandIntoRoundOrder(record.Environment.Round, record.PostgresReferences); err != nil {
			return ExpandIntoStudyReport{}, fmt.Errorf("%s/%s: %w", record.Dataset, record.Name, err)
		}
	}
	if len(series) == 0 {
		return ExpandIntoStudyReport{}, fmt.Errorf("artifact has no PostgreSQL ExpandInto study records")
	}

	report := ExpandIntoStudyReport{
		Version:        expandIntoStudyReportVersion,
		Protocol:       protocol,
		Confidence:     options.Confidence,
		Passed:         true,
		TrainingPassed: true,
		HoldoutPassed:  true,
	}
	keys := make([]key, 0, len(series))
	for caseKey := range series {
		keys = append(keys, caseKey)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].dataset < keys[j].dataset || keys[i].dataset == keys[j].dataset && keys[i].name < keys[j].name
	})
	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	qualifiedWinners := map[string]struct{}{}
	for caseIndex, caseKey := range keys {
		current := series[caseKey]
		entry := ExpandIntoStudyCase{
			Dataset:            caseKey.dataset,
			Name:               caseKey.name,
			Tier:               current.tier,
			QualificationSplit: current.split,
			Rounds:             len(current.rounds),
			Passed:             true,
		}
		for problem := range current.problems {
			entry.Reasons = append(entry.Reasons, problem)
		}
		sort.Strings(entry.Reasons)
		if len(entry.Reasons) > 0 {
			entry.Passed = false
		}
		if entry.Rounds < minimumRounds || entry.Rounds > maximumRounds {
			entry.Passed = false
			entry.Reasons = append(entry.Reasons, fmt.Sprintf("requires %d-%d rounds, got %d", minimumRounds, maximumRounds, entry.Rounds))
		}
		if protocol == referencePairProtocolConfirmation {
			for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
				if _, present := current.planModes[mode]; !present {
					entry.Passed = false
					entry.Reasons = append(entry.Reasons, "confirmation requires plan_cache_mode="+mode)
				}
			}
		}
		direct := current.arms[expandIntoStudyArms[0]].samples
		winnerMedian := time.Duration(1<<63 - 1)
		qualifiedWinnerMedian := time.Duration(1<<63 - 1)
		for armIndex, armName := range expandIntoStudyArms {
			arm := current.arms[armName]
			for _, round := range sortedRoundSet(current.rounds) {
				if len(arm.samples[round]) < minimumSamples {
					entry.Passed = false
					entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s round %d requires %d samples, got %d", armName, round, minimumSamples, len(arm.samples[round])))
				}
			}
			flat := flattenSamples(arm.samples, sortedRounds(arm.samples))
			evidence := ExpandIntoStudyArmEvidence{
				Name:             armName,
				Architecture:     arm.identity.architecture,
				ImplementationID: arm.identity.implementationID,
				SQLFingerprint:   normalizedSQLFingerprint(arm.identity.sql),
				Samples:          len(flat),
				Median:           time.Duration(durationQuantile(flat, .50)),
				P95:              time.Duration(durationQuantile(flat, .95)),
				PlanModes:        expandIntoPlanModes(arm.plans),
			}
			if evidence.Median < winnerMedian {
				winnerMedian, entry.Winner = evidence.Median, armName
			}
			if armName != expandIntoStudyArms[0] {
				baseline, candidate := matchedRounds(direct, arm.samples)
				if len(baseline) > 0 {
					seed := options.Seed + int64(caseIndex*31+armIndex)*7919
					median := bootstrapRoundMedianRatio(baseline, candidate, seed, gateOptions)
					evidence.MedianRatioToDirect = &median
					saving := bootstrapRoundMedianSaving(baseline, candidate, seed+1, gateOptions)
					evidence.MedianSavingToDirect = &saving
					if sampleCount(baseline) >= minimumP95Samples && sampleCount(candidate) >= minimumP95Samples {
						p95 := bootstrapStratifiedP95Ratio(baseline, candidate, seed+2, gateOptions)
						evidence.P95RatioToDirect = &p95
					}
					evidence.Material = median.Upper <= options.MaterialityRatio || saving.Lower >= options.MaterialityAbsolute
					evidence.P95Contained = evidence.P95RatioToDirect != nil && evidence.P95RatioToDirect.Upper <= options.P95RatioLimit
					evidence.QualifiedWinner = evidence.Material && evidence.P95Contained
					if evidence.QualifiedWinner && evidence.Median < qualifiedWinnerMedian {
						qualifiedWinnerMedian, entry.QualifiedWinner = evidence.Median, armName
					}
				}
			}
			entry.ArmResults = append(entry.ArmResults, evidence)
		}
		if protocol == referencePairProtocolConfirmation && entry.QualifiedWinner == "" {
			entry.Passed = false
			entry.Reasons = append(entry.Reasons, "no non-incumbent arm materially beats the direct pair join with p95 containment")
		}
		if protocol == referencePairProtocolConfirmation && entry.Passed {
			qualifiedWinners[entry.QualifiedWinner] = struct{}{}
		}
		if !entry.Passed {
			report.Passed = false
		}
		switch entry.QualificationSplit {
		case "training":
			report.TrainingCases++
			report.TrainingPassed = report.TrainingPassed && entry.Passed
		case "holdout":
			report.HoldoutCases++
			report.HoldoutPassed = report.HoldoutPassed && entry.Passed
		}
		report.Cases = append(report.Cases, entry)
	}
	report.TrainingPassed = report.TrainingCases > 0 && report.TrainingPassed
	report.HoldoutPassed = report.HoldoutCases > 0 && report.HoldoutPassed
	report.QualificationPassed = protocol == referencePairProtocolConfirmation &&
		report.TrainingPassed && report.HoldoutPassed && len(qualifiedWinners) == 1
	if len(qualifiedWinners) == 1 {
		for winner := range qualifiedWinners {
			report.Winner = winner
		}
	}
	report.PromotionEligible = report.QualificationPassed
	if protocol == referencePairProtocolConfirmation && !report.QualificationPassed {
		report.Passed = false
	}
	return report, nil
}

// sortedRoundSet returns declared measurement rounds in stable order, including
// rounds whose arms contain no usable warm sample.
func sortedRoundSet(rounds map[int]struct{}) []int {
	ordered := make([]int, 0, len(rounds))
	for round := range rounds {
		ordered = append(ordered, round)
	}
	sort.Ints(ordered)
	return ordered
}

// equalStrings reports whether two ordered string collections contain identical values.
func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

// validateExpandIntoRoundOrder enforces the predeclared doubled Williams schedule relative to the three selected arms.
func validateExpandIntoRoundOrder(round int, references []PostgresReferenceResult) error {
	base := make([]postgresReferenceSpec, len(expandIntoStudyArms))
	for idx, name := range expandIntoStudyArms {
		base[idx] = postgresReferenceSpec{name: name}
	}
	expected := referenceSpecsForRound(base, round)
	byName := map[string]int{}
	for _, reference := range references {
		if containsString(expandIntoStudyArms, reference.Name) {
			byName[reference.Name] = reference.MeasurementOrder
		}
	}
	for _, name := range expandIntoStudyArms {
		if byName[name] <= 0 {
			return fmt.Errorf("round %d is missing measurement order for %s", round, name)
		}
	}
	for idx := 1; idx < len(expected); idx++ {
		if byName[expected[idx-1].name] >= byName[expected[idx].name] {
			return fmt.Errorf("round %d lacks the declared three-arm carryover order", round)
		}
	}
	return nil
}

// containsString reports whether an exact string occurs in a collection.
func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// expandIntoPlanModes classifies parameterized index, Memoize, and hash alternatives per plan-cache mode.
func expandIntoPlanModes(plans map[string]map[string][]string) []ExpandIntoPlanMode {
	var modes []ExpandIntoPlanMode
	for mode, byFingerprint := range plans {
		evidence := ExpandIntoPlanMode{PlanCacheMode: mode}
		operators := map[string]struct{}{}
		for fingerprint, plan := range byFingerprint {
			evidence.Fingerprints = append(evidence.Fingerprints, fingerprint)
			joined := strings.ToLower(strings.Join(plan, "\n"))
			evidence.ParameterizedIndex = evidence.ParameterizedIndex || strings.Contains(joined, "index scan") && (strings.Contains(joined, "start_id") || strings.Contains(joined, "end_id"))
			evidence.Memoize = evidence.Memoize || strings.Contains(joined, "memoize")
			evidence.HashJoin = evidence.HashJoin || strings.Contains(joined, "hash join")
			for _, line := range plan {
				operator := expandIntoPlanOperator(line)
				if operator != "" {
					operators[operator] = struct{}{}
				}
			}
		}
		for operator := range operators {
			evidence.OperatorFamilies = append(evidence.OperatorFamilies, operator)
		}
		sort.Strings(evidence.Fingerprints)
		sort.Strings(evidence.OperatorFamilies)
		modes = append(modes, evidence)
	}
	sort.Slice(modes, func(i, j int) bool { return modes[i].PlanCacheMode < modes[j].PlanCacheMode })
	return modes
}

// expandIntoPlanOperator removes EXPLAIN decorations while retaining the physical operator family.
func expandIntoPlanOperator(line string) string {
	line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "->"))
	if line == "" || strings.HasPrefix(line, "Filter:") || strings.HasPrefix(line, "Index Cond:") || strings.HasPrefix(line, "Join Filter:") {
		return ""
	}
	if index := strings.Index(line, "  ("); index >= 0 {
		line = line[:index]
	}
	if index := strings.Index(line, " on "); index >= 0 {
		line = line[:index]
	}
	if index := strings.Index(line, " using "); index >= 0 {
		line = line[:index]
	}
	return strings.TrimSpace(line)
}

// createExpandIntoStudyReport reads, validates, fingerprints, and writes a three-arm study artifact.
func createExpandIntoStudyReport(artifactPath, outputPath string, options ExpandIntoStudyOptions) error {
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return err
	}
	report, err := buildExpandIntoStudyReport(records, options)
	if err != nil {
		return err
	}
	report.ArtifactSHA256, err = fileSHA256(artifactPath)
	if err != nil {
		return err
	}
	var output *os.File
	if outputPath == "" {
		output = os.Stdout
	} else {
		if err := ensureOutputDir(outputPath); err != nil {
			return err
		}
		output, err = os.Create(outputPath)
		if err != nil {
			return err
		}
		defer output.Close()
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if !report.Passed {
		return fmt.Errorf("ExpandInto %s evidence did not pass its declared protocol", report.Protocol)
	}
	return nil
}
