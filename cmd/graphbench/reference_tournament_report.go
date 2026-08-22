// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"
)

// referenceTournamentReportVersion reserves the stable protocol value used to recognize reference tournament report version across artifacts and executions.
const referenceTournamentReportVersion = 1

// ReferenceTournamentOptions defines a predeclared three- or five-arm Williams tournament.
// The first arm is always the incumbent.
type ReferenceTournamentOptions struct {
	// Seed makes randomized statistical procedures reproducible.
	Seed int64
	// BootstrapCount records the number of bootstrap count.
	BootstrapCount int
	// Confidence sets the requested statistical confidence level.
	Confidence float64
	// MaterialityRatio supplies the materiality ratio input to the ReferenceTournamentOptions contract.
	MaterialityRatio float64
	// MaterialityAbsolute supplies the materiality absolute input to the ReferenceTournamentOptions contract.
	MaterialityAbsolute time.Duration
	// P95RatioLimit supplies the p95 ratio limit input to the ReferenceTournamentOptions contract.
	P95RatioLimit float64
	// Arms supplies the arms input to the ReferenceTournamentOptions contract.
	Arms []string
	// Protocol identifies the protocol.
	Protocol string
}

// ReferenceTournamentPair groups state that must remain consistent while processing reference tournament pair.
type ReferenceTournamentPair struct {
	// Arm supplies the arm input to the ReferenceTournamentPair contract.
	Arm string `json:"arm"`
	// MedianRatio supplies the median ratio input to the ReferenceTournamentPair contract.
	MedianRatio RatioInterval `json:"median_ratio_to_incumbent"`
	// MedianSaving supplies the median saving input to the ReferenceTournamentPair contract.
	MedianSaving DurationInterval `json:"median_saving_vs_incumbent"`
	// P95Ratio supplies the p95 ratio input to the ReferenceTournamentPair contract.
	P95Ratio RatioInterval `json:"p95_ratio_to_incumbent"`
	// Material indicates whether material applies.
	Material bool `json:"material"`
	// P95Contained indicates whether p95 contained applies.
	P95Contained bool `json:"p95_contained"`
	// QualifiedWinner indicates whether qualified winner applies.
	QualifiedWinner bool `json:"qualified_winner"`
}

// ReferenceTournamentCase records the evidence and decision for one reference tournament workload.
type ReferenceTournamentCase struct {
	// Dataset identifies the fixture dataset that supplies the workload graph.
	Dataset string `json:"dataset"`
	// Name identifies the name.
	Name string `json:"name"`
	// QualificationSplit assigns the workload to training, holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// Winner supplies the winner input to the ReferenceTournamentCase contract.
	Winner string `json:"winner,omitempty"`
	// Rounds records the number of rounds.
	Rounds int `json:"rounds"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// Reasons explains each failed or inapplicable validation gate.
	Reasons []string `json:"reasons,omitempty"`
	// Pairs supplies the pairs input to the ReferenceTournamentCase contract.
	Pairs []ReferenceTournamentPair `json:"pairs"`
}

// ReferenceTournamentReport records the evidence and outcome produced by reference tournament.
type ReferenceTournamentReport struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// ArtifactSHA256 binds the referenced artifact content by SHA-256 digest.
	ArtifactSHA256 string `json:"artifact_sha256,omitempty"`
	// Protocol identifies the protocol.
	Protocol string `json:"protocol"`
	// Incumbent supplies the incumbent input to the ReferenceTournamentReport contract.
	Incumbent string `json:"incumbent"`
	// Winner supplies the winner input to the ReferenceTournamentReport contract.
	Winner string `json:"winner,omitempty"`
	// Arms supplies the arms input to the ReferenceTournamentReport contract.
	Arms []string `json:"arms"`
	// Confidence sets the requested statistical confidence level.
	Confidence float64 `json:"confidence_level"`
	// MaterialityRatio supplies the materiality ratio input to the ReferenceTournamentReport contract.
	MaterialityRatio float64 `json:"materiality_ratio"`
	// MaterialityAbsolute supplies the materiality absolute input to the ReferenceTournamentReport contract.
	MaterialityAbsolute time.Duration `json:"materiality_absolute_lower_limit"`
	// P95RatioLimit supplies the p95 ratio limit input to the ReferenceTournamentReport contract.
	P95RatioLimit float64 `json:"p95_ratio_upper_limit"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// PromotionEligible indicates whether promotion eligible applies.
	PromotionEligible bool `json:"promotion_eligible"`
	// TrainingPassed indicates whether training passed applies.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed indicates whether holdout passed applies.
	HoldoutPassed bool `json:"holdout_passed"`
	// Cases contains the per-workload evidence underlying the aggregate decision.
	Cases []ReferenceTournamentCase `json:"cases"`
}

// tournamentArmSeries accumulates matched observations used to evaluate tournament arm.
type tournamentArmSeries struct {
	// identity retains the identity while tournamentArmSeries is assembled or evaluated.
	identity string
	// samples retains the samples while tournamentArmSeries is assembled or evaluated.
	samples roundSamples
}

// tournamentCaseSeries accumulates matched observations used to evaluate tournament case.
type tournamentCaseSeries struct {
	// split retains the split while tournamentCaseSeries is assembled or evaluated.
	split string
	// arms retains the arms while tournamentCaseSeries is assembled or evaluated.
	arms map[string]*tournamentArmSeries
	// rounds retains the rounds while tournamentCaseSeries is assembled or evaluated.
	rounds map[int]struct{}
}

// buildReferenceTournamentReport builds reference tournament report.
func buildReferenceTournamentReport(records []CaseResult, options ReferenceTournamentOptions) (ReferenceTournamentReport, error) {
	if err := normalizeReferenceTournamentOptions(&options); err != nil {
		return ReferenceTournamentReport{}, err
	}
	minimumWarmups, minimumRounds, maximumRounds, minimumSamples, err := referenceTournamentRequirements(options.Protocol)
	if err != nil {
		return ReferenceTournamentReport{}, err
	}

	series := map[performanceKey]*tournamentCaseSeries{}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL || !recordContainsAnyReference(record, options.Arms) {
			continue
		}
		if err := addReferenceTournamentRecord(series, record, options.Arms, minimumWarmups); err != nil {
			return ReferenceTournamentReport{}, err
		}
	}
	if len(series) == 0 {
		return ReferenceTournamentReport{}, fmt.Errorf("artifact has no PostgreSQL reference tournament records")
	}

	report := ReferenceTournamentReport{
		Version:             referenceTournamentReportVersion,
		Protocol:            options.Protocol,
		Arms:                append([]string(nil), options.Arms...),
		Incumbent:           options.Arms[0],
		Confidence:          options.Confidence,
		MaterialityRatio:    options.MaterialityRatio,
		MaterialityAbsolute: options.MaterialityAbsolute,
		P95RatioLimit:       options.P95RatioLimit,
		Passed:              true,
		TrainingPassed:      true,
		HoldoutPassed:       true,
	}
	keys := sortedTournamentPerformanceKeys(series)
	gate := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	winners := map[string]struct{}{}
	for caseIndex, key := range keys {
		entry := evaluateReferenceTournamentCase(key, series[key], options, gate, caseIndex, minimumRounds, maximumRounds, minimumSamples)
		if entry.Passed {
			winners[entry.Winner] = struct{}{}
		} else {
			report.Passed = false
		}
		switch entry.QualificationSplit {
		case "training":
			report.TrainingPassed = report.TrainingPassed && entry.Passed
		case "holdout":
			report.HoldoutPassed = report.HoldoutPassed && entry.Passed
		}
		report.Cases = append(report.Cases, entry)
	}

	report.TrainingPassed = report.TrainingPassed && tournamentHasSplit(report.Cases, "training")
	report.HoldoutPassed = report.HoldoutPassed && tournamentHasSplit(report.Cases, "holdout")
	if len(winners) == 1 {
		for winner := range winners {
			report.Winner = winner
		}
	} else {
		report.Passed = false
	}
	report.Passed = report.Passed && report.TrainingPassed && report.HoldoutPassed && report.Winner != ""
	report.PromotionEligible = options.Protocol == referencePairProtocolConfirmation && report.Passed
	return report, nil
}

// normalizeReferenceTournamentOptions normalizes reference tournament options.
func normalizeReferenceTournamentOptions(options *ReferenceTournamentOptions) error {
	if len(options.Arms) != 3 && len(options.Arms) != 5 {
		return fmt.Errorf("reference tournament requires exactly 3 or 5 arms")
	}
	seen := map[string]struct{}{}
	for _, arm := range options.Arms {
		if arm == "" {
			return fmt.Errorf("reference tournament arm must not be empty")
		}
		if _, duplicate := seen[arm]; duplicate {
			return fmt.Errorf("reference tournament arms must be distinct")
		}
		seen[arm] = struct{}{}
	}
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.MaterialityRatio == 0 {
		options.MaterialityRatio = .95
	}
	if options.MaterialityRatio <= 0 || options.MaterialityRatio >= 1 {
		return fmt.Errorf("materiality ratio must be between 0 and 1")
	}
	if options.MaterialityAbsolute == 0 {
		options.MaterialityAbsolute = 100 * time.Microsecond
	}
	if options.MaterialityAbsolute < 0 {
		return fmt.Errorf("materiality absolute must not be negative")
	}
	if options.P95RatioLimit == 0 {
		options.P95RatioLimit = 1.05
	}
	if options.P95RatioLimit <= 0 {
		return fmt.Errorf("p95 ratio limit must be positive")
	}
	if options.Protocol == "" {
		options.Protocol = referencePairProtocolConfirmation
	}
	return nil
}

// referenceTournamentRequirements supports benchmark evidence processing for reference tournament requirements.
func referenceTournamentRequirements(protocol string) (int, int, int, int, error) {
	switch protocol {
	case referencePairProtocolDiscovery:
		return 5, 5, 20, 10, nil
	case referencePairProtocolConfirmation:
		return 20, 10, 20, 50, nil
	default:
		return 0, 0, 0, 0, fmt.Errorf("unsupported reference tournament protocol %q", protocol)
	}
}

// recordContainsAnyReference supports benchmark evidence processing for record contains any reference.
func recordContainsAnyReference(record CaseResult, arms []string) bool {
	for _, reference := range record.PostgresReferences {
		if slices.Contains(arms, reference.Name) {
			return true
		}
	}
	return false
}

// addReferenceTournamentRecord supports benchmark evidence processing for add reference tournament record.
func addReferenceTournamentRecord(series map[performanceKey]*tournamentCaseSeries, record CaseResult, arms []string, minimumWarmups int) error {
	if record.Status != StatusOK || record.Environment == nil || record.Environment.WarmupIterations < minimumWarmups {
		return fmt.Errorf("%s/%s lacks a successful %d-warmup PostgreSQL record", record.Dataset, record.Name, minimumWarmups)
	}
	if record.Shape.QualificationSplit != "training" && record.Shape.QualificationSplit != "holdout" {
		return fmt.Errorf("%s/%s requires a training or holdout qualification split", record.Dataset, record.Name)
	}
	key := performanceKey{
		dataset: record.Dataset,
		name:    record.Name,
		backend: ModePostgresSQL,
	}
	current := series[key]
	if current == nil {
		current = &tournamentCaseSeries{
			split:  record.Shape.QualificationSplit,
			arms:   map[string]*tournamentArmSeries{},
			rounds: map[int]struct{}{},
		}
		series[key] = current
	} else if current.split != record.Shape.QualificationSplit {
		return fmt.Errorf("%s/%s changes qualification split across rounds", record.Dataset, record.Name)
	}
	if record.Environment.Round < 1 {
		return fmt.Errorf("%s/%s has invalid tournament round %d", record.Dataset, record.Name, record.Environment.Round)
	}
	if _, duplicate := current.rounds[record.Environment.Round]; duplicate {
		return fmt.Errorf("%s/%s has duplicate tournament round %d", record.Dataset, record.Name, record.Environment.Round)
	}
	current.rounds[record.Environment.Round] = struct{}{}
	if err := validateTournamentRoundOrder(record.Environment.Round, arms, record.PostgresReferences); err != nil {
		return fmt.Errorf("%s/%s: %w", record.Dataset, record.Name, err)
	}
	for _, name := range arms {
		if err := addReferenceTournamentArm(current, record, name, minimumWarmups); err != nil {
			return err
		}
	}
	return nil
}

// addReferenceTournamentArm supports benchmark evidence processing for add reference tournament arm.
func addReferenceTournamentArm(current *tournamentCaseSeries, record CaseResult, name string, minimumWarmups int) error {
	reference := findReference(record.PostgresReferences, name)
	if reference == nil {
		return fmt.Errorf("%s/%s lacks tournament arm %s", record.Dataset, record.Name, name)
	}
	if !reference.FullComparator || reference.SemanticValidation != "exact_public_observation" || reference.RowCount != record.RowCount || !slices.Equal(reference.ObservedRows, record.ObservedRows) {
		return fmt.Errorf("%s/%s arm %s is not an exact public comparator", record.Dataset, record.Name, name)
	}
	if reference.Stats.WarmupIterations < minimumWarmups || reference.ImplementationID == "" || reference.SQLFingerprint == "" {
		return fmt.Errorf("%s/%s arm %s lacks warmups or identity", record.Dataset, record.Name, name)
	}
	identity := reference.Architecture + "\x00" + reference.ImplementationID + "\x00" + reference.SQLFingerprint + "\x00" + reference.Boundary
	arm := current.arms[name]
	if arm == nil {
		arm = &tournamentArmSeries{
			identity: identity,
			samples:  roundSamples{},
		}
		current.arms[name] = arm
	} else if arm.identity != identity {
		return fmt.Errorf("%s/%s arm %s identity changed", record.Dataset, record.Name, name)
	}
	for _, sample := range reference.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			arm.samples[record.Environment.Round] = append(arm.samples[record.Environment.Round], sample.Duration)
		}
	}
	return nil
}

// sortedTournamentPerformanceKeys returns the lookup keys used for sorted tournament performance.
func sortedTournamentPerformanceKeys(series map[performanceKey]*tournamentCaseSeries) []performanceKey {
	keys := make([]performanceKey, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].dataset < keys[j].dataset || keys[i].dataset == keys[j].dataset && keys[i].name < keys[j].name
	})
	return keys
}

// evaluateReferenceTournamentCase supports benchmark evidence processing for evaluate reference tournament case.
func evaluateReferenceTournamentCase(key performanceKey, current *tournamentCaseSeries, options ReferenceTournamentOptions, gate PerfGateOptions, caseIndex, minimumRounds, maximumRounds, minimumSamples int) ReferenceTournamentCase {
	entry := ReferenceTournamentCase{
		Dataset:            key.dataset,
		Name:               key.name,
		QualificationSplit: current.split,
		Rounds:             len(current.rounds),
		Passed:             true,
	}
	if entry.Rounds < minimumRounds || entry.Rounds > maximumRounds {
		entry.Passed = false
		entry.Reasons = append(entry.Reasons, fmt.Sprintf("requires %d-%d Williams rounds, got %d", minimumRounds, maximumRounds, entry.Rounds))
	}
	for _, name := range options.Arms {
		for _, round := range sortedRoundSet(current.rounds) {
			if len(current.arms[name].samples[round]) < minimumSamples {
				entry.Passed = false
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s round %d requires %d samples", name, round, minimumSamples))
			}
		}
	}

	incumbent := current.arms[options.Arms[0]].samples
	bestMedian := time.Duration(1<<63 - 1)
	for armIndex, name := range options.Arms[1:] {
		baseline, candidate := matchedRounds(incumbent, current.arms[name].samples)
		seed := options.Seed + int64(caseIndex*31+armIndex)*7919
		pair := ReferenceTournamentPair{
			Arm:          name,
			MedianRatio:  bootstrapRoundMedianRatio(baseline, candidate, seed, gate),
			MedianSaving: bootstrapRoundMedianSaving(baseline, candidate, seed+1, gate),
			P95Ratio:     bootstrapStratifiedP95Ratio(baseline, candidate, seed+2, gate),
		}
		pair.Material = pair.MedianRatio.Upper <= options.MaterialityRatio || pair.MedianSaving.Lower >= options.MaterialityAbsolute
		pair.P95Contained = pair.P95Ratio.Upper <= options.P95RatioLimit
		pair.QualifiedWinner = pair.Material && pair.P95Contained
		if pair.QualifiedWinner {
			median := time.Duration(durationQuantile(flattenSamples(candidate, sortedRounds(candidate)), .5))
			if median < bestMedian {
				bestMedian, entry.Winner = median, name
			}
		}
		entry.Pairs = append(entry.Pairs, pair)
	}
	if entry.Winner == "" {
		entry.Passed = false
		entry.Reasons = append(entry.Reasons, "no candidate materially beats the incumbent with p95 containment")
	}
	return entry
}

// tournamentHasSplit supports benchmark evidence processing for tournament has split.
func tournamentHasSplit(cases []ReferenceTournamentCase, split string) bool {
	for _, entry := range cases {
		if entry.QualificationSplit == split {
			return true
		}
	}
	return false
}

// validateTournamentRoundOrder validates tournament round order.
func validateTournamentRoundOrder(round int, arms []string, references []PostgresReferenceResult) error {
	base := make([]postgresReferenceSpec, len(arms))
	for idx, arm := range arms {
		base[idx] = postgresReferenceSpec{name: arm}
	}
	expected := referenceSpecsForRound(base, round)
	orders := map[string]int{}
	for _, reference := range references {
		if slices.Contains(arms, reference.Name) {
			orders[reference.Name] = reference.MeasurementOrder
		}
	}
	for idx, spec := range expected {
		// Production is measurement position one when more than one reference
		// arm is selected; the tournament occupies the contiguous suffix.
		if orders[spec.name] != idx+2 {
			return fmt.Errorf("round %d does not match the declared %d-arm Williams order", round, len(arms))
		}
	}
	return nil
}

// createReferenceTournamentReport creates reference tournament report.
func createReferenceTournamentReport(artifactPath, outputPath string, options ReferenceTournamentOptions) (bool, error) {
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return false, err
	}
	report, err := buildReferenceTournamentReport(records, options)
	if err != nil {
		return false, err
	}
	report.ArtifactSHA256, err = fileSHA256(artifactPath)
	if err != nil {
		return false, err
	}
	var output *os.File
	if outputPath == "" {
		output = os.Stdout
	} else {
		if err := ensureOutputDir(outputPath); err != nil {
			return false, err
		}
		output, err = os.Create(outputPath)
		if err != nil {
			return false, err
		}
		defer output.Close()
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return false, err
	}
	return report.PromotionEligible, nil
}
