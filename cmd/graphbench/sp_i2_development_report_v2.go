// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const spI2V2DevelopmentReportSchema = "sp-i2-v2-development-report-v1"

type SPI2V2DevelopmentReport struct {
	Schema                    string                       `json:"schema"`
	Generation                string                       `json:"generation"`
	ProtocolDeclarationSHA256 string                       `json:"protocol_declaration_sha256"`
	ArtifactSHA256            string                       `json:"artifact_sha256"`
	SourceCommit              string                       `json:"source_commit"`
	DirtyDiffSHA256           string                       `json:"dirty_diff_sha256"`
	BinarySHA256              string                       `json:"binary_sha256"`
	StatisticalImplementation string                       `json:"statistical_implementation"`
	Confidence                float64                      `json:"confidence_level"`
	BootstrapReplicates       int                          `json:"bootstrap_replicates"`
	Rounds                    int                          `json:"rounds"`
	TimedSamplesPerRound      int                          `json:"timed_samples_per_round"`
	PromotionEligible         bool                         `json:"promotion_eligible"`
	SelectedExecutor          string                       `json:"selected_executor"`
	Arms                      []SPI2V2DevelopmentArmReport `json:"arms"`
}

type SPI2V2DevelopmentArmReport struct {
	Executor string                        `json:"executor"`
	Eligible bool                          `json:"eligible"`
	Reasons  []string                      `json:"reasons,omitempty"`
	Ranking  *SPI2V2DevelopmentRanking     `json:"ranking,omitempty"`
	Cases    []SPI2V2DevelopmentCaseReport `json:"cases"`
}

type SPI2V2DevelopmentRanking struct {
	PlanNodeScore      int     `json:"plan_node_score"`
	PlanningRatioUpper float64 `json:"maximum_planning_ratio_upper_vs_e0"`
	P95RatioUpper      float64 `json:"maximum_p95_ratio_upper_vs_e0"`
	FixedOrder         int     `json:"fixed_order"`
}

type SPI2V2DevelopmentCaseReport struct {
	Dataset      string                          `json:"dataset"`
	Name         string                          `json:"name"`
	MaxPlanNodes int                             `json:"maximum_plan_node_count"`
	Contrasts    []SPI2V2DevelopmentCaseContrast `json:"contrasts"`
}

type SPI2V2DevelopmentCaseContrast struct {
	Comparator    string           `json:"comparator"`
	MedianRatio   RatioInterval    `json:"median_ratio"`
	MedianSaving  DurationInterval `json:"median_saving"`
	P95Ratio      RatioInterval    `json:"p95_ratio"`
	P95Saving     DurationInterval `json:"p95_saving"`
	PlanningRatio RatioInterval    `json:"planning_time_ratio"`
}

type spI2V2DevelopmentReportOptions struct {
	confidence          float64
	bootstrapReplicates int
}

type spI2V2DevelopmentSeries struct {
	samples      roundSamples
	planning     roundSamples
	maxPlanNodes int
}

func createSPI2V2DevelopmentReport(corpusRoot, artifact, output string) (SPI2V2DevelopmentReport, error) {
	protocolPath := filepath.Join(corpusRoot, "protocols", "sp_i2_distance_v2.json")
	if output != "" && (sameCleanPath(output, artifact) || sameCleanPath(output, protocolPath)) {
		return SPI2V2DevelopmentReport{}, fmt.Errorf("SP-I2 V2 development report output must not overwrite an input")
	}
	protocol, protocolSHA256, err := loadSPI2ProtocolV2(protocolPath)
	if err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	records, err := readJSONLFile(artifact)
	if err != nil {
		return SPI2V2DevelopmentReport{}, fmt.Errorf("read SP-I2 V2 development artifact: %w", err)
	}
	artifactSHA256, err := fileSHA256(artifact)
	if err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	report, err := buildSPI2V2DevelopmentReport(records, protocolSHA256, artifactSHA256, spI2V2DevelopmentReportOptions{
		confidence: protocol.Design.ConfidenceLevel, bootstrapReplicates: protocol.Design.BootstrapReplicates,
	})
	if err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	if output == "" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(report)
	} else {
		err = writeIndentedJSON(output, report)
	}
	return report, err
}

func buildSPI2V2DevelopmentReport(records []CaseResult, protocolSHA256, artifactSHA256 string, options spI2V2DevelopmentReportOptions) (SPI2V2DevelopmentReport, error) {
	if options.confidence <= 0 || options.confidence >= 1 || options.bootstrapReplicates <= 0 {
		return SPI2V2DevelopmentReport{}, fmt.Errorf("invalid SP-I2 V2 development statistical options")
	}
	if err := validateSPI2V2DevelopmentEvidence(records, spI2V2StudyTournament); err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	series, source, err := collectSPI2V2DevelopmentSeries(records)
	if err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	report, err := evaluateSPI2V2DevelopmentSeries(series, options)
	if err != nil {
		return SPI2V2DevelopmentReport{}, err
	}
	report.ProtocolDeclarationSHA256 = protocolSHA256
	report.ArtifactSHA256 = artifactSHA256
	report.SourceCommit = source.sourceCommit
	report.DirtyDiffSHA256 = source.dirtyDiffSHA256
	report.BinarySHA256 = source.binarySHA256
	return report, nil
}

func collectSPI2V2DevelopmentSeries(records []CaseResult) (map[performanceKey]map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries, spI2V2ComponentSourceIdentity, error) {
	declarations, err := canonicalSPI2Declarations()
	if err != nil {
		return nil, spI2V2ComponentSourceIdentity{}, err
	}
	series := make(map[performanceKey]map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries)
	var source spI2V2ComponentSourceIdentity
	for _, record := range records {
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		arm := optimize.ShortestPathExecutor(record.Environment.Arm)
		if err := validateSPI2V2DevelopmentReportRecord(record, arm, declarations[key]); err != nil {
			return nil, spI2V2ComponentSourceIdentity{}, err
		}
		currentSource := spI2V2ComponentSourceIdentity{
			sourceCommit: record.Environment.SourceCommit, dirtyDiffSHA256: record.Environment.DirtyDiffSHA256, binarySHA256: record.Environment.BinarySHA256,
		}
		if source.sourceCommit == "" {
			source = currentSource
		} else if source != currentSource {
			return nil, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 development artifact mixes source or binary identities")
		}
		if series[key] == nil {
			series[key] = make(map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries)
		}
		entry := series[key][arm]
		if entry == nil {
			entry = &spI2V2DevelopmentSeries{samples: roundSamples{}, planning: roundSamples{}}
			series[key][arm] = entry
		}
		round := record.Environment.Round
		for _, sample := range record.Stats.Samples {
			entry.samples[round] = append(entry.samples[round], sample.Duration)
		}
		entry.planning[round] = []time.Duration{time.Duration(*record.PostgresMetrics.PlanningMS * float64(time.Millisecond))}
		entry.maxPlanNodes = max(entry.maxPlanNodes, len(record.PostgresMetrics.PlanNodes))
	}
	if strings.TrimSpace(source.sourceCommit) == "" || !lowercaseSHA256(source.dirtyDiffSHA256) || !lowercaseSHA256(source.binarySHA256) {
		return nil, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 development artifact lacks a complete source and binary identity")
	}
	return series, source, nil
}

func validateSPI2V2DevelopmentReportRecord(record CaseResult, arm optimize.ShortestPathExecutor, declaration spI2CanonicalDeclaration) error {
	if record.PostgresEnvironment == nil || record.Fixture == nil || record.PostgresMetrics == nil || record.TraversalTelemetry == nil || record.Optimization == nil ||
		!strings.EqualFold(strings.TrimSpace(record.PostgresEnvironment.TransactionIsolation), "repeatable read") || len(record.PostgresPlanJSON) == 0 ||
		record.PostgresMetrics.PlanningMS == nil || *record.PostgresMetrics.PlanningMS <= 0 || len(record.PostgresMetrics.PlanNodes) == 0 {
		return fmt.Errorf("%s/%s arm %q lacks its canonical Repeatable Read plan observation", record.Dataset, record.Name, arm)
	}
	parsedPlan, err := parsePostgresPlanJSONMetrics(record.PostgresPlanJSON)
	if err != nil || parsedPlan.PlanningMS == nil || *parsedPlan.PlanningMS != *record.PostgresMetrics.PlanningMS {
		return fmt.Errorf("%s/%s arm %q canonical JSON plan contradicts its planning observation", record.Dataset, record.Name, arm)
	}
	testCase := declaration.testCase
	testCase.Source = record.Source
	expected := newCaseResult(testCase, ModePostgresSQL, nil)
	attachFixtureMetadata(&expected, *record.Fixture)
	if filepath.Base(record.Source) != "generated_sp_i2_distance_v1.json" || record.Category != testCase.Category || record.Cypher != testCase.Cypher ||
		record.WorkloadSHA256 != expected.WorkloadSHA256 || !reflect.DeepEqual(record.NodeParams, testCase.NodeParams) ||
		!reflect.DeepEqual(record.NodeListParams, testCase.NodeListParams) || !reflect.DeepEqual(record.Shape, testCase.Shape) || !record.StableObservation ||
		record.ExpectedRowCount == nil || testCase.Expected.RowCount == nil || *record.ExpectedRowCount != *testCase.Expected.RowCount || record.RowCount != *testCase.Expected.RowCount {
		return fmt.Errorf("%s/%s arm %q changes the exact open-corpus semantic contract", record.Dataset, record.Name, arm)
	}
	if err := validateExpectedObservations(testCase.Expected, record.ObservedRows); err != nil {
		return fmt.Errorf("%s/%s arm %q observation: %w", record.Dataset, record.Name, arm, err)
	}
	telemetry := record.TraversalTelemetry
	if err := ValidateTraversalExecutionTelemetry(telemetry); err != nil {
		return fmt.Errorf("%s/%s arm %q telemetry: %w", record.Dataset, record.Name, arm, err)
	}
	summary := telemetry.Summary
	if telemetry.Level != TraversalTelemetryLevelDiagnostic || telemetry.Diagnostic == nil || telemetry.Diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete ||
		telemetry.Diagnostic.PlanReplay == nil || summary.RequestedIdentity != string(arm) || summary.RuntimeIdentity != string(arm) ||
		summary.AppliedIdentity != string(arm) || summary.EmittedIdentity != optimize.ShortestPathPolicyI2DistanceGuardedV2 ||
		summary.FallbackExecuted == nil || *summary.FallbackExecuted || summary.Overflow == nil || *summary.Overflow ||
		summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable {
		return fmt.Errorf("%s/%s arm %q lacks an exact diagnostic non-fallback runtime receipt", record.Dataset, record.Name, arm)
	}
	if gate := evaluateProductionResourceGateCase(record); !gate.Passed {
		return fmt.Errorf("%s/%s arm %q resource/plan invariants failed: %s", record.Dataset, record.Name, arm, strings.Join(gate.Reasons, "; "))
	}
	return nil
}

func evaluateSPI2V2DevelopmentSeries(series map[performanceKey]map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries, options spI2V2DevelopmentReportOptions) (SPI2V2DevelopmentReport, error) {
	report := SPI2V2DevelopmentReport{
		Schema: spI2V2DevelopmentReportSchema, Generation: spI2GenerationV2, StatisticalImplementation: spI2HierBootstrapV2,
		Confidence: options.confidence, BootstrapReplicates: options.bootstrapReplicates, Rounds: 10, TimedSamplesPerRound: 100,
		PromotionEligible: false,
	}
	keys := make([]performanceKey, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].name < keys[j].name
	})
	if len(keys) == 0 {
		return report, fmt.Errorf("SP-I2 V2 development report has no cases")
	}
	armReports := make(map[optimize.ShortestPathExecutor]*SPI2V2DevelopmentArmReport, len(spI2V2DevelopmentArms))
	contrastIndex := make(map[optimize.ShortestPathExecutor]map[performanceKey]map[optimize.ShortestPathExecutor]SPI2V2DevelopmentCaseContrast)
	for _, arm := range spI2V2DevelopmentArms {
		entry := &SPI2V2DevelopmentArmReport{Executor: string(arm), Eligible: true}
		armReports[arm] = entry
		contrastIndex[arm] = make(map[performanceKey]map[optimize.ShortestPathExecutor]SPI2V2DevelopmentCaseContrast)
		for _, key := range keys {
			armSeries := series[key][arm]
			if armSeries == nil {
				return report, fmt.Errorf("%s/%s omits arm %q", key.dataset, key.name, arm)
			}
			caseReport := SPI2V2DevelopmentCaseReport{Dataset: key.dataset, Name: key.name, MaxPlanNodes: armSeries.maxPlanNodes}
			for _, comparator := range spI2V2Comparators(arm) {
				contrast, err := compareSPI2V2DevelopmentCase(key, comparator, arm, series[key][comparator], armSeries, options)
				if err != nil {
					return report, err
				}
				caseReport.Contrasts = append(caseReport.Contrasts, contrast)
				if contrastIndex[arm][key] == nil {
					contrastIndex[arm][key] = make(map[optimize.ShortestPathExecutor]SPI2V2DevelopmentCaseContrast)
				}
				contrastIndex[arm][key][comparator] = contrast
			}
			entry.Cases = append(entry.Cases, caseReport)
		}
	}
	for _, arm := range spI2V2DevelopmentArms[1:] {
		entry := armReports[arm]
		for _, key := range keys {
			baseline := contrastIndex[arm][key][optimize.ShortestPathExecutorI2GuardedDistanceV2E0]
			if baseline.MedianRatio.Upper > 1.02 || baseline.P95Ratio.Upper > 1.02 {
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s/%s exceeds the E0 wall-clock non-regression limit", key.dataset, key.name))
			}
		}
		switch arm {
		case optimize.ShortestPathExecutorI2GuardedDistanceV2E1:
			applySPI2V2CycleGate(entry, keys, contrastIndex[arm], optimize.ShortestPathExecutorI2GuardedDistanceV2E0, false)
		case optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P:
			applySPI2V2CycleGate(entry, keys, contrastIndex[arm], optimize.ShortestPathExecutorI2GuardedDistanceV2E1, true)
		case optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP:
			for _, parent := range []optimize.ShortestPathExecutor{optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P} {
				for _, key := range keys {
					contrast := contrastIndex[arm][key][parent]
					if contrast.MedianRatio.Upper > 1.02 || contrast.P95Ratio.Upper > 1.02 || contrast.PlanningRatio.Upper > 1.02 {
						entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s/%s exceeds a %s parent limit", key.dataset, key.name, parent))
					}
				}
			}
		}
		entry.Reasons = slices.Compact(entry.Reasons)
		entry.Eligible = len(entry.Reasons) == 0
	}
	// E1DP eligibility depends on the final parent decisions.
	combined := armReports[optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP]
	for _, parent := range []optimize.ShortestPathExecutor{optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P} {
		if !armReports[parent].Eligible {
			combined.Reasons = append(combined.Reasons, fmt.Sprintf("parent %s is ineligible", parent))
		}
	}
	combined.Reasons = slices.Compact(combined.Reasons)
	combined.Eligible = len(combined.Reasons) == 0

	var eligible []*SPI2V2DevelopmentArmReport
	for fixedOrder, arm := range spI2V2DevelopmentArms[1:] {
		entry := armReports[arm]
		if entry.Eligible {
			ranking := SPI2V2DevelopmentRanking{FixedOrder: fixedOrder + 1}
			for index, key := range keys {
				ranking.PlanNodeScore += series[key][arm].maxPlanNodes
				contrast := contrastIndex[arm][key][optimize.ShortestPathExecutorI2GuardedDistanceV2E0]
				if index == 0 || contrast.PlanningRatio.Upper > ranking.PlanningRatioUpper {
					ranking.PlanningRatioUpper = contrast.PlanningRatio.Upper
				}
				if index == 0 || contrast.P95Ratio.Upper > ranking.P95RatioUpper {
					ranking.P95RatioUpper = contrast.P95Ratio.Upper
				}
			}
			entry.Ranking = &ranking
			eligible = append(eligible, entry)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool { return lessSPI2V2DevelopmentRanking(*eligible[i].Ranking, *eligible[j].Ranking) })
	selected := armReports[optimize.ShortestPathExecutorI2GuardedDistanceV2E0]
	if len(eligible) > 0 {
		selected = eligible[0]
	}
	report.SelectedExecutor = selected.Executor
	for _, arm := range spI2V2DevelopmentArms {
		report.Arms = append(report.Arms, *armReports[arm])
	}
	return report, nil
}

func sameCleanPath(left, right string) bool {
	leftAbsolute, leftErr := filepath.Abs(left)
	rightAbsolute, rightErr := filepath.Abs(right)
	return leftErr == nil && rightErr == nil && filepath.Clean(leftAbsolute) == filepath.Clean(rightAbsolute)
}

func spI2V2Comparators(arm optimize.ShortestPathExecutor) []optimize.ShortestPathExecutor {
	switch arm {
	case optimize.ShortestPathExecutorI2GuardedDistanceV2E0:
		return nil
	case optimize.ShortestPathExecutorI2GuardedDistanceV2E1:
		return []optimize.ShortestPathExecutor{optimize.ShortestPathExecutorI2GuardedDistanceV2E0}
	case optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P:
		return []optimize.ShortestPathExecutor{optimize.ShortestPathExecutorI2GuardedDistanceV2E0, optimize.ShortestPathExecutorI2GuardedDistanceV2E1}
	case optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP:
		return []optimize.ShortestPathExecutor{optimize.ShortestPathExecutorI2GuardedDistanceV2E0, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P}
	default:
		return nil
	}
}

func compareSPI2V2DevelopmentCase(key performanceKey, comparator, candidate optimize.ShortestPathExecutor, baseline, treatment *spI2V2DevelopmentSeries, options spI2V2DevelopmentReportOptions) (SPI2V2DevelopmentCaseContrast, error) {
	if baseline == nil || treatment == nil {
		return SPI2V2DevelopmentCaseContrast{}, fmt.Errorf("%s/%s lacks %s/%s contrast series", key.dataset, key.name, comparator, candidate)
	}
	domain := string(comparator) + "-vs-" + string(candidate)
	medianRatio, medianSaving, err := bootstrapSPI2RoundMedianV2(baseline.samples, treatment.samples, key.dataset, key.name, domain+"-median", options.confidence, options.bootstrapReplicates)
	if err != nil {
		return SPI2V2DevelopmentCaseContrast{}, err
	}
	tail, err := bootstrapSPI2HierarchicalTailV2(baseline.samples, treatment.samples, key.dataset, key.name, domain+"-p95", .95, options.confidence, options.bootstrapReplicates)
	if err != nil {
		return SPI2V2DevelopmentCaseContrast{}, err
	}
	planningRatio, _, err := bootstrapSPI2RoundMedianV2(baseline.planning, treatment.planning, key.dataset, key.name, domain+"-planning", options.confidence, options.bootstrapReplicates)
	if err != nil {
		return SPI2V2DevelopmentCaseContrast{}, err
	}
	return SPI2V2DevelopmentCaseContrast{
		Comparator: string(comparator), MedianRatio: medianRatio, MedianSaving: medianSaving, P95Ratio: tail.Ratio,
		P95Saving: DurationInterval{Estimate: -tail.Change.Estimate, Lower: -tail.Change.Upper, Upper: -tail.Change.Lower}, PlanningRatio: planningRatio,
	}, nil
}

func applySPI2V2CycleGate(entry *SPI2V2DevelopmentArmReport, keys []performanceKey, contrasts map[performanceKey]map[optimize.ShortestPathExecutor]SPI2V2DevelopmentCaseContrast, comparator optimize.ShortestPathExecutor, planningGate bool) {
	cycleSeen := false
	for _, key := range keys {
		contrast := contrasts[key][comparator]
		if contrast.MedianRatio.Upper > 1.02 || contrast.P95Ratio.Upper > 1.02 {
			entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s/%s exceeds the %s wall-clock non-regression limit", key.dataset, key.name, comparator))
		}
		if planningGate && contrast.PlanningRatio.Upper > 1.02 {
			entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s/%s exceeds the %s planning-time limit", key.dataset, key.name, comparator))
		}
		if strings.Contains(key.name, "cycle-control") {
			cycleSeen = true
			if contrast.P95Ratio.Upper > .95 && contrast.P95Saving.Lower < 50*time.Microsecond {
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("%s/%s misses the %s cycle-control gain", key.dataset, key.name, comparator))
			}
		}
	}
	if !cycleSeen {
		entry.Reasons = append(entry.Reasons, "cycle-control case is missing")
	}
}

func lessSPI2V2DevelopmentRanking(left, right SPI2V2DevelopmentRanking) bool {
	if left.PlanNodeScore != right.PlanNodeScore {
		return left.PlanNodeScore < right.PlanNodeScore
	}
	if math.Float64bits(left.PlanningRatioUpper) != math.Float64bits(right.PlanningRatioUpper) {
		return left.PlanningRatioUpper < right.PlanningRatioUpper
	}
	if math.Float64bits(left.P95RatioUpper) != math.Float64bits(right.P95RatioUpper) {
		return left.P95RatioUpper < right.P95RatioUpper
	}
	return left.FixedOrder < right.FixedOrder
}
