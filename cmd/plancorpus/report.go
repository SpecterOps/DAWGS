package main

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/testutil"
)

// defaultTopPlans limits an unconfigured report to its 25 most expensive PostgreSQL plans.
const defaultTopPlans = 25

// postgresCostPattern extracts the total-cost upper bound from a PostgreSQL plan's cost range.
var postgresCostPattern = regexp.MustCompile(`cost=[0-9.]+\.\.([0-9.]+)`)

// PlanSummary aggregates captured plans by driver, lowering, and cost.
type PlanSummary struct {
	// Metadata captures build and baseline metadata.
	Metadata testutil.BaselineMetadata `json:"metadata"`
	// Drivers lists driver summaries in deterministic display order.
	Drivers []DriverSummary `json:"drivers"`
	// TopPostgresPlans lists the highest-cost PostgreSQL plans selected for the summary.
	TopPostgresPlans []CostedPlan `json:"top_postgres_plans,omitempty"`
	// PostgresOperators counts normalized PostgreSQL plan operators.
	PostgresOperators []Count `json:"postgres_operators,omitempty"`
	// Neo4jOperators lists normalized Neo4j operators found in the captured plan.
	Neo4jOperators []Count `json:"neo4j_operators,omitempty"`
	// PlannedLowerings lists SQL lowering opportunities identified before optimization.
	PlannedLowerings []Count `json:"planned_lowerings,omitempty"`
	// AppliedLowerings lists SQL lowerings actually applied during translation.
	AppliedLowerings []Count `json:"applied_lowerings,omitempty"`
	// SkippedLowerings lists identified SQL lowerings not applied.
	SkippedLowerings []Count `json:"skipped_lowerings,omitempty"`
	// SkippedReasons counts reasons identified lowerings were not applied.
	SkippedReasons []Count `json:"skipped_reasons,omitempty"`
	// FeatureCounts counts captured plans containing each normalized plan feature.
	FeatureCounts []Count `json:"feature_counts,omitempty"`
	// Errors lists failures observed while processing the record.
	Errors []PlanError `json:"errors,omitempty"`
}

// DriverSummary aggregates plan counts and operators for one database driver.
type DriverSummary struct {
	// Driver identifies the database driver that produced the plan or summary.
	Driver string `json:"driver"`
	// Records counts captured plan records produced by the driver.
	Records int `json:"records"`
	// Errors counts plan-capture failures reported by the driver.
	Errors int `json:"errors"`
}

// Count pairs a label with an aggregate count for serialized summaries.
type Count struct {
	// Name labels the operator, lowering, feature, or reason being counted.
	Name string `json:"name"`
	// Count records how many plan records contributed the named item.
	Count int `json:"count"`
}

// CostedPlan identifies a captured plan and its parsed PostgreSQL estimated cost.
type CostedPlan struct {
	// Cost records the PostgreSQL planner's estimated total cost.
	Cost float64 `json:"cost"`
	// Driver identifies the database driver that produced the plan or summary.
	Driver string `json:"driver"`
	// Source identifies the source corpus file.
	Source string `json:"source"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset,omitempty"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// PlanRoot identifies the root operator of the captured plan.
	PlanRoot string `json:"plan_root"`
	// PlannedLowerings lists SQL lowering opportunities identified before optimization.
	PlannedLowerings []string `json:"planned_lowerings,omitempty"`
	// AppliedLowerings lists SQL lowerings actually applied during translation.
	AppliedLowerings []string `json:"applied_lowerings,omitempty"`
	// SkippedLowerings lists identified SQL lowerings not applied.
	SkippedLowerings []string `json:"skipped_lowerings,omitempty"`
}

// PlanError records the driver, query, and failure for a plan that could not be summarized.
type PlanError struct {
	// Driver identifies the database driver that produced the plan or summary.
	Driver string `json:"driver"`
	// Source identifies the source corpus file.
	Source string `json:"source"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Error records the failure message when the operation did not succeed.
	Error string `json:"error"`
}

// buildSummary aggregates plan records by driver, operator, lowering, error, and estimated cost.
func buildSummary(records []PlanRecord, topN int) PlanSummary {
	if topN <= 0 {
		topN = defaultTopPlans
	}

	var (
		driverCounts           = map[string]*DriverSummary{}
		postgresOperatorCounts = map[string]int{}
		neo4jOperatorCounts    = map[string]int{}
		plannedLoweringCounts  = map[string]int{}
		appliedLoweringCounts  = map[string]int{}
		skippedLoweringCounts  = map[string]int{}
		skippedReasonCounts    = map[string]int{}
		featureCounts          = map[string]int{}
		summaryMetadata        testutil.BaselineMetadata
		errors                 []PlanError
		topPG                  []CostedPlan
	)

	for _, record := range records {
		if summaryMetadata == (testutil.BaselineMetadata{}) {
			summaryMetadata = record.Metadata
		}
		driver := driverCounts[record.Driver]
		if driver == nil {
			driver = &DriverSummary{Driver: record.Driver}
			driverCounts[record.Driver] = driver
		}
		driver.Records++

		if record.Error != "" {
			driver.Errors++
			errors = append(errors, PlanError{
				Driver: record.Driver,
				Source: record.Source,
				Name:   record.Name,
				Error:  record.Error,
			})
		}

		for _, operator := range record.PGOperators {
			postgresOperatorCounts[normalizePostgresOperator(operator)]++
		}
		for _, operator := range record.Neo4jOperators {
			neo4jOperatorCounts[operator]++
		}
		for _, lowering := range record.PlannedLowerings {
			plannedLoweringCounts[lowering]++
		}
		for _, lowering := range record.AppliedLowerings {
			appliedLoweringCounts[lowering]++
		}
		for _, lowering := range record.SkippedLowerings {
			skippedLoweringCounts[lowering.Name]++
			skippedReasonCounts[lowering.Name+": "+lowering.Reason]++
		}

		for _, line := range record.PGPlan {
			switch {
			case strings.Contains(line, "Recursive Union"):
				featureCounts["PostgreSQL Recursive Union"]++
			case strings.Contains(line, "Function Scan on unnest"):
				featureCounts["PostgreSQL Function Scan on unnest"]++
			case strings.Contains(line, "SubPlan "):
				featureCounts["PostgreSQL SubPlan"]++
			case strings.Contains(line, "Filter: satisfied"):
				featureCounts["PostgreSQL traversal satisfied filter"]++
			}
		}

		if len(record.PGPlan) > 0 && record.Error == "" {
			topPG = append(topPG, CostedPlan{
				Cost:             postgresEstimatedCost(record.PGPlan[0]),
				Driver:           record.Driver,
				Source:           record.Source,
				Dataset:          record.Dataset,
				Name:             record.Name,
				Cypher:           record.Cypher,
				PlanRoot:         record.PGPlan[0],
				PlannedLowerings: append([]string(nil), record.PlannedLowerings...),
				AppliedLowerings: append([]string(nil), record.AppliedLowerings...),
				SkippedLowerings: skippedLoweringLabels(record.SkippedLowerings),
			})
		}
	}

	sort.Slice(topPG, func(i, j int) bool {
		return topPG[i].Cost > topPG[j].Cost
	})
	if len(topPG) > topN {
		topPG = topPG[:topN]
	}

	return PlanSummary{
		Metadata:          summaryMetadata,
		Drivers:           sortedDriverSummaries(driverCounts),
		TopPostgresPlans:  topPG,
		PostgresOperators: sortedCounts(postgresOperatorCounts),
		Neo4jOperators:    sortedCounts(neo4jOperatorCounts),
		PlannedLowerings:  sortedCounts(plannedLoweringCounts),
		AppliedLowerings:  sortedCounts(appliedLoweringCounts),
		SkippedLowerings:  sortedCounts(skippedLoweringCounts),
		SkippedReasons:    sortedCounts(skippedReasonCounts),
		FeatureCounts:     sortedCounts(featureCounts),
		Errors:            errors,
	}
}

// skippedLoweringLabels renders skipped lowering names and reasons as stable report labels, preserving their plan order.
func skippedLoweringLabels(lowerings []translate.SkippedLowering) []string {
	if len(lowerings) == 0 {
		return nil
	}

	labels := make([]string, len(lowerings))
	for idx, lowering := range lowerings {
		labels[idx] = lowering.Name + ": " + lowering.Reason
	}

	return labels
}

// postgresEstimatedCost extracts the PostgreSQL planner's estimated total cost from plan text.
func postgresEstimatedCost(planRoot string) float64 {
	match := postgresCostPattern.FindStringSubmatch(planRoot)
	if len(match) != 2 {
		return 0
	}

	cost, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0
	}
	return cost
}

// normalizePostgresOperator removes plan decoration so equivalent PostgreSQL operator lines share one name.
func normalizePostgresOperator(operator string) string {
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return ""
	}
	if idx := strings.Index(operator, ":"); idx >= 0 {
		return operator[:idx]
	}
	if idx := strings.Index(operator, " on "); idx >= 0 {
		return operator[:idx]
	}
	if idx := strings.Index(operator, " using "); idx >= 0 {
		return operator[:idx]
	}
	return operator
}

// sortedDriverSummaries returns driver summaries ordered by driver name.
func sortedDriverSummaries(drivers map[string]*DriverSummary) []DriverSummary {
	sorted := make([]DriverSummary, 0, len(drivers))
	for _, summary := range drivers {
		sorted = append(sorted, *summary)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Driver < sorted[j].Driver
	})
	return sorted
}

// sortedCounts converts a count map to descending-count, name-tiebroken entries.
func sortedCounts(counts map[string]int) []Count {
	sorted := make([]Count, 0, len(counts))
	for name, count := range counts {
		if name == "" || count == 0 {
			continue
		}
		sorted = append(sorted, Count{Name: name, Count: count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Count == sorted[j].Count {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Count > sorted[j].Count
	})
	return sorted
}

// writeJSONSummary encodes a plan summary as indented JSON.
func writeJSONSummary(w io.Writer, summary PlanSummary) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}

// writeMarkdownSummary renders aggregate counts, expensive plans, and errors as Markdown.
func writeMarkdownSummary(w io.Writer, summary PlanSummary) error {
	writef := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}

	writeln := func(args ...any) error {
		_, err := fmt.Fprintln(w, args...)
		return err
	}

	writeCounts := func(title string, counts []Count, limit int) error {
		if len(counts) == 0 {
			return nil
		}
		if err := writef("\n## %s\n\n| Name | Count |\n| --- | ---: |\n", title); err != nil {
			return err
		}
		for idx, count := range counts {
			if limit > 0 && idx >= limit {
				break
			}
			if err := writef("| %s | %d |\n", markdownCell(count.Name), count.Count); err != nil {
				return err
			}
		}
		return nil
	}

	if err := writeln("# Cypher Plan Corpus Summary"); err != nil {
		return err
	}
	if err := writef("\nDAWGS version: `%s`\n", summary.Metadata.DAWGSVersion); err != nil {
		return err
	}
	if err := writeln("\n## Drivers\n\n| Driver | Records | Errors |\n| --- | ---: | ---: |"); err != nil {
		return err
	}
	for _, driver := range summary.Drivers {
		if err := writef("| %s | %d | %d |\n", markdownCell(driver.Driver), driver.Records, driver.Errors); err != nil {
			return err
		}
	}

	if len(summary.TopPostgresPlans) > 0 {
		if err := writeln("\n## Top PostgreSQL Plans\n\n| Cost | Source | Name | Root | Lowerings |\n| ---: | --- | --- | --- | --- |"); err != nil {
			return err
		}
		for _, plan := range summary.TopPostgresPlans {
			if err := writef(
				"| %.2f | %s | %s | %s | %s |\n",
				plan.Cost,
				markdownCell(plan.Source),
				markdownCell(plan.Name),
				markdownCell(plan.PlanRoot),
				markdownCell(strings.Join(plan.PlannedLowerings, ", ")),
			); err != nil {
				return err
			}
		}
	}

	if err := writeCounts("Feature Counts", summary.FeatureCounts, 0); err != nil {
		return err
	}
	if err := writeCounts("Planned Lowerings", summary.PlannedLowerings, 0); err != nil {
		return err
	}
	if err := writeCounts("Applied Lowerings", summary.AppliedLowerings, 0); err != nil {
		return err
	}
	if err := writeCounts("Skipped Lowerings", summary.SkippedLowerings, 0); err != nil {
		return err
	}
	if err := writeCounts("Skipped Lowering Reasons", summary.SkippedReasons, 0); err != nil {
		return err
	}
	if err := writeCounts("PostgreSQL Operators", summary.PostgresOperators, 25); err != nil {
		return err
	}
	if err := writeCounts("Neo4j Operators", summary.Neo4jOperators, 25); err != nil {
		return err
	}

	if len(summary.Errors) > 0 {
		if err := writeln("\n## Capture Errors\n\n| Driver | Source | Name | Error |\n| --- | --- | --- | --- |"); err != nil {
			return err
		}
		for _, captureError := range summary.Errors {
			if err := writef(
				"| %s | %s | %s | %s |\n",
				markdownCell(captureError.Driver),
				markdownCell(captureError.Source),
				markdownCell(captureError.Name),
				markdownCell(captureError.Error),
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// markdownCell escapes table delimiters and line breaks for a Markdown cell.
func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}
