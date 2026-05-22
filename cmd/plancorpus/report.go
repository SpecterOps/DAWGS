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
)

const defaultTopPlans = 25

var postgresCostPattern = regexp.MustCompile(`cost=[0-9.]+\.\.([0-9.]+)`)

type PlanSummary struct {
	Drivers           []DriverSummary `json:"drivers"`
	TopPostgresPlans  []CostedPlan    `json:"top_postgres_plans,omitempty"`
	PostgresOperators []Count         `json:"postgres_operators,omitempty"`
	Neo4jOperators    []Count         `json:"neo4j_operators,omitempty"`
	PlannedLowerings  []Count         `json:"planned_lowerings,omitempty"`
	AppliedLowerings  []Count         `json:"applied_lowerings,omitempty"`
	SkippedLowerings  []Count         `json:"skipped_lowerings,omitempty"`
	SkippedReasons    []Count         `json:"skipped_reasons,omitempty"`
	FeatureCounts     []Count         `json:"feature_counts,omitempty"`
	Errors            []PlanError     `json:"errors,omitempty"`
}

type DriverSummary struct {
	Driver  string `json:"driver"`
	Records int    `json:"records"`
	Errors  int    `json:"errors"`
}

type Count struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type CostedPlan struct {
	Cost             float64  `json:"cost"`
	Driver           string   `json:"driver"`
	Source           string   `json:"source"`
	Dataset          string   `json:"dataset,omitempty"`
	Name             string   `json:"name"`
	Cypher           string   `json:"cypher"`
	PlanRoot         string   `json:"plan_root"`
	PlannedLowerings []string `json:"planned_lowerings,omitempty"`
	AppliedLowerings []string `json:"applied_lowerings,omitempty"`
	SkippedLowerings []string `json:"skipped_lowerings,omitempty"`
}

type PlanError struct {
	Driver string `json:"driver"`
	Source string `json:"source"`
	Name   string `json:"name"`
	Error  string `json:"error"`
}

func buildSummary(records []PlanRecord, topN int) PlanSummary {
	if topN <= 0 {
		topN = defaultTopPlans
	}

	driverCounts := map[string]*DriverSummary{}
	postgresOperatorCounts := map[string]int{}
	neo4jOperatorCounts := map[string]int{}
	plannedLoweringCounts := map[string]int{}
	appliedLoweringCounts := map[string]int{}
	skippedLoweringCounts := map[string]int{}
	skippedReasonCounts := map[string]int{}
	featureCounts := map[string]int{}

	var (
		errors []PlanError
		topPG  []CostedPlan
	)

	for _, record := range records {
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

func writeJSONSummary(w io.Writer, summary PlanSummary) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}

func writeMarkdownSummary(w io.Writer, summary PlanSummary) error {
	writeCounts := func(title string, counts []Count, limit int) {
		if len(counts) == 0 {
			return
		}
		fmt.Fprintf(w, "\n## %s\n\n| Name | Count |\n| --- | ---: |\n", title)
		for idx, count := range counts {
			if limit > 0 && idx >= limit {
				break
			}
			fmt.Fprintf(w, "| %s | %d |\n", markdownCell(count.Name), count.Count)
		}
	}

	fmt.Fprintln(w, "# Cypher Plan Corpus Summary")
	fmt.Fprintln(w, "\n## Drivers\n\n| Driver | Records | Errors |\n| --- | ---: | ---: |")
	for _, driver := range summary.Drivers {
		fmt.Fprintf(w, "| %s | %d | %d |\n", markdownCell(driver.Driver), driver.Records, driver.Errors)
	}

	if len(summary.TopPostgresPlans) > 0 {
		fmt.Fprintln(w, "\n## Top PostgreSQL Plans\n\n| Cost | Source | Name | Root | Lowerings |\n| ---: | --- | --- | --- | --- |")
		for _, plan := range summary.TopPostgresPlans {
			fmt.Fprintf(
				w,
				"| %.2f | %s | %s | %s | %s |\n",
				plan.Cost,
				markdownCell(plan.Source),
				markdownCell(plan.Name),
				markdownCell(plan.PlanRoot),
				markdownCell(strings.Join(plan.PlannedLowerings, ", ")),
			)
		}
	}

	writeCounts("Feature Counts", summary.FeatureCounts, 0)
	writeCounts("Planned Lowerings", summary.PlannedLowerings, 0)
	writeCounts("Applied Lowerings", summary.AppliedLowerings, 0)
	writeCounts("Skipped Lowerings", summary.SkippedLowerings, 0)
	writeCounts("Skipped Lowering Reasons", summary.SkippedReasons, 0)
	writeCounts("PostgreSQL Operators", summary.PostgresOperators, 25)
	writeCounts("Neo4j Operators", summary.Neo4jOperators, 25)

	if len(summary.Errors) > 0 {
		fmt.Fprintln(w, "\n## Capture Errors\n\n| Driver | Source | Name | Error |\n| --- | --- | --- | --- |")
		for _, captureError := range summary.Errors {
			fmt.Fprintf(
				w,
				"| %s | %s | %s | %s |\n",
				markdownCell(captureError.Driver),
				markdownCell(captureError.Source),
				markdownCell(captureError.Name),
				markdownCell(captureError.Error),
			)
		}
	}

	return nil
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}
