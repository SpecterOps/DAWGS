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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/testutil"
)

type Summary struct {
	GeneratedAt  time.Time                 `json:"generated_at"`
	Metadata     testutil.BaselineMetadata `json:"metadata"`
	Modes        []ModeSummary             `json:"modes"`
	Cases        []CaseSummary             `json:"cases"`
	Regressions  []BaselineEntry           `json:"regressions,omitempty"`
	Improvements []BaselineEntry           `json:"improvements,omitempty"`
	CostModels   []CostModelCase           `json:"cost_models,omitempty"`
}

type CostModelCase struct {
	Dataset     string               `json:"dataset"`
	Name        string               `json:"name"`
	Boundary    string               `json:"boundary"`
	E2EMedian   time.Duration        `json:"e2e_median"`
	Attribution float64              `json:"attribution"`
	Components  []CostModelComponent `json:"components"`
}

type CostModelComponent struct {
	Name       string        `json:"name"`
	Interval   string        `json:"interval"`
	Median     time.Duration `json:"median"`
	P95        time.Duration `json:"p95"`
	Rows       int64         `json:"rows,omitempty"`
	ShareOfE2E float64       `json:"share_of_e2e,omitempty"`
	Confidence string        `json:"confidence"`
}

type ModeSummary struct {
	Mode           ExecutionMode `json:"mode"`
	Total          int           `json:"total"`
	OK             int           `json:"ok"`
	RowMismatch    int           `json:"row_mismatch"`
	Error          int           `json:"error"`
	NotImplemented int           `json:"not_implemented"`
}

type CaseSummary struct {
	Source   string                         `json:"source"`
	Dataset  string                         `json:"dataset"`
	Name     string                         `json:"name"`
	Category string                         `json:"category"`
	Modes    map[ExecutionMode]ModeCaseCell `json:"modes"`
}

type ModeCaseCell struct {
	Status         string              `json:"status"`
	Rows           int64               `json:"rows,omitempty"`
	Median         time.Duration       `json:"median,omitempty"`
	Baseline       *BaselineComparison `json:"baseline,omitempty"`
	FallbackReason string              `json:"fallback_reason,omitempty"`
	Error          string              `json:"error,omitempty"`
}

type BaselineEntry struct {
	Dataset        string        `json:"dataset"`
	Name           string        `json:"name"`
	Mode           ExecutionMode `json:"mode"`
	BaselineMedian time.Duration `json:"baseline_median"`
	CurrentMedian  time.Duration `json:"current_median"`
	Ratio          float64       `json:"ratio"`
}

func buildSummary(records []CaseResult) Summary {
	var (
		summary = Summary{
			GeneratedAt: time.Now().UTC(),
		}
		modeSummaries = map[ExecutionMode]*ModeSummary{}
		caseSummaries = map[string]*CaseSummary{}
	)

	for _, record := range records {
		if summary.Metadata == (testutil.BaselineMetadata{}) {
			summary.Metadata = record.Metadata
		}
		modeSummary := modeSummaries[record.ExecutionMode]
		if modeSummary == nil {
			modeSummary = &ModeSummary{Mode: record.ExecutionMode}
			modeSummaries[record.ExecutionMode] = modeSummary
		}
		modeSummary.Total++

		switch record.Status {
		case StatusOK:
			modeSummary.OK++
		case StatusRowMismatch:
			modeSummary.RowMismatch++
		case StatusError:
			modeSummary.Error++
		case StatusNotImplemented:
			modeSummary.NotImplemented++
		}

		var (
			caseKey     = record.Source + "\x00" + record.Dataset + "\x00" + record.Name
			caseSummary = caseSummaries[caseKey]
		)

		if caseSummary == nil {
			caseSummary = &CaseSummary{
				Source:   record.Source,
				Dataset:  record.Dataset,
				Name:     record.Name,
				Category: record.Category,
				Modes:    map[ExecutionMode]ModeCaseCell{},
			}
			caseSummaries[caseKey] = caseSummary
		}

		caseSummary.Modes[record.ExecutionMode] = ModeCaseCell{
			Status:         record.Status,
			Rows:           record.RowCount,
			Median:         record.Stats.Median,
			Baseline:       record.Baseline,
			FallbackReason: record.FallbackReason,
			Error:          record.Error,
		}

		if record.Baseline != nil {
			entry := BaselineEntry{
				Dataset:        record.Dataset,
				Name:           record.Name,
				Mode:           record.ExecutionMode,
				BaselineMedian: record.Baseline.BaselineMedian,
				CurrentMedian:  record.Baseline.CurrentMedian,
				Ratio:          record.Baseline.Ratio,
			}
			if record.Baseline.Ratio > 1 {
				summary.Regressions = append(summary.Regressions, entry)
			} else if record.Baseline.Ratio < 1 {
				summary.Improvements = append(summary.Improvements, entry)
			}
		}
		if record.RawPGXWaterfall != nil && len(record.RawPGXWaterfall.Samples) > 0 {
			summary.CostModels = append(summary.CostModels, buildBoundaryCostModel(record))
		}
	}

	for _, modeSummary := range modeSummaries {
		summary.Modes = append(summary.Modes, *modeSummary)
	}
	sort.Slice(summary.Modes, func(i, j int) bool {
		return summary.Modes[i].Mode < summary.Modes[j].Mode
	})

	for _, caseSummary := range caseSummaries {
		summary.Cases = append(summary.Cases, *caseSummary)
	}
	sort.Slice(summary.Cases, func(i, j int) bool {
		if summary.Cases[i].Dataset != summary.Cases[j].Dataset {
			return summary.Cases[i].Dataset < summary.Cases[j].Dataset
		}

		if summary.Cases[i].Name != summary.Cases[j].Name {
			return summary.Cases[i].Name < summary.Cases[j].Name
		}

		return summary.Cases[i].Source < summary.Cases[j].Source
	})

	sortBaselineEntries(summary.Regressions, true)
	sortBaselineEntries(summary.Improvements, false)
	sort.Slice(summary.CostModels, func(i, j int) bool {
		if summary.CostModels[i].Dataset != summary.CostModels[j].Dataset {
			return summary.CostModels[i].Dataset < summary.CostModels[j].Dataset
		}
		return summary.CostModels[i].Name < summary.CostModels[j].Name
	})
	return summary
}

func buildBoundaryCostModel(record CaseResult) CostModelCase {
	samples := record.RawPGXWaterfall.Samples
	total := boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.Total })
	e2e := durationFromQuantile(total, 0.50)
	components := []struct {
		name   string
		values []time.Duration
	}{
		{name: "Pool acquisition", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.PoolWait })},
		{name: "Transaction setup", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.Transaction })},
		{name: "Bind/prepare", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.BindPrepare })},
		{name: "First-row transfer/decode", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.FirstRow })},
		{name: "Remaining transfer/decode", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.AllRowsDecode })},
		{name: "Drain/close", values: boundaryDurations(samples, func(sample BoundarySample) time.Duration { return sample.DrainClose })},
	}
	model := CostModelCase{Dataset: record.Dataset, Name: record.Name, Boundary: record.RawPGXWaterfall.Boundary, E2EMedian: e2e}
	var attributed time.Duration
	for _, component := range components {
		median := durationFromQuantile(component.values, 0.50)
		attributed += median
		model.Components = append(model.Components, CostModelComponent{
			Name: component.name, Interval: "exclusive", Median: median, P95: durationFromQuantile(component.values, 0.95),
			Rows: samples[0].Rows, ShareOfE2E: durationShare(median, e2e), Confidence: "raw-pgx observed boundary",
		})
	}
	residual := e2e - attributed
	if residual < 0 {
		residual = 0
	}
	model.Components = append(model.Components, CostModelComponent{Name: "Unexplained residual", Interval: "derived", Median: residual, ShareOfE2E: durationShare(residual, e2e), Confidence: "derived"})
	model.Attribution = durationShare(e2e-residual, e2e)
	if record.PostgresMetrics != nil && record.PostgresMetrics.ExecutionMS != nil {
		server := time.Duration(*record.PostgresMetrics.ExecutionMS * float64(time.Millisecond))
		model.Components = append(model.Components, CostModelComponent{Name: "Server execution", Interval: "inclusive/overlapping", Median: server, ShareOfE2E: durationShare(server, e2e), Confidence: "single EXPLAIN diagnostic"})
	}
	return model
}

func boundaryDurations(samples []BoundarySample, selectDuration func(BoundarySample) time.Duration) []time.Duration {
	values := make([]time.Duration, len(samples))
	for idx, sample := range samples {
		values[idx] = selectDuration(sample)
	}
	return values
}

func durationFromQuantile(values []time.Duration, probability float64) time.Duration {
	return time.Duration(durationQuantile(values, probability))
}

func durationShare(component, total time.Duration) float64 {
	if total <= 0 {
		return 0
	}
	return float64(component) / float64(total)
}

func sortBaselineEntries(entries []BaselineEntry, descending bool) {
	sort.Slice(entries, func(i, j int) bool {
		if descending {
			return entries[i].Ratio > entries[j].Ratio
		}

		return entries[i].Ratio < entries[j].Ratio
	})
}

func writeMarkdownSummaryFile(path string, summary Summary) error {
	if err := ensureOutputDir(path); err != nil {
		return err
	}

	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer output.Close()

	return writeMarkdownSummary(output, summary)
}

func writeJSONSummaryFile(path string, summary Summary) error {
	if err := ensureOutputDir(path); err != nil {
		return err
	}

	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer output.Close()

	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}

func writeMarkdownSummary(w io.Writer, summary Summary) error {
	fmt.Fprintf(w, "# GraphBench Summary\n\n")
	fmt.Fprintf(w, "Generated: %s\n\n", summary.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(w, "DAWGS version: `%s`\n\n", summary.Metadata.DAWGSVersion)

	fmt.Fprintf(w, "## Modes\n\n")
	fmt.Fprintf(w, "| Mode | Total | OK | Row Mismatch | Error | Not Implemented |\n")
	fmt.Fprintf(w, "| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, mode := range summary.Modes {
		fmt.Fprintf(w, "| %s | %d | %d | %d | %d | %d |\n",
			mode.Mode,
			mode.Total,
			mode.OK,
			mode.RowMismatch,
			mode.Error,
			mode.NotImplemented,
		)
	}

	fmt.Fprintf(w, "\n## Cases\n\n")
	fmt.Fprintf(w, "| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |\n")
	fmt.Fprintf(w, "| --- | --- | --- | --- | --- | --- |\n")
	for _, testCase := range summary.Cases {
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s |\n",
			escapeMarkdown(testCase.Name),
			escapeMarkdown(testCase.Dataset),
			escapeMarkdown(testCase.Category),
			formatModeCell(testCase.Modes[ModePostgresSQL]),
			formatModeCell(testCase.Modes[ModeLocalTraversal]),
			formatModeCell(testCase.Modes[ModeNeo4j]),
		)
	}

	if len(summary.Regressions) > 0 {
		fmt.Fprintf(w, "\n## Baseline Regressions\n\n")
		writeBaselineTable(w, summary.Regressions)
	}
	if len(summary.Improvements) > 0 {
		fmt.Fprintf(w, "\n## Baseline Improvements\n\n")
		writeBaselineTable(w, summary.Improvements)
	}
	if len(summary.CostModels) > 0 {
		fmt.Fprintf(w, "\n## Raw PostgreSQL Cost Models\n\n")
		for _, model := range summary.CostModels {
			fmt.Fprintf(w, "### %s / %s\n\n", escapeMarkdown(model.Dataset), escapeMarkdown(model.Name))
			fmt.Fprintf(w, "Boundary attribution: %.1f%% of %s.\n\n", model.Attribution*100, formatDuration(model.E2EMedian))
			fmt.Fprintf(w, "| Component | Interval | Median | p95 | Share of E2E | Confidence |\n")
			fmt.Fprintf(w, "| --- | --- | ---: | ---: | ---: | --- |\n")
			for _, component := range model.Components {
				fmt.Fprintf(w, "| %s | %s | %s | %s | %.1f%% | %s |\n", escapeMarkdown(component.Name), component.Interval, formatDuration(component.Median), formatDuration(component.P95), component.ShareOfE2E*100, escapeMarkdown(component.Confidence))
			}
		}
	}

	return nil
}

func writeBaselineTable(w io.Writer, entries []BaselineEntry) {
	fmt.Fprintf(w, "| Case | Dataset | Mode | Baseline | Current | Ratio |\n")
	fmt.Fprintf(w, "| --- | --- | --- | ---: | ---: | ---: |\n")
	for _, entry := range entries {
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %.2fx |\n",
			escapeMarkdown(entry.Name),
			escapeMarkdown(entry.Dataset),
			entry.Mode,
			formatDuration(entry.BaselineMedian),
			formatDuration(entry.CurrentMedian),
			entry.Ratio,
		)
	}
}

func formatModeCell(cell ModeCaseCell) string {
	if cell.Status == "" {
		return "-"
	}

	var parts []string
	if cell.Median > 0 {
		parts = append(parts, formatDuration(cell.Median))
		if cell.Rows > 0 {
			parts = append(parts, fmt.Sprintf("rows=%d", cell.Rows))
		}
	} else {
		parts = append(parts, cell.Status)
	}

	if cell.Status != StatusOK && cell.Median > 0 {
		parts = append(parts, cell.Status)
	}
	if cell.Baseline != nil {
		parts = append(parts, fmt.Sprintf("%.2fx", cell.Baseline.Ratio))
	}
	if cell.FallbackReason != "" {
		parts = append(parts, cell.FallbackReason)
	}
	if cell.Error != "" {
		parts = append(parts, cell.Error)
	}

	return escapeMarkdown(strings.Join(parts, "; "))
}

func formatDuration(duration time.Duration) string {
	ms := float64(duration.Microseconds()) / 1000.0
	if ms < 1 {
		return fmt.Sprintf("%.2fms", ms)
	}
	if ms < 100 {
		return fmt.Sprintf("%.1fms", ms)
	}

	return fmt.Sprintf("%.0fms", ms)
}

func escapeMarkdown(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}
