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
	"runtime"
	"strings"
	"time"
	"unicode"

	pgv2 "github.com/specterops/dawgs/drivers/pg/v2"
)

const (
	reportFormatBenchfmt = "benchfmt"
	reportFormatJSON     = "json"
	reportFormatMarkdown = "markdown"

	shortestPathModeForced           = "forced"
	shortestPathModeProductionPolicy = "production_policy"
)

// Report holds all benchmark results and metadata.
type Report struct {
	Driver                        string      `json:"driver"`
	GitRef                        string      `json:"git_ref"`
	Date                          string      `json:"date"`
	Iterations                    int         `json:"iterations"`
	WarmupIterations              int         `json:"warmup_iterations"`
	Workers                       int         `json:"workers"`
	ShortestPathExecutor          string      `json:"shortest_path_executor,omitempty"`
	ShortestPathMode              string      `json:"shortest_path_mode,omitempty"`
	TraversalPolicyGeneration     uint64      `json:"traversal_policy_generation,omitempty"`
	TraversalPolicyManifestSHA256 string      `json:"traversal_policy_manifest_sha256,omitempty"`
	PostgreSQLPlanCacheMode       string      `json:"postgresql_plan_cache_mode,omitempty"`
	PostgreSQLJIT                 bool        `json:"postgresql_jit"`
	TranslationCache              *pgv2.Stats `json:"translation_cache,omitempty"`
	Results                       []Result    `json:"results"`
}

func writeReport(w io.Writer, r Report, format string) error {
	if !isReportFormat(format) {
		return fmt.Errorf("unsupported output format %q", format)
	}

	switch format {
	case reportFormatBenchfmt:
		return writeBenchfmt(w, r)
	case reportFormatJSON:
		return writeJSON(w, r)
	default:
		return writeMarkdown(w, r)
	}
}

func isReportFormat(format string) bool {
	switch format {
	case reportFormatBenchfmt, reportFormatJSON, reportFormatMarkdown:
		return true
	default:
		return false
	}
}

func writeJSON(w io.Writer, r Report) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

func writeMarkdown(w io.Writer, r Report) error {
	fmt.Fprintf(w, "# Benchmarks — %s @ %s (%s, %d iterations × %d workers, %d warm-up iterations)\n\n", r.Driver, r.GitRef, r.Date, r.Iterations, r.Workers, r.WarmupIterations)
	if r.ShortestPathExecutor != "" {
		switch r.ShortestPathMode {
		case shortestPathModeProductionPolicy:
			fmt.Fprintf(w, "Shortest-path executor: `%s` through production traversal policy generation %d (manifest `%s`); PostgreSQL plan cache: `%s`; JIT: `%t`.\n\n", r.ShortestPathExecutor, r.TraversalPolicyGeneration, r.TraversalPolicyManifestSHA256, r.PostgreSQLPlanCacheMode, r.PostgreSQLJIT)
		case shortestPathModeForced:
			fmt.Fprintf(w, "Shortest-path executor: `%s` forced at the benchmark boundary; PostgreSQL plan cache: `%s`; JIT: `%t`.\n\n", r.ShortestPathExecutor, r.PostgreSQLPlanCacheMode, r.PostgreSQLJIT)
		default:
			fmt.Fprintf(w, "Shortest-path executor: `%s`; PostgreSQL plan cache: `%s`; JIT: `%t`.\n\n", r.ShortestPathExecutor, r.PostgreSQLPlanCacheMode, r.PostgreSQLJIT)
		}
	}
	fmt.Fprintf(w, "| Query | Dataset | Rows | Distinct Rows | Duplicate Rows | Median | P95 | Max | Explain |\n")
	fmt.Fprintf(w, "|-------|---------|-----:|--------------:|---------------:|-------:|----:|----:|:--------|\n")

	for _, res := range r.Results {
		label := res.Section
		if res.Label != res.Dataset {
			label = res.Section + " / " + res.Label
		}

		fmt.Fprintf(w, "| %s | %s | %d | %s | %s | %s | %s | %s | %s |\n",
			label,
			res.Dataset,
			res.RowCount,
			fmtOptionalInt64(res.DistinctRowCount),
			fmtOptionalInt64(res.DuplicateRowCount),
			fmtDuration(res.Stats.Median),
			fmtDuration(res.Stats.P95),
			fmtDuration(res.Stats.Max),
			fmtExplainStatus(res.Explain),
		)
	}

	fmt.Fprintln(w)
	if r.TranslationCache != nil {
		cache := r.TranslationCache.Aggregate
		workspaces := r.TranslationCache.TraversalWorkspace
		statements := r.TranslationCache.PreparedStatements
		fmt.Fprintf(w, "V2 connection state: cache %d hits, %d misses, %d bypasses, %d evictions; workspaces %d initialized/%d reused; statements %d prepared/%d reused across %d live connections.\n\n", cache.Hits, cache.Misses, cache.Bypasses, cache.Evictions, workspaces.Initializations, workspaces.Reuses, statements.Prepared, statements.Reuses, r.TranslationCache.LiveConnections)
		shortest := r.TranslationCache.SQLGeneration.ShortestPath
		if shortest.Count > 0 {
			fmt.Fprintf(w, "V2 shortest-path generation (%d samples): parse %s, cache/bind %s, translate %s, format %s, dispatch %s total. Shared L2: %d hits, %d misses, %d entries/%d capacity.\n\n", shortest.Count, fmtDuration(shortest.Parse), fmtDuration(shortest.Cache), fmtDuration(shortest.Translate), fmtDuration(shortest.Format), fmtDuration(shortest.Dispatch), r.TranslationCache.SharedShortestPathTemplates.Hits, r.TranslationCache.SharedShortestPathTemplates.Misses, r.TranslationCache.SharedShortestPathTemplates.Entries, r.TranslationCache.SharedShortestPathTemplates.Capacity)
		}
		selection := r.TranslationCache.StrategySelection
		if selection.Incumbent+selection.ExactQueryCanary+selection.StructuralAuthorized > 0 {
			fmt.Fprintf(w, "V2 strategy selection: %d incumbent, %d exact-query canary, %d structurally authorized, %d structural-shadow, %d shape-unavailable observations.\n\n", selection.Incumbent, selection.ExactQueryCanary, selection.StructuralAuthorized, selection.StructuralShadow, selection.ShapeUnavailable)
		}
		shapeCache := r.TranslationCache.TraversalShapeCache
		if shapeCache.Hits+shapeCache.Misses > 0 {
			fmt.Fprintf(w, "V2 traversal shape cache: %d hits, %d misses, %d entries/%d capacity.\n\n", shapeCache.Hits, shapeCache.Misses, shapeCache.Entries, shapeCache.Capacity)
		}
	}
	return nil
}

func fmtOptionalInt64(value *int64) string {
	if value == nil {
		return "-"
	}

	return fmt.Sprintf("%d", *value)
}

func fmtExplainStatus(explain *ExplainResult) string {
	if explain == nil {
		return "-"
	}

	return "captured"
}

func writeBenchfmt(w io.Writer, r Report) error {
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	procs := runtime.GOMAXPROCS(0)

	fmt.Fprintf(w, "goos: %s\n", goos)
	fmt.Fprintf(w, "goarch: %s\n", goarch)
	fmt.Fprintf(w, "pkg: github.com/specterops/dawgs/cmd/benchmark\n")

	for _, res := range r.Results {
		benchName := benchName(r.Driver, res)

		for _, sample := range res.Samples {
			fmt.Fprintf(w, "%s-%d\t1\t%d ns/op\n", benchName, procs, sample.Nanoseconds())
		}
	}

	return nil
}

func benchName(driver string, res Result) string {
	parts := []string{
		"BenchmarkDawgsIntegration",
		sanitizeBenchNamePart(driver),
		sanitizeBenchNamePart(res.Dataset),
		sanitizeBenchNamePart(res.Section),
		sanitizeBenchNamePart(res.Label),
	}

	return strings.Join(parts, "/")
}

func sanitizeBenchNamePart(value string) string {
	var builder strings.Builder
	lastUnderscore := false

	for _, char := range value {
		switch {
		case char == '/' || char == '-' || char == '_':
			if char == '_' {
				if !lastUnderscore {
					builder.WriteRune(char)
				}
				lastUnderscore = true
			} else {
				builder.WriteRune(char)
				lastUnderscore = false
			}
		case unicode.IsLetter(char) || unicode.IsDigit(char):
			builder.WriteRune(char)
			lastUnderscore = false
		case unicode.IsSpace(char):
			if !lastUnderscore {
				builder.WriteByte('_')
			}
			lastUnderscore = true
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
			}
			lastUnderscore = true
		}
	}

	if builder.Len() == 0 {
		return "unknown"
	}

	return builder.String()
}

func fmtDuration(d time.Duration) string {
	ms := float64(d.Microseconds()) / 1000.0
	if ms < 1 {
		return fmt.Sprintf("%.2fms", ms)
	}
	if ms < 100 {
		return fmt.Sprintf("%.1fms", ms)
	}
	return fmt.Sprintf("%.0fms", ms)
}
