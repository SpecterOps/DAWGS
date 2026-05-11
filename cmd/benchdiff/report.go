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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func writeRunReport(path string, summary runSummary) error {
	var out bytes.Buffer
	cfg := summary.Config

	fmt.Fprintln(&out, "# Benchmark Diff")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "| Field | Value |")
	fmt.Fprintln(&out, "|-------|-------|")
	fmt.Fprintf(&out, "| Base | `%s` (`%s`) |\n", cfg.BaseRef, cfg.BaseShortSHA)
	fmt.Fprintf(&out, "| Target | `%s` (`%s`) |\n", cfg.TargetRef, cfg.TargetShortSHA)
	fmt.Fprintf(&out, "| Started | %s |\n", summary.StartedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "| Finished | %s |\n", summary.FinishedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "| Go | %s |\n", summary.GoVersion)
	fmt.Fprintf(&out, "| Platform | %s/%s |\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&out, "| Kind | `%s` |\n", cfg.Kind)
	fmt.Fprintf(&out, "| Output | `%s` |\n", cfg.OutDirAbs)
	if cfg.runsIntegrationBenchmarks() {
		fmt.Fprintf(&out, "| Driver | `%s` |\n", cfg.Driver)
		fmt.Fprintf(&out, "| Dataset Dir | `%s` |\n", cfg.DatasetDirAbs)
		fmt.Fprintf(&out, "| Integration Iterations | %d |\n", cfg.IntegrationIterations)
	}
	if cfg.runsUnitBenchmarks() {
		fmt.Fprintf(&out, "| Packages | `%s` |\n", cfg.Packages)
		fmt.Fprintf(&out, "| Bench | `%s` |\n", cfg.Bench)
		fmt.Fprintf(&out, "| Bench Count | %d |\n", cfg.BenchCount)
		fmt.Fprintf(&out, "| Benchtime | `%s` |\n", cfg.Benchtime)
	}
	if cfg.Threshold > 0 {
		fmt.Fprintf(&out, "| Regression Failure Threshold | %.2f%% |\n", cfg.Threshold)
	}
	fmt.Fprintln(&out)

	for _, comparison := range summary.Comparisons {
		fmt.Fprintf(&out, "## %s\n\n", comparison.Name)
		for _, note := range comparison.Notes {
			fmt.Fprintf(&out, "- %s\n", note)
		}
		if len(comparison.Notes) > 0 {
			fmt.Fprintln(&out)
		}

		fmt.Fprintf(&out, "- Base raw: `%s`\n", relOrAbs(cfg.OutDirAbs, comparison.BaseFile))
		fmt.Fprintf(&out, "- Target raw: `%s`\n", relOrAbs(cfg.OutDirAbs, comparison.TargetFile))
		fmt.Fprintf(&out, "- Benchstat: `%s`\n\n", relOrAbs(cfg.OutDirAbs, comparison.BenchstatFile))

		fmt.Fprintln(&out, "```text")
		fmt.Fprint(&out, comparison.Benchstat)
		if len(comparison.Benchstat) == 0 || comparison.Benchstat[len(comparison.Benchstat)-1] != '\n' {
			fmt.Fprintln(&out)
		}
		fmt.Fprintln(&out, "```")
		fmt.Fprintln(&out)

		writeRegressionSection(&out, comparison, cfg.Threshold)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	return os.WriteFile(path, out.Bytes(), 0644)
}

func writeRegressionSection(out *bytes.Buffer, comparison comparison, threshold float64) {
	if threshold <= 0 {
		return
	}

	fmt.Fprintf(out, "### Regressions Over %.2f%%\n\n", threshold)
	if len(comparison.Regressions) == 0 {
		fmt.Fprintln(out, "None.")
		fmt.Fprintln(out)
		return
	}

	fmt.Fprintln(out, "| Benchmark | Base Median | Target Median | Change |")
	fmt.Fprintln(out, "|-----------|------------:|--------------:|-------:|")
	for _, regression := range comparison.Regressions {
		fmt.Fprintf(out, "| `%s` | %.0f ns/op | %.0f ns/op | +%.2f%% |\n",
			regression.Name,
			regression.BaseMedianNS,
			regression.TargetMedianNS,
			regression.Percent,
		)
	}
	fmt.Fprintln(out)
}

func relOrAbs(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil || rel == "." || len(rel) >= len(path) {
		return path
	}

	return rel
}
