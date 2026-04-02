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
	"fmt"
	"io"
	"time"
)

// Report holds all benchmark results and metadata.
type Report struct {
	Driver     string
	GitRef     string
	Date       string
	Iterations int
	Results    []Result
}

func writeMarkdown(w io.Writer, r Report) error {
	fmt.Fprintf(w, "# Benchmarks — %s @ %s (%s, %d iterations)\n\n", r.Driver, r.GitRef, r.Date, r.Iterations)
	fmt.Fprintf(w, "| Query | Dataset | Median | P95 | Max |\n")
	fmt.Fprintf(w, "|-------|---------|-------:|----:|----:|\n")

	for _, res := range r.Results {
		label := res.Section
		if res.Label != res.Dataset {
			label = res.Section + " / " + res.Label
		}

		fmt.Fprintf(w, "| %s | %s | %s | %s | %s |\n",
			label,
			res.Dataset,
			fmtDuration(res.Stats.Median),
			fmtDuration(res.Stats.P95),
			fmtDuration(res.Stats.Max),
		)
	}

	fmt.Fprintln(w)
	return nil
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
