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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var benchmarkLinePattern = regexp.MustCompile(`^(Benchmark\S+)\s+\d+\s+([0-9]+(?:\.[0-9]+)?)\s+ns/op\b`)

type benchmarkSamples map[string][]float64

type regression struct {
	Name           string
	BaseMedianNS   float64
	TargetMedianNS float64
	Percent        float64
}

func parseBenchfmtNS(data []byte) benchmarkSamples {
	samples := benchmarkSamples{}
	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		matches := benchmarkLinePattern.FindStringSubmatch(scanner.Text())
		if len(matches) != 3 {
			continue
		}

		ns, err := strconv.ParseFloat(matches[2], 64)
		if err != nil {
			continue
		}

		samples[matches[1]] = append(samples[matches[1]], ns)
	}

	return samples
}

func parseBenchfmtNSFile(path string) (benchmarkSamples, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseBenchfmtNS(data), nil
}

func findRegressions(base, target benchmarkSamples, threshold float64) []regression {
	if threshold <= 0 {
		return nil
	}

	var regressions []regression
	for name, baseValues := range base {
		targetValues := target[name]
		if len(baseValues) == 0 || len(targetValues) == 0 {
			continue
		}

		baseMedian := median(baseValues)
		targetMedian := median(targetValues)
		if baseMedian <= 0 {
			continue
		}

		percent := ((targetMedian - baseMedian) / baseMedian) * 100
		if percent > threshold {
			regressions = append(regressions, regression{
				Name:           name,
				BaseMedianNS:   baseMedian,
				TargetMedianNS: targetMedian,
				Percent:        percent,
			})
		}
	}

	sort.Slice(regressions, func(i, j int) bool {
		return regressions[i].Percent > regressions[j].Percent
	})

	return regressions
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}

	return sorted[mid]
}

func writeIntegrationBenchfmt(w io.Writer, driver string, rows []markdownBenchmarkRow) error {
	fmt.Fprintf(w, "goos: %s\n", runtime.GOOS)
	fmt.Fprintf(w, "goarch: %s\n", runtime.GOARCH)
	fmt.Fprintln(w, "pkg: github.com/specterops/dawgs/cmd/benchmark")

	procs := runtime.GOMAXPROCS(0)
	for _, row := range rows {
		fmt.Fprintf(w, "%s-%d\t1\t%d ns/op\n", integrationBenchmarkName(driver, row.Dataset, row.Query), procs, row.Median.Nanoseconds())
	}

	return nil
}

func integrationBenchmarkName(driver, dataset, query string) string {
	return strings.Join([]string{
		"BenchmarkDawgsIntegration",
		sanitizeBenchNamePart(driver),
		sanitizeBenchNamePart(dataset),
		sanitizeBenchNamePart(query),
	}, "/")
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

func parseBenchmarkDuration(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "-" {
		return 0, fmt.Errorf("empty benchmark duration")
	}

	unitStart := len(trimmed)
	for idx, char := range trimmed {
		if (char < '0' || char > '9') && char != '.' {
			unitStart = idx
			break
		}
	}

	number, err := strconv.ParseFloat(strings.TrimSpace(trimmed[:unitStart]), 64)
	if err != nil {
		return 0, err
	}

	unit := strings.TrimSpace(trimmed[unitStart:])
	switch unit {
	case "ns":
		return time.Duration(math.Round(number)), nil
	case "us":
		return time.Duration(math.Round(number * float64(time.Microsecond))), nil
	case "ms":
		return time.Duration(math.Round(number * float64(time.Millisecond))), nil
	case "s":
		return time.Duration(math.Round(number * float64(time.Second))), nil
	default:
		return 0, fmt.Errorf("unsupported benchmark duration unit %q", unit)
	}
}
