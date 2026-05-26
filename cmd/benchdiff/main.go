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
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	benchKindAll         = "all"
	benchKindIntegration = "integration"
	benchKindUnit        = "unit"
)

type config struct {
	BaseRef               string
	TargetRef             string
	Kind                  string
	Packages              string
	Bench                 string
	BenchCount            int
	Benchtime             string
	Driver                string
	Connection            string
	Dataset               string
	LocalDataset          string
	DatasetDir            string
	IntegrationIterations int
	OutDir                string
	Benchstat             string
	FailRegression        string
	KeepWorktrees         bool
}

type resolvedConfig struct {
	config
	Root           string
	BaseSHA        string
	TargetSHA      string
	BaseShortSHA   string
	TargetShortSHA string
	DatasetDirAbs  string
	OutDirAbs      string
	Threshold      float64
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	cfg := config{}
	flags := flag.NewFlagSet("benchdiff", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	flags.StringVar(&cfg.BaseRef, "base", "main", "base git ref to benchmark")
	flags.StringVar(&cfg.TargetRef, "target", "HEAD", "target git ref to benchmark")
	flags.StringVar(&cfg.Kind, "kind", benchKindAll, "benchmark kind: all, unit, integration")
	flags.StringVar(&cfg.Packages, "packages", "./...", "package list for Go benchmarks")
	flags.StringVar(&cfg.Bench, "bench", ".", "Go benchmark regexp")
	flags.IntVar(&cfg.BenchCount, "bench-count", 10, "Go benchmark repetition count")
	flags.StringVar(&cfg.Benchtime, "benchtime", "1s", "Go benchmark benchtime")
	flags.StringVar(&cfg.Driver, "driver", "pg", "integration benchmark database driver")
	flags.StringVar(&cfg.Connection, "connection", "", "integration database connection string (or CONNECTION_STRING)")
	flags.StringVar(&cfg.Dataset, "dataset", "", "run only this integration dataset")
	flags.StringVar(&cfg.LocalDataset, "local-dataset", "", "additional local integration dataset")
	flags.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "integration testdata directory")
	flags.IntVar(&cfg.IntegrationIterations, "integration-iterations", 10, "timed iterations per integration scenario")
	flags.StringVar(&cfg.OutDir, "out", "", "output directory")
	flags.StringVar(&cfg.Benchstat, "benchstat", "auto", "benchstat command, auto, or none")
	flags.StringVar(&cfg.FailRegression, "fail-regression", "0", "fail when median ns/op regression exceeds this percent, e.g. 10%")
	flags.BoolVar(&cfg.KeepWorktrees, "keep-worktrees", false, "keep temporary git worktrees")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if !isBenchKind(cfg.Kind) {
		return config{}, fmt.Errorf("unsupported benchmark kind %q", cfg.Kind)
	}
	if cfg.BenchCount < 1 {
		return config{}, fmt.Errorf("bench-count must be at least 1")
	}
	if cfg.IntegrationIterations < 1 {
		return config{}, fmt.Errorf("integration-iterations must be at least 1")
	}
	if err := validateBenchtime(cfg.Benchtime); err != nil {
		return config{}, err
	}
	if _, err := parseRegressionThreshold(cfg.FailRegression); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func isBenchKind(kind string) bool {
	switch kind {
	case benchKindAll, benchKindIntegration, benchKindUnit:
		return true
	default:
		return false
	}
}

func (cfg config) runsUnitBenchmarks() bool {
	return cfg.Kind == benchKindAll || cfg.Kind == benchKindUnit
}

func (cfg config) runsIntegrationBenchmarks() bool {
	return cfg.Kind == benchKindAll || cfg.Kind == benchKindIntegration
}

func parseRegressionThreshold(value string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimSuffix(trimmed, "%")

	if trimmed == "" {
		return 0, fmt.Errorf("fail-regression must be a non-negative percent")
	}

	threshold, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid fail-regression %q: %w", value, err)
	}
	if threshold < 0 {
		return 0, fmt.Errorf("fail-regression must be a non-negative percent")
	}

	return threshold, nil
}

func validateBenchtime(value string) error {
	trimmed := strings.TrimSpace(value)
	if strings.HasSuffix(trimmed, "x") {
		count, err := strconv.Atoi(strings.TrimSuffix(trimmed, "x"))
		if err != nil || count < 1 {
			return fmt.Errorf("invalid benchtime %q", value)
		}

		return nil
	}

	if _, err := time.ParseDuration(trimmed); err != nil {
		return fmt.Errorf("invalid benchtime %q: %w", value, err)
	}

	return nil
}
