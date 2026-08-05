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
	"io"
	"os"
	"slices"
	"strings"

	"github.com/specterops/dawgs/testutil"
)

type config struct {
	CorpusRoot      string
	DatasetDir      string
	Connection      string
	PGConnection    string
	Neo4jConnection string
	Modes           []ExecutionMode
	Iterations      int
	Round           int
	OutputJSONL     string
	Summary         string
	SummaryJSON     string
	Baseline        string
	DAWGSVersion    string
	GateBaseline    string
	GateCandidate   string
	GateOutput      string
	GateSeed        int64
	Confidence      float64
	Regression      float64
}

func parseConfig(args []string, env func(string) string) (config, error) {
	flags := flag.NewFlagSet("graphbench", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		cfg      config
		rawModes string
	)

	flags.StringVar(&cfg.CorpusRoot, "corpus-root", "benchmark/testdata/scale", "scale corpus root")
	flags.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "dataset root")
	flags.StringVar(&cfg.Connection, "connection", env("CONNECTION_STRING"), "single backend connection string")
	flags.StringVar(&cfg.PGConnection, "pg-connection", env("PG_CONNECTION_STRING"), "PostgreSQL connection string")
	flags.StringVar(&cfg.Neo4jConnection, "neo4j-connection", env("NEO4J_CONNECTION_STRING"), "Neo4j connection string")
	flags.StringVar(&rawModes, "modes", string(ModePostgresSQL), "comma-separated execution modes")
	flags.IntVar(&cfg.Iterations, "iterations", 3, "timed iterations per case")
	flags.IntVar(&cfg.Round, "round", 1, "independent benchmark round identifier")
	flags.StringVar(&cfg.OutputJSONL, "jsonl-output", "", "JSONL output path (default: stdout)")
	flags.StringVar(&cfg.Summary, "summary", "", "markdown summary output path")
	flags.StringVar(&cfg.SummaryJSON, "summary-json", "", "JSON summary output path")
	flags.StringVar(&cfg.Baseline, "baseline", "", "previous JSONL output for baseline comparison")
	flags.StringVar(&cfg.DAWGSVersion, "dawgs-version", "", "DAWGS source version (auto-detected when empty)")
	flags.StringVar(&cfg.GateBaseline, "gate-baseline", "", "baseline JSONL artifact for comparison-only mode")
	flags.StringVar(&cfg.GateCandidate, "gate-candidate", "", "candidate JSONL artifact for comparison-only mode")
	flags.StringVar(&cfg.GateOutput, "gate-output", "", "performance-gate JSON output path (default: stdout)")
	flags.Int64Var(&cfg.GateSeed, "seed", 1, "deterministic bootstrap seed")
	flags.Float64Var(&cfg.Confidence, "confidence-level", 0.95, "bootstrap confidence level")
	flags.Float64Var(&cfg.Regression, "regression-threshold", 0.20, "allowed comparable-case regression ratio")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.Iterations < 1 {
		return config{}, fmt.Errorf("iterations must be at least 1")
	}
	if cfg.Round < 1 {
		return config{}, fmt.Errorf("round must be at least 1")
	}
	if (cfg.GateBaseline == "") != (cfg.GateCandidate == "") {
		return config{}, fmt.Errorf("gate-baseline and gate-candidate must be supplied together")
	}
	if cfg.Confidence <= 0 || cfg.Confidence >= 1 {
		return config{}, fmt.Errorf("confidence-level must be between 0 and 1")
	}
	if cfg.Regression < 0 {
		return config{}, fmt.Errorf("regression-threshold must not be negative")
	}

	modes, err := parseExecutionModes(rawModes)
	if err != nil {
		return config{}, err
	}
	cfg.Modes = modes

	return cfg, nil
}

func parseExecutionModes(raw string) ([]ExecutionMode, error) {
	var (
		modes []ExecutionMode
		seen  = map[ExecutionMode]struct{}{}
	)

	for _, part := range strings.Split(raw, ",") {
		mode, err := parseExecutionMode(part)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[mode]; duplicate {
			continue
		}

		seen[mode] = struct{}{}
		modes = append(modes, mode)
	}
	if len(modes) == 0 {
		return nil, fmt.Errorf("at least one execution mode is required")
	}

	return modes, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		fatal("%v", err)
	}
	if cfg.GateBaseline != "" {
		passed, err := comparePerformanceArtifacts(cfg.GateBaseline, cfg.GateCandidate, cfg.GateOutput, PerfGateOptions{
			Seed:                cfg.GateSeed,
			Confidence:          cfg.Confidence,
			RegressionThreshold: cfg.Regression,
		})
		if err != nil {
			fatal("compare performance artifacts: %v", err)
		}
		if !passed {
			fatal("performance gate failed")
		}
		return
	}

	corpus, err := loadScaleCorpus(cfg.CorpusRoot)
	if err != nil {
		fatal("load corpus: %v", err)
	}

	var (
		ctx     = context.Background()
		records []CaseResult
	)

	for _, mode := range modesForRound(cfg.Modes, cfg.Round) {
		switch mode {
		case ModePostgresSQL:
			pgConnection := cfg.PGConnection
			if pgConnection == "" {
				pgConnection = cfg.Connection
			}
			if pgConnection == "" {
				fatal("postgres_sql mode requires -pg-connection, -connection, PG_CONNECTION_STRING, or CONNECTION_STRING")
			}

			runner, err := newPostgresSQLRunner(ctx, cfg.DatasetDir, pgConnection, corpus)
			if err != nil {
				fatal("open postgres_sql runner: %v", err)
			}

			nextRecords, err := runner.Run(ctx, cfg.Iterations, corpus)
			closeErr := runner.Close(ctx)
			if err != nil {
				fatal("run postgres_sql: %v", err)
			}
			if closeErr != nil {
				fatal("close postgres_sql: %v", closeErr)
			}

			records = append(records, nextRecords...)

		case ModeNeo4j:
			neo4jConnection := cfg.Neo4jConnection
			if neo4jConnection == "" {
				neo4jConnection = cfg.Connection
			}
			if neo4jConnection == "" {
				fatal("neo4j mode requires -neo4j-connection, -connection, NEO4J_CONNECTION_STRING, or CONNECTION_STRING")
			}

			runner, err := newNeo4jRunner(ctx, cfg.DatasetDir, neo4jConnection, corpus)
			if err != nil {
				fatal("open neo4j runner: %v", err)
			}

			nextRecords, err := runner.Run(ctx, cfg.Iterations, corpus)
			closeErr := runner.Close(ctx)
			if err != nil {
				fatal("run neo4j: %v", err)
			}
			if closeErr != nil {
				fatal("close neo4j: %v", closeErr)
			}

			records = append(records, nextRecords...)

		case ModeLocalTraversal:
			records = append(records, runLocalTraversalPlaceholders(corpus)...)

		default:
			fatal("execution mode %s is not implemented yet", mode)
		}
	}

	if err := validateBackendObservations(records); err != nil {
		fatal("validate backend observations: %v", err)
	}

	metadata := testutil.ResolveBaselineMetadata(cfg.DAWGSVersion)
	for idx := range records {
		records[idx].Metadata = metadata
		setSampleRound(&records[idx].Stats, cfg.Round)
	}

	if cfg.Baseline != "" {
		if err := applyBaseline(cfg.Baseline, records); err != nil {
			fatal("compare baseline: %v", err)
		}
	}

	if err := writeJSONLFile(cfg.OutputJSONL, records); err != nil {
		fatal("write JSONL: %v", err)
	}

	summary := buildSummary(records)
	if cfg.Summary != "" {
		if err := writeMarkdownSummaryFile(cfg.Summary, summary); err != nil {
			fatal("write markdown summary: %v", err)
		}
	}
	if cfg.SummaryJSON != "" {
		if err := writeJSONSummaryFile(cfg.SummaryJSON, summary); err != nil {
			fatal("write JSON summary: %v", err)
		}
	}
}

func modesForRound(modes []ExecutionMode, round int) []ExecutionMode {
	ordered := append([]ExecutionMode(nil), modes...)
	if round%2 == 0 {
		slices.Reverse(ordered)
	}
	return ordered
}
