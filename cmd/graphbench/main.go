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
	"strconv"
	"strings"
	"time"

	"github.com/specterops/dawgs/testutil"
)

type config struct {
	CorpusRoot                string
	DatasetDir                string
	Connection                string
	PGConnection              string
	Neo4jConnection           string
	Modes                     []ExecutionMode
	Iterations                int
	WarmupIterations          int
	Round                     int
	Block                     int
	Arm                       string
	ArmOrder                  int
	RunUUID                   string
	Cases                     []string
	Datasets                  []string
	Categories                []string
	Tags                      []string
	OutputJSONL               string
	Summary                   string
	SummaryJSON               string
	Baseline                  string
	DAWGSVersion              string
	GateBaseline              string
	GateCandidate             string
	GateOutput                string
	GateSeed                  int64
	Confidence                float64
	Regression                float64
	GateTargets               []string
	MaterialityRatio          float64
	MaterialityAbsolute       time.Duration
	DestructiveLock           string
	AAArtifact                string
	AAOutput                  string
	PoolSize                  int
	Concurrency               []int
	SessionMemoryCeilingBytes int64
	PoolMemoryCeilingBytes    int64
	PostgresReferences        bool
	ConfirmLeft               string
	ConfirmRight              string
	ConfirmAA                 string
	ConfirmOutput             string
	ConfirmCases              []string
	DiagnosticGate            bool
	BundleDir                 string
	BuildCommand              string
}

func parseConfig(args []string, env func(string) string) (config, error) {
	flags := flag.NewFlagSet("graphbench", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		cfg             config
		rawModes        string
		rawGateTargets  string
		rawConcurrency  string
		rawCases        string
		rawDatasets     string
		rawCategories   string
		rawTags         string
		rawConfirmCases string
	)

	flags.StringVar(&cfg.CorpusRoot, "corpus-root", "benchmark/testdata/scale", "scale corpus root")
	flags.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "dataset root")
	flags.StringVar(&cfg.Connection, "connection", env("CONNECTION_STRING"), "single backend connection string")
	flags.StringVar(&cfg.PGConnection, "pg-connection", env("PG_CONNECTION_STRING"), "PostgreSQL connection string")
	flags.StringVar(&cfg.Neo4jConnection, "neo4j-connection", env("NEO4J_CONNECTION_STRING"), "Neo4j connection string")
	flags.StringVar(&rawModes, "modes", string(ModePostgresSQL), "comma-separated execution modes")
	flags.IntVar(&cfg.Iterations, "iterations", 3, "timed iterations per case")
	flags.IntVar(&cfg.WarmupIterations, "warmup-iterations", 1, "fixed untimed warmup iterations per case")
	flags.IntVar(&cfg.Round, "round", 1, "independent benchmark round identifier")
	flags.IntVar(&cfg.Block, "block", 1, "matched benchmark block identifier")
	flags.StringVar(&cfg.Arm, "arm", "unlabeled", "matched benchmark arm label")
	flags.IntVar(&cfg.ArmOrder, "arm-order", 0, "one-based execution order inside the matched block (0 when unpaired)")
	flags.StringVar(&cfg.RunUUID, "run-uuid", "", "run-series UUID (generated when empty)")
	flags.StringVar(&rawCases, "cases", "", "comma-separated exact case names")
	flags.StringVar(&rawDatasets, "datasets", "", "comma-separated exact dataset names")
	flags.StringVar(&rawCategories, "categories", "", "comma-separated exact category names")
	flags.StringVar(&rawTags, "tags", "", "comma-separated exact case tags")
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
	flags.StringVar(&rawGateTargets, "gate-targets", "", "comma-separated PostgreSQL case names expected to improve materially")
	flags.Float64Var(&cfg.MaterialityRatio, "materiality-ratio", 0.95, "target median-ratio upper bound")
	flags.DurationVar(&cfg.MaterialityAbsolute, "materiality-absolute", 100*time.Microsecond, "target median-saving lower bound")
	flags.StringVar(&cfg.DestructiveLock, "destructive-lock", ".coverage/graphbench.lock", "local lock file guarding destructive fixture reloads")
	flags.StringVar(&cfg.AAArtifact, "aa-artifact", "", "JSONL artifact used to calculate baseline A/A measurement resolution")
	flags.StringVar(&cfg.AAOutput, "aa-output", "", "A/A measurement-resolution JSON output path (default: stdout)")
	flags.IntVar(&cfg.PoolSize, "pool-size", 1, "PostgreSQL physical pool size")
	flags.StringVar(&rawConcurrency, "concurrency", "", "comma-separated opt-in PostgreSQL concurrency smoke levels")
	flags.Int64Var(&cfg.SessionMemoryCeilingBytes, "session-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes per PostgreSQL session")
	flags.Int64Var(&cfg.PoolMemoryCeilingBytes, "pool-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes for the complete PostgreSQL pool")
	flags.BoolVar(&cfg.PostgresReferences, "postgres-references", false, "capture C1 PostgreSQL component floors and full-query references")
	flags.StringVar(&cfg.ConfirmLeft, "confirm-left", "", "left JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmRight, "confirm-right", "", "right JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmAA, "confirm-aa", "", "optional block/reload A/A resolution report")
	flags.StringVar(&cfg.ConfirmOutput, "confirm-output", "", "paired confirmation JSON output path (default: stdout)")
	flags.StringVar(&rawConfirmCases, "confirm-cases", "", "comma-separated exact primary names for paired confirmation")
	flags.BoolVar(&cfg.DiagnosticGate, "diagnostic-gate", false, "allow comparison of matching diagnostic-only subsets")
	flags.StringVar(&cfg.BundleDir, "bundle-dir", "", "write a reconstructible capture bundle to this directory")
	flags.StringVar(&cfg.BuildCommand, "build-command", "go build -trimpath ./cmd/graphbench", "reproducible build command recorded in bundles")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.Iterations < 1 {
		return config{}, fmt.Errorf("iterations must be at least 1")
	}
	if cfg.WarmupIterations < 0 {
		return config{}, fmt.Errorf("warmup-iterations must not be negative")
	}
	if cfg.Round < 1 {
		return config{}, fmt.Errorf("round must be at least 1")
	}
	if cfg.Block < 1 {
		return config{}, fmt.Errorf("block must be at least 1")
	}
	if strings.TrimSpace(cfg.Arm) == "" {
		return config{}, fmt.Errorf("arm must not be empty")
	}
	if cfg.ArmOrder < 0 {
		return config{}, fmt.Errorf("arm-order must not be negative")
	}
	if cfg.PoolSize < 1 {
		return config{}, fmt.Errorf("pool-size must be at least 1")
	}
	if cfg.SessionMemoryCeilingBytes < 0 || cfg.PoolMemoryCeilingBytes < 0 {
		return config{}, fmt.Errorf("memory ceilings must not be negative")
	}
	if cfg.SessionMemoryCeilingBytes > 0 && cfg.PoolMemoryCeilingBytes > 0 && cfg.SessionMemoryCeilingBytes*int64(cfg.PoolSize) > cfg.PoolMemoryCeilingBytes {
		return config{}, fmt.Errorf("session memory ceiling times pool size exceeds pool memory ceiling")
	}
	for _, raw := range strings.Split(rawConcurrency, ",") {
		if raw = strings.TrimSpace(raw); raw == "" {
			continue
		}
		level, err := strconv.Atoi(raw)
		if err != nil || level < 1 {
			return config{}, fmt.Errorf("concurrency levels must be positive integers, got %q", raw)
		}
		if !slices.Contains(cfg.Concurrency, level) {
			cfg.Concurrency = append(cfg.Concurrency, level)
		}
	}
	if (cfg.GateBaseline == "") != (cfg.GateCandidate == "") {
		return config{}, fmt.Errorf("gate-baseline and gate-candidate must be supplied together")
	}
	if (cfg.ConfirmLeft == "") != (cfg.ConfirmRight == "") {
		return config{}, fmt.Errorf("confirm-left and confirm-right must be supplied together")
	}
	if cfg.ConfirmAA != "" && cfg.ConfirmLeft == "" {
		return config{}, fmt.Errorf("confirm-aa requires confirm-left and confirm-right")
	}
	modeCount := 0
	if cfg.GateBaseline != "" {
		modeCount++
	}
	if cfg.AAArtifact != "" {
		modeCount++
	}
	if cfg.ConfirmLeft != "" {
		modeCount++
	}
	if modeCount > 1 {
		return config{}, fmt.Errorf("performance-gate, A/A, and paired-confirmation modes are mutually exclusive")
	}
	if cfg.AAArtifact != "" && cfg.GateBaseline != "" {
		return config{}, fmt.Errorf("aa-artifact and performance-gate mode are mutually exclusive")
	}
	if cfg.Confidence <= 0 || cfg.Confidence >= 1 {
		return config{}, fmt.Errorf("confidence-level must be between 0 and 1")
	}
	if cfg.Regression < 0 {
		return config{}, fmt.Errorf("regression-threshold must not be negative")
	}
	if cfg.MaterialityRatio <= 0 || cfg.MaterialityRatio >= 1 {
		return config{}, fmt.Errorf("materiality-ratio must be between 0 and 1")
	}
	if cfg.MaterialityAbsolute < 0 {
		return config{}, fmt.Errorf("materiality-absolute must not be negative")
	}
	for _, target := range strings.Split(rawGateTargets, ",") {
		if target = strings.TrimSpace(target); target != "" {
			cfg.GateTargets = append(cfg.GateTargets, target)
		}
	}
	var err error
	if cfg.Cases, err = parseUniqueCSV("case", rawCases); err != nil {
		return config{}, err
	}
	if cfg.Datasets, err = parseUniqueCSV("dataset", rawDatasets); err != nil {
		return config{}, err
	}
	if cfg.Categories, err = parseUniqueCSV("category", rawCategories); err != nil {
		return config{}, err
	}
	if cfg.Tags, err = parseUniqueCSV("tag", rawTags); err != nil {
		return config{}, err
	}
	if cfg.ConfirmCases, err = parseUniqueCSV("confirmation case", rawConfirmCases); err != nil {
		return config{}, err
	}

	modes, err := parseExecutionModes(rawModes)
	if err != nil {
		return config{}, err
	}
	cfg.Modes = modes

	return cfg, nil
}

func parseUniqueCSV(kind, raw string) ([]string, error) {
	var values []string
	seen := map[string]struct{}{}
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("duplicate %s selector %q", kind, value)
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
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
		corpus, err := loadScaleCorpus(cfg.CorpusRoot)
		if err != nil {
			fatal("load gate corpus declaration: %v", err)
		}
		selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: cfg.Cases, Datasets: cfg.Datasets, Categories: cfg.Categories, Tags: cfg.Tags})
		if err != nil {
			fatal("select gate corpus: %v", err)
		}
		passed, err := comparePerformanceArtifacts(cfg.GateBaseline, cfg.GateCandidate, cfg.GateOutput, PerfGateOptions{
			Seed:                cfg.GateSeed,
			Confidence:          cfg.Confidence,
			RegressionThreshold: cfg.Regression,
			DeclaredBackends:    selected.DeclaredBackends(),
			TargetNames:         cfg.GateTargets,
			MaterialityRatio:    cfg.MaterialityRatio,
			MaterialityAbsolute: cfg.MaterialityAbsolute,
			DiagnosticMode:      cfg.DiagnosticGate,
		})
		if err != nil {
			fatal("compare performance artifacts: %v", err)
		}
		if !passed {
			fatal("performance gate failed")
		}
		return
	}
	if cfg.AAArtifact != "" {
		if err := createAAResolutionReport(cfg.AAArtifact, cfg.AAOutput, PerfGateOptions{
			Seed: cfg.GateSeed, Confidence: cfg.Confidence,
		}); err != nil {
			fatal("calculate A/A measurement resolution: %v", err)
		}
		return
	}
	if cfg.ConfirmLeft != "" {
		if err := createConfirmationReport(cfg.ConfirmLeft, cfg.ConfirmRight, cfg.ConfirmAA, cfg.ConfirmOutput, ConfirmationOptions{
			Seed: cfg.GateSeed, Confidence: cfg.Confidence, CaseNames: cfg.ConfirmCases,
		}); err != nil {
			fatal("calculate paired confirmation: %v", err)
		}
		return
	}

	runLock, err := acquireDestructiveRunLock(cfg.DestructiveLock)
	if err != nil {
		fatal("acquire destructive run lock: %v", err)
	}
	defer func() {
		if err := runLock.Close(); err != nil {
			fatal("release destructive run lock: %v", err)
		}
	}()

	fullCorpus, err := loadScaleCorpus(cfg.CorpusRoot)
	if err != nil {
		fatal("load corpus: %v", err)
	}
	corpus, selection, err := selectScaleCorpus(fullCorpus, CorpusSelectors{
		Cases: cfg.Cases, Datasets: cfg.Datasets, Categories: cfg.Categories, Tags: cfg.Tags,
	})
	if err != nil {
		fatal("select corpus: %v", err)
	}

	var (
		ctx       = context.Background()
		records   []CaseResult
		startedAt = time.Now()
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

			runner, err := newPostgresSQLRunner(ctx, cfg.DatasetDir, pgConnection, corpus, cfg.PoolSize, cfg.Concurrency, cfg.PostgresReferences)
			if err != nil {
				fatal("open postgres_sql runner: %v", err)
			}

			nextRecords, err := runner.Run(ctx, cfg.WarmupIterations, cfg.Iterations, corpus)
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

			nextRecords, err := runner.Run(ctx, cfg.WarmupIterations, cfg.Iterations, corpus)
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
	environment := resolveRunEnvironment(cfg, os.Args, selection, startedAt, time.Now())
	for idx := range records {
		records[idx].Metadata = metadata
		records[idx].Environment = &environment
		setSampleRunMetadata(&records[idx].Stats, environment)
		for referenceIdx := range records[idx].PostgresReferences {
			setSampleRunMetadata(&records[idx].PostgresReferences[referenceIdx].Stats, environment)
		}
	}

	if cfg.Baseline != "" {
		if err := applyBaseline(cfg.Baseline, records); err != nil {
			fatal("compare baseline: %v", err)
		}
	}

	if err := writeJSONLFile(cfg.OutputJSONL, records); err != nil {
		fatal("write JSONL: %v", err)
	}
	if cfg.BundleDir != "" {
		if err := writeCaptureBundle(cfg.BundleDir, corpus, records, environment); err != nil {
			fatal("write capture bundle: %v", err)
		}
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
