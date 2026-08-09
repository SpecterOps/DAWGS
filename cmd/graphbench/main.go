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

	"github.com/specterops/dawgs/databaseguard"
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
	AppendJSONL               bool
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
	ReferenceClosureArtifact  string
	ReferenceClosureOutput    string
	ReferenceClosureArm       string
	ReferencePairArtifact     string
	ReferencePairOutput       string
	ReferencePairBaseline     string
	ReferencePairCandidate    string
	ReferencePairProtocol     string
	PoolSize                  int
	Concurrency               []int
	SessionMemoryCeilingBytes int64
	PoolMemoryCeilingBytes    int64
	PostgresReferences        bool
	PostgresReferenceArms     []string
	PostgresForceShortest     string
	PostgresForceExpansion    string
	ConfirmLeft               string
	ConfirmRight              string
	ConfirmAA                 string
	ConfirmOutput             string
	ConfirmCases              []string
	DiagnosticGate            bool
	BundleDir                 string
	BuildCommand              string
	ExistingGraph             bool
	AnchorManifest            string
	Checkpoint                string
	Resume                    bool
	Progress                  string
	Discovery                 bool
	TimeoutClasses            []time.Duration
	DiscoverySampleFloor      int
	ResourceArtifact          string
	ResourceOutput            string
	BackendDeltaArtifact      string
	BackendDeltaOutput        string
}

func parseConfig(args []string, env func(string) string) (config, error) {
	flags := flag.NewFlagSet("graphbench", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		cfg               config
		rawModes          string
		rawGateTargets    string
		rawConcurrency    string
		rawCases          string
		rawDatasets       string
		rawCategories     string
		rawTags           string
		rawConfirmCases   string
		rawReferenceArms  string
		rawTimeoutClasses string
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
	flags.BoolVar(&cfg.AppendJSONL, "append-jsonl", false, "append a validated round to an existing JSONL run-series artifact")
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
	flags.StringVar(&cfg.ReferenceClosureArtifact, "reference-closure-artifact", "", "JSONL artifact containing matched production raw-pgx and PostgreSQL reference samples")
	flags.StringVar(&cfg.ReferenceClosureOutput, "reference-closure-output", "", "production/reference closure JSON output path (default: stdout)")
	flags.StringVar(&cfg.ReferenceClosureArm, "reference-closure-arm", "s3_unidirectional_trail_cte", "PostgreSQL full-comparator reference arm")
	flags.StringVar(&cfg.ReferencePairArtifact, "reference-pair-artifact", "", "JSONL artifact containing two matched PostgreSQL reference arms")
	flags.StringVar(&cfg.ReferencePairOutput, "reference-pair-output", "", "matched PostgreSQL reference-pair JSON output path (default: stdout)")
	flags.StringVar(&cfg.ReferencePairBaseline, "reference-pair-baseline", "", "baseline PostgreSQL reference arm")
	flags.StringVar(&cfg.ReferencePairCandidate, "reference-pair-candidate", "", "candidate PostgreSQL reference arm")
	flags.StringVar(&cfg.ReferencePairProtocol, "reference-pair-protocol", referencePairProtocolConfirmation, "reference-pair report protocol (confirmation or discovery)")
	flags.IntVar(&cfg.PoolSize, "pool-size", 1, "PostgreSQL physical pool size")
	flags.StringVar(&rawConcurrency, "concurrency", "", "comma-separated opt-in PostgreSQL concurrency smoke levels")
	flags.Int64Var(&cfg.SessionMemoryCeilingBytes, "session-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes per PostgreSQL session")
	flags.Int64Var(&cfg.PoolMemoryCeilingBytes, "pool-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes for the complete PostgreSQL pool")
	flags.BoolVar(&cfg.PostgresReferences, "postgres-references", false, "capture C1 PostgreSQL component floors and full-query references")
	flags.StringVar(&rawReferenceArms, "postgres-reference-arms", "", "comma-separated PostgreSQL reference arms (default: all applicable arms)")
	flags.StringVar(&cfg.PostgresForceShortest, "postgres-force-shortest-executor", "", "tool-only forced PostgreSQL shortest executor (supported: SP-S0, SP-S0-DIRECT, SP-S3-U-D, SP-S3-U-E+MAT-M0, SP-S4-C-D, SP-S4-C-WE+MAT-M0, ASP-A1-DAG)")
	flags.StringVar(&cfg.PostgresForceExpansion, "postgres-force-expansion-search", "", "tool-only forced PostgreSQL expansion search (supported: ADCS-A3)")
	flags.StringVar(&cfg.ConfirmLeft, "confirm-left", "", "left JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmRight, "confirm-right", "", "right JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmAA, "confirm-aa", "", "optional block/reload A/A resolution report")
	flags.StringVar(&cfg.ConfirmOutput, "confirm-output", "", "paired confirmation JSON output path (default: stdout)")
	flags.StringVar(&rawConfirmCases, "confirm-cases", "", "comma-separated exact primary names for paired confirmation")
	flags.BoolVar(&cfg.DiagnosticGate, "diagnostic-gate", false, "allow comparison of matching diagnostic-only subsets")
	flags.StringVar(&cfg.BundleDir, "bundle-dir", "", "write a reconstructible capture bundle to this directory")
	flags.StringVar(&cfg.BuildCommand, "build-command", "go build -trimpath ./cmd/graphbench", "reproducible build command recorded in bundles")
	flags.BoolVar(&cfg.ExistingGraph, "existing-graph", false, "run non-mutating PostgreSQL cases against an existing graph in read-write sessions without schema, load, clear, vacuum, or persistent writes")
	flags.StringVar(&cfg.AnchorManifest, "anchor-manifest", "", "versioned logical-key anchor manifest for existing-graph mode")
	flags.StringVar(&cfg.Checkpoint, "checkpoint", "", "atomic existing-graph checkpoint path")
	flags.BoolVar(&cfg.Resume, "resume", false, "resume completed records from the matching existing-graph checkpoint")
	flags.StringVar(&cfg.Progress, "progress", "", "append-only existing-graph progress JSONL path")
	flags.BoolVar(&cfg.Discovery, "discovery", false, "label the run adaptive discovery rather than fixed confirmation")
	flags.StringVar(&rawTimeoutClasses, "timeout-classes", "", "comma-separated predeclared per-case timeout classes used by discovery")
	flags.IntVar(&cfg.DiscoverySampleFloor, "discovery-sample-floor", 1, "minimum measured samples after adaptive discovery reduction")
	flags.StringVar(&cfg.ResourceArtifact, "resource-artifact", "", "JSONL artifact used to calculate the state/resource gate")
	flags.StringVar(&cfg.ResourceOutput, "resource-output", "", "state/resource gate JSON output path (default: stdout)")
	flags.StringVar(&cfg.BackendDeltaArtifact, "backend-delta-artifact", "", "JSONL artifact used for descriptive matched PostgreSQL/Neo4j deltas")
	flags.StringVar(&cfg.BackendDeltaOutput, "backend-delta-output", "", "descriptive backend-delta JSON output path (default: stdout)")

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
	if cfg.ReferenceClosureOutput != "" && cfg.ReferenceClosureArtifact == "" {
		return config{}, fmt.Errorf("reference-closure-output requires reference-closure-artifact")
	}
	if cfg.ReferencePairOutput != "" && cfg.ReferencePairArtifact == "" {
		return config{}, fmt.Errorf("reference-pair-output requires reference-pair-artifact")
	}
	if cfg.ReferencePairArtifact != "" && (cfg.ReferencePairBaseline == "" || cfg.ReferencePairCandidate == "") {
		return config{}, fmt.Errorf("reference-pair-artifact requires baseline and candidate arms")
	}
	if cfg.ReferencePairBaseline != "" && cfg.ReferencePairBaseline == cfg.ReferencePairCandidate {
		return config{}, fmt.Errorf("reference-pair baseline and candidate must differ")
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
	if cfg.ReferenceClosureArtifact != "" {
		modeCount++
	}
	if cfg.ReferencePairArtifact != "" {
		modeCount++
	}
	if cfg.ResourceArtifact != "" {
		modeCount++
	}
	if cfg.BackendDeltaArtifact != "" {
		modeCount++
	}
	if modeCount > 1 {
		return config{}, fmt.Errorf("performance-gate, A/A, paired-confirmation, reference-closure, reference-pair, resource-gate, and backend-delta modes are mutually exclusive")
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
	if cfg.AppendJSONL && cfg.OutputJSONL == "" {
		return config{}, fmt.Errorf("append-jsonl requires jsonl-output")
	}
	if cfg.ResourceOutput != "" && cfg.ResourceArtifact == "" {
		return config{}, fmt.Errorf("resource-output requires resource-artifact")
	}
	if cfg.BackendDeltaOutput != "" && cfg.BackendDeltaArtifact == "" {
		return config{}, fmt.Errorf("backend-delta-output requires backend-delta-artifact")
	}
	if cfg.DiscoverySampleFloor < 1 {
		return config{}, fmt.Errorf("discovery-sample-floor must be at least 1")
	}
	for _, raw := range strings.Split(rawTimeoutClasses, ",") {
		if raw = strings.TrimSpace(raw); raw != "" {
			timeout, err := time.ParseDuration(raw)
			if err != nil || timeout <= 0 {
				return config{}, fmt.Errorf("timeout classes must be positive durations, got %q", raw)
			}
			if len(cfg.TimeoutClasses) > 0 && timeout <= cfg.TimeoutClasses[len(cfg.TimeoutClasses)-1] {
				return config{}, fmt.Errorf("timeout classes must be strictly increasing")
			}
			cfg.TimeoutClasses = append(cfg.TimeoutClasses, timeout)
		}
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
	if cfg.PostgresReferenceArms, err = parseUniqueCSV("PostgreSQL reference arm", rawReferenceArms); err != nil {
		return config{}, err
	}
	for _, arm := range cfg.PostgresReferenceArms {
		if !validPostgresReferenceArm(arm) {
			return config{}, fmt.Errorf("unknown PostgreSQL reference arm %q", arm)
		}
	}
	if cfg.ReferenceClosureArtifact != "" && !validPostgresReferenceArm(cfg.ReferenceClosureArm) {
		return config{}, fmt.Errorf("unknown PostgreSQL reference closure arm %q", cfg.ReferenceClosureArm)
	}
	if len(cfg.PostgresReferenceArms) > 0 {
		cfg.PostgresReferences = true
	}
	if cfg.PostgresForceShortest != "" && cfg.PostgresForceShortest != "SP-S0" && cfg.PostgresForceShortest != "SP-S0-DIRECT" && cfg.PostgresForceShortest != "SP-S3-U-D" && cfg.PostgresForceShortest != "SP-S3-U-E+MAT-M0" && cfg.PostgresForceShortest != "SP-S4-C-D" && cfg.PostgresForceShortest != "SP-S4-C-WE+MAT-M0" && cfg.PostgresForceShortest != "ASP-A1-DAG" {
		return config{}, fmt.Errorf("unsupported PostgreSQL forced shortest executor %q", cfg.PostgresForceShortest)
	}
	if cfg.PostgresForceExpansion != "" && cfg.PostgresForceExpansion != "ADCS-A3" {
		return config{}, fmt.Errorf("unsupported PostgreSQL forced expansion search %q", cfg.PostgresForceExpansion)
	}
	if cfg.PostgresForceShortest != "" && cfg.PostgresForceExpansion != "" {
		return config{}, fmt.Errorf("PostgreSQL shortest and expansion search forces are mutually exclusive")
	}

	modes, err := parseExecutionModes(rawModes)
	if err != nil {
		return config{}, err
	}
	cfg.Modes = modes
	if cfg.ExistingGraph {
		if cfg.AnchorManifest == "" {
			return config{}, fmt.Errorf("existing-graph mode requires anchor-manifest")
		}
		if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL {
			return config{}, fmt.Errorf("existing-graph mode currently requires only postgres_sql mode")
		}
		if cfg.Resume && cfg.Checkpoint == "" {
			return config{}, fmt.Errorf("resume requires checkpoint")
		}
		if len(cfg.TimeoutClasses) > 0 && !cfg.Discovery {
			return config{}, fmt.Errorf("timeout-classes require discovery mode")
		}
	} else if cfg.Resume || cfg.AnchorManifest != "" || cfg.Checkpoint != "" || cfg.Progress != "" || cfg.Discovery || len(cfg.TimeoutClasses) > 0 {
		return config{}, fmt.Errorf("existing-graph workflow flags require existing-graph mode")
	}

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
	if cfg.ReferenceClosureArtifact != "" {
		passed, err := createReferenceClosureReport(cfg.ReferenceClosureArtifact, cfg.ReferenceClosureOutput, ReferenceClosureOptions{
			Seed: cfg.GateSeed, Confidence: cfg.Confidence, ReferenceName: cfg.ReferenceClosureArm,
			RatioUpperLimit: 1.10, AbsoluteResolution: cfg.MaterialityAbsolute,
		})
		if err != nil {
			fatal("calculate production/reference closure: %v", err)
		}
		if !passed {
			fatal("production/reference closure failed")
		}
		return
	}
	if cfg.ReferencePairArtifact != "" {
		if err := createReferencePairReport(cfg.ReferencePairArtifact, cfg.ReferencePairOutput, ReferencePairOptions{
			Seed: cfg.GateSeed, Confidence: cfg.Confidence,
			BaselineName: cfg.ReferencePairBaseline, CandidateName: cfg.ReferencePairCandidate, Protocol: cfg.ReferencePairProtocol,
		}); err != nil {
			fatal("calculate matched reference pair: %v", err)
		}
		return
	}
	if cfg.ResourceArtifact != "" {
		passed, err := createResourceGateReport(cfg.ResourceArtifact, cfg.ResourceOutput)
		if err != nil {
			fatal("calculate state/resource gate: %v", err)
		}
		if !passed {
			fatal("state/resource gate failed")
		}
		return
	}
	if cfg.BackendDeltaArtifact != "" {
		if err := createBackendDeltaReport(cfg.BackendDeltaArtifact, cfg.BackendDeltaOutput); err != nil {
			fatal("calculate descriptive backend deltas: %v", err)
		}
		return
	}

	if !cfg.ExistingGraph {
		for _, mode := range cfg.Modes {
			var connection string
			switch mode {
			case ModePostgresSQL:
				connection = cfg.PGConnection
			case ModeNeo4j:
				connection = cfg.Neo4jConnection
			default:
				continue
			}
			if connection == "" {
				connection = cfg.Connection
			}
			if connection == "" {
				continue
			}
			if err := databaseguard.Validate(
				connection,
				os.Getenv(databaseguard.AllowDestructiveEnv),
				os.Getenv(databaseguard.DisposableTargetsEnv),
			); err != nil {
				fatal("refuse destructive GraphBench target: %v", err)
			}
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
	}

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
	var existingManifest ExistingGraphAnchorManifest
	checkpointCorpusHash := corpusIdentity(corpus)
	if cfg.ExistingGraph {
		existingManifest, err = loadExistingGraphAnchorManifest(cfg.AnchorManifest)
		if err != nil {
			fatal("load existing-graph anchor manifest: %v", err)
		}
		if err := validateExistingGraphCorpus(corpus, existingManifest); err != nil {
			fatal("validate existing-graph corpus: %v", err)
		}
		if cfg.Resume {
			records, err = readExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash)
			if err != nil {
				fatal("resume existing-graph checkpoint: %v", err)
			}
		}
	}

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

			var existingOptions *existingGraphRunnerOptions
			if cfg.ExistingGraph {
				completed := map[string]bool{}
				for _, key := range sortedCompletedKeys(records) {
					completed[key] = true
				}
				existingOptions = &existingGraphRunnerOptions{
					Manifest: existingManifest, ProgressPath: cfg.Progress, Discovery: cfg.Discovery,
					TimeoutClasses: append([]time.Duration(nil), cfg.TimeoutClasses...), SampleFloor: cfg.DiscoverySampleFloor,
					Completed: completed,
					OnRecord: func(record CaseResult) error {
						records = append(records, record)
						return writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, records)
					},
					OnComplete: func(postNodes, postEdges int64) error {
						for idx := range records {
							if records[idx].ExistingGraph != nil {
								records[idx].ExistingGraph.PostNodeCount = postNodes
								records[idx].ExistingGraph.PostEdgeCount = postEdges
							}
						}
						return writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, records)
					},
				}
			}
			runner, err := newPostgresSQLRunnerWithExistingGraph(ctx, cfg.DatasetDir, pgConnection, corpus, cfg.PoolSize, cfg.Round, cfg.Concurrency, cfg.PostgresReferences, cfg.PostgresReferenceArms, cfg.PostgresForceShortest, cfg.PostgresForceExpansion, existingOptions)
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

			if !cfg.ExistingGraph {
				records = append(records, nextRecords...)
			} else {
				// OnRecord appends each completed record atomically. A resumed run
				// may have no new records, while a complete run refreshes the final
				// before/after cardinality proof below.
				if err := writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, records); err != nil {
					fatal("finalize existing-graph checkpoint: %v", err)
				}
			}

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

	var writeErr error
	if cfg.AppendJSONL {
		writeErr = appendJSONLFile(cfg.OutputJSONL, records)
	} else {
		writeErr = writeJSONLFile(cfg.OutputJSONL, records)
	}
	if writeErr != nil {
		fatal("write JSONL: %v", writeErr)
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
