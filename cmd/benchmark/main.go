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
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	_ "github.com/specterops/dawgs/drivers/neo4j"
)

type postgresBenchmarkDriver interface {
	KindMapper() pg.KindMapper
	DefaultGraph() (model.Graph, bool)
}

func main() {
	var (
		driver                  = flag.String("driver", "pg", "database driver (pg, neo4j)")
		connStr                 = flag.String("connection", "", "database connection string (or CONNECTION_STRING)")
		iterations              = flag.Int("iterations", 10, "timed iterations per scenario")
		warmup                  = flag.Int("warmup", 1, "untimed iterations per worker (zero measures cold queries)")
		workers                 = flag.Int("workers", 1, "concurrent workers per scenario")
		pgCache                 = flag.Int("pg-cache-entries", pg.DefaultRuntimeConfig().TranslationCacheEntries, "translations retained per physical connection")
		pgSharedSP              = flag.Int("pg-shared-shortest-path-template-entries", pg.DefaultRuntimeConfig().SharedShortestPathTemplateEntries, "immutable shortest-path templates shared across physical connections (zero disables)")
		pgSPExecutor            = flag.String("pg-shortest-path-executor", "", "benchmark-only qualified shortest-path executor identity (default uses production routing)")
		pgPolicy                = flag.String("pg-traversal-policy-manifest", "", "verified promotion manifest to install through the PostgreSQL traversal-policy path")
		pgPolicyGen             = flag.Uint64("pg-traversal-policy-generation", 1, "nonzero generation for -pg-traversal-policy-manifest")
		pgPolicyPreflight       = flag.String("pg-traversal-policy-preflight-manifest", "", "provisional manifest used only to derive the candidate SQL anchor")
		pgPolicyPreflightOutput = flag.String("pg-traversal-policy-preflight-output", "", "JSON destination for the non-promotional traversal-policy preflight")
		pgPlanMode              = flag.String("pg-plan-cache-mode", "auto", "PostgreSQL plan cache mode for shortest-path benchmark modes (auto, force_custom_plan, force_generic_plan)")
		pgJIT                   = flag.Bool("pg-jit", true, "enable PostgreSQL JIT transaction-locally for shortest-path benchmark modes")
		pgMinConns              = flag.Int("pg-min-conns", int(pg.DefaultRuntimeConfig().Pool.MinConnections), "minimum physical PostgreSQL connections")
		pgMaxConns              = flag.Int("pg-max-conns", int(pg.DefaultRuntimeConfig().Pool.MaxConnections), "maximum physical PostgreSQL connections")
		output                  = flag.String("output", "", "output file (default: stdout)")
		format                  = flag.String("format", reportFormatMarkdown, "output format (markdown, json, benchfmt)")
		jsonOutput              = flag.String("json-output", "", "JSON output file for baseline comparison")
		explain                 = flag.Bool("explain", false, "capture PostgreSQL EXPLAIN (ANALYZE, BUFFERS) for Cypher scenarios")
		datasetDir              = flag.String("dataset-dir", "integration/testdata", "path to testdata directory")
		localDataset            = flag.String("local-dataset", "", "additional local dataset (e.g. local/phantom)")
		onlyDataset             = flag.String("dataset", "", "run only this dataset (e.g. diamond, local/phantom)")
	)

	flag.Parse()

	if err := validateIterations(*iterations); err != nil {
		fatal("%v", err)
	}
	if err := validateBenchmarkConcurrency(*warmup, *workers); err != nil {
		fatal("%v", err)
	}
	if !isReportFormat(*format) {
		fatal("unsupported output format %q", *format)
	}
	if *pgPolicy != "" || *pgPolicyPreflight != "" {
		if *driver != pg.DriverName {
			fatal("traversal-policy benchmark modes require -driver pg")
		}
		if *pgSPExecutor != "" {
			fatal("traversal-policy benchmark modes cannot be combined with -pg-shortest-path-executor")
		}
		if *onlyDataset == "" {
			fatal("traversal-policy benchmark modes require -dataset so their exact-query path is unambiguous")
		}
		if *pgPolicy != "" && *explain {
			fatal("-explain cannot be combined with -pg-traversal-policy-manifest because the explainer does not bypass the live policy gate")
		}
	}
	if *pgPolicy != "" && *pgPolicyPreflight != "" {
		fatal("-pg-traversal-policy-manifest cannot be combined with -pg-traversal-policy-preflight-manifest")
	}
	if *pgPolicyPreflight != "" && *pgPolicyPreflightOutput == "" {
		fatal("-pg-traversal-policy-preflight-manifest requires -pg-traversal-policy-preflight-output")
	}
	runtimeConfig, err := benchmarkRuntimeConfig(*pgCache, *pgSharedSP, *pgMinConns, *pgMaxConns)
	if err != nil {
		fatal("invalid PostgreSQL runtime configuration: %v", err)
	}

	conn := *connStr
	if conn == "" {
		conn = os.Getenv("CONNECTION_STRING")
	}
	if conn == "" {
		fatal("no connection string: set -connection flag or CONNECTION_STRING env var")
	}

	ctx := context.Background()
	db, err := openBenchmarkDatabaseWithRuntimeConfig(ctx, *driver, conn, size.Gibibyte, runtimeConfig)
	if err != nil {
		fatal("failed to open database: %v", err)
	}
	defer db.Close(ctx)

	var traversalPolicy *pg.TraversalPolicy
	var traversalPolicyPreflight *benchmarkTraversalPromotionManifest
	if *pgPolicy != "" {
		policy, err := loadBenchmarkTraversalPolicy(*pgPolicy, *pgPolicyGen)
		if err != nil {
			fatal("load PostgreSQL traversal policy manifest: %v", err)
		}
		policyDriver, ok := db.(traversalPolicyBenchmarkDriver)
		if !ok {
			fatal("PostgreSQL benchmark driver does not support traversal-policy installation")
		}
		if err := policyDriver.SetTraversalPolicy(policy); err != nil {
			fatal("install PostgreSQL traversal policy: %v", err)
		}
		traversalPolicy = &policy
		fmt.Fprintf(os.Stderr, "installed PostgreSQL traversal policy generation=%d candidate=%s manifest=%s\n", policy.Generation, policy.ShortestPathExecutor, policy.PromotionManifestSHA256)
	}
	if *pgPolicyPreflight != "" {
		_, manifest, err := loadBenchmarkTraversalPromotionManifest(*pgPolicyPreflight)
		if err != nil {
			fatal("load PostgreSQL traversal policy preflight manifest: %v", err)
		}
		traversalPolicyPreflight = &manifest
	}

	// Build dataset list
	var datasets []string
	if *onlyDataset != "" {
		datasets = []string{*onlyDataset}
	} else {
		datasets = defaultDatasets
		if *localDataset != "" {
			datasets = append(datasets, *localDataset)
		}
	}
	if traversalPolicy != nil || traversalPolicyPreflight != nil {
		queryAllowlist := []string(nil)
		if traversalPolicy != nil {
			queryAllowlist = traversalPolicy.QuerySHA256Allowlist
		} else {
			for _, bucket := range traversalPolicyPreflight.Buckets {
				queryAllowlist = append(queryAllowlist, bucket.QuerySHA256...)
			}
		}
		selectionPolicy := pg.TraversalPolicy{QuerySHA256Allowlist: queryAllowlist}
		for _, dataset := range datasets {
			if _, err := selectTraversalPolicyScenarios(scenariosForDataset(dataset, opengraph.IDMap{}), selectionPolicy); err != nil {
				fatal("select manifest-authorized traversal benchmark scenario: %v", err)
			}
		}
	}

	// Scan all datasets for kinds and assert schema
	nodeKinds, edgeKinds := scanKinds(*datasetDir, datasets)

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "integration_test",
			Nodes: nodeKinds,
			Edges: edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: "integration_test"},
	}

	if err := db.AssertSchema(ctx, schema); err != nil {
		fatal("failed to assert schema: %v", err)
	}

	var runOptions RunOptions
	if *explain {
		if !isPostgresBenchmarkDriver(*driver) {
			fmt.Fprintf(os.Stderr, "  explain capture is only supported for pg; continuing without plans\n")
		} else if pgDB, ok := db.(postgresBenchmarkDriver); !ok {
			fmt.Fprintf(os.Stderr, "  explain capture unavailable for %T; continuing without plans\n", db)
		} else if defaultGraph, hasDefaultGraph := pgDB.DefaultGraph(); !hasDefaultGraph {
			fatal("failed to resolve default graph for explain capture")
		} else {
			runOptions.Explain = newPostgresExplainerWithExecutor(pgDB.KindMapper(), defaultGraph.ID, optimize.ShortestPathExecutor(*pgSPExecutor))
		}
	}
	if *pgSPExecutor != "" {
		if *driver != pg.DriverName {
			fatal("-pg-shortest-path-executor requires -driver pg")
		}
		pgDB, ok := db.(postgresBenchmarkDriver)
		if !ok {
			fatal("PostgreSQL benchmark driver does not expose translation metadata")
		}
		defaultGraph, found := pgDB.DefaultGraph()
		if !found {
			fatal("failed to resolve default graph for shortest-path executor benchmark")
		}
		wrapped, err := newShortestExecutorBenchmarkDatabase(db, pgDB.KindMapper(), defaultGraph, optimize.ShortestPathExecutor(*pgSPExecutor), *pgPlanMode, *pgJIT)
		if err != nil {
			fatal("configure shortest-path executor benchmark: %v", err)
		}
		db = wrapped
	}
	if traversalPolicy != nil {
		wrapped, err := newTraversalPolicyBenchmarkDatabase(db, *pgPlanMode, *pgJIT)
		if err != nil {
			fatal("configure traversal-policy benchmark: %v", err)
		}
		db = wrapped
	}

	report := Report{
		Driver:                  *driver,
		GitRef:                  gitRef(),
		Date:                    time.Now().Format("2006-01-02"),
		Iterations:              *iterations,
		WarmupIterations:        *warmup,
		Workers:                 *workers,
		ShortestPathExecutor:    *pgSPExecutor,
		PostgreSQLPlanCacheMode: *pgPlanMode,
		PostgreSQLJIT:           *pgJIT,
	}
	if traversalPolicy != nil {
		report.ShortestPathExecutor = string(traversalPolicy.ShortestPathExecutor)
		report.ShortestPathMode = shortestPathModeProductionPolicy
		report.TraversalPolicyGeneration = traversalPolicy.Generation
		report.TraversalPolicyManifestSHA256 = traversalPolicy.PromotionManifestSHA256
	} else if *pgSPExecutor != "" {
		report.ShortestPathMode = shortestPathModeForced
	}

	for _, ds := range datasets {
		fmt.Fprintf(os.Stderr, "benchmarking %s...\n", ds)

		// Clear graph
		if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		}); err != nil {
			fmt.Fprintf(os.Stderr, "  clear failed: %v\n", err)
			continue
		}

		// Load dataset
		path := *datasetDir + "/" + ds + ".json"
		idMap, err := loadDataset(ctx, db, path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  load failed: %v\n", err)
			continue
		}

		fmt.Fprintf(os.Stderr, "  loaded %d nodes\n", len(idMap))

		// Run scenarios
		scenarios := scenariosForDataset(ds, idMap)
		if traversalPolicy != nil || traversalPolicyPreflight != nil {
			queryAllowlist := []string(nil)
			if traversalPolicy != nil {
				queryAllowlist = traversalPolicy.QuerySHA256Allowlist
			} else {
				for _, bucket := range traversalPolicyPreflight.Buckets {
					queryAllowlist = append(queryAllowlist, bucket.QuerySHA256...)
				}
			}
			scenarios, err = selectTraversalPolicyScenarios(scenarios, pg.TraversalPolicy{QuerySHA256Allowlist: queryAllowlist})
			if err != nil {
				fatal("select manifest-authorized traversal benchmark scenario: %v", err)
			}
			if traversalPolicy != nil {
				fmt.Fprintf(os.Stderr, "  manifest-authorized scenario: %s/%s\n", scenarios[0].Section, scenarios[0].Label)
			}
		}
		if traversalPolicyPreflight != nil {
			pgDB, ok := db.(postgresBenchmarkDriver)
			if !ok {
				fatal("PostgreSQL benchmark driver does not expose translation metadata for policy preflight")
			}
			defaultGraph, found := pgDB.DefaultGraph()
			if !found {
				fatal("failed to resolve default graph for traversal policy preflight")
			}
			preflight, err := renderTraversalPolicyPreflight(ctx, pgDB.KindMapper(), defaultGraph, scenarios[0], *traversalPolicyPreflight)
			if err != nil {
				fatal("render traversal policy preflight: %v", err)
			}
			if err := writeTraversalPolicyPreflight(*pgPolicyPreflight, *pgPolicyPreflightOutput, preflight); err != nil {
				fatal("write traversal policy preflight: %v", err)
			}
			fmt.Fprintf(os.Stderr, "  wrote non-promotional traversal policy preflight %s (query=%s sql=%s)\n", *pgPolicyPreflightOutput, preflight.QuerySHA256, preflight.SQLSHA256)
			return
		}
		for _, s := range scenarios {
			runOptions.WarmupIterations = *warmup
			runOptions.Workers = *workers
			result, err := runScenario(ctx, db, s, *iterations, runOptions)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  %s/%s failed: %v\n", s.Section, s.Label, err)
				continue
			}

			report.Results = append(report.Results, result)
			fmt.Fprintf(os.Stderr, "  %s/%s: rows=%d distinct=%s duplicates=%s median=%s p95=%s max=%s explain=%s\n",
				s.Section, s.Label,
				result.RowCount,
				fmtOptionalInt64(result.DistinctRowCount),
				fmtOptionalInt64(result.DuplicateRowCount),
				fmtDuration(result.Stats.Median),
				fmtDuration(result.Stats.P95),
				fmtDuration(result.Stats.Max),
				fmtExplainStatus(result.Explain),
			)
		}
	}
	if statsProvider, ok := db.(interface{ TranslationCacheStats() pg.Stats }); ok {
		stats := statsProvider.TranslationCacheStats()
		report.TranslationCache = &stats
		fmt.Fprintf(os.Stderr, "PostgreSQL connection state: cache hits=%d misses=%d bypasses=%d evictions=%d; workspaces initialized=%d reused=%d; statements prepared=%d reused=%d; live_connections=%d pool=%d-%d\n",
			stats.Aggregate.Hits,
			stats.Aggregate.Misses,
			stats.Aggregate.Bypasses,
			stats.Aggregate.Evictions,
			stats.TraversalWorkspace.Initializations,
			stats.TraversalWorkspace.Reuses,
			stats.PreparedStatements.Prepared,
			stats.PreparedStatements.Reuses,
			stats.LiveConnections,
			stats.MinConnections,
			stats.MaxConnections,
		)
	}

	// Write report
	var mdOut *os.File
	if *output != "" {
		var err error
		mdOut, err = os.Create(*output)
		if err != nil {
			fatal("failed to create output: %v", err)
		}
		defer mdOut.Close()
	} else {
		mdOut = os.Stdout
	}

	if err := writeReport(mdOut, report, *format); err != nil {
		fatal("failed to write report: %v", err)
	}

	if *output != "" {
		fmt.Fprintf(os.Stderr, "wrote %s\n", *output)
	}

	if *jsonOutput != "" {
		jsonOut, err := os.Create(*jsonOutput)
		if err != nil {
			fatal("failed to create JSON output: %v", err)
		}
		defer jsonOut.Close()

		if err := writeJSON(jsonOut, report); err != nil {
			fatal("failed to write JSON output: %v", err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", *jsonOutput)
	}
}

func openBenchmarkDatabase(ctx context.Context, driverName, connection string, graphQueryMemoryLimit size.Size) (graph.Database, error) {
	return openBenchmarkDatabaseWithRuntimeConfig(ctx, driverName, connection, graphQueryMemoryLimit, pg.DefaultRuntimeConfig())
}

func openBenchmarkDatabaseWithRuntimeConfig(ctx context.Context, driverName, connection string, graphQueryMemoryLimit size.Size, runtimeConfig pg.RuntimeConfig) (graph.Database, error) {
	cfg := dawgs.Config{
		GraphQueryMemoryLimit: graphQueryMemoryLimit,
		ConnectionString:      connection,
	}

	switch driverName {
	case pg.DriverName:
		poolConfig, err := pgxpool.ParseConfig(connection)
		if err != nil {
			return nil, fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
		}
		pool, err := pg.NewPoolWithRuntimeConfig(ctx, poolConfig, runtimeConfig)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
		}
		return pg.NewDriver(graphQueryMemoryLimit, pool), nil

	default:
		return dawgs.Open(ctx, driverName, cfg)
	}
}

func benchmarkRuntimeConfig(cacheEntries, sharedShortestPathTemplateEntries, minConnections, maxConnections int) (pg.RuntimeConfig, error) {
	const maxInt32 = int(^uint32(0) >> 1)
	if cacheEntries < 0 {
		return pg.RuntimeConfig{}, fmt.Errorf("translation cache entries must not be negative: %d", cacheEntries)
	}
	if sharedShortestPathTemplateEntries < 0 {
		return pg.RuntimeConfig{}, fmt.Errorf("shared shortest-path template entries must not be negative: %d", sharedShortestPathTemplateEntries)
	}
	if minConnections < 0 || minConnections > maxInt32 {
		return pg.RuntimeConfig{}, fmt.Errorf("minimum connections must be between 0 and %d: %d", maxInt32, minConnections)
	}
	if maxConnections < 1 || maxConnections > maxInt32 {
		return pg.RuntimeConfig{}, fmt.Errorf("maximum connections must be between 1 and %d: %d", maxInt32, maxConnections)
	}
	if minConnections > maxConnections {
		return pg.RuntimeConfig{}, fmt.Errorf("minimum connections %d exceeds maximum connections %d", minConnections, maxConnections)
	}
	return pg.RuntimeConfig{
		TranslationCacheEntries:           cacheEntries,
		SharedShortestPathTemplateEntries: sharedShortestPathTemplateEntries,
		Pool: &pg.PoolConfig{
			MinConnections: int32(minConnections),
			MaxConnections: int32(maxConnections),
		},
	}, nil
}

func isPostgresBenchmarkDriver(driverName string) bool {
	return driverName == pg.DriverName
}

func scanKinds(datasetDir string, datasets []string) (graph.Kinds, graph.Kinds) {
	var nodeKinds, edgeKinds graph.Kinds

	for _, ds := range datasets {
		path := datasetDir + "/" + ds + ".json"
		f, err := os.Open(path)
		if err != nil {
			continue
		}

		doc, err := opengraph.ParseDocument(f)
		f.Close()
		if err != nil {
			continue
		}

		nk, ek := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nk...)
		edgeKinds = edgeKinds.Add(ek...)
	}

	return nodeKinds, edgeKinds
}

func loadDataset(ctx context.Context, db graph.Database, path string) (opengraph.IDMap, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return opengraph.Load(ctx, db, f)
}

func gitRef() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
