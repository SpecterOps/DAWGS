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
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgv2 "github.com/specterops/dawgs/drivers/pg/v2"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	_ "github.com/specterops/dawgs/drivers/neo4j"
)

const pgV2BenchmarkDriver = "pg-v2"

type postgresBenchmarkDriver interface {
	KindMapper() pg.KindMapper
	DefaultGraph() (model.Graph, bool)
}

func main() {
	var (
		driver       = flag.String("driver", "pg", "database driver (pg, pg-v2, neo4j)")
		connStr      = flag.String("connection", "", "database connection string (or CONNECTION_STRING)")
		iterations   = flag.Int("iterations", 10, "timed iterations per scenario")
		output       = flag.String("output", "", "output file (default: stdout)")
		format       = flag.String("format", reportFormatMarkdown, "output format (markdown, json, benchfmt)")
		jsonOutput   = flag.String("json-output", "", "JSON output file for baseline comparison")
		explain      = flag.Bool("explain", false, "capture PostgreSQL EXPLAIN (ANALYZE, BUFFERS) for Cypher scenarios")
		datasetDir   = flag.String("dataset-dir", "integration/testdata", "path to testdata directory")
		localDataset = flag.String("local-dataset", "", "additional local dataset (e.g. local/phantom)")
		onlyDataset  = flag.String("dataset", "", "run only this dataset (e.g. diamond, local/phantom)")
	)

	flag.Parse()

	if err := validateIterations(*iterations); err != nil {
		fatal("%v", err)
	}
	if !isReportFormat(*format) {
		fatal("unsupported output format %q", *format)
	}

	conn := *connStr
	if conn == "" {
		conn = os.Getenv("CONNECTION_STRING")
	}
	if conn == "" {
		fatal("no connection string: set -connection flag or CONNECTION_STRING env var")
	}

	ctx := context.Background()
	db, err := openBenchmarkDatabase(ctx, *driver, conn, size.Gibibyte)
	if err != nil {
		fatal("failed to open database: %v", err)
	}
	defer db.Close(ctx)

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
			fmt.Fprintf(os.Stderr, "  explain capture is only supported for pg and pg-v2; continuing without plans\n")
		} else if pgDB, ok := db.(postgresBenchmarkDriver); !ok {
			fmt.Fprintf(os.Stderr, "  explain capture unavailable for %T; continuing without plans\n", db)
		} else if defaultGraph, hasDefaultGraph := pgDB.DefaultGraph(); !hasDefaultGraph {
			fatal("failed to resolve default graph for explain capture")
		} else {
			runOptions.Explain = newPostgresExplainer(pgDB.KindMapper(), defaultGraph.ID)
		}
	}

	report := Report{
		Driver:     *driver,
		GitRef:     gitRef(),
		Date:       time.Now().Format("2006-01-02"),
		Iterations: *iterations,
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
		for _, s := range scenariosForDataset(ds, idMap) {
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
		pool, err := pg.NewPool(poolConfig)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
		}
		cfg.Pool = pool
		return dawgs.Open(ctx, driverName, cfg)

	case pgV2BenchmarkDriver:
		poolConfig, err := pgxpool.ParseConfig(connection)
		if err != nil {
			return nil, fmt.Errorf("parse PostgreSQL v2 pool configuration: %w", err)
		}
		pool, err := pgv2.NewDefaultPool(ctx, poolConfig)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL v2 pool: %w", err)
		}
		return pgv2.NewDriver(graphQueryMemoryLimit, pool), nil

	default:
		return dawgs.Open(ctx, driverName, cfg)
	}
}

func isPostgresBenchmarkDriver(driverName string) bool {
	return driverName == pg.DriverName || driverName == pgV2BenchmarkDriver
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
