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

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	_ "github.com/specterops/dawgs/drivers/neo4j"
)

func main() {
	var (
		driver       = flag.String("driver", "pg", "database driver (pg, neo4j)")
		connStr      = flag.String("connection", "", "database connection string (or PG_CONNECTION_STRING)")
		iterations   = flag.Int("iterations", 10, "timed iterations per scenario")
		output       = flag.String("output", "", "markdown output file (default: stdout)")
		datasetDir   = flag.String("dataset-dir", "integration/testdata", "path to testdata directory")
		localDataset = flag.String("local-dataset", "", "additional local dataset (e.g. local/phantom)")
		onlyDataset  = flag.String("dataset", "", "run only this dataset (e.g. diamond, local/phantom)")
		dbcfg        = drivers.DatabaseConfiguration{}
	)

	flag.Parse()

	conn := *connStr
	if conn == "" {
		conn = os.Getenv("PG_CONNECTION_STRING")
	}
	if conn == "" {
		fatal("no connection string: set -connection flag or PG_CONNECTION_STRING env var")
	}

	ctx := context.Background()

	cfg := dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      conn,
	}

	dbcfg.Connection = conn

	if *driver == pg.DriverName {
		pool, err := pg.NewPool(dbcfg)
		if err != nil {
			fatal("failed to create pool: %v", err)
		}
		cfg.Pool = pool
	}

	db, err := dawgs.Open(ctx, *driver, cfg)
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
			result, err := runScenario(ctx, db, s, *iterations)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  %s/%s failed: %v\n", s.Section, s.Label, err)
				continue
			}

			report.Results = append(report.Results, result)
			fmt.Fprintf(os.Stderr, "  %s/%s: median=%s p95=%s max=%s\n",
				s.Section, s.Label,
				fmtDuration(result.Stats.Median),
				fmtDuration(result.Stats.P95),
				fmtDuration(result.Stats.Max),
			)
		}
	}

	// Write markdown
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

	if err := writeMarkdown(mdOut, report); err != nil {
		fatal("failed to write markdown: %v", err)
	}

	if *output != "" {
		fmt.Fprintf(os.Stderr, "wrote %s\n", *output)
	}
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
