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
	"strings"
)

type config struct {
	CorpusRoot      string
	DatasetDir      string
	Connection      string
	PGConnection    string
	Neo4jConnection string
	Modes           []ExecutionMode
	Iterations      int
	OutputJSONL     string
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
	flags.StringVar(&cfg.OutputJSONL, "jsonl-output", "", "JSONL output path (default: stdout)")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.Iterations < 1 {
		return config{}, fmt.Errorf("iterations must be at least 1")
	}

	modes, err := parseExecutionModes(rawModes)
	if err != nil {
		return config{}, err
	}
	cfg.Modes = modes

	return cfg, nil
}

func parseExecutionModes(raw string) ([]ExecutionMode, error) {
	var modes []ExecutionMode
	seen := map[ExecutionMode]struct{}{}

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

	corpus, err := loadScaleCorpus(cfg.CorpusRoot)
	if err != nil {
		fatal("load corpus: %v", err)
	}

	ctx := context.Background()
	var records []CaseResult

	for _, mode := range cfg.Modes {
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

	if err := writeJSONLFile(cfg.OutputJSONL, records); err != nil {
		fatal("write JSONL: %v", err)
	}
}
