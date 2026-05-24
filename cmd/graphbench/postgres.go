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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

type postgresSQLRunner struct {
	datasetDir string
	db         graph.Database
	pgDriver   *pg.Driver
	graphID    int32
}

func newPostgresSQLRunner(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus) (*postgresSQLRunner, error) {
	poolCfg, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}

	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connection,
		Pool:                  pool,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open PostgreSQL database: %w", err)
	}

	nodeKinds, edgeKinds, err := scanDatasetKinds(datasetDir, scaleCorpusDatasets(corpus))
	if err != nil {
		_ = db.Close(ctx)
		return nil, err
	}

	if err := db.AssertSchema(ctx, benchmarkSchema(nodeKinds, edgeKinds)); err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("assert PostgreSQL schema: %w", err)
	}

	pgDriver, ok := db.(*pg.Driver)
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("expected *pg.Driver, got %T", db)
	}

	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("PostgreSQL default graph is not set")
	}

	return &postgresSQLRunner{
		datasetDir: datasetDir,
		db:         db,
		pgDriver:   pgDriver,
		graphID:    defaultGraph.ID,
	}, nil
}

func (s *postgresSQLRunner) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}

	return s.db.Close(ctx)
}

func (s *postgresSQLRunner) Run(ctx context.Context, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	var records []CaseResult
	casesByDataset := scaleCasesByDataset(corpus)

	for _, datasetName := range scaleCorpusDatasets(corpus) {
		if err := clearGraph(ctx, s.db); err != nil {
			return nil, fmt.Errorf("clear graph for %s: %w", datasetName, err)
		}

		idMap, err := loadDataset(ctx, s.db, s.datasetDir, datasetName)
		if err != nil {
			return nil, err
		}

		for _, testCase := range casesByDataset[datasetName] {
			if !testCase.Supports(ModePostgresSQL) {
				continue
			}

			record := s.runCase(ctx, iterations, testCase, idMap)
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *postgresSQLRunner) runCase(ctx context.Context, iterations int, testCase ScaleCase, idMap opengraph.IDMap) CaseResult {
	params, err := resolveCaseParams(testCase, idMap)
	record := newCaseResult(testCase, ModePostgresSQL, params)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	rowCount, stats, err := measureCypher(ctx, s.db, testCase.Cypher, params, iterations)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	record.RowCount = rowCount
	record.Stats = stats
	applyRowExpectation(&record)

	explain, err := s.explain(ctx, testCase.Cypher, params)
	if err != nil {
		if record.Status == StatusOK {
			record.Status = StatusError
			record.Error = err.Error()
		}
		return record
	}

	record.SQL = explain.SQL
	record.PostgresPlan = explain.Plan
	record.PostgresMetrics = &explain.Metrics
	record.Optimization = &explain.Optimization
	return record
}

type postgresExplain struct {
	SQL          string
	Plan         []string
	Metrics      PostgresPlanMetrics
	Optimization translate.OptimizationSummary
}

func (s *postgresSQLRunner) explain(ctx context.Context, cypherQuery string, params map[string]any) (postgresExplain, error) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		return postgresExplain{}, err
	}

	translation, err := translate.Translate(ctx, regularQuery, s.pgDriver.KindMapper(), params, s.graphID)
	if err != nil {
		return postgresExplain{}, err
	}

	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		return postgresExplain{}, err
	}

	var plan []string
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) "+sqlQuery, translation.Parameters)
		defer result.Close()

		for result.Next() {
			values := result.Values()
			if len(values) == 0 {
				continue
			}

			plan = append(plan, fmt.Sprint(values[0]))
		}

		return result.Error()
	}); err != nil {
		return postgresExplain{}, err
	}

	return postgresExplain{
		SQL:          sqlQuery,
		Plan:         plan,
		Metrics:      parsePostgresPlanMetrics(plan),
		Optimization: translation.Optimization,
	}, nil
}

var (
	postgresPlanningPattern  = regexp.MustCompile(`Planning Time: ([0-9.]+) ms`)
	postgresExecutionPattern = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)
	postgresBufferPattern    = regexp.MustCompile(`(?:(shared|temp) )?(hit|read|dirtied|written)=([0-9]+)`)
)

func parsePostgresPlanMetrics(plan []string) PostgresPlanMetrics {
	var metrics PostgresPlanMetrics
	for _, line := range plan {
		if metrics.PlanningMS == nil {
			if match := postgresPlanningPattern.FindStringSubmatch(line); match != nil {
				if parsed, err := strconv.ParseFloat(match[1], 64); err == nil {
					metrics.PlanningMS = &parsed
				}
			}
		}

		if metrics.ExecutionMS == nil {
			if match := postgresExecutionPattern.FindStringSubmatch(line); match != nil {
				if parsed, err := strconv.ParseFloat(match[1], 64); err == nil {
					metrics.ExecutionMS = &parsed
				}
			}
		}

		if strings.Contains(line, "Buffers:") && metrics.Buffers == (Buffers{}) {
			metrics.Buffers = parsePostgresBuffers(line)
		}
	}

	return metrics
}

func parsePostgresBuffers(line string) Buffers {
	var (
		buffers     Buffers
		bufferScope string
	)

	for _, match := range postgresBufferPattern.FindAllStringSubmatch(line, -1) {
		value, err := strconv.ParseInt(match[3], 10, 64)
		if err != nil {
			continue
		}

		if match[1] != "" {
			bufferScope = match[1]
		}

		switch bufferScope + "_" + match[2] {
		case "shared_hit":
			buffers.SharedHit = value
		case "shared_read":
			buffers.SharedRead = value
		case "shared_dirtied":
			buffers.SharedDirtied = value
		case "temp_read":
			buffers.TempRead = value
		case "temp_written":
			buffers.TempWritten = value
		}
	}

	return buffers
}
