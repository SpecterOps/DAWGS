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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
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
	datasetDir  string
	db          graph.Database
	pgDriver    *pg.Driver
	pool        *pgxpool.Pool
	graphID     int32
	backendPID  string
	poolSize    int
	concurrency []int
	environment PostgresEnvironment
	references  bool
}

func newPostgresSQLRunner(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus, poolSize int, concurrency []int, references bool) (*postgresSQLRunner, error) {
	poolCfg, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
	}
	// GraphBench needs first-call and steady-state samples from an identifiable
	// physical session. A single-connection pool makes that relationship
	// deterministic while retaining the production pool hooks.
	poolCfg.MinConns = int32(poolSize)
	poolCfg.MaxConns = int32(poolSize)
	// pg.NewPool applies the production driver's fixed 5/50 pool sizing. The
	// benchmark must preserve the requested size so a size-one run can prove
	// that all samples in a case used the same physical session.
	poolCfg.AfterConnect = pg.AfterPooledConnectionEstablished
	poolCfg.AfterRelease = pg.AfterPooledConnectionRelease
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
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
	var backendPID int32
	if err := pool.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID); err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("identify PostgreSQL benchmark connection: %w", err)
	}
	var postgresEnvironment PostgresEnvironment
	if err := pool.QueryRow(ctx, `select version(), current_database(), current_setting('plan_cache_mode'), current_setting('work_mem'), current_setting('temp_file_limit'), (select count(*) from graph), pg_postmaster_start_time(), (select oid::int8 from pg_database where datname = current_database()), current_setting('autovacuum')`).Scan(
		&postgresEnvironment.Version,
		&postgresEnvironment.Database,
		&postgresEnvironment.PlanCacheMode,
		&postgresEnvironment.WorkMem,
		&postgresEnvironment.TempFileLimit,
		&postgresEnvironment.GraphPartitionCount,
		&postgresEnvironment.PostmasterStartedAt,
		&postgresEnvironment.DatabaseOID,
		&postgresEnvironment.Autovacuum,
	); err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("capture PostgreSQL environment: %w", err)
	}

	return &postgresSQLRunner{
		datasetDir:  datasetDir,
		db:          db,
		pgDriver:    pgDriver,
		pool:        pool,
		graphID:     defaultGraph.ID,
		backendPID:  strconv.FormatInt(int64(backendPID), 10),
		poolSize:    poolSize,
		concurrency: append([]int(nil), concurrency...),
		environment: postgresEnvironment,
		references:  references,
	}, nil
}

func (s *postgresSQLRunner) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}

	return s.db.Close(ctx)
}

func (s *postgresSQLRunner) Run(ctx context.Context, warmupIterations, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	var (
		records        []CaseResult
		casesByDataset = scaleCasesByDataset(corpus)
	)

	for _, datasetName := range scaleCorpusDatasets(corpus) {
		fixture, err := fixtureMetadata(s.datasetDir, datasetName)
		if err != nil {
			return nil, err
		}
		if err := clearGraph(ctx, s.db); err != nil {
			return nil, fmt.Errorf("clear graph for %s: %w", datasetName, err)
		}

		idMap, err := loadDataset(ctx, s.db, s.datasetDir, datasetName)
		if err != nil {
			return nil, err
		}
		if _, err := s.pool.Exec(ctx, "vacuum (analyze) node, edge"); err != nil {
			return nil, fmt.Errorf("vacuum and analyze %s fixture: %w", datasetName, err)
		}
		if err := s.pool.QueryRow(ctx, `select pg_total_relation_size('node'), pg_total_relation_size('edge'), coalesce((select string_agg(relname || ':' || coalesce(last_analyze::text, 'never'), ',' order by relname) from pg_stat_all_tables where relname in ('node', 'edge')), '')`).Scan(
			&s.environment.NodeRelationBytes, &s.environment.EdgeRelationBytes, &s.environment.AnalyzeState,
		); err != nil {
			return nil, fmt.Errorf("capture %s fixture relation sizes: %w", datasetName, err)
		}

		for _, testCase := range casesByDataset[datasetName] {
			if !testCase.Supports(ModePostgresSQL) {
				continue
			}

			if err := s.resetCaseSession(ctx); err != nil {
				return nil, fmt.Errorf("reset PostgreSQL session for %s: %w", testCase.Name, err)
			}

			record := s.runCase(ctx, warmupIterations, iterations, testCase, idMap)
			record.Fixture = &fixture
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *postgresSQLRunner) resetCaseSession(ctx context.Context) error {
	s.pool.Reset()
	if s.poolSize != 1 {
		s.backendPID = ""
		return nil
	}

	var backendPID int32
	if err := s.pool.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID); err != nil {
		return err
	}
	s.backendPID = strconv.FormatInt(int64(backendPID), 10)
	return nil
}

func (s *postgresSQLRunner) runCase(ctx context.Context, warmupIterations, iterations int, testCase ScaleCase, idMap opengraph.IDMap) CaseResult {
	params, err := resolveCaseParams(testCase, idMap)
	record := newCaseResult(testCase, ModePostgresSQL, params)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	if testCase.WriteScenario == nil {
		rowCount, observedRows, stats, err := measureCypherWithWarmups(ctx, s.db, testCase.Cypher, params, testCase.Expected, idMap, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		record.RowCount = rowCount
		record.ObservedRows = observedRows
		record.Stats = stats
		labelLatencySamples(&record.Stats, ModePostgresSQL, testCase)
		for idx := range record.Stats.Samples {
			record.Stats.Samples[idx].ConnectionID = s.backendPID
		}
		applyRowExpectation(&record)
	} else {
		scenario, err := resolveWriteScenario(testCase, idMap)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		measurement, stats, err := measureWriteCypherWithWarmups(ctx, s.db, testCase.Cypher, params, scenario, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		record.MatchedCount = &measurement.Matched
		record.AffectedCount = &measurement.Affected
		record.PostState = measurement.PostState
		record.Stats = stats
		labelLatencySamples(&record.Stats, ModePostgresSQL, testCase)
		for idx := range record.Stats.Samples {
			record.Stats.Samples[idx].ConnectionID = s.backendPID
		}
	}
	if s.poolSize == 1 {
		var backendPID int32
		if err := s.pool.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID); err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("verify PostgreSQL benchmark connection: %v", err)
			return record
		}
		if current := strconv.FormatInt(int64(backendPID), 10); current != s.backendPID {
			record.Status = StatusError
			record.Error = fmt.Sprintf("PostgreSQL physical connection changed during case: %s -> %s", s.backendPID, current)
			return record
		}
	}

	explain, err := s.explain(ctx, testCase.Cypher, params, testCase.WriteScenario != nil)
	if err != nil {
		if record.Status == StatusOK {
			record.Status = StatusError
			record.Error = err.Error()
		}
		return record
	}

	record.SQL = explain.SQL
	record.SQLFingerprint = sqlFingerprint(explain.SQL)
	postgresEnvironment := s.environment
	record.PostgresEnvironment = &postgresEnvironment
	record.PostgresPlan = explain.Plan
	record.PostgresPlanJSON = explain.PlanJSON
	record.PostgresMetrics = &explain.Metrics
	record.Optimization = &explain.Optimization
	if explain.Optimization.LoweringPlan != nil {
		var fallbackReasons []string
		for _, decision := range explain.Optimization.LoweringPlan.ShortestPathExecutor {
			if decision.FallbackReason != "" && !slices.Contains(fallbackReasons, decision.FallbackReason) {
				fallbackReasons = append(fallbackReasons, decision.FallbackReason)
			}
		}
		record.FallbackReason = strings.Join(fallbackReasons, ",")
	}
	if s.references && testCase.WriteScenario == nil {
		waterfall, err := measureCompileWaterfall(ctx, testCase.Cypher, params, s.pgDriver.KindMapper(), s.graphID, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("client compile waterfall: %v", err)
			return record
		}
		record.ClientWaterfall = &waterfall
		rawWaterfall, err := measureRawPGXWaterfall(ctx, s.pool, explain.SQL, explain.Parameters, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("raw pgx waterfall: %v", err)
			return record
		}
		if len(rawWaterfall.Samples) > 0 && rawWaterfall.Samples[0].Rows != record.RowCount {
			record.Status = StatusError
			record.Error = fmt.Sprintf("raw pgx row count %d differs from CySQL row count %d", rawWaterfall.Samples[0].Rows, record.RowCount)
			return record
		}
		record.RawPGXWaterfall = &rawWaterfall
		roundTrip, err := measureRawPGXWaterfall(ctx, s.pool, "select 1", nil, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("raw pgx round trip: %v", err)
			return record
		}
		record.RawPGXRoundTrip = &roundTrip
		references, err := s.measureReferences(ctx, testCase, params, idMap, record.ObservedRows, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("PostgreSQL references: %v", err)
			return record
		}
		record.PostgresReferences = references
	}
	if testCase.WriteScenario == nil && len(s.concurrency) > 0 {
		blocks, err := measurePostgresConcurrency(ctx, s.pool, explain.SQL, explain.Parameters, s.poolSize, s.concurrency, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("concurrency smoke: %v", err)
			return record
		}
		record.Concurrency = blocks
	}
	return record
}

type postgresExplain struct {
	SQL          string
	Plan         []string
	PlanJSON     json.RawMessage
	Metrics      PostgresPlanMetrics
	Optimization translate.OptimizationSummary
	Parameters   map[string]any
}

func (s *postgresSQLRunner) explain(ctx context.Context, cypherQuery string, params map[string]any, write bool) (postgresExplain, error) {
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

	var (
		plan     []string
		planJSON json.RawMessage
	)
	runExplain := func(tx graph.Transaction) error {
		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) "+sqlQuery, translation.Parameters)
		defer result.Close()

		for result.Next() {
			values := result.Values()
			if len(values) == 0 {
				continue
			}

			plan = append(plan, fmt.Sprint(values[0]))
		}

		if err := result.Error(); err != nil {
			return err
		}
		if !write {
			jsonResult := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) "+sqlQuery, translation.Parameters)
			defer jsonResult.Close()
			if jsonResult.Next() && len(jsonResult.Values()) > 0 {
				switch value := jsonResult.Values()[0].(type) {
				case []byte:
					planJSON = append(json.RawMessage(nil), value...)
				case string:
					planJSON = append(json.RawMessage(nil), value...)
				default:
					encoded, err := json.Marshal(value)
					if err != nil {
						return err
					}
					planJSON = encoded
				}
			}
			if err := jsonResult.Error(); err != nil {
				return err
			}
		}
		if write {
			return errScaleWriteRollback
		}
		return nil
	}

	var explainErr error
	if write {
		explainErr = s.db.WriteTransaction(ctx, runExplain)
		if errors.Is(explainErr, errScaleWriteRollback) {
			explainErr = nil
		}
	} else {
		explainErr = s.db.ReadTransaction(ctx, runExplain)
	}
	if explainErr != nil {
		return postgresExplain{}, explainErr
	}

	return postgresExplain{
		SQL:          sqlQuery,
		Plan:         plan,
		PlanJSON:     planJSON,
		Metrics:      parsePostgresPlanMetrics(plan),
		Optimization: translation.Optimization,
		Parameters:   translation.Parameters,
	}, nil
}

var (
	postgresPlanningPattern  = regexp.MustCompile(`Planning Time: ([0-9.]+) ms`)
	postgresExecutionPattern = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)
	postgresBufferPattern    = regexp.MustCompile(`(?:(shared|local|temp) )?(hit|read|dirtied|written)=([0-9]+)`)
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
		case "shared_written":
			buffers.SharedWritten = value
		case "local_hit":
			buffers.LocalHit = value
		case "local_read":
			buffers.LocalRead = value
		case "local_dirtied":
			buffers.LocalDirtied = value
		case "local_written":
			buffers.LocalWritten = value
		case "temp_read":
			buffers.TempRead = value
		case "temp_written":
			buffers.TempWritten = value
		}
	}

	return buffers
}
