//go:build manual_integration

package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
)

const postgresIngestBenchmarkHeapSampleInterval = 10 * time.Millisecond

var postgresIngestBenchmarkGraphSequence atomic.Uint64

type postgresIngestBenchmarkLocality struct {
	coveringIndex bool
	cluster       bool
}

type postgresIngestBenchmarkDatabase struct {
	ctx         context.Context
	driver      *Driver
	pool        *pgxpool.Pool
	beginTx     func(context.Context) (pgx.Tx, error)
	cleanupExec func(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type postgresIngestBenchmarkGraph struct {
	target graph.Graph
	model  model.Graph
}

type postgresIngestBenchmarkPlan struct {
	label string
	json  string
}

type postgresIngestBenchmarkIteration struct {
	elapsed        time.Duration
	entities       int64
	peakHeapBytes  uint64
	tableBytes     int64
	indexBytes     int64
	walBytes       int64
	walAvailable   bool
	walUnavailable error
	stats          IngestStats
	plans          []postgresIngestBenchmarkPlan
}

type postgresIngestBenchmarkTotals struct {
	samples            int64
	elapsed            time.Duration
	entities           int64
	peakHeapBytes      uint64
	tableBytes         int64
	indexBytes         int64
	walBytes           int64
	walSamples         int64
	identityRows       int64
	hashMatches        int64
	stagedMutations    int64
	committedMutations int64
	spoolBytes         int64
	clusterDuration    time.Duration
}

type postgresIngestBenchmarkHeapSampler struct {
	stop chan struct{}
	done chan struct{}
	peak atomic.Uint64
}

func BenchmarkPostgresHashFilteredIngest(b *testing.B) {
	testDB := newPostgresIngestBenchmarkDatabase(b)
	config, err := loadPostgresIngestBenchmarkConfig(os.LookupEnv)
	if err != nil {
		b.Fatalf("load PostgreSQL ingest benchmark configuration: %v", err)
	}
	dataset, err := newPostgresIngestBenchmarkDataset(
		config.NodeCount,
		config.EdgeCount,
		config.ChangePercent,
	)
	if err != nil {
		b.Fatalf("build PostgreSQL ingest benchmark dataset: %v", err)
	}
	if err := assertPostgresIngestBenchmarkKinds(b.Context(), testDB.driver.SchemaManager, dataset); err != nil {
		b.Fatal(err)
	}

	b.Logf(
		"PostgreSQL ingest benchmark configuration: nodes=%d edges=%d change_percent=%d buckets=%v cluster=%v batch_size=%d",
		config.NodeCount,
		config.EdgeCount,
		config.ChangePercent,
		config.BucketCounts,
		config.ClusterModes,
		postgresIngestBenchmarkBatchSize,
	)

	for _, scenario := range postgresIngestBenchmarkScenarios {
		b.Run(string(scenario), func(b *testing.B) {
			b.Run(string(postgresIngestBenchmarkBatchPath)+"/natural", func(b *testing.B) {
				runPostgresIngestBenchmarkCase(
					b,
					testDB,
					dataset,
					scenario,
					postgresIngestBenchmarkBatchPath,
					config.BucketCounts[0],
					postgresIngestBenchmarkLocality{},
				)
			})

			for _, bucketCount := range config.BucketCounts {
				for _, coveringIndex := range []bool{false, true} {
					for _, cluster := range config.ClusterModes {
						locality := postgresIngestBenchmarkLocality{
							coveringIndex: coveringIndex,
							cluster:       cluster,
						}
						name := fmt.Sprintf(
							"%s/buckets_%d/%s",
							postgresIngestBenchmarkIngestPath,
							bucketCount,
							locality.name(),
						)
						b.Run(name, func(b *testing.B) {
							runPostgresIngestBenchmarkCase(
								b,
								testDB,
								dataset,
								scenario,
								postgresIngestBenchmarkIngestPath,
								bucketCount,
								locality,
							)
						})
					}
				}
			}
		})
	}
}

func newPostgresIngestBenchmarkDatabase(b *testing.B) *postgresIngestBenchmarkDatabase {
	b.Helper()

	connectionString := os.Getenv("CONNECTION_STRING")
	if connectionString == "" {
		b.Skip("CONNECTION_STRING env var is not set")
	}
	parsed, err := url.Parse(connectionString)
	if err != nil {
		b.Fatalf("parse CONNECTION_STRING for PostgreSQL ingest benchmark: %v", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		b.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	ctx, cancel := context.WithCancel(context.Background())
	poolConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		cancel()
		b.Fatalf("parse PostgreSQL pool configuration for ingest benchmark: %v", err)
	}
	pool, err := NewPool(poolConfig)
	if err != nil {
		cancel()
		b.Fatalf("create PostgreSQL pool for ingest benchmark: %v", err)
	}
	b.Cleanup(func() {
		cancel()
		pool.Close()
	})
	if err := pool.Ping(ctx); err != nil {
		b.Fatalf("ping PostgreSQL for ingest benchmark: %v", err)
	}

	driver := NewDriver(0, pool)
	if err := driver.AssertSchema(ctx, graph.Schema{}); err != nil {
		b.Fatalf("assert PostgreSQL schema for ingest benchmark: %v", err)
	}

	return &postgresIngestBenchmarkDatabase{
		ctx:         ctx,
		driver:      driver,
		pool:        pool,
		beginTx:     pool.Begin,
		cleanupExec: pool.Exec,
	}
}

func runPostgresIngestBenchmarkCase(
	b *testing.B,
	testDB *postgresIngestBenchmarkDatabase,
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
	path postgresIngestBenchmarkPath,
	bucketCount int,
	locality postgresIngestBenchmarkLocality,
) {
	b.Helper()
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()

	var (
		totals    postgresIngestBenchmarkTotals
		explained bool
		walWarned bool
	)
	for range b.N {
		iteration, err := testDB.runIteration(
			b,
			dataset,
			scenario,
			path,
			bucketCount,
			locality,
			!explained && path == postgresIngestBenchmarkIngestPath,
		)
		if err != nil {
			b.Fatalf(
				"run PostgreSQL ingest benchmark %s/%s/%s: %v",
				scenario,
				path,
				locality.name(),
				err,
			)
		}
		if iteration.walUnavailable != nil && !walWarned {
			b.Logf("WAL metric unavailable (benchmark continues): %v", iteration.walUnavailable)
			walWarned = true
		}
		for _, plan := range iteration.plans {
			b.Logf("%s EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON): %s", plan.label, plan.json)
		}
		if len(iteration.plans) > 0 {
			explained = true
		}
		totals.add(iteration)
	}

	if err := totals.report(b, path == postgresIngestBenchmarkIngestPath); err != nil {
		b.Fatalf("report PostgreSQL ingest benchmark metrics: %v", err)
	}
}

func (s *postgresIngestBenchmarkDatabase) runIteration(
	b *testing.B,
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
	path postgresIngestBenchmarkPath,
	bucketCount int,
	locality postgresIngestBenchmarkLocality,
	explain bool,
) (result postgresIngestBenchmarkIteration, resultErr error) {
	b.Helper()

	testGraph, err := s.newGraph()
	if err != nil {
		return result, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, s.cleanupGraph(testGraph))
	}()

	if locality.coveringIndex {
		if path != postgresIngestBenchmarkIngestPath {
			return result, fmt.Errorf("covering-index locality is only valid for Driver.Ingest")
		}
		if err := s.createCoveringIndexes(testGraph); err != nil {
			return result, err
		}
	}

	if scenario.requiresSeed() {
		seedStats, err := runPostgresIngestBenchmarkSeed(
			path,
			func(seedScenario postgresIngestBenchmarkScenario) error {
				return s.runBatchOperation(testGraph, dataset, seedScenario)
			},
			func(seedScenario postgresIngestBenchmarkScenario) (IngestStats, error) {
				return s.driver.Ingest(
					s.ctx,
					testGraph.target,
					dataset.input(seedScenario),
					IngestOptions{
						BucketCount:        bucketCount,
						ClusterAfterIngest: locality.cluster,
					},
				)
			},
		)
		if err != nil {
			return result, fmt.Errorf("seed PostgreSQL ingest benchmark graph: %w", err)
		}
		if path == postgresIngestBenchmarkIngestPath {
			if err := validatePostgresIngestBenchmarkStats(
				dataset,
				ingestBenchmarkFreshInsert,
				bucketCount,
				locality.cluster,
				seedStats,
			); err != nil {
				return result, fmt.Errorf("validate PostgreSQL ingest benchmark seed statistics: %w", err)
			}
		}
		if err := s.validateLogicalState(testGraph, dataset, ingestBenchmarkFreshInsert); err != nil {
			return result, fmt.Errorf("validate PostgreSQL ingest benchmark seed logical state: %w", err)
		}
		if err := s.vacuumAnalyzeTargetPartitions(testGraph); err != nil {
			return result, fmt.Errorf("maintain PostgreSQL ingest benchmark seed: %w", err)
		}
	}

	runtime.GC()
	heapSampler := startPostgresIngestBenchmarkHeapSampler()
	startLSN, walStartErr := s.currentWALLSN()

	b.StartTimer()
	started := time.Now()
	var stats IngestStats
	switch path {
	case postgresIngestBenchmarkBatchPath:
		err = s.runBatchOperation(testGraph, dataset, scenario)
	case postgresIngestBenchmarkIngestPath:
		stats, err = s.driver.Ingest(
			s.ctx,
			testGraph.target,
			dataset.input(scenario),
			IngestOptions{
				BucketCount:        bucketCount,
				ClusterAfterIngest: locality.cluster,
			},
		)
	default:
		err = fmt.Errorf("unknown PostgreSQL ingest benchmark path %q", path)
	}
	result.elapsed = time.Since(started)
	b.StopTimer()
	result.peakHeapBytes = heapSampler.stopAndRead()
	endLSN, walEndErr := s.currentWALLSN()
	if walStartErr != nil || walEndErr != nil {
		result.walUnavailable = errors.Join(walStartErr, walEndErr)
	} else if walBytes, walDiffErr := s.walDifference(startLSN, endLSN); walDiffErr != nil {
		result.walUnavailable = walDiffErr
	} else {
		result.walBytes = walBytes
		result.walAvailable = true
	}
	if err != nil {
		return result, err
	}
	if result.elapsed <= 0 {
		return result, fmt.Errorf("timed operation reported non-positive duration %s", result.elapsed)
	}

	result.stats = stats
	nodeInputs, edgeInputs := dataset.inputCounts(scenario)
	result.entities = int64(nodeInputs) + int64(edgeInputs)
	if path == postgresIngestBenchmarkIngestPath {
		if err := validatePostgresIngestBenchmarkStats(
			dataset,
			scenario,
			bucketCount,
			locality.cluster,
			stats,
		); err != nil {
			return result, err
		}
	}
	if err := s.validateLogicalState(testGraph, dataset, scenario); err != nil {
		return result, err
	}

	result.tableBytes, result.indexBytes, err = s.relationSizes(testGraph)
	if err != nil {
		return result, err
	}
	if result.tableBytes <= 0 || result.indexBytes <= 0 {
		return result, fmt.Errorf(
			"PostgreSQL ingest benchmark relation metrics are non-positive: table=%d index=%d",
			result.tableBytes,
			result.indexBytes,
		)
	}

	if explain {
		if err := s.vacuumAnalyzeTargetPartitions(testGraph); err != nil {
			return result, fmt.Errorf("maintain PostgreSQL ingest benchmark partitions before EXPLAIN: %w", err)
		}
		result.plans, err = s.explainHashReads(testGraph, dataset, scenario, bucketCount)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (s *postgresIngestBenchmarkDatabase) vacuumAnalyzeTargetPartitions(
	testGraph *postgresIngestBenchmarkGraph,
) error {
	return vacuumAnalyzePostgresIngestBenchmarkPartitions(
		s.ctx,
		func(ctx context.Context, statement string) error {
			_, err := s.pool.Exec(ctx, statement)
			return err
		},
		testGraph.model.Partitions.Node.Name,
		testGraph.model.Partitions.Edge.Name,
	)
}

func (s *postgresIngestBenchmarkDatabase) runBatchOperation(
	testGraph *postgresIngestBenchmarkGraph,
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
) error {
	input := dataset.input(scenario)

	return s.driver.BatchOperation(s.ctx, func(batch graph.Batch) error {
		targeted := batch.WithGraph(testGraph.target)
		for node, iteratorErr := range input.Nodes {
			if iteratorErr != nil {
				return iteratorErr
			}
			if err := targeted.UpdateNodeBy(graph.NodeUpdate{
				Node:               graph.PrepareNode(node.Properties, node.Kinds...),
				IdentityKind:       dataset.benchmarkNode,
				IdentityProperties: []string{"objectid"},
			}); err != nil {
				return err
			}
		}
		for edge, iteratorErr := range input.Edges {
			if iteratorErr != nil {
				return iteratorErr
			}
			start := graph.PrepareNode(
				graph.AsProperties(map[string]any{"objectid": edge.StartObjectID}),
				dataset.benchmarkNode,
			)
			end := graph.PrepareNode(
				graph.AsProperties(map[string]any{"objectid": edge.EndObjectID}),
				dataset.benchmarkNode,
			)
			if err := targeted.UpdateRelationshipBy(graph.RelationshipUpdate{
				Relationship:            graph.PrepareRelationship(edge.Properties, edge.Kind),
				Start:                   start,
				StartIdentityKind:       dataset.benchmarkNode,
				StartIdentityProperties: []string{"objectid"},
				End:                     end,
				EndIdentityKind:         dataset.benchmarkNode,
				EndIdentityProperties:   []string{"objectid"},
			}); err != nil {
				return err
			}
		}

		return nil
	}, graph.WithBatchSize(postgresIngestBenchmarkBatchSize))
}

func (s *postgresIngestBenchmarkDatabase) newGraph() (*postgresIngestBenchmarkGraph, error) {
	name := fmt.Sprintf(
		"dawgs_ingest_bench_%d_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
		postgresIngestBenchmarkGraphSequence.Add(1),
	)
	target := graph.Graph{
		Name: name,
		NodeConstraints: []graph.Constraint{{
			Field: "objectid",
			Type:  graph.BTreeIndex,
		}},
	}

	var graphModel model.Graph
	err := s.driver.SchemaManager.WriteTransaction(s.ctx, func(tx graph.Transaction) error {
		var err error
		graphModel, err = s.driver.SchemaManager.AssertGraph(tx, target)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL ingest benchmark graph: %w", err)
	}

	return &postgresIngestBenchmarkGraph{
		target: target,
		model:  graphModel,
	}, nil
}

func (s *postgresIngestBenchmarkDatabase) cleanupGraph(
	testGraph *postgresIngestBenchmarkGraph,
) error {
	if testGraph == nil {
		return nil
	}
	defer evictPostgresIngestBenchmarkGraph(s.driver.SchemaManager, testGraph.target.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var cleanupErr error
	for _, tableName := range []string{
		testGraph.model.Partitions.Edge.Name,
		testGraph.model.Partitions.Node.Name,
	} {
		statement := "drop table if exists " + pgx.Identifier{tableName}.Sanitize() + ";"
		if _, err := s.cleanupExec(ctx, statement); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop benchmark partition %q: %w", tableName, err))
		}
	}
	if _, err := s.cleanupExec(
		ctx,
		"delete from graph where id = $1 and name = $2",
		testGraph.model.ID,
		testGraph.target.Name,
	); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete benchmark graph row: %w", err))
	}

	return cleanupErr
}

func (s *postgresIngestBenchmarkDatabase) createCoveringIndexes(
	testGraph *postgresIngestBenchmarkGraph,
) (resultErr error) {
	tx, err := s.beginTx(s.ctx)
	if err != nil {
		return fmt.Errorf("begin benchmark covering-index creation: %w", err)
	}
	defer func() {
		rollbackCtx, cancelRollback := newIngestRollbackCleanupContext()
		defer cancelRollback()

		rollbackErr := tx.Rollback(rollbackCtx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			resultErr = errors.Join(resultErr, fmt.Errorf("roll back benchmark covering-index creation: %w", rollbackErr))
		}
	}()

	definitions := []struct {
		tableName string
		indexName string
		keys      string
		include   string
	}{
		{
			tableName: testGraph.model.Partitions.Node.Name,
			indexName: fmt.Sprintf("dawgs_ingest_bench_node_%d_cover_idx", testGraph.model.ID),
			keys:      "id_hash, (properties->>'objectid')",
			include:   "content_hash",
		},
		{
			tableName: testGraph.model.Partitions.Edge.Name,
			indexName: fmt.Sprintf("dawgs_ingest_bench_edge_%d_cover_idx", testGraph.model.ID),
			keys:      "id_hash, start_object_id, kind_id, end_object_id",
			include:   "content_hash",
		},
	}
	for _, definition := range definitions {
		createStatement := fmt.Sprintf(
			"create index %s on %s using btree (%s) include (%s);",
			pgx.Identifier{definition.indexName}.Sanitize(),
			pgx.Identifier{definition.tableName}.Sanitize(),
			definition.keys,
			definition.include,
		)
		if _, err := tx.Exec(s.ctx, createStatement); err != nil {
			return fmt.Errorf("create graph-local benchmark covering index %q: %w", definition.indexName, err)
		}
	}

	if err := tx.Commit(s.ctx); err != nil {
		return fmt.Errorf("commit benchmark covering-index creation: %w", err)
	}

	return nil
}

func (s *postgresIngestBenchmarkDatabase) validateLogicalState(
	testGraph *postgresIngestBenchmarkGraph,
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
) error {
	validation, err := validatePostgresIngestBenchmarkLogicalRecords(
		dataset.expectedLogicalRecords(scenario),
		s.logicalRecords(testGraph),
	)
	if err != nil {
		return fmt.Errorf("validate exact PostgreSQL ingest benchmark logical state: %w", err)
	}
	if validation.Nodes != int64(dataset.nodeCount) || validation.Edges != int64(dataset.edgeCount) {
		return fmt.Errorf(
			"PostgreSQL ingest benchmark logical state counts mismatch: nodes=%d want=%d edges=%d want=%d",
			validation.Nodes,
			dataset.nodeCount,
			validation.Edges,
			dataset.edgeCount,
		)
	}

	return nil
}

func (s *postgresIngestBenchmarkDatabase) logicalRecords(
	testGraph *postgresIngestBenchmarkGraph,
) iter.Seq2[postgresIngestBenchmarkLogicalRecord, error] {
	return func(yield func(postgresIngestBenchmarkLogicalRecord, error) bool) {
		if !s.yieldLogicalNodes(testGraph, yield) {
			return
		}
		s.yieldLogicalEdges(testGraph, yield)
	}
}

func (s *postgresIngestBenchmarkDatabase) yieldLogicalNodes(
	testGraph *postgresIngestBenchmarkGraph,
	yield func(postgresIngestBenchmarkLogicalRecord, error) bool,
) bool {
	rows, err := s.pool.Query(s.ctx, formatPostgresIngestBenchmarkLogicalNodes(testGraph.model))
	if err != nil {
		yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("query benchmark logical nodes: %w", err))
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var (
			objectID      string
			kinds         []string
			propertiesRaw string
		)
		if err := rows.Scan(&objectID, &kinds, &propertiesRaw); err != nil {
			yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("scan benchmark logical node: %w", err))
			return false
		}
		properties, err := decodePostgresIngestBenchmarkLogicalProperties(propertiesRaw)
		if err != nil {
			yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("decode benchmark logical node properties: %w", err))
			return false
		}
		if !yield(postgresIngestBenchmarkLogicalRecord{
			recordType: postgresIngestBenchmarkLogicalNode,
			objectID:   objectID,
			kinds:      kinds,
			properties: properties,
		}, nil) {
			return false
		}
	}
	if err := rows.Err(); err != nil {
		yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("iterate benchmark logical nodes: %w", err))
		return false
	}

	return true
}

func (s *postgresIngestBenchmarkDatabase) yieldLogicalEdges(
	testGraph *postgresIngestBenchmarkGraph,
	yield func(postgresIngestBenchmarkLogicalRecord, error) bool,
) bool {
	rows, err := s.pool.Query(s.ctx, formatPostgresIngestBenchmarkLogicalEdges(testGraph.model))
	if err != nil {
		yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("query benchmark logical edges: %w", err))
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var (
			startObjectID string
			edgeKind      string
			endObjectID   string
			propertiesRaw string
		)
		if err := rows.Scan(&startObjectID, &edgeKind, &endObjectID, &propertiesRaw); err != nil {
			yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("scan benchmark logical edge: %w", err))
			return false
		}
		properties, err := decodePostgresIngestBenchmarkLogicalProperties(propertiesRaw)
		if err != nil {
			yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("decode benchmark logical edge properties: %w", err))
			return false
		}
		if !yield(postgresIngestBenchmarkLogicalRecord{
			recordType:    postgresIngestBenchmarkLogicalEdge,
			startObjectID: startObjectID,
			edgeKind:      edgeKind,
			endObjectID:   endObjectID,
			properties:    properties,
		}, nil) {
			return false
		}
	}
	if err := rows.Err(); err != nil {
		yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf("iterate benchmark logical edges: %w", err))
		return false
	}

	return true
}

func decodePostgresIngestBenchmarkLogicalProperties(raw string) (*graph.Properties, error) {
	encoded := []byte(raw)
	if err := validateEncodedIngestJSONSurrogates(encoded); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()

	var properties map[string]any
	if err := decoder.Decode(&properties); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("logical properties contain trailing JSON")
	}
	if properties == nil {
		return nil, fmt.Errorf("logical properties must be a JSON object")
	}

	return graph.AsProperties(properties), nil
}

func (s *postgresIngestBenchmarkDatabase) relationSizes(
	testGraph *postgresIngestBenchmarkGraph,
) (int64, int64, error) {
	var tableBytes, indexBytes int64
	err := s.pool.QueryRow(s.ctx, `
		select (pg_table_size($1::regclass) + pg_table_size($2::regclass))::bigint,
		       (pg_indexes_size($1::regclass) + pg_indexes_size($2::regclass))::bigint
	`,
		pgx.Identifier{testGraph.model.Partitions.Node.Name}.Sanitize(),
		pgx.Identifier{testGraph.model.Partitions.Edge.Name}.Sanitize(),
	).Scan(&tableBytes, &indexBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("read PostgreSQL ingest benchmark relation sizes: %w", err)
	}

	return tableBytes, indexBytes, nil
}

func (s *postgresIngestBenchmarkDatabase) currentWALLSN() (string, error) {
	var lsn string
	if err := s.pool.QueryRow(s.ctx, "select pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return "", fmt.Errorf("read pg_current_wal_lsn: %w", err)
	}
	if lsn == "" {
		return "", fmt.Errorf("pg_current_wal_lsn returned an empty value")
	}

	return lsn, nil
}

func (s *postgresIngestBenchmarkDatabase) walDifference(startLSN string, endLSN string) (int64, error) {
	var bytes int64
	if err := s.pool.QueryRow(
		s.ctx,
		"select pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint",
		endLSN,
		startLSN,
	).Scan(&bytes); err != nil {
		return 0, fmt.Errorf("read pg_wal_lsn_diff: %w", err)
	}
	if bytes < 0 {
		return 0, fmt.Errorf("pg_wal_lsn_diff returned negative bytes %d", bytes)
	}

	return bytes, nil
}

func (s *postgresIngestBenchmarkDatabase) explainHashReads(
	testGraph *postgresIngestBenchmarkGraph,
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
	bucketCount int,
) ([]postgresIngestBenchmarkPlan, error) {
	buckets, err := newIngestBucketSet(uint64(bucketCount))
	if err != nil {
		return nil, err
	}

	firstNode, err := firstPostgresIngestBenchmarkNode(dataset, scenario)
	if err != nil {
		return nil, err
	}
	firstEdge, err := firstPostgresIngestBenchmarkEdge(dataset, scenario)
	if err != nil {
		return nil, err
	}

	nodeRange := buckets.Range(buckets.Bucket(hashIngestNodeIdentity(firstNode.ObjectID)))
	edgeRange := buckets.Range(buckets.Bucket(hashIngestEdgeIdentity(
		firstEdge.StartObjectID,
		firstEdge.Kind.String(),
		firstEdge.EndObjectID,
	)))

	plans := make([]postgresIngestBenchmarkPlan, 0, 2)
	for _, explanation := range []struct {
		label       string
		statement   string
		bucketRange ingestBucketRange
	}{
		{
			label:       "node narrow identity read",
			statement:   pgquery.FormatSelectIngestNodeHashes(testGraph.model, nodeRange.Upper == nil),
			bucketRange: nodeRange,
		},
		{
			label:       "edge narrow identity read",
			statement:   pgquery.FormatSelectIngestEdgeHashes(testGraph.model, edgeRange.Upper == nil),
			bucketRange: edgeRange,
		},
	} {
		arguments := []any{explanation.bucketRange.Lower}
		if explanation.bucketRange.Upper != nil {
			arguments = append(arguments, *explanation.bucketRange.Upper)
		}
		statement := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " +
			strings.TrimSuffix(explanation.statement, ";")
		var plan []byte
		if err := s.pool.QueryRow(s.ctx, statement, arguments...).Scan(&plan); err != nil {
			return nil, fmt.Errorf("explain benchmark %s: %w", explanation.label, err)
		}
		plans = append(plans, postgresIngestBenchmarkPlan{
			label: explanation.label,
			json:  string(plan),
		})
	}

	return plans, nil
}

func firstPostgresIngestBenchmarkNode(
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
) (*IngestNode, error) {
	for node, err := range dataset.nodeSequence(scenario) {
		return node, err
	}

	return nil, fmt.Errorf("PostgreSQL ingest benchmark scenario %q has no node input", scenario)
}

func firstPostgresIngestBenchmarkEdge(
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
) (*IngestEdge, error) {
	for edge, err := range dataset.edgeSequence(scenario) {
		return edge, err
	}

	return nil, fmt.Errorf("PostgreSQL ingest benchmark scenario %q has no edge input", scenario)
}

func validatePostgresIngestBenchmarkStats(
	dataset postgresIngestBenchmarkDataset,
	scenario postgresIngestBenchmarkScenario,
	bucketCount int,
	cluster bool,
	stats IngestStats,
) error {
	nodeBuckets, edgeBuckets, err := dataset.expectedBucketMetrics(scenario, bucketCount)
	if err != nil {
		return fmt.Errorf("derive PostgreSQL ingest benchmark bucket metrics: %w", err)
	}
	nodeInputs, edgeInputs := dataset.inputCounts(scenario)
	for _, phase := range []struct {
		label         string
		actual        IngestPhaseStats
		input         int
		changed       int
		bucketMetrics postgresIngestBenchmarkBucketMetrics
	}{
		{
			label:         "node",
			actual:        stats.Nodes,
			input:         nodeInputs,
			changed:       dataset.changedNodes,
			bucketMetrics: nodeBuckets,
		},
		{
			label:         "edge",
			actual:        stats.Edges,
			input:         edgeInputs,
			changed:       dataset.changedEdges,
			bucketMetrics: edgeBuckets,
		},
	} {
		if err := validatePostgresIngestBenchmarkPhaseStats(scenario, phase); err != nil {
			return err
		}
	}

	if stats.TotalDuration <= 0 {
		return fmt.Errorf("PostgreSQL ingest benchmark total duration is non-positive: %s", stats.TotalDuration)
	}
	if cluster && stats.ClusterDuration <= 0 {
		return fmt.Errorf("PostgreSQL ingest benchmark cluster duration is non-positive: %s", stats.ClusterDuration)
	}
	if !cluster && stats.ClusterDuration != 0 {
		return fmt.Errorf("PostgreSQL ingest benchmark cluster duration is %s with clustering disabled", stats.ClusterDuration)
	}

	return nil
}

func validatePostgresIngestBenchmarkPhaseStats(
	scenario postgresIngestBenchmarkScenario,
	phase struct {
		label         string
		actual        IngestPhaseStats
		input         int
		changed       int
		bucketMetrics postgresIngestBenchmarkBucketMetrics
	},
) error {
	actual := phase.actual
	if actual.InputRecords != int64(phase.input) || actual.CoalescedRecords != int64(phase.input) {
		return fmt.Errorf(
			"PostgreSQL ingest benchmark %s input metrics mismatch: input=%d coalesced=%d want=%d",
			phase.label,
			actual.InputRecords,
			actual.CoalescedRecords,
			phase.input,
		)
	}
	if actual.PopulatedBuckets != phase.bucketMetrics.PopulatedBuckets {
		return fmt.Errorf(
			"PostgreSQL ingest benchmark %s populated buckets=%d want=%d",
			phase.label,
			actual.PopulatedBuckets,
			phase.bucketMetrics.PopulatedBuckets,
		)
	}
	if actual.SpoolBytes <= 0 || actual.Duration <= 0 {
		return fmt.Errorf(
			"PostgreSQL ingest benchmark %s phase metrics are non-positive: spool=%d duration=%s",
			phase.label,
			actual.SpoolBytes,
			actual.Duration,
		)
	}

	var (
		identityRows = phase.bucketMetrics.IdentityRowsRead
		hashMatches  int64
		inserts      int64
		updates      int64
		committed    int64
	)
	switch scenario {
	case ingestBenchmarkFreshInsert:
		inserts = int64(phase.input)
		committed = int64(phase.input)
	case ingestBenchmarkDenseFullReplay:
		hashMatches = int64(phase.input)
	case ingestBenchmarkDenseChange:
		hashMatches = int64(phase.input - phase.changed)
		updates = int64(phase.changed)
		committed = int64(phase.changed)
	case ingestBenchmarkPartialMergeNoop:
		updates = int64(phase.input)
		committed = int64(phase.input)
	case ingestBenchmarkSparseChange:
		updates = int64(phase.input)
		committed = int64(phase.input)
	default:
		return fmt.Errorf("unknown PostgreSQL ingest benchmark scenario %q", scenario)
	}

	if actual.IdentityRowsRead != identityRows ||
		actual.HashMatches != hashMatches ||
		actual.StagedInserts != inserts ||
		actual.StagedUpdates != updates ||
		actual.CommittedMutations != committed {
		return fmt.Errorf(
			"PostgreSQL ingest benchmark %s metrics mismatch: identity=%d/%d matches=%d/%d inserts=%d/%d updates=%d/%d committed=%d/%d",
			phase.label,
			actual.IdentityRowsRead,
			identityRows,
			actual.HashMatches,
			hashMatches,
			actual.StagedInserts,
			inserts,
			actual.StagedUpdates,
			updates,
			actual.CommittedMutations,
			committed,
		)
	}

	return nil
}

func TestValidatePostgresIngestBenchmarkStatsRequiresExactBucketMetrics(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	if err != nil {
		t.Fatal(err)
	}
	stats := IngestStats{
		Nodes: IngestPhaseStats{
			InputRecords:       2,
			CoalescedRecords:   2,
			PopulatedBuckets:   2,
			IdentityRowsRead:   5,
			StagedUpdates:      2,
			CommittedMutations: 2,
			SpoolBytes:         1,
			Duration:           time.Nanosecond,
		},
		Edges: IngestPhaseStats{
			InputRecords:       3,
			CoalescedRecords:   3,
			PopulatedBuckets:   2,
			IdentityRowsRead:   7,
			StagedUpdates:      3,
			CommittedMutations: 3,
			SpoolBytes:         1,
			Duration:           time.Nanosecond,
		},
		TotalDuration: 2 * time.Nanosecond,
	}

	if err := validatePostgresIngestBenchmarkStats(
		dataset,
		ingestBenchmarkSparseChange,
		4,
		false,
		stats,
	); err != nil {
		t.Fatalf("exact benchmark metrics failed validation: %v", err)
	}

	tests := []struct {
		name          string
		mutate        func(*IngestStats)
		errorContains string
	}{
		{
			name: "node populated buckets",
			mutate: func(stats *IngestStats) {
				stats.Nodes.PopulatedBuckets = 1
			},
			errorContains: "node populated buckets",
		},
		{
			name: "edge populated buckets",
			mutate: func(stats *IngestStats) {
				stats.Edges.PopulatedBuckets = 1
			},
			errorContains: "edge populated buckets",
		},
		{
			name: "node whole-table identity read",
			mutate: func(stats *IngestStats) {
				stats.Nodes.IdentityRowsRead = int64(dataset.nodeCount)
			},
			errorContains: "node metrics mismatch",
		},
		{
			name: "edge whole-table identity read",
			mutate: func(stats *IngestStats) {
				stats.Edges.IdentityRowsRead = int64(dataset.edgeCount)
			},
			errorContains: "edge metrics mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := stats
			test.mutate(&invalid)

			err := validatePostgresIngestBenchmarkStats(
				dataset,
				ingestBenchmarkSparseChange,
				4,
				false,
				invalid,
			)

			if err == nil || !strings.Contains(err.Error(), test.errorContains) {
				t.Fatalf("expected error containing %q, got %v", test.errorContains, err)
			}
		})
	}
}

func TestPostgresIngestBenchmarkCleanupEvictsOnlyTargetCacheEntryOnDatabaseErrors(t *testing.T) {
	manager := NewSchemaManager(nil, 0)
	targetModel := model.Graph{ID: 41}
	unrelatedModel := model.Graph{ID: 42}
	manager.graphs["target"] = targetModel
	manager.graphs["unrelated"] = unrelatedModel
	cause := errors.New("cleanup unavailable")
	execCalls := 0
	database := &postgresIngestBenchmarkDatabase{
		driver: &Driver{SchemaManager: manager},
		cleanupExec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			execCalls++
			return pgconn.CommandTag{}, cause
		},
	}
	testGraph := &postgresIngestBenchmarkGraph{
		target: graph.Graph{Name: "target"},
		model: model.Graph{
			ID: 41,
			Partitions: model.GraphPartitions{
				Node: model.NewGraphPartition("node_41"),
				Edge: model.NewGraphPartition("edge_41"),
			},
		},
	}

	err := database.cleanupGraph(testGraph)

	if !errors.Is(err, cause) {
		t.Fatalf("expected cleanup error %v, got %v", cause, err)
	}
	if execCalls != 3 {
		t.Fatalf("cleanup executed %d statements, want 3", execCalls)
	}
	if _, found := manager.graphs["target"]; found {
		t.Fatal("target benchmark graph remained cached after cleanup errors")
	}
	if actual, found := manager.graphs["unrelated"]; !found || actual.ID != unrelatedModel.ID {
		t.Fatalf("unrelated graph cache entry changed: found=%t actual=%+v", found, actual)
	}
}

func TestPostgresIngestBenchmarkCoveringIndexCommitFailureUsesIndependentBoundedRollbackContext(t *testing.T) {
	commitErr := errors.New("commit failed")
	rollbackErr := errors.New("rollback failed")
	testGraph := &postgresIngestBenchmarkGraph{
		model: model.Graph{
			ID: 41,
			Partitions: model.GraphPartitions{
				Node: model.NewGraphPartition("node_41"),
				Edge: model.NewGraphPartition("edge_41"),
			},
		},
	}
	tests := []struct {
		name           string
		rollbackErr    error
		wantExactError string
		wantJoined     bool
	}{
		{
			name:           "closed transaction rollback is suppressed",
			rollbackErr:    pgx.ErrTxClosed,
			wantExactError: "commit benchmark covering-index creation: commit failed",
		},
		{
			name:        "genuine rollback failure is joined",
			rollbackErr: rollbackErr,
			wantJoined:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			if err != nil {
				t.Fatalf("create benchmark transaction mock: %v", err)
			}
			observation := ingestRollbackContextObservation{}
			observingDB := observingIngestRollbackDB{
				inner:       pool,
				observation: &observation,
			}
			database := &postgresIngestBenchmarkDatabase{
				ctx:     context.Background(),
				beginTx: observingDB.Begin,
			}

			pool.ExpectBegin()
			pool.ExpectExec(
				`create index "dawgs_ingest_bench_node_41_cover_idx" on "node_41" using btree (id_hash, (properties->>'objectid')) include (content_hash);`,
			).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
			pool.ExpectExec(
				`create index "dawgs_ingest_bench_edge_41_cover_idx" on "edge_41" using btree (id_hash, start_object_id, kind_id, end_object_id) include (content_hash);`,
			).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
			pool.ExpectCommit().WillReturnError(commitErr)
			pool.ExpectRollback().WillReturnError(test.rollbackErr)

			err = database.createCoveringIndexes(testGraph)

			if !errors.Is(err, commitErr) {
				t.Fatalf("covering-index error %v does not contain commit error %v", err, commitErr)
			}
			if test.wantExactError != "" && err.Error() != test.wantExactError {
				t.Fatalf("covering-index error = %q, want %q", err, test.wantExactError)
			}
			if test.wantJoined && !errors.Is(err, rollbackErr) {
				t.Fatalf("covering-index error %v does not contain rollback error %v", err, rollbackErr)
			}
			if errors.Is(test.rollbackErr, pgx.ErrTxClosed) && errors.Is(err, pgx.ErrTxClosed) {
				t.Fatalf("covering-index error unexpectedly contains expected closed-transaction rollback: %v", err)
			}
			requireBoundedIngestRollbackContext(t, observation)
			if err := pool.ExpectationsWereMet(); err != nil {
				t.Fatalf("benchmark transaction expectations: %v", err)
			}
		})
	}
}

func startPostgresIngestBenchmarkHeapSampler() *postgresIngestBenchmarkHeapSampler {
	sampler := &postgresIngestBenchmarkHeapSampler{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	sampler.sample()
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(postgresIngestBenchmarkHeapSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sampler.sample()
			case <-sampler.stop:
				sampler.sample()
				return
			}
		}
	}()

	return sampler
}

func (s *postgresIngestBenchmarkHeapSampler) sample() {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	for current := s.peak.Load(); memory.HeapAlloc > current; current = s.peak.Load() {
		if s.peak.CompareAndSwap(current, memory.HeapAlloc) {
			return
		}
	}
}

func (s *postgresIngestBenchmarkHeapSampler) stopAndRead() uint64 {
	close(s.stop)
	<-s.done
	return s.peak.Load()
}

func (s postgresIngestBenchmarkLocality) name() string {
	switch {
	case s.coveringIndex && s.cluster:
		return "covering_index_cluster_after_ingest"
	case s.coveringIndex:
		return "covering_index"
	case s.cluster:
		return "cluster_after_ingest"
	default:
		return "natural"
	}
}

func (s *postgresIngestBenchmarkTotals) add(iteration postgresIngestBenchmarkIteration) {
	s.samples++
	s.elapsed += iteration.elapsed
	s.entities += iteration.entities
	s.peakHeapBytes += iteration.peakHeapBytes
	s.tableBytes += iteration.tableBytes
	s.indexBytes += iteration.indexBytes
	if iteration.walAvailable {
		s.walBytes += iteration.walBytes
		s.walSamples++
	}
	s.identityRows += iteration.stats.Nodes.IdentityRowsRead + iteration.stats.Edges.IdentityRowsRead
	s.hashMatches += iteration.stats.Nodes.HashMatches + iteration.stats.Edges.HashMatches
	s.stagedMutations += iteration.stats.Nodes.StagedInserts + iteration.stats.Nodes.StagedUpdates +
		iteration.stats.Edges.StagedInserts + iteration.stats.Edges.StagedUpdates
	s.committedMutations += iteration.stats.Nodes.CommittedMutations + iteration.stats.Edges.CommittedMutations
	s.spoolBytes += iteration.stats.Nodes.SpoolBytes + iteration.stats.Edges.SpoolBytes
	s.clusterDuration += iteration.stats.ClusterDuration
}

func (s postgresIngestBenchmarkTotals) report(b *testing.B, ingestPath bool) error {
	b.Helper()
	if s.samples == 0 || s.elapsed <= 0 || s.entities <= 0 {
		return fmt.Errorf(
			"benchmark totals are vacuous: samples=%d elapsed=%s entities=%d",
			s.samples,
			s.elapsed,
			s.entities,
		)
	}

	samples := float64(s.samples)
	b.ReportMetric(float64(s.entities)/s.elapsed.Seconds(), "entities/s")
	b.ReportMetric(float64(s.peakHeapBytes)/samples, "peak-heap-B/op")
	b.ReportMetric(float64(s.tableBytes)/samples, "table-B/op")
	b.ReportMetric(float64(s.indexBytes)/samples, "index-B/op")
	if s.walSamples > 0 {
		b.ReportMetric(float64(s.walBytes)/float64(s.walSamples), "wal-B/op")
	}
	if ingestPath {
		b.ReportMetric(float64(s.identityRows)/samples, "identity-rows/op")
		b.ReportMetric(float64(s.hashMatches)/samples, "hash-matches/op")
		b.ReportMetric(float64(s.stagedMutations)/samples, "staged-mutations/op")
		b.ReportMetric(float64(s.committedMutations)/samples, "committed-mutations/op")
		b.ReportMetric(float64(s.spoolBytes)/samples, "spool-B/op")
		b.ReportMetric(float64(s.clusterDuration.Nanoseconds())/samples, "cluster-ns/op")
	}

	return nil
}
