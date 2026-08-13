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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

// postgresSQLRunner owns PostgreSQL translation, connection, graph, and executor settings.
type postgresSQLRunner struct {
	// datasetDir locates fixture and corpus files on disk.
	datasetDir string
	// db provides graph transactions for fixture preparation and query execution.
	db graph.Database
	// pgDriver provides PostgreSQL graph access and kind mapping.
	pgDriver *pg.Driver
	// pool supplies PostgreSQL connections for translated and raw execution.
	pool *pgxpool.Pool
	// graphID selects the PostgreSQL graph partition used for translation, fixture validation, and execution.
	graphID int32
	// backendPID records the physical PostgreSQL session used to label samples and detect connection changes.
	backendPID string
	// poolSize records the maximum PostgreSQL connections available to the runner.
	poolSize int
	// round identifies the measurement round used to balance execution order.
	round int
	// concurrency lists worker counts measured by the PostgreSQL runner.
	concurrency []int
	// environment accumulates PostgreSQL environment evidence for the current runner.
	environment PostgresEnvironment
	// references enables independent PostgreSQL reference execution for the runner.
	references bool
	// referenceArms lists independent PostgreSQL reference arms measured by the runner.
	referenceArms []string
	// toolOptions carries forced translation-executor selections for diagnostic runs.
	toolOptions translate.ToolOptions
	// productionManifest supplies the immutable guarded candidate identity used
	// for pre-closure production-boundary measurement.
	productionManifest *PromotionManifest
	// repeatableRead measures an incumbent or tool arm under an explicit stable
	// snapshot for comparison with an admission-equivalent production candidate.
	repeatableRead bool
	// traversalTelemetry selects opt-in summary or untimed diagnostic traversal evidence.
	traversalTelemetry string
	// existingGraph supplies live-graph anchors, checkpoints, and callbacks to the runner.
	existingGraph *existingGraphRunnerOptions
}

// existingGraphRunnerOptions supplies live-graph anchors and completed-workload state to the PostgreSQL runner.
type existingGraphRunnerOptions struct {
	// Manifest supplies validated live-graph anchors and identity metadata to the runner.
	Manifest ExistingGraphAnchorManifest
	// ProgressPath selects the append-only progress artifact written by the runner.
	ProgressPath string
	// Discovery enables adaptive live-graph discovery instead of the fixed confirmation protocol.
	Discovery bool
	// TimeoutClasses lists the increasing per-attempt deadlines applied during adaptive discovery.
	TimeoutClasses []time.Duration
	// SampleFloor sets the minimum timed samples required for each live-graph attempt.
	SampleFloor int
	// Completed maps completed live-graph case keys to fixture-bound identities.
	Completed map[string]string
	// OnRecord receives each completed live-graph CaseResult for immediate persistence.
	OnRecord func(CaseResult) error
	// OnComplete records final live-graph node and relationship counts after successful execution.
	OnComplete func(int64, int64) error
}

// setProductionManifest loads a provisional promotion manifest. Evidence may
// be empty because this mode exists to produce that evidence; all fields that
// determine SQL selection and runtime behavior are still validated here.
func (s *postgresSQLRunner) setProductionManifest(path string) error {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var manifest PromotionManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode provisional promotion manifest: %w", err)
	}
	if manifest.Version != promotionManifestVersion || manifest.ExecutionBoundary != "guarded_dual_arm" || strings.TrimSpace(manifest.SelectorVersion) == "" {
		return fmt.Errorf("provisional manifest must be version 2 with a selector and guarded_dual_arm boundary")
	}
	expectedFallback := map[string]string{
		string(optimize.ShortestPathExecutorASPI1DAG):                      string(optimize.ShortestPathExecutorASPA1DAG),
		string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness): string(optimize.ShortestPathExecutorS4CanonicalWitness),
		string(optimize.ExpansionSearchPolicyOrientationProbeV1):           string(optimize.ExpansionSearchStepwiseForward),
	}[manifest.Candidate]
	if expectedFallback == "" || manifest.FallbackExecutor != expectedFallback {
		return fmt.Errorf("unsupported candidate/fallback pair %s -> %s", manifest.Candidate, manifest.FallbackExecutor)
	}
	if manifest.Candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV1) {
		expectedCaps := orientationPromotionCaps()
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("orientation-probe-v1 requires exactly four immutable caps")
		}
		for name, expected := range expectedCaps {
			if manifest.Caps[name] != expected {
				return fmt.Errorf("orientation-probe-v1 cap %s must equal %d", name, expected)
			}
		}
	} else {
		expectedCaps := []string{"state_limit", "predecessor_limit", "enumeration_limit", "output_bytes_limit"}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("guarded shortest candidate requires exactly four immutable caps")
		}
		for _, name := range expectedCaps {
			if manifest.Caps[name] <= 0 {
				return fmt.Errorf("guarded shortest candidate cap %s must be positive", name)
			}
		}
	}
	seenQueries := map[string]struct{}{}
	for _, bucket := range manifest.Buckets {
		if len(bucket.QuerySHA256) == 0 {
			return fmt.Errorf("production bucket %q has no exact query cohort", bucket.Name)
		}
		for _, digest := range bucket.QuerySHA256 {
			if !isLowerHexSHA256(digest) {
				return fmt.Errorf("production bucket %q contains an invalid query digest", bucket.Name)
			}
			if _, found := seenQueries[digest]; found {
				return fmt.Errorf("production query digest %s is authorized more than once", digest)
			}
			seenQueries[digest] = struct{}{}
		}
	}
	if len(seenQueries) == 0 {
		return fmt.Errorf("provisional manifest has no exact query cohort")
	}
	s.productionManifest = &manifest
	return nil
}

func (s *postgresSQLRunner) productionOptions(cypherQuery string) (translate.ProductionOptions, error) {
	manifest := s.productionManifest
	if manifest == nil {
		return translate.ProductionOptions{}, fmt.Errorf("production manifest is not configured")
	}
	digest := pg.TraversalPolicyQuerySHA256(cypherQuery)
	for _, bucket := range manifest.Buckets {
		if !slices.Contains(bucket.QuerySHA256, digest) {
			continue
		}
		options := translate.ProductionOptions{
			AuthorizedBucket: &translate.ProductionTraversalBucket{
				Direction: bucket.Direction, ObservationMode: bucket.ObservationMode,
				MinimumDepth: int64(bucket.MinimumDepth), MaximumDepth: int64(bucket.MaximumDepth),
				RelationshipKindCount: bucket.RelationshipKindCount, UntypedRelationship: bucket.UntypedRelationship,
			},
			SelectorVersion: manifest.SelectorVersion,
		}
		if manifest.Candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV1) {
			options.EnableExpansionOrientation = true
		} else {
			options.ShortestPathExecutor = optimize.ShortestPathExecutor(manifest.Candidate)
			options.ShortestPathCaps = &translate.ProductionShortestPathCaps{
				StateLimit: manifest.Caps["state_limit"], PredecessorLimit: manifest.Caps["predecessor_limit"],
				EnumerationLimit: manifest.Caps["enumeration_limit"], OutputBytesLimit: manifest.Caps["output_bytes_limit"],
			}
		}
		return options, nil
	}
	return translate.ProductionOptions{}, fmt.Errorf("query SHA-256 %s is absent from the provisional production manifest", digest)
}

// newPostgresSQLRunner opens a PostgreSQL benchmark runner for managed-fixture execution.
func newPostgresSQLRunner(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus, poolSize, round int, concurrency []int, references bool, referenceArms []string, forceShortest, forceExpansion string) (*postgresSQLRunner, error) {
	return newPostgresSQLRunnerWithExistingGraph(ctx, datasetDir, connection, corpus, poolSize, round, concurrency, references, referenceArms, forceShortest, forceExpansion, nil)
}

// newPostgresSQLRunnerWithExistingGraph opens a PostgreSQL benchmark runner with optional live-graph state.
func newPostgresSQLRunnerWithExistingGraph(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus, poolSize, round int, concurrency []int, references bool, referenceArms []string, forceShortest, forceExpansion string, existing *existingGraphRunnerOptions) (*postgresSQLRunner, error) {
	if existing == nil {
		if err := databaseguard.ValidateEnvironment(connection); err != nil {
			return nil, fmt.Errorf("refuse destructive PostgreSQL GraphBench target: %w", err)
		}
	}

	poolCfg, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
	}
	// GraphBench needs first-call and steady-state samples from an identifiable
	// physical session. A single-connection pool makes that relationship
	// deterministic while retaining the production pool hooks.
	poolCfg.MinConns = int32(poolSize)
	poolCfg.MaxConns = int32(poolSize)
	if compactBidirectionalSnapshotRequired(references, referenceArms, forceShortest) {
		if poolCfg.ConnConfig.RuntimeParams == nil {
			poolCfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolCfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	}
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

	if existing == nil {
		nodeKinds, edgeKinds, err := scanDatasetKinds(datasetDir, scaleCorpusDatasets(corpus))
		if err != nil {
			_ = db.Close(ctx)
			return nil, err
		}

		if err := db.AssertSchema(ctx, benchmarkSchema(nodeKinds, edgeKinds)); err != nil {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("assert PostgreSQL schema: %w", err)
		}
	}

	pgDriver, ok := db.(*pg.Driver)
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("expected *pg.Driver, got %T", db)
	}
	if existing != nil {
		if err := pgDriver.SetDefaultGraph(ctx, graph.Graph{
			Name: existing.Manifest.Graph,
		}); err != nil {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("select existing PostgreSQL graph: %w", err)
		}
		if err := pgDriver.Fetch(ctx); err != nil {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("fetch existing PostgreSQL kinds: %w", err)
		}
	}

	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("PostgreSQL default graph is not set")
	}
	if existing != nil && existing.Manifest.Graph != "" && existing.Manifest.Graph != defaultGraph.Name {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("anchor manifest graph %q does not match PostgreSQL default graph %q", existing.Manifest.Graph, defaultGraph.Name)
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
		datasetDir:    datasetDir,
		db:            db,
		pgDriver:      pgDriver,
		pool:          pool,
		graphID:       defaultGraph.ID,
		backendPID:    strconv.FormatInt(int64(backendPID), 10),
		poolSize:      poolSize,
		round:         round,
		concurrency:   append([]int(nil), concurrency...),
		environment:   postgresEnvironment,
		references:    references,
		referenceArms: append([]string(nil), referenceArms...),
		toolOptions: translate.ToolOptions{
			ForceShortestPathExecutor:    optimize.ShortestPathExecutor(forceShortest),
			ForceExpansionSearchStrategy: optimize.ExpansionSearchStrategy(forceExpansion),
		},
		existingGraph: existing,
	}, nil
}

// compactBidirectionalSnapshotRequired reports whether any selected production
// or reference arm can execute the multi-statement B1/B2 workspace kernel.
func compactBidirectionalSnapshotRequired(references bool, referenceArms []string, forceShortest string) bool {
	switch optimize.ShortestPathExecutor(forceShortest) {
	case optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB1AlternatingNodeWitness,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		optimize.ShortestPathExecutorASPB1AlternatingNodeDAG,
		optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG:
		return true
	}
	if !references {
		return false
	}
	if len(referenceArms) == 0 {
		return true
	}
	for _, arm := range referenceArms {
		switch arm {
		case "sp_b1_strict_alternating_distance",
			"sp_b1_strict_alternating_witness_m0",
			"sp_b2_smaller_frontier_distance",
			"sp_b2_smaller_frontier_witness_m0",
			"asp_b1_bidirectional_dag_strict_m0",
			"asp_b2_bidirectional_dag_smaller_frontier_m0":
			return true
		}
	}
	return false
}

// Close releases the graph database and PostgreSQL pool owned by the runner.
func (s *postgresSQLRunner) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}

	return s.db.Close(ctx)
}

// Run measures supported corpus cases against managed fixtures or the configured preexisting graph.
func (s *postgresSQLRunner) Run(ctx context.Context, warmupIterations, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	if s.existingGraph != nil {
		return s.runExistingGraph(ctx, warmupIterations, iterations, corpus)
	}
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
		if err := s.captureAndValidateFixture(ctx, &fixture); err != nil {
			return nil, fmt.Errorf("validate %s fixture: %w", datasetName, err)
		}
		activePartitions := fmt.Sprintf("vacuum (analyze) node_%d, edge_%d", s.graphID, s.graphID)
		if _, err := s.pool.Exec(ctx, activePartitions); err != nil {
			return nil, fmt.Errorf("vacuum and analyze %s fixture: %w", datasetName, err)
		}
		if err := s.pool.QueryRow(ctx, `select pg_total_relation_size(format('node_%s', $1::int4)::regclass), pg_total_relation_size(format('edge_%s', $1::int4)::regclass), coalesce((select string_agg(relname || ':' || coalesce(last_analyze::text, 'never'), ',' order by relname) from pg_stat_all_tables where relname in (format('node_%s', $1::int4), format('edge_%s', $1::int4))), '')`, s.graphID).Scan(
			&fixture.NodeRelationBytes, &fixture.EdgeRelationBytes, &s.environment.AnalyzeState,
		); err != nil {
			return nil, fmt.Errorf("capture %s fixture relation sizes: %w", datasetName, err)
		}
		s.environment.NodeRelationBytes = fixture.NodeRelationBytes
		s.environment.EdgeRelationBytes = fixture.EdgeRelationBytes

		for _, testCase := range casesByDataset[datasetName] {
			if !testCase.Supports(ModePostgresSQL) {
				continue
			}

			if err := s.resetCaseSession(ctx); err != nil {
				return nil, fmt.Errorf("reset PostgreSQL session for %s: %w", testCase.Name, err)
			}

			record := s.runCase(ctx, warmupIterations, iterations, testCase, idMap)
			attachFixtureMetadata(&record, fixture)
			records = append(records, record)
		}
	}

	return records, nil
}

// runExistingGraph executes eligible live-graph cases, honoring checkpoints and progress callbacks.
func (s *postgresSQLRunner) runExistingGraph(ctx context.Context, warmupIterations, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	options := s.existingGraph
	if err := validateExistingGraphCorpus(corpus, options.Manifest); err != nil {
		return nil, err
	}
	anchors, err := s.resolveExistingGraphAnchors(ctx, options.Manifest)
	if err != nil {
		return nil, err
	}
	idMap := idMapForManifest(anchors)
	preNodes, preEdges, err := s.existingGraphCounts(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.captureExistingGraphEnvironment(ctx); err != nil {
		return nil, err
	}
	databaseDigest := sha256.Sum256([]byte(s.environment.Database))
	s.environment.Database = "sha256:" + hex.EncodeToString(databaseDigest[:])
	fixture := FixtureMetadata{
		Dataset: "existing_graph",
		Checksum: strings.Join([]string{
			options.Manifest.Checksum,
			options.Manifest.ContentIdentity,
			s.environment.SchemaFingerprint,
			s.environment.IndexFingerprint,
		}, ":"),
		PhysicalValidated: true,
		PhysicalNodeCount: preNodes,
		PhysicalEdgeCount: preEdges,
		NodeRelationBytes: s.environment.NodeRelationBytes,
		EdgeRelationBytes: s.environment.EdgeRelationBytes,
		Configuration:     "existing_graph_read_only",
	}
	if err := validateCompletedWorkloads(options.Completed, corpus, fixture); err != nil {
		return nil, err
	}
	var records []CaseResult
	for _, testCase := range corpus.Cases {
		if !testCase.Supports(ModePostgresSQL) {
			continue
		}
		caseKey := existingGraphCaseKey(ModePostgresSQL, testCase)
		if _, completed := options.Completed[caseKey]; completed {
			continue
		}
		if err := appendExistingGraphProgress(options.ProgressPath, ExistingGraphProgress{
			Stage:   "case",
			CaseKey: caseKey,
		}); err != nil {
			return nil, err
		}
		if err := s.resetCaseSession(ctx); err != nil {
			return nil, fmt.Errorf("reset PostgreSQL session for %s: %w", testCase.Name, err)
		}
		record := s.runExistingGraphCase(ctx, warmupIterations, iterations, testCase, idMap)
		attachFixtureMetadata(&record, fixture)
		record.ExistingGraph.PreNodeCount, record.ExistingGraph.PreEdgeCount = preNodes, preEdges
		redactExistingGraphRecord(&record, options.Manifest, anchors)
		records = append(records, record)
		if options.OnRecord != nil {
			if err := options.OnRecord(record); err != nil {
				return nil, err
			}
		}
	}
	postNodes, postEdges, err := s.existingGraphCounts(ctx)
	if err != nil {
		return nil, err
	}
	if preNodes != postNodes || preEdges != postEdges {
		return nil, fmt.Errorf("existing graph cardinality changed: nodes %d -> %d, edges %d -> %d", preNodes, postNodes, preEdges, postEdges)
	}
	for idx := range records {
		records[idx].ExistingGraph.PostNodeCount, records[idx].ExistingGraph.PostEdgeCount = postNodes, postEdges
	}
	if options.OnComplete != nil {
		if err := options.OnComplete(postNodes, postEdges); err != nil {
			return nil, err
		}
	}
	if err := appendExistingGraphProgress(options.ProgressPath, ExistingGraphProgress{
		Stage:  "complete",
		Detail: fmt.Sprintf("nodes=%d edges=%d", postNodes, postEdges),
	}); err != nil {
		return nil, err
	}
	return records, nil
}

// runExistingGraphCase executes the fixed-confirmation or adaptive timeout protocol for one read-only workload against a preexisting graph.
func (s *postgresSQLRunner) runExistingGraphCase(ctx context.Context, warmupIterations, iterations int, testCase ScaleCase, idMap opengraph.IDMap) CaseResult {
	options := s.existingGraph
	timeouts := options.TimeoutClasses
	if len(timeouts) == 0 {
		timeouts = []time.Duration{0}
	}
	live := &ExistingGraphRun{
		ManifestSHA256:  options.Manifest.Checksum,
		ContentIdentity: options.Manifest.ContentIdentity,
		Protocol:        "fixed_confirmation",
		Adaptive:        options.Discovery,
	}
	if options.Discovery {
		live.Protocol = "adaptive_discovery"
	}
	var record CaseResult
	for idx, timeout := range timeouts {
		measured := iterations
		warmups := warmupIterations
		if options.Discovery && idx > 0 {
			measured = max(options.SampleFloor, iterations>>idx)
			warmups = warmupIterations >> idx
		}
		attemptCtx := ctx
		cancel := func() {}
		if timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		record = s.runCase(attemptCtx, warmups, measured, testCase, idMap)
		attemptErr := attemptCtx.Err()
		cancel()
		attempt := ExistingGraphAttempt{
			Timeout:         timeout,
			WarmupSamples:   warmups,
			MeasuredSamples: measured,
			Status:          record.Status,
			Error:           record.Error,
		}
		live.Attempts = append(live.Attempts, attempt)
		if attemptErr == nil || !options.Discovery {
			break
		}
		_ = appendExistingGraphProgress(options.ProgressPath, ExistingGraphProgress{
			Stage:   "timeout",
			CaseKey: existingGraphCaseKey(ModePostgresSQL, testCase),
			Detail:  timeout.String(),
		})
	}
	record.ExistingGraph = live
	return record
}

// resolveLogicalExistingGraphAnchor looks up one logical-key anchor and rejects missing or ambiguous matches.
func (s *postgresSQLRunner) resolveLogicalExistingGraphAnchor(ctx context.Context, name, logicalKey string) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `select id from node where graph_id = $1 and properties ->> 'logical_key' = $2 order by id limit 2`, s.graphID, logicalKey)
	if err != nil {
		return nil, fmt.Errorf("resolve anchor %s: %w", name, err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve anchor %s rows: %w", name, err)
	}

	return ids, nil
}

// resolveExistingGraphAnchors resolves every manifest anchor to exactly one PostgreSQL node identifier.
func (s *postgresSQLRunner) resolveExistingGraphAnchors(ctx context.Context, manifest ExistingGraphAnchorManifest) (map[string]graph.ID, error) {
	anchors := make(map[string]graph.ID, len(manifest.Anchors))
	for name, anchor := range manifest.Anchors {
		var ids []int64
		if anchor.PhysicalID == nil {
			if resolvedIDs, err := s.resolveLogicalExistingGraphAnchor(ctx, name, anchor.LogicalKey); err != nil {
				return nil, err
			} else {
				ids = resolvedIDs
			}
		} else {
			var (
				kindIDs    string
				properties string
				id         int64
			)

			if err := s.pool.QueryRow(ctx, `select id, kind_ids::text, properties::text from node where graph_id = $1 and id = $2`, s.graphID, *anchor.PhysicalID).Scan(&id, &kindIDs, &properties); err != nil {
				return nil, fmt.Errorf("resolve physical anchor %s: %w", name, err)
			}

			digest := sha256.Sum256([]byte(kindIDs + "\n" + properties))
			actual := "sha256:" + hex.EncodeToString(digest[:])
			if actual != anchor.ContentSHA256 {
				return nil, fmt.Errorf("physical anchor %s content identity mismatch", name)
			}

			ids = append(ids, id)
		}

		if len(ids) != 1 {
			return nil, fmt.Errorf("anchor %s resolved to %d nodes; exactly one is required", name, len(ids))
		}

		if anchor.Kind != "" {
			var matches bool
			if err := s.pool.QueryRow(ctx, `select exists(select 1 from node n join kind k on k.id = any(n.kind_ids) where n.graph_id = $1 and n.id = $2 and k.name = $3)`, s.graphID, ids[0], anchor.Kind).Scan(&matches); err != nil {
				return nil, err
			}

			if !matches {
				return nil, fmt.Errorf("anchor %s does not have declared kind %s", name, anchor.Kind)
			}
		}

		anchors[name] = graph.ID(ids[0])
	}

	return anchors, nil
}

// existingGraphCounts returns node and relationship counts for the selected PostgreSQL graph.
func (s *postgresSQLRunner) existingGraphCounts(ctx context.Context) (int64, int64, error) {
	var nodes, edges int64
	if err := s.pool.QueryRow(ctx, `select (select count(*) from node where graph_id = $1), (select count(*) from edge where graph_id = $1)`, s.graphID).Scan(&nodes, &edges); err != nil {
		return 0, 0, err
	}

	return nodes, edges, nil
}

// captureExistingGraphEnvironment records live graph relation sizes and normalized schema and index fingerprints.
func (s *postgresSQLRunner) captureExistingGraphEnvironment(ctx context.Context) error {
	if err := s.pool.QueryRow(ctx, `select pg_total_relation_size(format('node_%s', $1::int4)::regclass), pg_total_relation_size(format('edge_%s', $1::int4)::regclass)`, s.graphID).Scan(&s.environment.NodeRelationBytes, &s.environment.EdgeRelationBytes); err != nil {
		return err
	}
	return s.pool.QueryRow(ctx, `select
		md5(coalesce((select string_agg(table_name || ':' || column_name || ':' || data_type, ',' order by table_name, ordinal_position) from information_schema.columns where table_schema = current_schema() and table_name in ('graph','kind','node','edge')), '')),
		md5(coalesce((select string_agg(indexname || ':' || indexdef, ',' order by indexname) from pg_indexes where schemaname = current_schema() and (tablename in ('node','edge') or tablename in (format('node_%s',$1::int4), format('edge_%s',$1::int4)))), ''))`, s.graphID).Scan(&s.environment.SchemaFingerprint, &s.environment.IndexFingerprint)
}

// captureAndValidateFixture records physical fixture sizes and rejects cardinality or checksum drift.
func (s *postgresSQLRunner) captureAndValidateFixture(ctx context.Context, fixture *FixtureMetadata) error {
	if err := s.pool.QueryRow(ctx, `select (select count(*) from node where graph_id = $1), (select count(*) from edge where graph_id = $1)`, s.graphID).Scan(
		&fixture.PhysicalNodeCount,
		&fixture.PhysicalEdgeCount,
	); err != nil {
		return fmt.Errorf("count physical graph rows: %w", err)
	}
	if fixture.PhysicalNodeCount != int64(fixture.NodeCount) || fixture.PhysicalEdgeCount != int64(fixture.EdgeCount) {
		return fmt.Errorf(
			"physical cardinality mismatch: nodes=%d want=%d edges=%d want=%d",
			fixture.PhysicalNodeCount,
			fixture.NodeCount,
			fixture.PhysicalEdgeCount,
			fixture.EdgeCount,
		)
	}
	fixture.PhysicalValidated = true

	return nil
}

// resetCaseSession drops pooled session state between cases and, for single-connection runs, records the replacement backend PID used to verify isolation.
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

// runCase resolves fixture parameters, executes the PostgreSQL read or write measurement path, captures plans and cache statistics, and returns one CaseResult.
func (s *postgresSQLRunner) runCase(ctx context.Context, warmupIterations, iterations int, testCase ScaleCase, idMap opengraph.IDMap) (record CaseResult) {
	params, err := resolveCaseParams(testCase, idMap)
	record = newCaseResult(testCase, ModePostgresSQL, params)
	defer func() {
		stats := s.pgDriver.ParseCacheStats()
		record.ParseCache = &stats
	}()
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	if testCase.WriteScenario == nil {
		var (
			rowCount     int64
			observedRows []string
			stats        DurationStats
		)
		readOptions := s.readTransactionOptions()

		if !hasForcedToolOptions(s.toolOptions) && s.productionManifest == nil {
			if len(readOptions) == 0 {
				rowCount, observedRows, stats, err = measureCypherWithWarmups(ctx, s.db, testCase.Cypher, params, testCase.Expected, idMap, warmupIterations, iterations)
			} else {
				rowCount, observedRows, stats, err = measureCypherWithWarmupsOptions(ctx, s.db, testCase.Cypher, params, testCase.Expected, idMap, warmupIterations, iterations, readOptions...)
			}
		} else {
			translation, sqlQuery, translateErr := s.translateCypher(ctx, testCase.Cypher, params)
			if translateErr != nil {
				err = translateErr
			} else {
				requestedIdentity := timedRuntimeAttestationIdentity(translation)
				if requestedIdentity == "" {
					if len(readOptions) == 0 {
						rowCount, observedRows, stats, err = measureRawSQLWithWarmups(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations)
					} else {
						rowCount, observedRows, stats, err = measureRawSQLWithWarmupsOptions(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations, readOptions...)
					}
				} else if s.poolSize != 1 {
					// Exact per-sample receipts require one physical session. Larger
					// pools remain useful for operational smoke testing, but their
					// samples intentionally lack promotion-grade attestation.
					if len(readOptions) == 0 {
						rowCount, observedRows, stats, err = measureRawSQLWithWarmups(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations)
					} else {
						rowCount, observedRows, stats, err = measureRawSQLWithWarmupsOptions(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations, readOptions...)
					}
				} else if attestor, attestorErr := newPostgresTimedReadAttestor(s.pool, s.poolSize, requestedIdentity); attestorErr != nil {
					err = attestorErr
				} else if len(readOptions) == 0 {
					rowCount, observedRows, stats, err = measureRawSQLWithWarmupsAndAttestation(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations, attestor)
				} else {
					rowCount, observedRows, stats, err = measureRawSQLWithWarmupsAndAttestationOptions(ctx, s.db, sqlQuery, translation.Parameters, testCase.Expected, idMap, warmupIterations, iterations, attestor, readOptions...)
				}
			}
		}
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

	if s.existingGraph != nil {
		_ = appendExistingGraphProgress(s.existingGraph.ProgressPath, ExistingGraphProgress{
			Stage:   "plan",
			CaseKey: existingGraphCaseKey(ModePostgresSQL, testCase),
		})
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
		for _, decision := range explain.Optimization.LoweringPlan.ExpansionSearchStrategy {
			if decision.FallbackReason != "" && !slices.Contains(fallbackReasons, decision.FallbackReason) {
				fallbackReasons = append(fallbackReasons, decision.FallbackReason)
			}
		}
		record.FallbackReason = strings.Join(fallbackReasons, ",")
	}
	if s.references && testCase.WriteScenario == nil {
		waterfall, err := measureCompileWaterfall(ctx, testCase.Cypher, params, s.pgDriver.KindMapper(), s.graphID, iterations, s.toolOptions)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("client compile waterfall: %v", err)
			return record
		}
		record.ClientWaterfall = &waterfall
		productionOrder, referenceOrder := referenceClosureMeasurementOrder(len(s.referenceArms) == 1, s.round)
		var references []PostgresReferenceResult
		if referenceOrder == 1 {
			references, err = s.measureReferences(ctx, testCase, params, idMap, record.ObservedRows, warmupIterations, iterations)
			if err != nil {
				record.Status = StatusError
				record.Error = fmt.Sprintf("PostgreSQL references: %v", err)
				return record
			}
			setReferenceMeasurementOrder(references, referenceOrder)
		}
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
		rawWaterfall.MeasurementOrder = productionOrder
		record.RawPGXWaterfall = &rawWaterfall
		if referenceOrder != 1 {
			references, err = s.measureReferences(ctx, testCase, params, idMap, record.ObservedRows, warmupIterations, iterations)
			if err != nil {
				record.Status = StatusError
				record.Error = fmt.Sprintf("PostgreSQL references: %v", err)
				return record
			}
			setReferenceMeasurementOrder(references, referenceOrder)
		}
		record.PostgresReferences = references
		roundTrip, err := measureRawPGXWaterfall(ctx, s.pool, "select 1", nil, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("raw pgx round trip: %v", err)
			return record
		}
		record.RawPGXRoundTrip = &roundTrip
	}
	if testCase.WriteScenario == nil && len(s.concurrency) > 0 {
		if s.existingGraph != nil {
			_ = appendExistingGraphProgress(s.existingGraph.ProgressPath, ExistingGraphProgress{
				Stage:   "concurrency",
				CaseKey: existingGraphCaseKey(ModePostgresSQL, testCase),
			})
		}
		blocks, err := measurePostgresConcurrency(ctx, s.pool, explain.SQL, explain.Parameters, s.poolSize, s.concurrency, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = fmt.Sprintf("concurrency smoke: %v", err)
			return record
		}
		record.Concurrency = blocks
	}
	if testCase.WriteScenario == nil {
		if err := s.attachPostgresTraversalTelemetry(ctx, &record, explain.Parameters); err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}
		setSampleTraversalRuntimeMetadata(&record.Stats, record.TraversalTelemetry)
	}
	return record
}

// readTransactionOptions returns the one stable-snapshot contract shared by
// every PostgreSQL timing and plan-replay path. Provisional production
// manifests always require Repeatable Read; tool tournaments opt into the same
// isolation with -postgres-repeatable-read.
func (s *postgresSQLRunner) readTransactionOptions() []graph.TransactionOption {
	if s.productionManifest == nil && !s.repeatableRead {
		return nil
	}
	return []graph.TransactionOption{pg.OptionSetTransactionIsolation(pgx.RepeatableRead)}
}

func timedRuntimeAttestationIdentity(translation translate.Result) string {
	outcome, ok := singleTraversalOutcome(translation.Optimization.TargetOutcomes)
	if !ok {
		return ""
	}
	requested := outcome.Candidate
	if requested == "" {
		requested = outcome.Selected
	}
	if strings.HasPrefix(requested, "SP-B1-") || strings.HasPrefix(requested, "SP-B2-") ||
		strings.HasPrefix(requested, "ASP-B1-") || strings.HasPrefix(requested, "ASP-B2-") ||
		requested == string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness) ||
		requested == string(optimize.ShortestPathExecutorASPI1DAG) ||
		outcome.EmittedPolicy == string(optimize.ExpansionSearchPolicyOrientationProbeV1) {
		return requested
	}
	return ""
}

// referenceClosureMeasurementOrder returns the balanced production/reference order for a measurement round.
func referenceClosureMeasurementOrder(singleSelectedReference bool, round int) (production, reference int) {
	if singleSelectedReference && round > 0 && round%2 == 0 {
		return 2, 1
	}
	return 1, 2
}

// setReferenceMeasurementOrder assigns consecutive execution positions to reference results beginning at order.
func setReferenceMeasurementOrder(references []PostgresReferenceResult, order int) {
	for idx := range references {
		references[idx].MeasurementOrder = order + idx
	}
}

// postgresExplain contains translated SQL and normalized PostgreSQL EXPLAIN evidence.
type postgresExplain struct {
	// SQL contains the rendered SQL statement.
	SQL string
	// Plan contains normalized PostgreSQL text-plan lines.
	Plan []string
	// PlanJSON contains structured backend plan evidence.
	PlanJSON json.RawMessage
	// Metrics contains normalized PostgreSQL plan counters and resources.
	Metrics PostgresPlanMetrics
	// Optimization captures translation optimization and lowering decisions.
	Optimization translate.OptimizationSummary
	// Parameters contains translated SQL parameters keyed by placeholder name.
	Parameters map[string]any
}

// explain translates a Cypher query and returns normalized SQL and PostgreSQL EXPLAIN evidence.
func (s *postgresSQLRunner) explain(ctx context.Context, cypherQuery string, params map[string]any, write bool) (postgresExplain, error) {
	translation, sqlQuery, err := s.translateCypher(ctx, cypherQuery, params)
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
			jsonResult := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING ON, FORMAT JSON) "+sqlQuery, translation.Parameters)
			defer jsonResult.Close()
			if jsonResult.Next() && len(jsonResult.Values()) > 0 {
				planJSON, err = encodePostgresPlanJSON(jsonResult.Values()[0])
				if err != nil {
					return err
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
	} else if readOptions := s.readTransactionOptions(); len(readOptions) > 0 {
		explainErr = s.db.ReadTransaction(ctx, runExplain, readOptions...)
	} else {
		explainErr = s.db.ReadTransaction(ctx, runExplain)
	}
	if explainErr != nil {
		return postgresExplain{}, explainErr
	}

	metrics := parsePostgresPlanMetrics(plan)
	if len(planJSON) > 0 {
		if structured, err := parsePostgresPlanJSONMetrics(planJSON); err == nil {
			metrics = structured
		}
	}
	return postgresExplain{
		SQL:          sqlQuery,
		Plan:         plan,
		PlanJSON:     planJSON,
		Metrics:      metrics,
		Optimization: translation.Optimization,
		Parameters:   translation.Parameters,
	}, nil
}

// translateCypher parses and translates Cypher, applying forced tool options when configured.
func (s *postgresSQLRunner) translateCypher(ctx context.Context, cypherQuery string, params map[string]any) (translate.Result, string, error) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		return translate.Result{}, "", err
	}

	var translation translate.Result
	if s.productionManifest != nil {
		options, optionsErr := s.productionOptions(cypherQuery)
		if optionsErr != nil {
			return translate.Result{}, "", optionsErr
		}
		translation, err = translate.TranslateWithProductionOptions(ctx, regularQuery, s.pgDriver.KindMapper(), params, s.graphID, options)
	} else if !hasForcedToolOptions(s.toolOptions) {
		translation, err = translate.Translate(ctx, regularQuery, s.pgDriver.KindMapper(), params, s.graphID)
	} else {
		translation, err = translate.TranslateForTool(ctx, regularQuery, s.pgDriver.KindMapper(), params, s.graphID, s.toolOptions)
	}
	if err != nil {
		return translate.Result{}, "", err
	}

	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		return translate.Result{}, "", err
	}
	return translation, sqlQuery, nil
}

// hasForcedToolOptions reports whether either executor-selection override is configured.
func hasForcedToolOptions(options translate.ToolOptions) bool {
	return options.ForceShortestPathExecutor != "" || options.ForceExpansionSearchStrategy != "" ||
		options.EnableExpansionOrientationTournament || options.EnableExpansionOrientationShadow
}

// encodePostgresPlanJSON normalizes byte, string, or structured EXPLAIN JSON into json.RawMessage.
func encodePostgresPlanJSON(value any) (json.RawMessage, error) {
	switch typed := value.(type) {
	case []byte:
		return append(json.RawMessage(nil), typed...), nil
	case string:
		return append(json.RawMessage(nil), typed...), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		return json.RawMessage(encoded), nil
	}
}

var (
	// postgresPlanningPattern extracts milliseconds from a PostgreSQL Planning Time summary line.
	postgresPlanningPattern = regexp.MustCompile(`Planning Time: ([0-9.]+) ms`)

	// postgresExecutionPattern extracts milliseconds from a PostgreSQL Execution Time summary line.
	postgresExecutionPattern = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)

	// postgresBufferPattern extracts storage class, operation, and page count from PostgreSQL buffer counters.
	postgresBufferPattern = regexp.MustCompile(`(?:(shared|local|temp) )?(hit|read|dirtied|written)=([0-9]+)`)
)

// parsePostgresPlanMetrics extracts planning, execution, and buffer counters from PostgreSQL text-plan lines.
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

// parsePostgresBuffers extracts shared, local, and temporary buffer counters from one plan line.
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
