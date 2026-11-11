//go:build manual_integration

package integration

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// pathBatchStudyQuery pairs one SQL arm with its bound values and expected result cardinality.
type pathBatchStudyQuery struct {
	// name identifies the query arm in logs and study artifacts.
	name string
	// sql contains the SQL statement executed by this query arm.
	sql string
	// params supplies separately bound values for the SQL statement.
	params map[string]any
	// rows records the expected fully consumed result cardinality.
	rows int
}

// pathBatchStudySample records first-decoded-row and complete-consumption latency for one replay.
type pathBatchStudySample struct {
	// FirstRowMS measures elapsed milliseconds until the first path is decoded.
	FirstRowMS float64 `json:"first_decoded_row_ms"`
	// TotalMS measures elapsed milliseconds through complete result consumption.
	TotalMS float64 `json:"total_ms"`
	// Rows records the number of decoded paths in the sample.
	Rows int `json:"rows"`
}

// pathBatchStudyMemory records client allocations and sampled heap growth from a separate query replay.
type pathBatchStudyMemory struct {
	// TotalAllocBytes records process allocation growth during a separate consumption replay.
	TotalAllocBytes uint64 `json:"client_total_alloc_bytes"`
	// SampledExtraHeapBytes records sampled heap growth above the replay's initial heap size.
	SampledExtraHeapBytes uint64 `json:"client_sampled_extra_heap_bytes"`
	// SamplingIntervalMicros records the heap sampler's interval in microseconds.
	SamplingIntervalMicros int `json:"sampling_interval_micros"`
}

// pathBatchStudyPlan records the execution and resource observations exposed by PostgreSQL EXPLAIN.
type pathBatchStudyPlan struct {
	// ExecutionMS records PostgreSQL's reported execution time in milliseconds.
	ExecutionMS float64 `json:"execution_ms"`
	// PlanningMS records PostgreSQL's reported planning time in milliseconds.
	PlanningMS float64 `json:"planning_ms"`
	// PlanningUsedKB records reported planning memory usage in kilobytes.
	PlanningUsedKB int64 `json:"planning_used_kb"`
	// MaxVisibleNodeKB records the largest visible executor-node memory observation, when available.
	MaxVisibleNodeKB *int64 `json:"max_visible_executor_node_kb"`
	// MaxDiskSortKB records the largest reported disk sort usage in kilobytes.
	MaxDiskSortKB int64 `json:"max_disk_sort_kb"`
	// MaxHashBatches records the largest reported hash batch count.
	MaxHashBatches int64 `json:"max_hash_batches"`
	// TempReadBytes records temporary block reads converted to bytes.
	TempReadBytes int64 `json:"temp_read_bytes"`
	// TempWrittenBytes records temporary block writes converted to bytes.
	TempWrittenBytes int64 `json:"temp_written_bytes"`
	// MaterializedPaths records identifiable path materialization rows, when available.
	MaterializedPaths *int64 `json:"materialized_path_rows"`
	// HydratedNodes records identifiable node hydration rows, when available.
	HydratedNodes *int64 `json:"hydrated_node_rows"`
	// HydratedEdges records identifiable relationship hydration rows, when available.
	HydratedEdges *int64 `json:"hydrated_edge_rows"`
	// FrontierRows retains the row counts reported by recursive traversal frontiers.
	FrontierRows []int64 `json:"recursive_frontier_rows"`
}

// pathBatchStudyRecord combines timing and separate resource observations for one fixture and query arm.
type pathBatchStudyRecord struct {
	// Fixture identifies the generated graph topology used by the measurement.
	Fixture string `json:"fixture"`
	// PropertyBytes records the configured property payload bytes per entity.
	PropertyBytes int `json:"property_bytes_per_entity"`
	// Paths records the number of paths made available by the fixture.
	Paths int `json:"available_paths"`
	// Nodes records the fixture's node cardinality.
	Nodes int `json:"fixture_nodes"`
	// Edges records the fixture's relationship cardinality.
	Edges int `json:"fixture_edges"`
	// Query identifies the query arm measured by this record.
	Query string `json:"query"`
	// Samples retains the measured warm consumption observations.
	Samples []pathBatchStudySample `json:"samples"`
	// WarmupSamples retains the observations excluded from the warm median.
	WarmupSamples []pathBatchStudySample `json:"warmup_samples"`
	// Median contains separate first-row and complete-consumption medians.
	Median pathBatchStudySample `json:"median"`
	// Memory records the separate replay's client allocation and sampled heap observations.
	Memory pathBatchStudyMemory `json:"memory"`
	// Plan records the normal-work-memory execution plan observations.
	Plan pathBatchStudyPlan `json:"plan"`
	// LowWorkMem records a separate replay with PostgreSQL work memory limited to 64 kilobytes.
	LowWorkMem pathBatchStudyPlan `json:"work_mem_64kb_plan"`
}

// pathBatchStudyEnvironment records the stable capture settings shared by all query arms.
type pathBatchStudyEnvironment struct {
	// CapturedUTC records when the study captured its environment settings.
	CapturedUTC time.Time `json:"captured_utc"`
	// GoVersion identifies the Go runtime used for client consumption.
	GoVersion string `json:"go_version"`
	// GOMAXPROCS records the client's available Go execution parallelism.
	GOMAXPROCS int `json:"gomaxprocs"`
	// TimingRoundsAfterWarmup records how many observations contribute to warm medians.
	TimingRoundsAfterWarmup int `json:"timing_rounds_after_warmup"`
	// WarmupRounds records how many initial observations are retained outside the median.
	WarmupRounds int `json:"warmup_rounds"`
	// MemoryNote describes the limits of sampled client and visible server memory metrics.
	MemoryNote string `json:"memory_note"`
	// PostgresVersion records the server's version description.
	PostgresVersion string `json:"postgres_version"`
	// WorkMem records the PostgreSQL work-memory setting used by the normal replay.
	WorkMem string `json:"work_mem"`
	// HashMemMultiplier records the server's hash-operation memory multiplier.
	HashMemMultiplier string `json:"hash_mem_multiplier"`
	// BlockSize records the PostgreSQL block size used to convert I/O counters to bytes.
	BlockSize string `json:"block_size"`
	// PlanCacheMode records how PostgreSQL selects prepared statement plans.
	PlanCacheMode string `json:"plan_cache_mode"`
	// JIT records the PostgreSQL just-in-time compilation setting.
	JIT string `json:"jit"`
}

// pathBatchStudyPhysicalLayout records storage occupied by the fixture's graph partitions.
type pathBatchStudyPhysicalLayout struct {
	// NodeHeapBytes records the node partition's heap size in bytes.
	NodeHeapBytes int64 `json:"node_heap_bytes"`
	// EdgeHeapBytes records the edge partition's heap size in bytes.
	EdgeHeapBytes int64 `json:"edge_heap_bytes"`
}

const (
	// pathBatchStudyServerSettingsQuery reads the server settings captured with timing observations.
	pathBatchStudyServerSettingsQuery = `select version(),current_setting('work_mem'),current_setting('hash_mem_multiplier'),current_setting('block_size'),current_setting('plan_cache_mode'),current_setting('jit')`

	// pathBatchStudyPhysicalLayoutQuery reads heap sizes for the driver's selected graph partitions.
	pathBatchStudyPhysicalLayoutQuery = "select pg_relation_size('%s'::regclass)::int8, pg_relation_size('%s'::regclass)::int8"
)

// TestPostgreSQLPathBatchPerformance is an opt-in diagnostic, with correctness
// assertions but no machine-dependent latency thresholds. Timings include Raw,
// Next, driver decoding and graph.Path mapping, not fixture setup or comparison.
// Memory sampling and EXPLAIN are separate replays of the same query/snapshot.
func TestPostgreSQLPathBatchPerformance(t *testing.T) {
	if os.Getenv(pathBatchStudyEnv) != pathBatchStudyEnabledValue {
		t.Skip("set PATH_BATCH_STUDY=1 for the path materialization experiment")
	}
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CloseOnly,
		GraphName:            "path_batch_experiment",
		ExtraNodeKinds:       graph.Kinds{graph.StringKind(pathBatchNodeKind)},
		ExtraEdgeKinds:       graph.StringsToKinds([]string{pathBatchEdgeKindA, pathBatchEdgeKindB}),
	})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	settings := pathBatchStudyEnvironment{
		CapturedUTC:             time.Now().UTC(),
		GoVersion:               runtime.Version(),
		GOMAXPROCS:              runtime.GOMAXPROCS(0),
		TimingRoundsAfterWarmup: pathBatchStudySampleRounds,
		WarmupRounds:            pathBatchStudyWarmupRounds,
		MemoryNote:              "Client streaming/discard run; 1ms sampled process heap, not exact peak. PostgreSQL maximum visible node memory is partial; helper internals and whole-backend peak are unavailable.",
	}
	require.NoError(t, session.PGPool.QueryRow(session.Ctx, pathBatchStudyServerSettingsQuery).
		Scan(&settings.PostgresVersion, &settings.WorkMem, &settings.HashMemMultiplier,
			&settings.BlockSize, &settings.PlanCacheMode, &settings.JIT))

	pathBatchStudyWrite(t, "environment.json", settings)
	var records []pathBatchStudyRecord
	for _, scenario := range []struct {
		// name identifies the generated fixture topology.
		name string
		// width controls the fixture branching width.
		width int
		// bytes sets the property payload bytes per entity.
		bytes int
	}{
		{
			name:  "shared",
			width: 4,
			bytes: 0,
		}, {
			name:  "low_overlap",
			width: 512,
			bytes: 0,
		},
		{
			name:  "shared",
			width: 4,
			bytes: 2048,
		}, {
			name:  "low_overlap",
			width: 512,
			bytes: 2048,
		},
		{
			name:  "shared_scale",
			width: 8,
			bytes: 0,
		},
	} {
		t.Run(fmt.Sprintf("%s/properties=%d", scenario.name, scenario.bytes), func(t *testing.T) {
			// Rollbacks leave physical dead tuples. Reclaim this experiment's
			// empty partitions outside a transaction so earlier scenarios do
			// not inflate repeated scans in later materializer comparisons.
			for _, table := range []string{selected.Partitions.Node.Name, selected.Partitions.Edge.Name} {
				_, err := session.PGPool.Exec(session.Ctx, "VACUUM (ANALYZE) "+table)
				require.NoError(t, err)
			}
			var fixture *opengraph.Graph
			var count int
			if scenario.name == "low_overlap" {
				fixture, count = pathBatchLowOverlapFixture(scenario.width, scenario.bytes)
			} else {
				fixture, count = pathBatchSharedFixture(scenario.width, scenario.bytes)
			}
			require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
				// Statistics must describe this topology, not a previous rolled-back
				// fixture. Analyze only this experiment's graph partitions.
				pathBatchStudyExec(t, tx, "ANALYZE "+selected.Partitions.Node.Name)
				pathBatchStudyExec(t, tx, "ANALYZE "+selected.Partitions.Edge.Name)
				physical := tx.Raw(fmt.Sprintf(pathBatchStudyPhysicalLayoutQuery,
					selected.Partitions.Node.Name, selected.Partitions.Edge.Name), nil)
				require.True(t, physical.Next())

				var physicalLayout pathBatchStudyPhysicalLayout
				require.NoError(t, physical.Scan(&physicalLayout.NodeHeapBytes, &physicalLayout.EdgeHeapBytes))
				pathBatchStudyWrite(t, fmt.Sprintf("%s-%d-physical-layout.json", scenario.name, scenario.bytes),
					physicalLayout)
				physical.Close()
				require.NoError(t, physical.Error())
				baseCypher := "MATCH p=(s:PathBatchNode)-[:PathBatchA|PathBatchB*3..3]->(e) WHERE id(s)=$root RETURN p"
				prepare := func(cypher string) (translate.Result, string, map[string]any) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), cypher)
					require.NoError(t, err)
					translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(),
						map[string]any{"root": ids["root"].Int64()}, selected.ID,
						translate.Options{
							OptimizerMode: translate.OptimizerEnabled,
						})
					require.NoError(t, err)
					formatted, err := translate.Translated(translation)
					require.NoError(t, err)
					parameters := maps.Clone(translation.Parameters)
					maps.Copy(parameters, formatted.Parameters)
					return translation, strings.TrimSuffix(formatted.Statement, ";"), parameters
				}
				translation, baseline, parameters := prepare(baseCypher)
				skeleton, batchParams, reason := pathBatchSkeleton(translation)
				require.Empty(t, reason)
				batch := pathBatchSQL(skeleton, selected.ID, graph.DirectionOutbound)
				_, limited, limitedParams := prepare(baseCypher + " LIMIT 1")
				page := pathBatchSQL("select * from ("+skeleton+") page_source limit 1", selected.ID, graph.DirectionOutbound)
				queries := []pathBatchStudyQuery{
					{
						name:   "current_full",
						sql:    baseline,
						params: parameters,
						rows:   count,
					},
					{
						name:   "batch_full",
						sql:    batch,
						params: batchParams,
						rows:   count,
					},
					{
						name:   "current_limit1",
						sql:    limited,
						params: limitedParams,
						rows:   1,
					},
					{
						name:   "batch_outer_limit1",
						sql:    batch + " limit 1",
						params: batchParams,
						rows:   1,
					},
					{
						name:   "batch_page_limit1",
						sql:    page + " limit 1",
						params: batchParams,
						rows:   1,
					},
				}
				// Without ORDER BY, any valid result occurrence may satisfy LIMIT 1.
				// Compare its complete value against the full multiset, not an
				// arbitrary incumbent first row. Full-result arms match multiplicity.
				expected := pathBatchFullValues(t, tx.Raw(baseline, parameters))
				require.Len(t, expected, count)
				for _, query := range queries {
					actual := pathBatchFullValues(t, tx.Raw(query.sql, query.params))
					require.Len(t, actual, query.rows, query.name)
					if query.rows == count {
						require.Equal(t, expected, actual, query.name)
					} else {
						index := sort.SearchStrings(expected, actual[0])
						require.True(t, index < len(expected) && expected[index] == actual[0], query.name)
					}
				}
				samples := make([][]pathBatchStudySample, len(queries))
				warmups := make([][]pathBatchStudySample, len(queries))
				// Six warmup cycles cross PostgreSQL's initial custom-plan phase.
				// Preserve warmup samples so transitions remain reviewable.
				for round := range pathBatchStudyWarmupRounds + pathBatchStudySampleRounds {
					for offset := range len(queries) {
						index := (round + offset) % len(queries)
						sample := pathBatchStudyConsume(t, tx, queries[index])
						if round >= pathBatchStudyWarmupRounds {
							samples[index] = append(samples[index], sample)
						} else {
							warmups[index] = append(warmups[index], sample)
						}
					}
				}
				for index, query := range queries {
					record := pathBatchStudyRecord{
						Fixture:       scenario.name,
						PropertyBytes: scenario.bytes,
						Paths:         count,
						Nodes:         len(fixture.Nodes),
						Edges:         len(fixture.Edges),
						Query:         query.name,
						Samples:       samples[index],
						WarmupSamples: warmups[index],
						Median:        pathBatchStudyMedian(samples[index]),
					}
					record.Memory = pathBatchStudyClientMemory(t, tx, query)
					record.Plan = pathBatchStudyExplain(t, tx, query, scenario.name, scenario.bytes, "default")
					pathBatchStudyExec(t, tx, "SET LOCAL work_mem='64kB'")
					record.LowWorkMem = pathBatchStudyExplain(t, tx, query, scenario.name, scenario.bytes, "64kb")
					pathBatchStudyExec(t, tx, "SET LOCAL work_mem='"+strings.ReplaceAll(settings.WorkMem, "'", "''")+"'")
					if strings.Contains(query.name, "batch_") {
						require.NotNil(t, record.Plan.MaterializedPaths)
						require.NotNil(t, record.Plan.HydratedNodes)
						require.NotNil(t, record.Plan.HydratedEdges)
						if query.name == "batch_page_limit1" {
							require.EqualValues(t, 1, *record.Plan.MaterializedPaths)
							require.EqualValues(t, 4, *record.Plan.HydratedNodes)
							require.EqualValues(t, 3, *record.Plan.HydratedEdges)
						} else {
							require.EqualValues(t, count, *record.Plan.MaterializedPaths)
							require.EqualValues(t, len(fixture.Nodes), *record.Plan.HydratedNodes)
							require.EqualValues(t, len(fixture.Edges), *record.Plan.HydratedEdges)
						}
					}
					records = append(records, record)
					t.Logf("%s: first=%.3fms total=%.3fms allocated=%.2fMiB sampled-extra-heap=%.2fMiB server=%.3fms temp=%.2fMiB (64kB temp=%.2fMiB)",
						query.name, record.Median.FirstRowMS, record.Median.TotalMS,
						float64(record.Memory.TotalAllocBytes)/(1<<20), float64(record.Memory.SampledExtraHeapBytes)/(1<<20),
						record.Plan.ExecutionMS, float64(record.Plan.TempWrittenBytes)/(1<<20), float64(record.LowWorkMem.TempWrittenBytes)/(1<<20))
				}
				return nil
			}))
		})
	}
	pathBatchStudyWrite(t, "results.json", records)
}

// pathBatchStudyConsume fully decodes a path result while recording first-row and total consumption times.
func pathBatchStudyConsume(t *testing.T, tx graph.Transaction, query pathBatchStudyQuery) pathBatchStudySample {
	t.Helper()
	start := time.Now()
	result := tx.Raw(query.sql, query.params)
	defer result.Close()
	var sample pathBatchStudySample
	for result.Next() {
		var path graph.Path
		require.NoError(t, result.Scan(&path))
		if sample.Rows == 0 {
			sample.FirstRowMS = float64(time.Since(start)) / float64(time.Millisecond)
		}
		sample.Rows++
	}
	result.Close()
	sample.TotalMS = float64(time.Since(start)) / float64(time.Millisecond)
	require.NoError(t, result.Error())
	require.Equal(t, query.rows, sample.Rows, query.name)
	return sample
}

// pathBatchStudyMedian computes separate first-row and total-duration medians from equally sized result samples.
func pathBatchStudyMedian(samples []pathBatchStudySample) pathBatchStudySample {
	first, total := make([]float64, len(samples)), make([]float64, len(samples))
	for index, sample := range samples {
		first[index], total[index] = sample.FirstRowMS, sample.TotalMS
	}
	sort.Float64s(first)
	sort.Float64s(total)
	return pathBatchStudySample{
		FirstRowMS: first[len(first)/2],
		TotalMS:    total[len(total)/2],
		Rows:       samples[0].Rows,
	}
}

// pathBatchStudyClientMemory replays consumption while sampling client heap growth and allocated bytes.
func pathBatchStudyClientMemory(t *testing.T, tx graph.Transaction, query pathBatchStudyQuery) pathBatchStudyMemory {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	peak := before.HeapAlloc
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		ticker := time.NewTicker(pathBatchMemorySamplingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var sample runtime.MemStats
				runtime.ReadMemStats(&sample)
				peak = max(peak, sample.HeapAlloc)
			case <-done:
				return
			}
		}
	}()
	func() {
		defer close(done)
		pathBatchStudyConsume(t, tx, query)
	}()
	sampler.Wait()
	runtime.ReadMemStats(&after)
	peak = max(peak, after.HeapAlloc)
	return pathBatchStudyMemory{
		TotalAllocBytes:        after.TotalAlloc - before.TotalAlloc,
		SampledExtraHeapBytes:  peak - before.HeapAlloc,
		SamplingIntervalMicros: int(pathBatchMemorySamplingInterval / time.Microsecond),
	}
}

// pathBatchStudyExec consumes a setup statement and checks its final result error.
func pathBatchStudyExec(t *testing.T, tx graph.Transaction, statement string) {
	t.Helper()
	result := tx.Raw(statement, nil)
	for result.Next() {
	}
	result.Close()
	require.NoError(t, result.Error())
}

// pathBatchStudyExplain captures execution plans and extracts the study's visible PostgreSQL resource metrics.
func pathBatchStudyExplain(t *testing.T, tx graph.Transaction, query pathBatchStudyQuery, fixture string, bytes int, memory string) pathBatchStudyPlan {
	t.Helper()
	result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, MEMORY, TIMING OFF, SUMMARY, FORMAT JSON) "+query.sql, query.params)
	require.True(t, result.Next(), "%v", result.Error())
	plan, err := normalizeExplainPlan(result.Values()[0])
	require.NoError(t, err)
	require.False(t, result.Next())
	result.Close()
	require.NoError(t, result.Error())
	pathBatchStudyWrite(t, fmt.Sprintf("%s-%d-%s-%s-plan.json", fixture, bytes, query.name, memory), plan)
	pathBatchStudyWrite(t, fmt.Sprintf("%s-%d-%s.sql.json", fixture, bytes, query.name), query.sql)
	summary := plan.([]any)[0].(map[string]any)
	number := func(value any) float64 {
		switch value := value.(type) {
		case float64:
			return value
		case int64:
			return float64(value)
		default:
			return 0
		}
	}
	integer := func(value any) int64 { result, _ := asInt64(value); return result }
	metrics := pathBatchStudyPlan{
		ExecutionMS: number(summary["Execution Time"]),
		PlanningMS:  number(summary["Planning Time"]),
	}
	if planning, ok := summary["Planning"].(map[string]any); ok {
		metrics.PlanningUsedKB = integer(planning["Memory Used"])
	}
	root := summary["Plan"].(map[string]any)
	// PostgreSQL block size is queried, not assumed from its usual default.
	blockResult := tx.Raw("SELECT current_setting('block_size')::int8", nil)
	require.True(t, blockResult.Next())
	blockBytes := integer(blockResult.Values()[0])
	blockResult.Close()
	require.NoError(t, blockResult.Error())
	metrics.TempReadBytes = integer(root["Temp Read Blocks"]) * blockBytes
	metrics.TempWrittenBytes = integer(root["Temp Written Blocks"]) * blockBytes
	var visit func(map[string]any)
	visit = func(node map[string]any) {
		if value, exists := node["Peak Memory Usage"]; exists {
			peak := integer(value)
			if metrics.MaxVisibleNodeKB == nil || peak > *metrics.MaxVisibleNodeKB {
				metrics.MaxVisibleNodeKB = &peak
			}
		}
		if node["Sort Space Type"] == "Disk" {
			metrics.MaxDiskSortKB = max(metrics.MaxDiskSortKB, integer(node["Sort Space Used"]))
		}
		metrics.MaxHashBatches = max(metrics.MaxHashBatches, integer(node["Hash Batches"]))
		rows := integer(node["Actual Rows"])
		switch node["Subplan Name"] {
		case "CTE path_rows":
			metrics.MaterializedPaths = &rows
		case "CTE node_records":
			metrics.HydratedNodes = &rows
		case "CTE edge_records":
			metrics.HydratedEdges = &rows
		}
		if node["Node Type"] == "Recursive Union" {
			metrics.FrontierRows = append(metrics.FrontierRows, rows)
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				visit(child.(map[string]any))
			}
		}
	}
	visit(root)
	return metrics
}

// pathBatchStudyWrite writes an indented JSON artifact into the configured study report directory.
func pathBatchStudyWrite(t *testing.T, name string, value any) {
	t.Helper()
	if directory := os.Getenv(pathBatchReportDirectoryEnv); directory != "" {
		require.NoError(t, os.MkdirAll(directory, 0o755))
		data, err := json.MarshalIndent(value, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), append(data, '\n'), 0o644))
	}
}
