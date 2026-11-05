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

type pathBatchStudyQuery struct {
	name, sql string
	params    map[string]any
	rows      int
}

type pathBatchStudySample struct {
	FirstRowMS float64 `json:"first_decoded_row_ms"`
	TotalMS    float64 `json:"total_ms"`
	Rows       int     `json:"rows"`
}

type pathBatchStudyMemory struct {
	TotalAllocBytes        uint64 `json:"client_total_alloc_bytes"`
	SampledExtraHeapBytes  uint64 `json:"client_sampled_extra_heap_bytes"`
	SamplingIntervalMicros int    `json:"sampling_interval_micros"`
}

type pathBatchStudyPlan struct {
	ExecutionMS       float64 `json:"execution_ms"`
	PlanningMS        float64 `json:"planning_ms"`
	PlanningUsedKB    int64   `json:"planning_used_kb"`
	MaxVisibleNodeKB  *int64  `json:"max_visible_executor_node_kb"`
	MaxDiskSortKB     int64   `json:"max_disk_sort_kb"`
	MaxHashBatches    int64   `json:"max_hash_batches"`
	TempReadBytes     int64   `json:"temp_read_bytes"`
	TempWrittenBytes  int64   `json:"temp_written_bytes"`
	MaterializedPaths *int64  `json:"materialized_path_rows"`
	HydratedNodes     *int64  `json:"hydrated_node_rows"`
	HydratedEdges     *int64  `json:"hydrated_edge_rows"`
	FrontierRows      []int64 `json:"recursive_frontier_rows"`
}

type pathBatchStudyRecord struct {
	Fixture       string                 `json:"fixture"`
	PropertyBytes int                    `json:"property_bytes_per_entity"`
	Paths         int                    `json:"available_paths"`
	Nodes         int                    `json:"fixture_nodes"`
	Edges         int                    `json:"fixture_edges"`
	Query         string                 `json:"query"`
	Samples       []pathBatchStudySample `json:"samples"`
	WarmupSamples []pathBatchStudySample `json:"warmup_samples"`
	Median        pathBatchStudySample   `json:"median"`
	Memory        pathBatchStudyMemory   `json:"memory"`
	Plan          pathBatchStudyPlan     `json:"plan"`
	LowWorkMem    pathBatchStudyPlan     `json:"work_mem_64kb_plan"`
}

// TestPostgreSQLPathBatchPerformance is an opt-in diagnostic, with correctness
// assertions but no machine-dependent latency thresholds. Timings include Raw,
// Next, driver decoding and graph.Path mapping, not fixture setup or comparison.
// Memory sampling and EXPLAIN are separate replays of the same query/snapshot.
func TestPostgreSQLPathBatchPerformance(t *testing.T) {
	if os.Getenv("PATH_BATCH_STUDY") != "1" {
		t.Skip("set PATH_BATCH_STUDY=1 for the path materialization experiment")
	}
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true,
		SkipIfDriverMismatch: true, CleanupMode: CloseOnly, GraphName: "path_batch_experiment",
		ExtraNodeKinds: graph.Kinds{graph.StringKind(pathBatchNodeKind)},
		ExtraEdgeKinds: graph.StringsToKinds([]string{pathBatchEdgeKindA, pathBatchEdgeKindB})})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	settings := map[string]any{"captured_utc": time.Now().UTC(), "go_version": runtime.Version(),
		"gomaxprocs": runtime.GOMAXPROCS(0), "timing_rounds_after_warmup": 5,
		"warmup_rounds": 6,
		"memory_note":   "Client streaming/discard run; 1ms sampled process heap, not exact peak. PostgreSQL maximum visible node memory is partial; helper internals and whole-backend peak are unavailable."}
	var serverVersion, workMem, hashMultiplier, blockSize, planCacheMode, jit string
	require.NoError(t, session.PGPool.QueryRow(session.Ctx,
		"select version(),current_setting('work_mem'),current_setting('hash_mem_multiplier'),current_setting('block_size'),current_setting('plan_cache_mode'),current_setting('jit')").
		Scan(&serverVersion, &workMem, &hashMultiplier, &blockSize, &planCacheMode, &jit))
	settings["postgres_version"], settings["work_mem"] = serverVersion, workMem
	settings["hash_mem_multiplier"], settings["block_size"] = hashMultiplier, blockSize
	settings["plan_cache_mode"], settings["jit"] = planCacheMode, jit
	pathBatchStudyWrite(t, "environment.json", settings)
	var records []pathBatchStudyRecord
	for _, scenario := range []struct {
		name  string
		width int
		bytes int
	}{
		{"shared", 4, 0}, {"low_overlap", 512, 0},
		{"shared", 4, 2048}, {"low_overlap", 512, 2048},
		{"shared_scale", 8, 0},
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
				physical := tx.Raw(fmt.Sprintf("select pg_relation_size('%s'::regclass)::int8, pg_relation_size('%s'::regclass)::int8",
					selected.Partitions.Node.Name, selected.Partitions.Edge.Name), nil)
				require.True(t, physical.Next())
				pathBatchStudyWrite(t, fmt.Sprintf("%s-%d-physical-layout.json", scenario.name, scenario.bytes),
					map[string]any{"node_heap_bytes": physical.Values()[0], "edge_heap_bytes": physical.Values()[1]})
				physical.Close()
				require.NoError(t, physical.Error())
				baseCypher := "MATCH p=(s:PathBatchNode)-[:PathBatchA|PathBatchB*3..3]->(e) WHERE id(s)=$root RETURN p"
				prepare := func(cypher string) (translate.Result, string, map[string]any) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), cypher)
					require.NoError(t, err)
					translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(),
						map[string]any{"root": ids["root"].Int64()}, selected.ID,
						translate.Options{OptimizerMode: translate.OptimizerEnabled})
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
					{"current_full", baseline, parameters, count},
					{"batch_full", batch, batchParams, count},
					{"current_limit1", limited, limitedParams, 1},
					{"batch_outer_limit1", batch + " limit 1", batchParams, 1},
					{"batch_page_limit1", page + " limit 1", batchParams, 1},
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
				for round := range 11 {
					for offset := range len(queries) {
						index := (round + offset) % len(queries)
						sample := pathBatchStudyConsume(t, tx, queries[index])
						if round >= 6 {
							samples[index] = append(samples[index], sample)
						} else {
							warmups[index] = append(warmups[index], sample)
						}
					}
				}
				for index, query := range queries {
					record := pathBatchStudyRecord{Fixture: scenario.name, PropertyBytes: scenario.bytes,
						Paths: count, Nodes: len(fixture.Nodes), Edges: len(fixture.Edges), Query: query.name,
						Samples: samples[index], WarmupSamples: warmups[index], Median: pathBatchStudyMedian(samples[index])}
					record.Memory = pathBatchStudyClientMemory(t, tx, query)
					record.Plan = pathBatchStudyExplain(t, tx, query, scenario.name, scenario.bytes, "default")
					pathBatchStudyExec(t, tx, "SET LOCAL work_mem='64kB'")
					record.LowWorkMem = pathBatchStudyExplain(t, tx, query, scenario.name, scenario.bytes, "64kb")
					pathBatchStudyExec(t, tx, "SET LOCAL work_mem='"+strings.ReplaceAll(workMem, "'", "''")+"'")
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

func pathBatchStudyMedian(samples []pathBatchStudySample) pathBatchStudySample {
	first, total := make([]float64, len(samples)), make([]float64, len(samples))
	for index, sample := range samples {
		first[index], total[index] = sample.FirstRowMS, sample.TotalMS
	}
	sort.Float64s(first)
	sort.Float64s(total)
	return pathBatchStudySample{FirstRowMS: first[len(first)/2], TotalMS: total[len(total)/2], Rows: samples[0].Rows}
}

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
		ticker := time.NewTicker(time.Millisecond)
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
	return pathBatchStudyMemory{TotalAllocBytes: after.TotalAlloc - before.TotalAlloc,
		SampledExtraHeapBytes: peak - before.HeapAlloc, SamplingIntervalMicros: 1000}
}

func pathBatchStudyExec(t *testing.T, tx graph.Transaction, statement string) {
	t.Helper()
	result := tx.Raw(statement, nil)
	for result.Next() {
	}
	result.Close()
	require.NoError(t, result.Error())
}

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
	metrics := pathBatchStudyPlan{ExecutionMS: number(summary["Execution Time"]), PlanningMS: number(summary["Planning Time"])}
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

func pathBatchStudyWrite(t *testing.T, name string, value any) {
	t.Helper()
	if directory := os.Getenv("PATH_BATCH_REPORT_DIR"); directory != "" {
		require.NoError(t, os.MkdirAll(directory, 0o755))
		data, err := json.MarshalIndent(value, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), append(data, '\n'), 0o644))
	}
}
