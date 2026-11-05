//go:build manual_integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

type pathBatchCorpusCase struct {
	source string
	test   testCase
}

type pathBatchCorpusInventory struct {
	Mode                 translate.OptimizerMode `json:"mode"`
	Source               string                  `json:"source"`
	Name                 string                  `json:"name"`
	Cypher               string                  `json:"cypher"`
	DirectPathProjection bool                    `json:"direct_path_projection"`
	Eligible             bool                    `json:"eligible"`
	ExcludedReason       string                  `json:"excluded_reason,omitempty"`
	IsolatableColumns    []int                   `json:"isolatable_path_columns,omitempty"`
}

// Offline inventory distinguishes actual bound-path projections from the many
// scalar/node queries in the corpus. It does not claim a live comparison: the
// full corpus test below supplies that evidence separately.
func TestPostgreSQLPathBatchCorpusInventory(t *testing.T) {
	entries, nodes, edges := pathBatchCorpus(t)
	mapper := pgutil.NewInMemoryKindMapper()
	for _, kind := range nodes.Add(edges...) {
		mapper.Put(kind)
	}
	var inventory []pathBatchCorpusInventory
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		pathProjections, eligiblePaths, isolatableColumns := 0, 0, 0
		excludedPaths := map[string]int{}
		for _, entry := range entries {
			tc := entry.test
			record := pathBatchCorpusInventory{Mode: mode, Source: entry.source, Name: tc.Name, Cypher: tc.Cypher}
			parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
			if err != nil {
				record.ExcludedReason = "parse error"
			} else {
				record.DirectPathProjection = pathBatchDirectPathProjection(parsed)
				if parseAssertion(t, tc.Assert).expectQueryError {
					record.ExcludedReason = "expected query error"
				} else {
					ids := opengraph.IDMap{}
					if tc.Fixture != nil {
						for index, node := range tc.Fixture.Nodes {
							ids[node.ID] = graph.ID(index + 1)
						}
					}
					params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, ids)
					translation, err := translate.TranslateWithOptions(context.Background(), parsed, mapper, params, 1,
						translate.Options{OptimizerMode: mode})
					if err != nil {
						record.ExcludedReason = "translation error: " + err.Error()
					} else {
						_, _, record.ExcludedReason = pathBatchSkeleton(translation)
						record.Eligible = record.ExcludedReason == ""
						for _, column := range pathBatchProjectionCandidates(translation) {
							record.IsolatableColumns = append(record.IsolatableColumns, column.index)
						}
						isolatableColumns += len(record.IsolatableColumns)
					}
				}
			}
			inventory = append(inventory, record)
			if record.DirectPathProjection {
				pathProjections++
				if record.Eligible {
					eligiblePaths++
				} else {
					excludedPaths[record.ExcludedReason]++
				}
			}
		}
		t.Logf("optimizer=%s: %d direct bound-path projections, %d candidate-eligible; %d additional isolatable path columns; path exclusions=%v", mode, pathProjections, eligiblePaths, isolatableColumns, excludedPaths)
	}
	pathBatchStudyWrite(t, "corpus-inventory.json", inventory)
}

func pathBatchDirectPathProjection(query *cypher.RegularQuery) bool {
	paths := map[string]bool{}
	pathBatchVisit(query, func(node any) {
		if part, ok := node.(cypher.PatternPart); ok && part.Variable != nil {
			paths[part.Variable.Symbol] = true
		}
	})
	// WITH aliases retain their path type. Repeat until alias propagation is
	// stable so longer multipart chains are included in this inventory.
	for changed := true; changed; {
		changed = false
		pathBatchVisit(query, func(node any) {
			if item, ok := node.(cypher.ProjectionItem); ok && item.Alias != nil {
				if variable, ok := item.Expression.(*cypher.Variable); ok && paths[variable.Symbol] && !paths[item.Alias.Symbol] {
					paths[item.Alias.Symbol], changed = true, true
				}
			}
		})
	}
	if query.SingleQuery == nil {
		return false
	}
	final := query.SingleQuery.SinglePartQuery
	if query.SingleQuery.MultiPartQuery != nil {
		final = query.SingleQuery.MultiPartQuery.SinglePartQuery
	}
	if final == nil || final.Return == nil || final.Return.Projection == nil {
		return false
	}
	if final.Return.Projection.All && len(paths) != 0 {
		return true
	}
	for _, expression := range final.Return.Projection.Items {
		if item, ok := expression.(*cypher.ProjectionItem); ok {
			expression = item.Expression
		}
		if variable, ok := expression.(*cypher.Variable); ok && paths[variable.Symbol] {
			return true
		}
		if variable, ok := expression.(*cypher.Variable); ok && variable.Symbol == cypher.TokenLiteralAsterisk && len(paths) != 0 {
			return true
		}
		if function, ok := expression.(*cypher.FunctionInvocation); ok && strings.EqualFold(function.Name, "shortestPath") {
			return true
		}
	}
	return false
}

// The candidate is compared only where it actually replaces final path
// assembly. Unsupported cases are counted by reason, never run through a
// fallback and reported as candidate successes. Both ordinary case files and
// every rendered template/metamorphic variant are inventoried.
func TestPostgreSQLPathBatchCompleteCorpus(t *testing.T) {
	entries, nodeKinds, edgeKinds := pathBatchCorpus(t)
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		GraphName: "path_batch_corpus", CleanupMode: CloseOnly, ExtraNodeKinds: nodeKinds, ExtraEdgeKinds: edgeKinds})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			compared, rows := 0, 0
			excluded := map[string]int{}
			for _, entry := range entries {
				tc := entry.test
				if parseAssertion(t, tc.Assert).expectQueryError {
					excluded["expected query error"]++
					continue
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
				if err != nil {
					excluded["parse error"]++
					continue
				}
				// Eligibility does not depend on the generated database IDs. Avoid
				// loading a fixture at all for excluded projections.
				dummyIDs := opengraph.IDMap{}
				if tc.Fixture != nil {
					for index, node := range tc.Fixture.Nodes {
						dummyIDs[node.ID] = graph.ID(index + 1)
					}
				}
				params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, dummyIDs)
				translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), params, selected.ID,
					translate.Options{OptimizerMode: mode})
				if err != nil {
					excluded["translation error"]++
					continue
				}
				if _, _, reason := pathBatchSkeleton(translation); reason != "" {
					excluded[reason]++
					continue
				}
				t.Run(entry.source+"/"+tc.Name, func(t *testing.T) {
					require.NotNil(t, tc.Fixture)
					require.NoError(t, session.WithRollbackFixture(t, tc.Fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
						params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, ids)
						parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
						require.NoError(t, err)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), params, selected.ID,
							translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						skeleton, bindings, reason := pathBatchSkeleton(translation)
						require.Empty(t, reason)
						baseline := pathBatchFullValues(t, executeTemplateTranslation(t, tx, translation))
						candidate := pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth), bindings))
						require.Equal(t, baseline, candidate, "complete ordered path values differ: %s", tc.Cypher)
						compared++
						rows += len(candidate)
						return nil
					}))
				})
			}
			require.Positive(t, compared)
			names := make([]string, 0, len(excluded))
			for name := range excluded {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				t.Logf("excluded: %d %s", excluded[name], name)
			}
			t.Logf("complete path comparisons: %d/%d corpus entries, %d result occurrences, optimizer=%s", compared, len(entries), rows, mode)
		})
	}
}

func pathBatchCorpus(t *testing.T) ([]pathBatchCorpusCase, graph.Kinds, graph.Kinds) {
	t.Helper()
	templates := loadCypherTemplateFiles(t)
	nodeKinds, edgeKinds := cypherTemplateKinds(templates)
	var entries []pathBatchCorpusCase
	for _, file := range templates {
		source := filepath.ToSlash(file.path)
		for _, family := range file.Families {
			for _, variant := range family.Variants {
				entries = append(entries, pathBatchCorpusCase{source: source, test: testCase{
					Name: family.Name + "/" + variant.Name, Cypher: renderCypherTemplate(t, family.Template, variant.Vars),
					Params: mergeParams(family.Params, variant.Params), NodeParams: mergeStringMap(family.NodeParams, variant.NodeParams),
					NodeListParams: mergeStringListMap(family.NodeListParams, variant.NodeListParams),
					Assert:         variant.Assert, Fixture: family.Fixture}})
			}
		}
		for _, family := range file.Metamorphic {
			for _, query := range family.Queries {
				entries = append(entries, pathBatchCorpusCase{source: source, test: testCase{
					Name: family.Name + "/" + query.Name, Cypher: query.Cypher, Params: query.Params,
					Assert: json.RawMessage(`"no_error"`), Fixture: family.Fixture}})
			}
		}
	}
	paths, err := filepath.Glob("testdata/cases/*.json")
	require.NoError(t, err)
	datasets := map[string]*opengraph.Graph{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var file caseFile
		require.NoError(t, json.Unmarshal(raw, &file))
		dataset := file.Dataset
		if dataset == "" {
			dataset = "base"
		}
		fixture, found := datasets[dataset]
		if !found {
			input, err := os.Open(datasetPath(dataset))
			require.NoError(t, err)
			document, err := opengraph.ParseDocument(input)
			require.NoError(t, input.Close())
			require.NoError(t, err)
			fixture = &document.Graph
			datasets[dataset] = fixture
		}
		for _, tc := range file.Cases {
			if tc.Fixture == nil {
				tc.Fixture = fixture
			}
			nodes, edges := tc.Fixture.Kinds()
			nodeKinds, edgeKinds = nodeKinds.Add(nodes...), edgeKinds.Add(edges...)
			entries = append(entries, pathBatchCorpusCase{source: filepath.ToSlash(path), test: tc})
		}
	}
	return entries, nodeKinds, edgeKinds
}

func TestPostgreSQLPathBatchSemantics(t *testing.T) {
	fixture := &opengraph.Graph{}
	for _, id := range []string{"root", "a", "b", "isolated"} {
		pathBatchAddNode(fixture, id, 128)
	}
	for _, edge := range [][3]string{{"root", "a", pathBatchEdgeKindA}, {"root", "a", pathBatchEdgeKindB},
		{"a", "b", pathBatchEdgeKindA}, {"b", "a", pathBatchEdgeKindB}, {"a", "a", pathBatchEdgeKindA},
		{"b", "root", pathBatchEdgeKindB}} {
		pathBatchAddEdge(fixture, edge[0], edge[1], edge[2], 128)
	}
	nodes, edges := fixture.Kinds()
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		GraphName: "path_batch_corpus", CleanupMode: CloseOnly, ExtraNodeKinds: nodes, ExtraEdgeKinds: edges})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	queries := map[string]string{
		"outbound parallel edges, self loops, and cycles": "MATCH p=(s)-[:PathBatchA|PathBatchB*1..3]->(t) WHERE id(s)=$root RETURN p",
		"incoming logical order":                          "MATCH p=(t)<-[:PathBatchA|PathBatchB*1..3]-(s) WHERE id(s)=$root RETURN p",
		"undirected fixed hop":                            "MATCH p=(s)-[:PathBatchA|PathBatchB]-(t) WHERE id(s)=$root RETURN p",
		"zero length isolated root":                       "MATCH p=(s)-[:PathBatchA|PathBatchB*0..2]->(t) WHERE id(s)=$isolated RETURN p",
		"mixed direction segments":                        "MATCH p=(s)-[:PathBatchA*1..2]->(m)<-[:PathBatchB*1..2]-(t) WHERE id(s)=$root RETURN p",
		"duplicate carried roots":                         "UNWIND [1,1] AS input MATCH (s) WHERE id(s)=$root WITH s MATCH p=(s)-[:PathBatchA|PathBatchB*0..2]->(t) RETURN p",
		"optional null path":                              "MATCH (s) WHERE id(s)=$isolated OPTIONAL MATCH p=(s)-[:PathBatchA|PathBatchB*1..2]->(t) RETURN p",
		"direct path":                                     "MATCH p=(s)-[:PathBatchA]->(t) WHERE id(s)=$root RETURN p",
	}
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		for name, query := range queries {
			t.Run(string(mode)+"/"+name, func(t *testing.T) {
				require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(),
						map[string]any{"root": ids["root"].Int64(), "isolated": ids["isolated"].Int64()}, selected.ID, translate.Options{OptimizerMode: mode})
					require.NoError(t, err)
					skeleton, bindings, reason := pathBatchSkeleton(translation)
					require.Empty(t, reason, query)
					baseline := pathBatchFullValues(t, executeTemplateTranslation(t, tx, translation))
					candidate := pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth), bindings))
					require.NotEmpty(t, baseline)
					require.Equal(t, baseline, candidate)
					if name != "mixed direction segments" && name != "undirected fixed hop" {
						direction := graph.DirectionOutbound
						if name == "incoming logical order" {
							direction = graph.DirectionInbound
						}
						require.Equal(t, baseline, pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, direction), bindings)),
							"directed fast materializer differs")
					}
					if strings.Contains(name, "null") {
						require.Equal(t, []string{"null"}, candidate)
					}
					return nil
				}))
			})
		}
	}
	// Override precedence is per occurrence, first known value wins. Exercise
	// empty edge arrays alongside NULL paths without depending on translation.
	t.Run("known node overrides, zero length, duplicates, and null", func(t *testing.T) {
		require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
			skeleton := fmt.Sprintf(`select (n.id,n.kind_ids,n.properties)::nodecomposite as root,
array[]::int8[] as edge_ids,
array[(n.id,n.kind_ids,'{"override":"first"}'::jsonb)::nodecomposite,
      (n.id,n.kind_ids,'{"override":"second"}'::jsonb)::nodecomposite]::nodecomposite[] as known_nodes,
v.is_null from node n cross join (values(false),(false),(true)) v(is_null)
where n.graph_id=%d and n.id=%d`, selected.ID, ids["root"])
			baselineSQL := `select case when is_null then null::pathcomposite else ordered_edge_ids_to_path(` + fmt.Sprint(selected.ID) + `,root,edge_ids,known_nodes) end as p from (` + skeleton + `) input`
			baseline := pathBatchFullValues(t, tx.Raw(baselineSQL, nil))
			candidate := pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth), nil))
			require.Len(t, candidate, 3)
			require.Equal(t, baseline, candidate)
			require.Contains(t, strings.Join(candidate, "\n"), "first")
			require.NotContains(t, strings.Join(candidate, "\n"), "second")
			return nil
		}))
	})
	t.Run("graph scoped lookups with colliding entity IDs", func(t *testing.T) {
		require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
			skeleton := fmt.Sprintf(`select (r.id,r.kind_ids,r.properties)::nodecomposite as root,
array[e.id]::int8[] as edge_ids,array[]::nodecomposite[] as known_nodes,false as is_null
from public.node r join public.edge e on e.graph_id=r.graph_id and e.start_id=r.id
where r.graph_id=%d and r.id=%d order by e.id limit 1`, selected.ID, ids["root"])
			baselineSQL := `select ordered_edge_ids_to_path(` + fmt.Sprint(selected.ID) + `,root,edge_ids,known_nodes) as p from (` + skeleton + `) input`
			baseline := pathBatchFullValues(t, tx.Raw(baselineSQL, nil))
			// Read-only CTE relations create graph-local ID collisions without
			// installing a second persistent graph or changing its sequences.
			shadow := fmt.Sprintf(`with node as (
select id,graph_id,kind_ids,properties from public.node where graph_id=%d
union all select id,-1,kind_ids,'{"wrong_graph":true}'::jsonb from public.node where graph_id=%d
), edge as (
select id,graph_id,start_id,end_id,kind_id,properties from public.edge where graph_id=%d
union all select id,-1,start_id,end_id,kind_id,'{"wrong_graph":true}'::jsonb from public.edge where graph_id=%d
) select result.* from (%s) result`, selected.ID, selected.ID, selected.ID, selected.ID,
				pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth))
			candidate := pathBatchFullValues(t, tx.Raw(shadow, nil))
			require.Len(t, candidate, 1)
			require.Equal(t, baseline, candidate)
			require.NotContains(t, strings.Join(candidate, "\n"), "wrong_graph")
			return nil
		}))
	})
}

type pathBatchProjectionCandidate struct {
	index       int
	translation translate.Result
}

// Isolating one path column removes only other final SELECT items. DISTINCT,
// grouping, modifiers, mutations, and earlier path materialization still use
// the main eligibility guard. Other columns' values are not claimed equivalent.
func pathBatchProjectionCandidates(translation translate.Result) []pathBatchProjectionCandidate {
	query, ok := translation.Statement.(pgsql.Query)
	if !ok {
		return nil
	}
	selection, ok := query.Body.(pgsql.Select)
	if !ok || len(selection.Projection) < 2 {
		return nil
	}
	var candidates []pathBatchProjectionCandidate
	for index, item := range selection.Projection {
		isolatedSelection := selection
		isolatedSelection.Projection = pgsql.Projection{item}
		isolatedQuery := query
		isolatedQuery.Body = isolatedSelection
		isolated := translation
		isolated.Statement = isolatedQuery
		if _, _, reason := pathBatchSkeleton(isolated); reason == "" {
			candidates = append(candidates, pathBatchProjectionCandidate{index: index, translation: isolated})
		}
	}
	return candidates
}

// This supplements the single-column corpus comparisons with complete path
// values from the original multi-column query. Only each path column is
// replaced in the candidate; this is not a comparison of the other columns.
func TestPostgreSQLPathBatchProjectedColumns(t *testing.T) {
	entries, nodes, edges := pathBatchCorpus(t)
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		GraphName: "path_batch_corpus", CleanupMode: CloseOnly, ExtraNodeKinds: nodes, ExtraEdgeKinds: edges})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			queries, columns, rows := 0, 0, 0
			for _, entry := range entries {
				tc := entry.test
				if parseAssertion(t, tc.Assert).expectQueryError {
					continue
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
				if err != nil {
					continue
				}
				ids := opengraph.IDMap{}
				if tc.Fixture != nil {
					for index, node := range tc.Fixture.Nodes {
						ids[node.ID] = graph.ID(index + 1)
					}
				}
				params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, ids)
				translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), params, selected.ID, translate.Options{OptimizerMode: mode})
				if err != nil || len(pathBatchProjectionCandidates(translation)) == 0 {
					continue
				}
				t.Run(entry.source+"/"+tc.Name, func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, tc.Fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
						require.NoError(t, err)
						params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, ids)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), params, selected.ID, translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						candidates := pathBatchProjectionCandidates(translation)
						for _, column := range candidates {
							baseline := pathBatchColumnFullValues(t, executeTemplateTranslation(t, tx, translation), column.index)
							skeleton, bindings, reason := pathBatchSkeleton(column.translation)
							require.Empty(t, reason)
							candidate := pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth), bindings))
							require.Equal(t, baseline, candidate, "complete path column %d differs: %s", column.index, tc.Cypher)
							columns++
							rows += len(candidate)
						}
						queries++
						return nil
					}))
				})
			}
			require.Positive(t, columns)
			t.Logf("isolated path-column comparisons: %d queries, %d columns, %d result occurrences, optimizer=%s", queries, columns, rows, mode)
		})
	}
}

func pathBatchColumnFullValues(t *testing.T, result graph.Result, column int) []string {
	t.Helper()
	defer result.Close()
	values := []string{}
	for result.Next() {
		require.Greater(t, len(result.Values()), column)
		raw := result.Values()[column]
		if pathBatchNull(raw) {
			values = append(values, "null")
			continue
		}
		var path graph.Path
		require.True(t, result.Mapper().Map(raw, &path), "cannot map %T to path", raw)
		values = append(values, pathBatchFullValue(t, path))
	}
	require.NoError(t, result.Error())
	sort.Strings(values)
	return values
}

// Protect the experiment's boundaries: candidate coverage must not silently
// include a query which retained its old per-path materializer.
func TestPostgreSQLPathBatchEligibility(t *testing.T) {
	path := pgsql.FunctionCall{Function: pgsql.FunctionOrderedEdgeIDsToPath, CastType: pgsql.PathComposite,
		Parameters: []pgsql.Expression{pgsql.NewLiteral(int32(1), pgsql.Int4),
			pgsql.CompositeValue{DataType: pgsql.NodeComposite, Values: []pgsql.Expression{
				pgsql.NewLiteral(int64(1), pgsql.Int8), pgsql.ArrayLiteral{CastType: pgsql.Int2Array}, pgsql.NullLiteral()}},
			pgsql.ArrayLiteral{CastType: pgsql.Int8Array}, pgsql.ArrayLiteral{CastType: pgsql.NodeCompositeArray}}}
	base := pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{path}}}
	for _, tc := range []struct {
		name, reason string
		query        pgsql.Query
		stages       []translate.QueryStage
	}{
		{name: "plain final path", query: base},
		{name: "final limit", query: pgsql.Query{Body: base.Body, Limit: pgsql.NewLiteral(1, pgsql.Int8)}, reason: "final LIMIT/OFFSET/ORDER BY"},
		{name: "distinct", query: pgsql.Query{Body: pgsql.Select{Distinct: true, Projection: pgsql.Projection{path}}}, reason: "final DISTINCT/grouping"},
		{name: "two columns", query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{path, path}}}, reason: "multiple result columns"},
		{name: "set operation", query: pgsql.Query{Body: pgsql.SetOperation{}}, reason: "set operation"},
		{name: "mutation stages", query: base, stages: []translate.QueryStage{{}}, reason: "multipart mutation"},
		{name: "earlier materialization", query: pgsql.Query{Body: base.Body,
			CommonTableExpressions: &pgsql.With{Expressions: []pgsql.CommonTableExpression{{Alias: pgsql.TableAlias{Name: "earlier"}, Query: base}}}},
			reason: "earlier/additional path materialization"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skeleton, _, reason := pathBatchSkeleton(translate.Result{Statement: tc.query, Stages: tc.stages, GraphID: 1})
			require.Equal(t, tc.reason, reason)
			if reason == "" {
				require.NotEmpty(t, skeleton)
				require.NotContains(t, skeleton, "ordered_edge_ids_to_path")
			}
		})
	}
}
