//go:build manual_integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

type pathBatchPageCoverage struct {
	Mode       translate.OptimizerMode `json:"mode"`
	Source     string                  `json:"source"`
	Name       string                  `json:"name"`
	Comparison string                  `json:"comparison"`
	Reason     string                  `json:"excluded_reason,omitempty"`
	Rows       int                     `json:"rows,omitempty"`
}

// An unordered LIMIT may choose any valid subset. Truncating pages therefore
// check complete-value membership, row count, and duplicate multiplicity.
// Equality is asserted only when the page includes the entire result multiset.
func TestPostgreSQLPathBatchCorpusPages(t *testing.T) {
	entries, nodes, edges := pathBatchCorpus(t)
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		GraphName: "path_batch_corpus", CleanupMode: CloseOnly, ExtraNodeKinds: nodes, ExtraEdgeKinds: edges})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	var coverage []pathBatchPageCoverage
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			equal, membership, rows := 0, 0, 0
			for _, entry := range entries {
				tc := entry.test
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
				if err != nil || !pathBatchDirectPathProjection(parsed) {
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
				if err != nil {
					continue
				}
				query, ok := translation.Statement.(pgsql.Query)
				if !ok || query.Limit == nil && query.Offset == nil && len(query.OrderBy) == 0 {
					continue
				}
				record := pathBatchPageCoverage{Mode: mode, Source: entry.source, Name: tc.Name, Comparison: "excluded"}
				if parseAssertion(t, tc.Assert).expectQueryError {
					record.Reason = "expected query error"
				} else if _, _, reason := pathBatchLimitedSkeleton(translation); reason != "" {
					record.Reason = reason
				} else if len(query.OrderBy) != 0 {
					record.Reason = "corpus sort does not establish a proven total path order"
				} else if _, _, valid := pathBatchPageBounds(query, translation.Parameters); !valid {
					record.Reason = "page bounds are not nonnegative integer literals/parameters"
				}
				if record.Reason != "" {
					coverage = append(coverage, record)
					t.Logf("page excluded: %s/%s: %s", entry.source, tc.Name, record.Reason)
					continue
				}
				t.Run(entry.source+"/"+tc.Name, func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, tc.Fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.Cypher)
						require.NoError(t, err)
						params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, ids)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), params, selected.ID, translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						query := translation.Statement.(pgsql.Query)
						limit, offset, valid := pathBatchPageBounds(query, translation.Parameters)
						require.True(t, valid)
						unlimited := translation
						query.Limit, query.Offset = nil, nil
						unlimited.Statement = query
						allowed := pathBatchFullValues(t, executeTemplateTranslation(t, tx, unlimited))
						originalPage := pathBatchFullValues(t, executeTemplateTranslation(t, tx, translation))
						skeleton, bindings, reason := pathBatchLimitedSkeleton(translation)
						require.Empty(t, reason)
						candidatePage := pathBatchFullValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth), bindings))
						want := max(int64(len(allowed))-offset, 0)
						if limit != nil {
							want = min(want, *limit)
						}
						require.Len(t, originalPage, int(want))
						require.Len(t, candidatePage, int(want))
						pathBatchRequireSubmultiset(t, allowed, originalPage)
						pathBatchRequireSubmultiset(t, allowed, candidatePage)
						if offset == 0 && want == int64(len(allowed)) {
							require.Equal(t, allowed, originalPage)
							require.Equal(t, allowed, candidatePage)
							record.Comparison = "equal complete multiset (nontruncating)"
							equal++
						} else {
							record.Comparison = "complete-value multiset membership and page count"
							membership++
						}
						record.Rows = len(candidatePage)
						rows += len(candidatePage)
						coverage = append(coverage, record)
						return nil
					}))
				})
			}
			require.Positive(t, equal+membership)
			t.Logf("source-limited corpus pages: %d complete-multiset equal, %d truncating membership/count, %d path occurrences, optimizer=%s", equal, membership, rows, mode)
		})
	}
	pathBatchStudyWrite(t, "corpus-pages.json", coverage)
}

func pathBatchPageBounds(query pgsql.Query, parameters map[string]any) (*int64, int64, bool) {
	var limit *int64
	if query.Limit != nil {
		value, ok := pathBatchSelectionInteger(query.Limit, parameters)
		if !ok || value < 0 {
			return nil, 0, false
		}
		limit = &value
	}
	var offset int64
	if query.Offset != nil {
		value, ok := pathBatchSelectionInteger(query.Offset, parameters)
		if !ok || value < 0 {
			return nil, 0, false
		}
		offset = value
	}
	return limit, offset, true
}

func pathBatchSelectionInteger(expression pgsql.Expression, parameters map[string]any) (int64, bool) {
	switch value := expression.(type) {
	case pgsql.Literal:
		if !value.Null {
			return asInt64(value.Value)
		}
	case pgsql.Parameter:
		return asInt64(parameters[value.Identifier.String()])
	case *pgsql.Parameter:
		return asInt64(parameters[value.Identifier.String()])
	}
	return 0, false
}

func pathBatchRequireSubmultiset(t *testing.T, allowed, selected []string) {
	t.Helper()
	counts := map[string]int{}
	for _, value := range allowed {
		counts[value]++
	}
	for _, value := range selected {
		require.Positive(t, counts[value], "page contains an invalid complete path or excessive duplicate: %s", value)
		counts[value]--
	}
}

func TestPostgreSQLPathBatchOrderedPages(t *testing.T) {
	fixture, _ := pathBatchSharedFixture(2, 128)
	nodes, edges := fixture.Kinds()
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		GraphName: "path_batch_corpus", CleanupMode: CloseOnly, ExtraNodeKinds: nodes, ExtraEdgeKinds: edges})
	driver := session.DB.(*pg.Driver)
	selected, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		for _, page := range [][2]int{{0, 1}, {1, 2}, {100, 1}} {
			t.Run(fmt.Sprintf("%s/skip=%d/limit=%d", mode, page[0], page[1]), func(t *testing.T) {
				require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
					// Anchored outgoing one-hop paths have one distinct edge ID per
					// path. ORDER BY id(r) therefore gives a total order here.
					query := fmt.Sprintf("MATCH p=(s)-[r:PathBatchA|PathBatchB]->(t) WHERE id(s)=$root RETURN p ORDER BY id(r) SKIP %d LIMIT %d", page[0], page[1])
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(),
						map[string]any{"root": ids["root"].Int64()},
						selected.ID, translate.Options{OptimizerMode: mode})
					require.NoError(t, err)
					original := pathBatchPageOrderedValues(t, executeTemplateTranslation(t, tx, translation))
					skeleton, bindings, reason := pathBatchLimitedSkeleton(translation)
					require.Empty(t, reason)
					candidate := pathBatchPageOrderedValues(t, tx.Raw(pathBatchSQL(skeleton, selected.ID, graph.DirectionBoth)+" order by p.page_rank", bindings))
					require.Equal(t, original, candidate)
					require.Len(t, candidate, min(max(4-page[0], 0), page[1]))
					return nil
				}))
			})
		}
	}
}

func pathBatchPageOrderedValues(t *testing.T, result graph.Result) []string {
	t.Helper()
	defer result.Close()
	values := []string{}
	for result.Next() {
		require.Len(t, result.Values(), 1)
		raw := result.Values()[0]
		if pathBatchNull(raw) {
			values = append(values, "null")
			continue
		}
		var path graph.Path
		require.True(t, result.Mapper().Map(raw, &path), "cannot map %T to path", raw)
		values = append(values, pathBatchFullValue(t, path))
	}
	require.NoError(t, result.Error())
	return values
}

func TestPostgreSQLPathBatchLimitedEligibility(t *testing.T) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p=(s)-[r]->(t) RETURN p LIMIT 1")
	require.NoError(t, err)
	translation, err := translate.Translate(context.Background(), parsed, pgutil.NewInMemoryKindMapper(), nil, 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		orderBy *pgsql.OrderBy
		allowed bool
	}{
		{name: "limit before hydration", allowed: true},
		{name: "path result alias", orderBy: &pgsql.OrderBy{Expression: pgsql.Identifier("p")}},
		{name: "path-consuming or volatile function", orderBy: &pgsql.OrderBy{Expression: pgsql.FunctionCall{Function: "random", CastType: pgsql.Float8}}},
		{name: "positional path ordering", orderBy: &pgsql.OrderBy{Expression: pgsql.NewLiteral(1, pgsql.Int4)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := translation
			query := translation.Statement.(pgsql.Query)
			if tc.orderBy != nil {
				query.OrderBy = []*pgsql.OrderBy{tc.orderBy}
			}
			candidate.Statement = query
			_, _, reason := pathBatchLimitedSkeleton(candidate)
			if tc.allowed {
				require.Empty(t, reason)
			} else {
				require.NotEmpty(t, reason)
			}
			_, _, defaultReason := pathBatchSkeleton(candidate)
			require.Equal(t, "final LIMIT/OFFSET/ORDER BY", defaultReason)
		})
	}
}
