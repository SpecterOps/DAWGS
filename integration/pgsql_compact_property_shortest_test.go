//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

const (
	// compactPropertyShortestCorpusName identifies the shared endpoint-resolution regression family.
	compactPropertyShortestCorpusName = "compact_property_shortest_shapes"

	// compactPropertyShortestCorpusPath selects the fixtures committed before public read queries.
	compactPropertyShortestCorpusPath = "testdata/templates/compact_property_shortest_shapes.json"

	// selectCompactV2PreparedStatementsSQL proves compact SQL ran on the leased session after query cleanup.
	selectCompactV2PreparedStatementsSQL = `
		SELECT count(*)
		FROM pg_prepared_statements
		WHERE statement LIKE '%all_shortest_paths_dag_v2(%'
		  AND statement NOT LIKE '%pg_prepared_statements%'`
)

// TestPostgreSQLCompactPropertyShortestCompilerModes keeps the optimized
// property-anchor lowering equivalent to the complete traversal implementation.
func TestPostgreSQLCompactPropertyShortestCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, compactPropertyShortestCorpusName)
}

// TestPostgreSQLCompactPropertyShortestDefaultQueries exercises the public
// query entry point without asking callers to set an isolation level. Fixtures
// are committed before each read so the query can own its snapshot lifetime.
func TestPostgreSQLCompactPropertyShortestDefaultQueries(t *testing.T) {
	var corpus cypherTemplateFile

	for _, file := range loadCypherTemplateFiles(t) {
		if file.path == compactPropertyShortestCorpusPath {
			corpus = file
			break
		}
	}
	require.NotEmpty(t, corpus.Families)

	var (
		nodeKinds, edgeKinds = cypherTemplateKinds([]cypherTemplateFile{corpus})
		session              = Open(t, Options{
			RequireDriver:        pg.DriverName,
			SkipIfNoConnection:   true,
			SkipIfDriverMismatch: true,
			CleanupMode:          CleanupGraph,
			ExtraNodeKinds:       nodeKinds,
			ExtraEdgeKinds:       edgeKinds,
		})
		previous = pg.SetOptimizedTranslation(true)
	)
	t.Cleanup(func() { pg.SetOptimizedTranslation(previous) })

	for _, family := range corpus.Families {
		for _, variant := range family.Variants {
			query := renderCypherTemplate(t, family.Template, variant.Vars)
			parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
			require.NoError(t, err)

			if !translate.UsesBudgetedAllShortestPaths(parsed) {
				continue
			}

			t.Run(family.Name+"/"+variant.Name, func(t *testing.T) {
				var idMap opengraph.IDMap
				require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
					if err := tx.Nodes().Delete(); err != nil {
						return err
					}

					var err error
					idMap, err = opengraph.WriteGraphTx(tx, family.Fixture)
					return err
				}))

				parameters := resolveFixtureParams(t, mergeParams(family.Params, variant.Params), mergeStringMap(family.NodeParams, variant.NodeParams), mergeStringListMap(family.NodeListParams, variant.NodeListParams), idMap)
				require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
					result := tx.Query(query, parameters)
					parseAssertion(t, variant.Assert).checkResult(t, result, newAssertionContext(idMap))
					result.Close()

					if err := result.Error(); err != nil {
						return err
					}

					// Prepared SQL belongs to the same leased physical connection,
					// even after the query-owned transaction has completed.
					executed := tx.Raw(selectCompactV2PreparedStatementsSQL, nil)
					defer executed.Close()

					require.True(t, executed.Next())

					var compactStatements int64
					require.NoError(t, graph.ScanNextResult(executed, &compactStatements))
					require.Positive(t, compactStatements, "the ordinary query must execute the compact SQL")

					return executed.Error()
				}))
			})
		}
	}
}
