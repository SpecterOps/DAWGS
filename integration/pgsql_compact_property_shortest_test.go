//go:build manual_integration

package integration

import (
	"strings"
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

	// selectCompactV2PreparedExecutionsSQL snapshots execution-plan uses for each exact compact statement on the leased session.
	selectCompactV2PreparedExecutionsSQL = `
		SELECT statement, generic_plans + custom_plans
		FROM pg_prepared_statements
		WHERE statement LIKE '%all_shortest_paths_dag_v2(%'
		  AND statement NOT LIKE '%pg_prepared_statements%'`

	// compactPropertyShortestLegacyControlCypher preserves the same disjoint endpoint results while requiring correlated-pair fallback.
	compactPropertyShortestLegacyControlCypher = `
		MATCH p=allShortestPaths((s)-[*1..]->(t))
		WHERE s.name=$source AND t.objectid=$target AND id(s)<>id(t)
		RETURN p`
)

// loadCompactPropertyShortestCorpus retrieves the shared fixtures without depending on backend eligibility.
func loadCompactPropertyShortestCorpus(t *testing.T) cypherTemplateFile {
	t.Helper()

	for _, file := range loadCypherTemplateFiles(t) {
		if file.path == compactPropertyShortestCorpusPath {
			require.NotEmpty(t, file.Families)
			return file
		}
	}

	t.Fatalf("template corpus %q is missing", compactPropertyShortestCorpusPath)
	return cypherTemplateFile{}
}

// requireCompactPropertyShortestInventory checks every fixture against an independent compact-or-fallback expectation before live execution.
func requireCompactPropertyShortestInventory(t *testing.T, corpus cypherTemplateFile) map[string]map[string]bool {
	t.Helper()

	expectedVariants := map[string]map[string]bool{
		"property endpoint resolution and multiplicity": {
			"all matching endpoint pairs retain pair-specific distance":   true,
			"scalar path observations retain all shortest multiplicities": true,
			"explicit bound above 64":                                     true,
			"typed union retains parallel alternatives":                   true,
			"single relationship kind changes qualifying path set":        true,
			"missing source":                                  true,
			"missing target":                                  true,
			"string predicate rejects JSON boolean":           true,
			"string predicate rejects JSON number":            true,
			"IDs and residual property predicates":            true,
			"incoming search preserves logical path order":    true,
			"inline property anchors":                         true,
			"property guards remain attached to ID anchors":   true,
			"SET targets preserves result and post-state":     false,
			"SET targets without RETURN":                      false,
			"CREATE observes path multiplicity":               false,
			"CREATE without RETURN":                           false,
			"staged WITH and SET preserve endpoint selection": false,
		},
		"overlapping endpoint sets with explicit inequality": {
			"outgoing inequality excludes overlapping self pairs": false,
			"outgoing inequality reversed operands":               false,
			"outgoing inequality excludes the only self pair":     false,
			"incoming inequality excludes overlapping self pairs": false,
			"incoming inequality reversed operands":               false,
			"incoming inequality excludes the only self pair":     false,
		},
		"open property anchors have no policy hop ceiling": {
			"open property anchors reach 15 hops":           true,
			"open property anchors reach 16 hops":           true,
			"open property anchors reach 17 hops":           true,
			"open property anchors reach 63 hops":           true,
			"open property anchors reach 64 hops":           true,
			"open property anchors reach 65 hops":           true,
			"open property anchors reach 128 hops":          true,
			"open property anchors reach 129 hops":          true,
			"explicit bound still excludes a deeper target": true,
		},
	}

	var (
		compactVariants int
		seenFamilies    = map[string]bool{}
	)

	require.Len(t, corpus.Families, len(expectedVariants))
	for _, family := range corpus.Families {
		familyExpectations, knownFamily := expectedVariants[family.Name]
		require.True(t, knownFamily, "family %q needs an independent eligibility expectation", family.Name)
		require.False(t, seenFamilies[family.Name], "duplicate fixture family %q", family.Name)
		seenFamilies[family.Name] = true
		require.Len(t, family.Variants, len(familyExpectations))

		seenVariants := map[string]bool{}
		for _, variant := range family.Variants {
			expectedCompact, knownVariant := familyExpectations[variant.Name]
			require.True(t, knownVariant, "variant %q needs an independent eligibility expectation", variant.Name)
			require.False(t, seenVariants[variant.Name], "duplicate fixture variant %q", variant.Name)
			seenVariants[variant.Name] = true

			require.NotEmpty(t, strings.TrimSpace(string(variant.Assert)), "variant %q must assert its result", variant.Name)
			require.NotEqual(t, "null", strings.TrimSpace(string(variant.Assert)), "variant %q must assert its result", variant.Name)

			query := renderCypherTemplate(t, family.Template, variant.Vars)
			parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
			require.NoError(t, err)
			require.Equal(t, expectedCompact, translate.UsesBudgetedAllShortestPaths(parsed), "eligibility changed for %s/%s", family.Name, variant.Name)

			if expectedCompact {
				compactVariants++
			}
		}
	}
	require.Positive(t, compactVariants, "the inventory must exercise public compact queries")

	return expectedVariants
}

// readCompactV2PreparedExecutions captures per-statement counters so retained SQL alone cannot prove a later invocation used compact search.
func readCompactV2PreparedExecutions(tx graph.Transaction) (map[string]int64, error) {
	result := tx.Raw(selectCompactV2PreparedExecutionsSQL, nil)
	defer result.Close()

	executions := map[string]int64{}
	for result.Next() {
		var (
			statement string
			count     int64
		)
		if err := graph.ScanNextResult(result, &statement, &count); err != nil {
			return nil, err
		}

		executions[statement] = count
	}

	return executions, result.Error()
}

// compactV2PreparedExecutionDelta counts new uses of exact V2 statements while ignoring entries evicted between snapshots.
func compactV2PreparedExecutionDelta(before map[string]int64, after map[string]int64) int64 {
	var executions int64
	for statement, count := range after {
		if delta := count - before[statement]; delta > 0 {
			executions += delta
		}
	}
	return executions
}

// TestPostgreSQLCompactPropertyShortestCompilerModes keeps the optimized
// property-anchor lowering equivalent to the complete traversal implementation.
func TestPostgreSQLCompactPropertyShortestCompilerModes(t *testing.T) {
	requireCompactPropertyShortestInventory(t, loadCompactPropertyShortestCorpus(t))
	runPostgreSQLTemplateCompilerModes(t, compactPropertyShortestCorpusName)
}

// TestPostgreSQLCompactPropertyShortestDefaultQueries exercises the public
// query entry point without asking callers to set an isolation level. Fixtures
// are committed before each read so the query can own its snapshot lifetime.
func TestPostgreSQLCompactPropertyShortestDefaultQueries(t *testing.T) {
	var (
		corpus               = loadCompactPropertyShortestCorpus(t)
		expectedVariants     = requireCompactPropertyShortestInventory(t, corpus)
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

	var executedQueries int

	for _, family := range corpus.Families {
		for _, variant := range family.Variants {
			query := renderCypherTemplate(t, family.Template, variant.Vars)
			if !expectedVariants[family.Name][variant.Name] {
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
					before, err := readCompactV2PreparedExecutions(tx)
					if err != nil {
						return err
					}

					result := tx.Query(query, parameters)
					parseAssertion(t, variant.Assert).checkResult(t, result, newAssertionContext(idMap))
					result.Close()

					if err := result.Error(); err != nil {
						return err
					}

					after, err := readCompactV2PreparedExecutions(tx)
					if err != nil {
						return err
					}
					require.Equal(t, int64(1), compactV2PreparedExecutionDelta(before, after), "this invocation must execute compact SQL")
					executedQueries++

					if variant.Name == "all matching endpoint pairs retain pair-specific distance" {
						legacy := tx.Query(compactPropertyShortestLegacyControlCypher, parameters)
						parseAssertion(t, variant.Assert).checkResult(t, legacy, newAssertionContext(idMap))
						legacy.Close()
						if err := legacy.Error(); err != nil {
							return err
						}

						afterLegacy, err := readCompactV2PreparedExecutions(tx)
						if err != nil {
							return err
						}
						require.NotEmpty(t, afterLegacy, "the control must retain earlier compact SQL")
						require.Zero(t, compactV2PreparedExecutionDelta(after, afterLegacy), "retained compact SQL must not qualify a fallback invocation")
					}

					return nil
				}))
			})
		}
	}
	require.Positive(t, executedQueries, "the test must execute public compact queries")
}
