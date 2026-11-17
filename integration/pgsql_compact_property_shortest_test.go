//go:build manual_integration

package integration

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	// selectCompactPropertyShortestBudgetSQL reads cumulative admissions inside the caller-owned stable transaction.
	selectCompactPropertyShortestBudgetSQL = `
		SELECT current_setting('dawgs.traversal_states')::bigint,
		       current_setting('dawgs.traversal_bytes')::bigint`

	// compactPropertyShortestLegacyControlCypher preserves the same disjoint endpoint results while requiring correlated-pair fallback.
	compactPropertyShortestLegacyControlCypher = `
		MATCH p=allShortestPaths((s)-[*1..]->(t))
		WHERE s.name=$source AND t.objectid=$target AND s.name<>t.objectid
		RETURN p`

	// compactPropertyShortestSelfEndpointSQLState identifies the established PostgreSQL error for forbidden self-endpoint searches.
	compactPropertyShortestSelfEndpointSQLState = "22023"
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
			"missing source":                                            true,
			"missing target":                                            true,
			"string predicate rejects JSON boolean":                     true,
			"string predicate rejects JSON number":                      true,
			"IDs and residual property predicates":                      true,
			"incoming search preserves logical path order":              true,
			"inline property anchors":                                   true,
			"property guards remain attached to ID anchors":             true,
			"SET targets preserves result and post-state":               false,
			"SET targets without RETURN":                                false,
			"CREATE observes path multiplicity":                         false,
			"CREATE without RETURN":                                     false,
			"staged WITH and SET preserve endpoint selection":           false,
			"NOT endpoint equality retains fallback":                    false,
			"mixed OR endpoint inequality retains fallback":             false,
			"endpoint property correlation retains fallback":            false,
			"later WITH endpoint inequality preserves disjoint results": true,
		},
		"overlapping endpoint sets with explicit inequality": {
			"outgoing inequality excludes overlapping self pairs":         true,
			"outgoing inequality reversed operands":                       true,
			"outgoing inequality excludes the only self pair":             true,
			"incoming inequality excludes overlapping self pairs":         true,
			"incoming inequality reversed operands":                       true,
			"incoming inequality excludes the only self pair":             true,
			"outgoing parenthesized inequality":                           true,
			"outgoing parenthesized ID inequality":                        true,
			"outgoing parenthesized reversed inequality":                  true,
			"outgoing inequality preserves scalar multiplicity":           true,
			"incoming parenthesized inequality":                           true,
			"incoming parenthesized ID inequality":                        true,
			"incoming parenthesized reversed inequality":                  true,
			"incoming inequality preserves scalar multiplicity":           true,
			"SET with endpoint inequality preserves paths and post-state": false,
			"SET with endpoint inequality without RETURN":                 false,
			"CREATE with endpoint inequality preserves multiplicity":      false,
			"CREATE with endpoint inequality without RETURN":              false,
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

// TestPostgreSQLCompactPropertyShortestSelfPairErrors checks early self-pair exclusion and preserves existing errors for unsupported or later filters.
func TestPostgreSQLCompactPropertyShortestSelfPairErrors(t *testing.T) {
	var (
		corpus = loadCompactPropertyShortestCorpus(t)
		family cypherTemplateFamily
	)
	for _, candidate := range corpus.Families {
		if candidate.Name == "overlapping endpoint sets with explicit inequality" {
			family = candidate
			break
		}
	}
	require.NotNil(t, family.Fixture)

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
		pgDriver         = session.DB.(*pg.Driver)
		defaultGraph, ok = pgDriver.DefaultGraph()
	)
	require.True(t, ok)

	for _, variant := range family.Variants {
		if !strings.HasSuffix(variant.Name, "excludes the only self pair") {
			continue
		}

		t.Run(variant.Name+" before pair admission", func(t *testing.T) {
			require.NoError(t, session.WithRollbackFixture(t, family.Fixture, true, func(tx graph.Transaction, idMap opengraph.IDMap) error {
				parameters := resolveFixtureParams(t, mergeParams(family.Params, variant.Params), nil, nil, idMap)
				result := tx.Query(renderCypherTemplate(t, family.Template, variant.Vars), parameters)
				parseAssertion(t, variant.Assert).checkResult(t, result, newAssertionContext(idMap))
				result.Close()
				if err := result.Error(); err != nil {
					return err
				}

				budget := tx.Raw(selectCompactPropertyShortestBudgetSQL, nil)
				require.True(t, budget.Next())

				var (
					states int64
					bytes  int64
				)
				require.NoError(t, graph.ScanNextResult(budget, &states, &bytes))
				budget.Close()
				require.NoError(t, budget.Error())
				require.Equal(t, int64(2), states, "excluded self pairs must not consume pair or search states")
				require.Equal(t, int64(128), bytes, "only the two endpoint memberships are admitted")
				return nil
			}, pg.OptionSetTransactionIsolation(pgx.RepeatableRead)))
		})
	}

	for _, boundary := range []struct {
		// name identifies the predicate placement whose error boundary must remain unchanged.
		name string

		// query retains overlapping endpoints until the established self-pair check executes.
		query string

		// compact records eligibility independently of the production selector.
		compact bool
	}{
		{
			name:    "NOT equality remains a fallback with the existing self-pair error",
			query:   "MATCH p=allShortestPaths((s)-[:CompactA*1..]->(t)) WHERE s.name=$source AND t.objectid=$target AND NOT(id(s)=id(t)) RETURN p",
			compact: false,
		},
		{
			name:    "later WITH cannot remove the preceding self-pair error",
			query:   "MATCH p=allShortestPaths((s)-[:CompactA*1..]->(t)) WHERE s.name=$source AND t.objectid=$target WITH p,s,t WHERE id(s)<>id(t) RETURN p",
			compact: true,
		},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), boundary.query)
			require.NoError(t, err)
			require.Equal(t, boundary.compact, translate.UsesBudgetedAllShortestPaths(parsed))

			for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
				t.Run(string(mode), func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, family.Fixture, true, func(tx graph.Transaction, idMap opengraph.IDMap) error {
						parameters := resolveFixtureParams(t, family.Params, family.NodeParams, family.NodeListParams, idMap)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, pgDriver.KindMapper(), parameters, defaultGraph.ID,
							translate.Options{
								OptimizerMode:                   mode,
								DisableBudgetedAllShortestPaths: mode == translate.OptimizerDisabled,
							})
						require.NoError(t, err)

						formatted, err := translate.Translated(translation)
						require.NoError(t, err)
						require.Equal(t, boundary.compact && mode == translate.OptimizerEnabled,
							strings.Contains(formatted.Statement, "all_shortest_paths_dag_v2("))

						result := executeTemplateTranslation(t, tx, translation)
						for result.Next() {
						}
						result.Close()

						var pgError *pgconn.PgError
						require.ErrorAs(t, result.Error(), &pgError)
						require.Equal(t, compactPropertyShortestSelfEndpointSQLState, pgError.Code)
						return nil
					}, pg.OptionSetTransactionIsolation(pgx.RepeatableRead)))
				})
			}
		})
	}
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

					var (
						result    = tx.Query(query, parameters)
						assertion = parseAssertion(t, variant.Assert)
					)
					assertion.checkResult(t, result, newAssertionContext(idMap))
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
