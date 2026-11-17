//go:build manual_integration

package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

const (
	// compactEndpointInequalityQuery excludes self pairs before open compact search while retaining property-based endpoint lookup.
	compactEndpointInequalityQuery = `MATCH p=allShortestPaths((s)-[:CompactSearchStep*]->(t)) WHERE s.name=$source AND t.objectid=$target AND id(s)<>id(t) RETURN p`

	// compactEndpointReversedInequalityQuery exercises a parenthesized comparison with the endpoint operands reversed.
	compactEndpointReversedInequalityQuery = `MATCH p=allShortestPaths((s)-[:CompactSearchStep*]->(t)) WHERE s.name=$source AND t.objectid=$target AND ((id(t))<>(id(s))) RETURN p`

	// compactEndpointUnguardedQuery retains the existing self-endpoint error when no inequality excludes the pair.
	compactEndpointUnguardedQuery = `MATCH p=allShortestPaths((s)-[:CompactSearchStep*]->(t)) WHERE s.name=$source AND t.objectid=$target RETURN p`

	// compactEndpointSelfProperty identifies one isolated node from both endpoint relations.
	compactEndpointSelfProperty = "inequality-self"

	// compactEndpointOverlapSource matches the source-only and shared isolated nodes.
	compactEndpointOverlapSource = "inequality-overlap-source"

	// compactEndpointOverlapTarget matches the target-only and shared isolated nodes.
	compactEndpointOverlapTarget = "inequality-overlap-target"
)

// compactEndpointInequalityFixture combines complete path anchors with isolated self and overlapping endpoint relations.
type compactEndpointInequalityFixture struct {
	// driver owns the isolated lifecycle graph and its one physical connection.
	driver *Driver

	// distinct selects the existing two-branch diamond.
	distinct compactBuilderEndpointPair

	// same selects one isolated node from both endpoint relations.
	same compactBuilderEndpointPair
}

// setupCompactEndpointInequalityDriver adds isolated overlapping anchors to the existing driver fixture.
func setupCompactEndpointInequalityDriver(t *testing.T) compactEndpointInequalityFixture {
	t.Helper()
	fixture := compactEndpointInequalityFixture{
		driver: setupCompactSearchDriver(t, 1),
	}
	require.NoError(t, fixture.driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		if endpoints, err := compactBuilderEndpoints(tx); err != nil {
			return err
		} else if self, err := tx.CreateNode(graph.NewProperties().Set("name", compactEndpointSelfProperty).Set("objectid", compactEndpointSelfProperty), graph.StringKind("CompactSearchNode")); err != nil {
			return err
		} else {
			fixture.distinct = endpoints
			fixture.same = compactBuilderEndpointPair{
				source: self,
				target: self,
			}
		}

		for _, properties := range []*graph.Properties{
			graph.NewProperties().Set("name", compactEndpointOverlapSource),
			graph.NewProperties().Set("name", compactEndpointOverlapSource).Set("objectid", compactEndpointOverlapTarget),
			graph.NewProperties().Set("objectid", compactEndpointOverlapTarget),
		} {
			if _, err := tx.CreateNode(properties, graph.StringKind("CompactSearchNode")); err != nil {
				return err
			}
		}
		return nil
	}))
	return fixture
}

// compactEndpointInequalityMode describes one public entry point for the same endpoint-pair predicate.
type compactEndpointInequalityMode struct {
	// name labels the public query form.
	name string

	// statement supplies the text entry point's complete Cypher query.
	statement string

	// builder selects the relationship-builder API instead of raw Cypher text.
	builder bool
}

// compactEndpointInequalityModes enumerates both text orientations and the expression-preserving builder form.
func compactEndpointInequalityModes() []compactEndpointInequalityMode {
	return []compactEndpointInequalityMode{
		{
			name:      "text angle inequality",
			statement: compactEndpointInequalityQuery,
		},
		{
			name:      "text reversed parenthesized inequality",
			statement: compactEndpointReversedInequalityQuery,
		},
		{
			name:    "builder inequality",
			builder: true,
		},
	}
}

// consumeCompactEndpointInequality validates complete paths through one public entry point and its selected compact lifecycle.
func consumeCompactEndpointInequality(t *testing.T, tx graph.Transaction, mode compactEndpointInequalityMode, endpoints compactBuilderEndpointPair, count int, compact, owned bool) error {
	t.Helper()
	owner := tx.(*transaction)
	checkPath := func(path graph.Path) {
		require.Len(t, path.Edges, 2)
		require.Equal(t, endpoints.source.ID, path.Root().ID)
		require.Equal(t, endpoints.target.ID, path.Terminal().ID)
	}
	if mode.builder {
		return tx.Relationships().Filter(query.And(
			query.Equals(query.StartID(), endpoints.source.ID),
			query.Equals(query.EndID(), endpoints.target.ID),
			query.KindIn(query.Relationship(), graph.StringKind("CompactSearchStep")),
			cypher.NewComparison(query.StartID(), cypher.OperatorNotEquals, query.EndID()),
		)).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			_, compactCursor := cursor.(*compactPathCursor)
			require.Equal(t, compact, compactCursor)
			if count > 0 {
				require.Equal(t, owned, owner.compactSearch != nil)
				if owned {
					require.Equal(t, pgx.RepeatableRead, owner.isolation)
				}
			}

			actual := 0
			for path := range cursor.Chan() {
				checkPath(path)
				actual++
			}
			require.Equal(t, count, actual)
			return cursor.Error()
		})
	}

	result := tx.Query(mode.statement, map[string]any{
		"source": endpoints.source.Properties.Map["name"],
		"target": endpoints.target.Properties.Map["objectid"],
	})
	defer result.Close()
	require.NoError(t, result.Error())
	require.Equal(t, owned, owner.compactSearch != nil)
	if owned {
		require.Equal(t, pgx.RepeatableRead, owner.isolation)
	}
	actual := 0
	for result.Next() {
		var path graph.Path
		require.NoError(t, graph.ScanNextResult(result, &path))
		checkPath(path)
		actual++
	}
	require.Equal(t, count, actual)
	return result.Error()
}

// TestCompactEndpointInequalityDriverOwnsSnapshotAndRebindsWarmEndpoints checks automatic routing and cached distinct-to-self-to-distinct parameter changes.
func TestCompactEndpointInequalityDriverOwnsSnapshotAndRebindsWarmEndpoints(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	var (
		fixture     = setupCompactEndpointInequalityDriver(t)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	for _, mode := range compactEndpointInequalityModes() {
		t.Run(mode.name, func(t *testing.T) {
			require.NoError(t, fixture.driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				owner := tx.(*transaction)
				translationHits := func() uint64 {
					if mode.builder {
						return uint64(owner.schemaManager.compilationCache.Stats().Hits)
					}
					return owner.translationCache.(*connectionTranslationCache).statsSnapshot().Hits
				}
				for iteration, endpoints := range []compactBuilderEndpointPair{fixture.distinct, fixture.same, fixture.distinct} {
					count := 2
					if endpoints.source.ID == endpoints.target.ID {
						count = 0
					}
					beforeHits := translationHits()
					require.Nil(t, owner.tx)
					require.NoError(t, consumeCompactEndpointInequality(t, tx, mode, endpoints, count, true, true))
					require.Nil(t, owner.tx)
					require.Nil(t, owner.compactSearch)
					require.Equal(t, byte('I'), owner.conn.Conn().PgConn().TxStatus())
					if iteration > 0 {
						require.Greater(t, translationHits(), beforeHits)
					}
				}
				return nil
			}))
		})
	}
}

// TestCompactEndpointInequalityDriverPreservesCallerSnapshots checks stable transaction reuse and unchanged Read Committed fallback.
func TestCompactEndpointInequalityDriverPreservesCallerSnapshots(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	var (
		fixture     = setupCompactEndpointInequalityDriver(t)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	for _, mode := range compactEndpointInequalityModes() {
		t.Run(mode.name, func(t *testing.T) {
			for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.ReadCommitted, pgx.RepeatableRead} {
				require.NoError(t, fixture.driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					var (
						owner    = tx.(*transaction)
						callerTx = owner.tx
						cache    = &recordingCompactTranslationCache{
							CypherTranslationCache: owner.translationCache,
						}
					)
					owner.translationCache = cache
					require.NoError(t, consumeCompactEndpointInequality(t, tx, mode, fixture.distinct, 2, stableSnapshotIsolation(isolation), false))
					require.Same(t, callerTx, owner.tx)
					require.Nil(t, owner.compactSearch)
					require.Equal(t, isolation, owner.isolation)
					if !mode.builder {
						if stableSnapshotIsolation(isolation) {
							require.Contains(t, cache.sql, "all_shortest_paths_dag_v2")
						} else {
							require.NotContains(t, cache.sql, "all_shortest_paths_dag_v2")
							require.Contains(t, cache.sql, "unidirectional_asp_harness_v2")
						}
					}
					return nil
				}, OptionSetTransactionIsolation(isolation)))
			}
		})
	}
}

// compactEndpointInequalityBudgetCase records the exact cumulative admissions expected for isolated resolved endpoints.
type compactEndpointInequalityBudgetCase struct {
	// name labels the endpoint relation and its admission boundary.
	name string

	// source is the source name property matched by the text query.
	source string

	// target is the terminal object ID property matched by the text query.
	target string

	// limits bounds cumulative admitted records and estimated bytes.
	limits traversalLimits

	// states is the successful query's complete admitted state count.
	states int64

	// bytes is the successful query's complete estimated byte count.
	bytes int64

	// failurePhase identifies the expected admission failure, or remains empty for successful exhaustion.
	failurePhase string
}

// TestCompactEndpointInequalityDriverChargesOnlyAdmittedPairs checks that excluded self pairs cost no pair or root admissions and all other pairs share one budget.
func TestCompactEndpointInequalityDriverChargesOnlyAdmittedPairs(t *testing.T) {
	var (
		fixture     = setupCompactEndpointInequalityDriver(t)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	for _, test := range []compactEndpointInequalityBudgetCase{
		{
			name:   "self endpoints exact",
			source: compactEndpointSelfProperty,
			target: compactEndpointSelfProperty,
			limits: traversalLimits{stateLimit: 2, byteLimit: 128},
			states: 2,
			bytes:  128,
		},
		{
			name:         "self endpoints state boundary",
			source:       compactEndpointSelfProperty,
			target:       compactEndpointSelfProperty,
			limits:       traversalLimits{stateLimit: 1, byteLimit: 128},
			failurePhase: "endpoint",
		},
		{
			name:         "self endpoints byte boundary",
			source:       compactEndpointSelfProperty,
			target:       compactEndpointSelfProperty,
			limits:       traversalLimits{stateLimit: 2, byteLimit: 127},
			failurePhase: "endpoint",
		},
		{
			name:   "overlapping endpoints exact",
			source: compactEndpointOverlapSource,
			target: compactEndpointOverlapTarget,
			limits: traversalLimits{stateLimit: 10, byteLimit: 544},
			states: 4 + 3 + 3,
			bytes:  4*64 + 3*64 + 3*32,
		},
		{
			name:         "overlapping endpoints state boundary",
			source:       compactEndpointOverlapSource,
			target:       compactEndpointOverlapTarget,
			limits:       traversalLimits{stateLimit: 9, byteLimit: 544},
			failurePhase: "asp_seen",
		},
		{
			name:         "overlapping endpoints byte boundary",
			source:       compactEndpointOverlapSource,
			target:       compactEndpointOverlapTarget,
			limits:       traversalLimits{stateLimit: 10, byteLimit: 543},
			failurePhase: "asp_seen",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture.driver.traversalLimits = test.limits
			err := fixture.driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result := tx.Query(compactEndpointInequalityQuery, map[string]any{
					"source": test.source,
					"target": test.target,
				})
				require.False(t, result.Next())
				result.Close()
				if err := result.Error(); err != nil {
					return err
				}

				stats := tx.Raw(readCompactSearchBudgetTotalsSQL, nil)
				defer stats.Close()
				require.True(t, stats.Next(), "%v", stats.Error())
				var states, bytes int64
				require.NoError(t, graph.ScanNextResult(stats, &states, &bytes))
				require.Equal(t, test.states, states)
				require.Equal(t, test.bytes, bytes)
				return stats.Error()
			}, OptionSetTransactionIsolation(pgx.RepeatableRead))
			if test.failurePhase != "" {
				var postgresError *pgconn.PgError
				require.True(t, errors.As(err, &postgresError), "%v", err)
				require.Equal(t, "54000", postgresError.Code)
				require.Contains(t, postgresError.Detail, "phase="+test.failurePhase+" ")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCompactEndpointInequalityDriverUnguardedSelfRetainsError checks that pair exclusion does not weaken the existing unguarded self-endpoint contract.
func TestCompactEndpointInequalityDriverUnguardedSelfRetainsError(t *testing.T) {
	fixture := setupCompactEndpointInequalityDriver(t)
	err := fixture.driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(compactEndpointUnguardedQuery, map[string]any{
			"source": compactEndpointSelfProperty,
			"target": compactEndpointSelfProperty,
		})
		defer result.Close()
		require.False(t, result.Next())
		return result.Error()
	})
	var postgresError *pgconn.PgError
	require.True(t, errors.As(err, &postgresError), "%v", err)
	require.Equal(t, "22023", postgresError.Code)
}
