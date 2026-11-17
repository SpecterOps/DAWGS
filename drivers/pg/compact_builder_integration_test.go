//go:build manual_integration

package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// compactBuilderEndpointPair groups the independently resolved Pathfinding anchors.
type compactBuilderEndpointPair struct {
	// source is the starting node found by its source property.
	source *graph.Node

	// target is the terminal node found by its object ID property.
	target *graph.Node
}

// compactBuilderEndpoints resolves both anchors through the normal node-builder API.
func compactBuilderEndpoints(tx graph.Transaction) (compactBuilderEndpointPair, error) {
	if source, err := tx.Nodes().Filter(query.Equals(query.NodeProperty("name"), "source")).First(); err != nil {
		return compactBuilderEndpointPair{}, err
	} else if target, err := tx.Nodes().Filter(query.Equals(query.NodeProperty("objectid"), "target")).First(); err != nil {
		return compactBuilderEndpointPair{}, err
	} else {
		return compactBuilderEndpointPair{
			source: source,
			target: target,
		}, nil
	}
}

// compactBuilderFetch runs the node-lookup and relationship-builder sequence used by Pathfinding.
func compactBuilderFetch(tx graph.Transaction, kinds graph.Kinds, delegate func(graph.Cursor[graph.Path]) error) error {
	endpoints, err := compactBuilderEndpoints(tx)
	if err != nil {
		return err
	}

	criteria := []graph.Criteria{query.Equals(query.StartID(), endpoints.source.ID), query.Equals(query.EndID(), endpoints.target.ID)}
	if len(kinds) > 0 {
		criteria = append(criteria, query.KindIn(query.Relationship(), kinds...))
	}

	return tx.Relationships().Filter(query.And(criteria...)).FetchAllShortestPaths(delegate)
}

// setupCompactBuilderDriver adds a wrong-kind shortcut so losing a KindIn filter changes the shortest paths.
func setupCompactBuilderDriver(t *testing.T, connections int32) *Driver {
	t.Helper()
	driver := setupCompactSearchDriver(t, connections)
	require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		if endpoints, err := compactBuilderEndpoints(tx); err != nil {
			return err
		} else if _, err := tx.CreateRelationshipByIDs(endpoints.source.ID, endpoints.target.ID, graph.StringKind("CompactExcludedStep"), graph.NewProperties()); err != nil {
			return err
		} else {
			return nil
		}
	}))
	return driver
}

// compactBuilderPathfindingCase describes the kind filter and complete expected paths for one Pathfinding builder variant.
type compactBuilderPathfindingCase struct {
	// name identifies the relationship-filter variant.
	name string

	// kinds constrains the accepted relationship kind domain.
	kinds graph.Kinds

	// count is the complete expected shortest-path multiplicity.
	count int

	// depth is the expected minimum relationship count per returned path.
	depth int
}

// TestCompactBuilderBHEPathfindingCallSequence checks typed and untyped Pathfinding builders select compact search without losing kind filters.
func TestCompactBuilderBHEPathfindingCallSequence(t *testing.T) {
	driver := setupCompactBuilderDriver(t, 1)
	// Match BHE's normal operation-level limits without adding query bounds.
	driver.traversalLimits = traversalLimits{
		stateLimit: 500000,
		byteLimit:  512 * 1024 * 1024,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, test := range []compactBuilderPathfindingCase{
		{
			name:  "untyped",
			count: 1,
			depth: 1,
		},
		{
			name:  "typed",
			kinds: graph.Kinds{graph.StringKind("CompactSearchStep")},
			count: 2,
			depth: 2,
		},
		{
			name:  "typed alternatives",
			kinds: graph.Kinds{graph.StringKind("CompactSearchStep"), graph.StringKind("CompactExcludedStep")},
			count: 1,
			depth: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for iteration := 0; iteration < 2; iteration++ {
				require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					owner := tx.(*transaction)
					err := compactBuilderFetch(tx, test.kinds, func(cursor graph.Cursor[graph.Path]) error {
						require.NotNil(t, owner.compactSearch)
						require.Equal(t, pgx.RepeatableRead, owner.isolation)
						_, compactCursor := cursor.(*compactPathCursor)
						require.True(t, compactCursor)
						count := 0
						for path := range cursor.Chan() {
							require.Len(t, path.Edges, test.depth)
							if len(test.kinds) > 0 {
								for _, edge := range path.Edges {
									require.True(t, test.kinds.ContainsOneOf(edge.Kind))
								}
							}
							count++
						}
						require.Equal(t, test.count, count)
						return cursor.Error()
					})
					require.Nil(t, owner.tx)
					require.Nil(t, owner.compactSearch)
					return err
				}))
			}
		})
	}
}

// TestCompactBuilderPreservesCallerIsolationAndCompilerDisable checks that builder optimization respects caller transaction ownership and the compiler switch.
func TestCompactBuilderPreservesCallerIsolationAndCompilerDisable(t *testing.T) {
	var (
		driver      = setupCompactBuilderDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable, pgx.ReadCommitted} {
		require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
			owner := tx.(*transaction)
			callerTx := owner.tx
			return compactBuilderFetch(tx, graph.Kinds{graph.StringKind("CompactSearchStep")}, func(cursor graph.Cursor[graph.Path]) error {
				_, compactCursor := cursor.(*compactPathCursor)
				require.Equal(t, stableSnapshotIsolation(isolation), compactCursor)
				count := 0
				for path := range cursor.Chan() {
					require.Len(t, path.Edges, 2)
					count++
				}
				require.Equal(t, 2, count)
				require.Same(t, callerTx, owner.tx)
				require.Nil(t, owner.compactSearch)
				require.Equal(t, isolation, owner.isolation)
				return cursor.Error()
			})
		}, OptionSetTransactionIsolation(isolation)))
	}
	previous := SetOptimizedTranslation(false)
	defer SetOptimizedTranslation(previous)
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		return compactBuilderFetch(tx, nil, func(cursor graph.Cursor[graph.Path]) error {
			_, compactCursor := cursor.(*compactPathCursor)
			require.False(t, compactCursor)
			require.Nil(t, owner.tx)
			for range cursor.Chan() {
			}
			return cursor.Error()
		})
	}))
}

// TestCompactBuilderEarlyCloseAndCancellationReleaseLease verifies that cursor cleanup finishes before the builder connection is reused.
func TestCompactBuilderEarlyCloseAndCancellationReleaseLease(t *testing.T) {
	driver := setupCompactBuilderDriver(t, 1)
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			owner := tx.(*transaction)
			err := compactBuilderFetch(tx, graph.Kinds{graph.StringKind("CompactSearchStep")}, func(cursor graph.Cursor[graph.Path]) error {
				<-cursor.Chan()
				if canceled {
					cancel()
				}
				cursor.Close()
				if canceled {
					return cursor.Error()
				}
				return nil // Explicit early Close is permitted by the delegate API.
			})
			require.Nil(t, owner.tx)
			require.Nil(t, owner.compactSearch)
			return err
		})
		cancel()
		if canceled {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, err)
		}
		require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
			return compactBuilderFetch(tx, graph.Kinds{graph.StringKind("CompactSearchStep")}, func(cursor graph.Cursor[graph.Path]) error {
				count := 0
				for range cursor.Chan() {
					count++
				}
				require.Equal(t, 2, count)
				return cursor.Error()
			})
		}))
	}
}

// TestCompactBuilderResourceFailureIsNotHiddenByDelegate checks that a delegate cannot swallow compact admission failures or poison the next operation.
func TestCompactBuilderResourceFailureIsNotHiddenByDelegate(t *testing.T) {
	driver := setupCompactBuilderDriver(t, 1)
	driver.traversalLimits = traversalLimits{
		stateLimit: 1,
		byteLimit:  1024 * 1024,
	}
	err := driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		// Perform node lookups before applying the restrictive budget to the
		// compact traversal, as the real Pathfinding entry point does.
		return compactBuilderFetch(tx, graph.Kinds{graph.StringKind("CompactSearchStep")}, func(cursor graph.Cursor[graph.Path]) error {
			for range cursor.Chan() {
			}
			return nil
		})
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "54000")
	require.False(t, errors.Is(err, context.Canceled))
	driver.traversalLimits.stateLimit = 500000
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		return compactBuilderFetch(tx, graph.Kinds{graph.StringKind("CompactSearchStep")}, func(cursor graph.Cursor[graph.Path]) error {
			for range cursor.Chan() {
			}
			return cursor.Error()
		})
	}))
}
