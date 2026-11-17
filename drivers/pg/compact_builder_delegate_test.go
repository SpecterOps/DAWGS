package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// TestCompactBuilderDelegateReuseRetainsOuterErrorPolicy checks that nested builder execution cannot change the outer result's final-error handling.
func TestCompactBuilderDelegateReuseRetainsOuterErrorPolicy(t *testing.T) {
	for _, outerCompact := range []bool{false, true} {
		subtestName := "legacy outer"
		if outerCompact {
			subtestName = "compact outer"
		}
		t.Run(subtestName, func(t *testing.T) {
			setOptimizedTranslationForTest(t, outerCompact)
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)

			var (
				ctx     = context.Background()
				manager = NewSchemaManager(nil, 0)
				owner   = &transaction{
					ctx:           ctx,
					tx:            mock,
					isolation:     pgx.RepeatableRead,
					schemaManager: manager,
				}
				builder = newLiveQuery(ctx, owner, func() (int32, error) { return 7, nil })
				lateErr = errors.New("outer result failed during close")
			)
			t.Cleanup(func() {
				manager.parseCache.Close()
				manager.compilationCache.Close()
			})
			owner.kindMapper = &transactionKindMapper{
				tx:       owner,
				snapshot: manager.kindCache.current.Load(),
			}
			builder.queryBuilder.Apply(query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(11)),
				query.Equals(query.EndID(), graph.ID(12)),
			)))

			outerStatement := "unidirectional_asp_harness_v2"
			if outerCompact {
				mock.ExpectExec("select set_config").WithArgs(pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				outerStatement = "all_shortest_paths_dag_v2"
			}
			mock.ExpectQuery(outerStatement).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(
				pgxmock.NewRows([]string{"p"}).AddRow(nil).CloseError(lateErr),
			)
			if outerCompact {
				mock.ExpectQuery("select").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(11)))
			} else {
				mock.ExpectExec("select set_config").WithArgs(pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				mock.ExpectQuery("all_shortest_paths_dag_v2").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"p"}))
			}

			queryError := builder.QueryAllShortestPaths(func(outer graph.Result) error {
				require.Equal(t, outerCompact, builder.budgetedCompact)
				outer.Close()
				require.ErrorIs(t, outer.Error(), lateErr)

				if outerCompact {
					require.NoError(t, builder.Query(func(nested graph.Result) error {
						for nested.Next() {
						}
						return nested.Error()
					}, query.Returning(query.StartID())))
				} else {
					previous := SetOptimizedTranslation(true)
					defer SetOptimizedTranslation(previous)

					require.NoError(t, builder.QueryAllShortestPaths(func(nested graph.Result) error {
						for nested.Next() {
						}
						return nested.Error()
					}))
				}

				require.Equal(t, !outerCompact, builder.budgetedCompact)
				return nil // The outer execution still owns its original late-error policy.
			}, query.Returning(query.Path()))
			if outerCompact {
				require.ErrorIs(t, queryError, lateErr)
			} else {
				require.NoError(t, queryError)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
