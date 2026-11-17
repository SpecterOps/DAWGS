package pg

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs"
	"github.com/stretchr/testify/require"
)

// TestTraversalLimitConstructor verifies public configuration reaches the PostgreSQL driver without normalization.
func TestTraversalLimitConstructor(t *testing.T) {
	for name, config := range map[string]dawgs.Config{
		"inherited": {},
		"states":    {TraversalStateLimit: 100},
		"bytes":     {TraversalByteLimit: 4096},
		"both":      {TraversalStateLimit: 100, TraversalByteLimit: 4096},
		"int64":     {TraversalStateLimit: math.MaxInt64, TraversalByteLimit: math.MaxInt64},
	} {
		t.Run(name, func(t *testing.T) {
			database, err := dawgs.Open(context.Background(), DriverName, config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, database.Close(context.Background())) })

			driver, ok := database.(*Driver)
			require.True(t, ok)
			require.Equal(t, config.TraversalStateLimit, driver.traversalLimits.stateLimit)
			require.Equal(t, config.TraversalByteLimit, driver.traversalLimits.byteLimit)
		})
	}
}

// TestTraversalLimitConstructorRejectsNegative verifies invalid limits fail before a connection is required.
func TestTraversalLimitConstructorRejectsNegative(t *testing.T) {
	for field, config := range map[string]dawgs.Config{
		"TraversalStateLimit": {TraversalStateLimit: -1},
		"TraversalByteLimit":  {TraversalByteLimit: -1},
	} {
		t.Run(field, func(t *testing.T) {
			database, err := dawgs.Open(context.Background(), DriverName, config)
			require.Nil(t, database)
			require.ErrorContains(t, err, field+" must not be negative")
		})
	}
}

// TestTraversalLimitScopeInheritance verifies zero configuration adds no SQL or restoration work.
func TestTraversalLimitScopeInheritance(t *testing.T) {
	scope := traversalLimitScope{}

	require.NoError(t, scope.apply(context.Background(), nil, false))
	require.NoError(t, scope.apply(context.Background(), nil, true))
	require.NoError(t, scope.restore(context.Background(), nil))
	require.Nil(t, scope.previous)
}

// traversalLimitScopeCase pairs independent constructor overrides with their expected SQL bindings.
type traversalLimitScopeCase struct {
	// limits supplies the constructor overrides under test.
	limits traversalLimits

	// stateValue is the exact PostgreSQL state value, or nil to preserve the existing setting.
	stateValue any

	// byteValue is the exact PostgreSQL byte value, or nil to preserve the existing setting.
	byteValue any
}

// TestTraversalLimitScopeExplicit verifies transaction-local application and independent setting inheritance.
func TestTraversalLimitScopeExplicit(t *testing.T) {
	for name, testCase := range map[string]traversalLimitScopeCase{
		"states": {
			limits:     traversalLimits{stateLimit: 100},
			stateValue: "100",
		},
		"bytes": {
			limits:    traversalLimits{byteLimit: 4096},
			byteValue: "4096",
		},
		"both": {
			limits:     traversalLimits{stateLimit: 100, byteLimit: 4096},
			stateValue: "100",
			byteValue:  "4096",
		},
		"int64": {
			limits:     traversalLimits{stateLimit: math.MaxInt64, byteLimit: math.MaxInt64},
			stateValue: "9223372036854775807",
			byteValue:  "9223372036854775807",
		},
	} {
		t.Run(name, func(t *testing.T) {
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			scope := traversalLimitScope{limits: testCase.limits}
			mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
				traversalStateLimitSetting, testCase.stateValue,
				traversalByteLimitSetting, testCase.byteValue, true,
			).WillReturnResult(pgxmock.NewResult("SELECT", 1))

			require.NoError(t, scope.apply(context.Background(), mock, true))
			require.Nil(t, scope.previous, "PostgreSQL restores local settings at transaction end")
			require.NoError(t, scope.restore(context.Background(), mock))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestTraversalLimitScopeAutocommit verifies exact session restoration, including previously unset settings.
func TestTraversalLimitScopeAutocommit(t *testing.T) {
	for name, previous := range map[string]traversalLimitSettings{
		"unset":    {},
		"existing": {stateLimit: "75000", byteLimit: "33554432"},
	} {
		t.Run(name, func(t *testing.T) {
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			scope := traversalLimitScope{limits: traversalLimits{stateLimit: 100, byteLimit: 4096}}
			mock.ExpectQuery(readTraversalLimitsSQL).WithArgs(
				traversalStateLimitSetting, traversalByteLimitSetting,
			).WillReturnRows(pgxmock.NewRows([]string{"states", "bytes"}).AddRow(previous.stateLimit, previous.byteLimit))
			mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
				traversalStateLimitSetting, "100", traversalByteLimitSetting, "4096", false,
			).WillReturnResult(pgxmock.NewResult("SELECT", 1))

			require.NoError(t, scope.apply(context.Background(), mock, false))
			require.Equal(t, &previous, scope.previous)
			mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
				traversalStateLimitSetting, previous.stateLimit,
				traversalByteLimitSetting, previous.byteLimit, false,
			).WillReturnResult(pgxmock.NewResult("SELECT", 1))

			require.NoError(t, scope.restore(context.Background(), mock))
			require.Nil(t, scope.previous)
			require.NoError(t, scope.restore(context.Background(), nil), "cleanup is idempotent")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestTraversalLimitScopeIndependentRestoration leaves the unconfigured setting untouched during cleanup.
func TestTraversalLimitScopeIndependentRestoration(t *testing.T) {
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	scope := traversalLimitScope{
		limits:   traversalLimits{stateLimit: 100},
		previous: &traversalLimitSettings{stateLimit: "50", byteLimit: "4096"},
	}
	mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
		traversalStateLimitSetting, "50", traversalByteLimitSetting, nil, false,
	).WillReturnResult(pgxmock.NewResult("SELECT", 1))

	require.NoError(t, scope.restore(context.Background(), mock))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestTraversalLimitScopeSetupFailures verifies failed setup retains only snapshots that require cleanup.
func TestTraversalLimitScopeSetupFailures(t *testing.T) {
	failure := errors.New("settings unavailable")
	for _, phase := range []string{"snapshot", "session write", "local write"} {
		t.Run(phase, func(t *testing.T) {
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			scope := traversalLimitScope{limits: traversalLimits{byteLimit: 4096}}
			local := phase == "local write"
			if !local {
				snapshot := mock.ExpectQuery(readTraversalLimitsSQL).WithArgs(
					traversalStateLimitSetting, traversalByteLimitSetting,
				)
				if phase == "snapshot" {
					snapshot.WillReturnError(failure)
				} else {
					snapshot.WillReturnRows(pgxmock.NewRows([]string{"states", "bytes"}).AddRow("", "8192"))
				}
			}
			if phase != "snapshot" {
				mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
					traversalStateLimitSetting, nil, traversalByteLimitSetting, "4096", local,
				).WillReturnError(failure)
			}

			require.ErrorIs(t, scope.apply(context.Background(), mock, local), failure)
			if phase == "session write" {
				require.NotNil(t, scope.previous)
				mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
					traversalStateLimitSetting, nil, traversalByteLimitSetting, "8192", false,
				).WillReturnResult(pgxmock.NewResult("SELECT", 1))
			} else {
				require.Nil(t, scope.previous)
			}

			require.NoError(t, scope.restore(context.Background(), mock))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// traversalLimitContextObserver records whether cleanup retains cancellation or lacks a deadline.
type traversalLimitContextObserver struct {
	// driver delegates the SQL execution after its context is inspected.
	driver

	// contextError records cancellation visible to the cleanup SQL.
	contextError error

	// deadline records the bounded cleanup deadline.
	deadline time.Time
}

// Exec observes the cleanup context before forwarding the setting restoration.
func (s *traversalLimitContextObserver) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	s.contextError = ctx.Err()
	s.deadline, _ = ctx.Deadline()
	return s.driver.Exec(ctx, sql, arguments...)
}

// TestTraversalLimitScopeCanceledCleanup verifies restoration survives cancellation and returns SQL failures.
func TestTraversalLimitScopeCanceledCleanup(t *testing.T) {
	failure := errors.New("connection lost")
	for name, cleanupError := range map[string]error{"success": nil, "failure": failure} {
		t.Run(name, func(t *testing.T) {
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			var (
				ctx, cancel = context.WithCancel(context.Background())
				scope       = traversalLimitScope{
					limits:   traversalLimits{byteLimit: 4096},
					previous: &traversalLimitSettings{},
				}
				observer = &traversalLimitContextObserver{driver: mock}
			)
			cancel()
			write := mock.ExpectExec(writeTraversalLimitsSQL).WithArgs(
				traversalStateLimitSetting, nil, traversalByteLimitSetting, "", false,
			)
			if cleanupError != nil {
				write.WillReturnError(cleanupError)
			} else {
				write.WillReturnResult(pgxmock.NewResult("SELECT", 1))
			}

			restoreError := scope.restore(ctx, observer)
			if cleanupError != nil {
				require.ErrorIs(t, restoreError, cleanupError)
			} else {
				require.NoError(t, restoreError)
			}

			require.NoError(t, observer.contextError)
			require.WithinDuration(t, time.Now().Add(traversalLimitCleanupTimeout), observer.deadline, time.Second)
			require.Nil(t, scope.previous)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
