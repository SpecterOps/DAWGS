// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func newSuffixRetryMockTransaction(t *testing.T) (*transaction, pgxmock.PgxConnIface) {
	t.Helper()
	ctx := context.Background()
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	pgxTx, err := mock.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectRollback()
		require.NoError(t, pgxTx.Rollback(ctx))
		mock.ExpectClose()
		require.NoError(t, mock.Close(ctx))
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return &transaction{
		schemaManager: &SchemaManager{},
		ctx:           ctx,
		tx:            pgxTx,
		isolation:     pgx.RepeatableRead,
	}, mock
}

type suffixRetryTestResult struct {
	rows  [][]any
	index int
	err   error
}

func (s *suffixRetryTestResult) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *suffixRetryTestResult) Keys() []string            { return []string{"value"} }
func (s *suffixRetryTestResult) Values() []any             { return s.rows[s.index-1] }
func (s *suffixRetryTestResult) Mapper() graph.ValueMapper { return graph.ValueMapper{} }
func (s *suffixRetryTestResult) Scan(...any) error         { return nil }
func (s *suffixRetryTestResult) Error() error              { return s.err }
func (s *suffixRetryTestResult) Close()                    {}

func TestBufferGraphResultPublishesOnlyCompleteBoundedRows(t *testing.T) {
	buffered, branch, err := bufferGraphResult(&suffixRetryTestResult{
		rows: [][]any{{"a"}, {"b"}},
	}, SuffixReverseRetryLimits{OutputRows: 2, OutputBytes: 64})
	require.NoError(t, err)
	require.Empty(t, branch)
	require.Equal(t, []string{"value"}, buffered.Keys())
	require.True(t, buffered.Next())
	require.Equal(t, []any{"a"}, buffered.Values())
	require.True(t, buffered.Next())
	require.Equal(t, []any{"b"}, buffered.Values())
	require.False(t, buffered.Next())
}

func TestBufferGraphResultFailsClosedToRetryCaps(t *testing.T) {
	tests := []struct {
		name   string
		limits SuffixReverseRetryLimits
		branch string
	}{
		{name: "rows", limits: SuffixReverseRetryLimits{OutputRows: 1, OutputBytes: 64}, branch: "forward_retry_output_rows"},
		{name: "bytes", limits: SuffixReverseRetryLimits{OutputRows: 2, OutputBytes: 3}, branch: "forward_retry_output_bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffered, branch, err := bufferGraphResult(&suffixRetryTestResult{
				rows: [][]any{{"alpha"}, {"beta"}},
			}, test.limits)
			require.NoError(t, err)
			require.Nil(t, buffered)
			require.Equal(t, test.branch, branch)
		})
	}
}

func TestBufferGraphResultPropagatesCandidateFailure(t *testing.T) {
	expected := errors.New("candidate failed")
	buffered, branch, err := bufferGraphResult(&suffixRetryTestResult{err: expected}, SuffixReverseRetryLimits{OutputRows: 1, OutputBytes: 1})
	require.ErrorIs(t, err, expected)
	require.Nil(t, buffered)
	require.Empty(t, branch)
}

func TestSuffixReverseRetryFallbackResultRecordsCompletionOnlyAfterDrain(t *testing.T) {
	completed := 0
	result := &suffixReverseRetryFallbackResult{
		Result:   &suffixRetryTestResult{rows: [][]any{{"fallback-row"}}},
		complete: func() error { completed++; return nil },
	}

	require.NoError(t, result.Error())
	require.Equal(t, 0, completed)
	require.True(t, result.Next())
	require.Equal(t, []any{"fallback-row"}, result.Values())
	require.Equal(t, 0, completed)
	require.False(t, result.Next())
	require.Equal(t, 1, completed)
	require.False(t, result.Next())
	require.Equal(t, 1, completed)
}

func TestSuffixReverseRetryFallbackResultDoesNotRecordCompletionAfterFailure(t *testing.T) {
	completed := 0
	expected := errors.New("fallback failed")
	result := &suffixReverseRetryFallbackResult{
		Result:   &suffixRetryTestResult{err: expected},
		complete: func() error { completed++; return nil },
	}

	require.False(t, result.Next())
	require.ErrorIs(t, result.Error(), expected)
	require.Zero(t, completed)
}

func TestRawSuffixReverseRetryPublishesCompletedCandidate(t *testing.T) {
	tx, mock := newSuffixRetryMockTransaction(t)
	mock.ExpectExec("savepoint " + suffixReverseRetrySavepoint).WillReturnResult(pgxmock.NewResult("SAVEPOINT", 0))
	mock.ExpectExec("select set_config('dawgs.suffix_reverse_retry_status', '', true)").WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery("candidate").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(
		pgxmock.NewRows([]string{"value"}).AddRow("candidate-row"),
	)
	mock.ExpectQuery("select current_setting('dawgs.suffix_reverse_retry_status', true)").WillReturnRows(
		pgxmock.NewRows([]string{"current_setting"}).AddRow("reverse_complete"),
	)
	mock.ExpectExec("release savepoint " + suffixReverseRetrySavepoint).WillReturnResult(pgxmock.NewResult("RELEASE", 0))

	result := tx.RawSuffixReverseRetry("candidate", "fallback", nil, nil, SuffixReverseRetryLimits{OutputRows: 2, OutputBytes: 64})
	require.NoError(t, result.Error())
	require.True(t, result.Next())
	require.Equal(t, []any{"candidate-row"}, result.Values())
	require.False(t, result.Next())
}

func TestRawSuffixReverseRetryRollsBackCandidateBeforeExactForward(t *testing.T) {
	tx, mock := newSuffixRetryMockTransaction(t)
	mock.ExpectExec("savepoint " + suffixReverseRetrySavepoint).WillReturnResult(pgxmock.NewResult("SAVEPOINT", 0))
	mock.ExpectExec("select set_config('dawgs.suffix_reverse_retry_status', '', true)").WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery("candidate").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"value"}))
	mock.ExpectQuery("select current_setting('dawgs.suffix_reverse_retry_status', true)").WillReturnRows(
		pgxmock.NewRows([]string{"current_setting"}).AddRow("forward_retry_state_overflow"),
	)
	mock.ExpectExec("rollback to savepoint " + suffixReverseRetrySavepoint).WillReturnResult(pgxmock.NewResult("ROLLBACK", 0))
	mock.ExpectExec("release savepoint " + suffixReverseRetrySavepoint).WillReturnResult(pgxmock.NewResult("RELEASE", 0))
	mock.ExpectExec("select public.record_requested_traversal_runtime_attestation_v1($1, false, $2)").
		WithArgs("forward_retry_state_overflow", "EXPANSION-STEPWISE-FORWARD").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery("fallback").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(
		pgxmock.NewRows([]string{"value"}).AddRow("fallback-row"),
	)
	mock.ExpectExec("select public.record_requested_traversal_runtime_attestation_v1($1, true, $2)").
		WithArgs("exact_forward_retry_complete", "EXPANSION-STEPWISE-FORWARD").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))

	result := tx.RawSuffixReverseRetry("candidate", "fallback", nil, nil, SuffixReverseRetryLimits{OutputRows: 2, OutputBytes: 64})
	defer result.Close()
	require.NoError(t, result.Error())
	require.True(t, result.Next())
	require.Equal(t, []any{"fallback-row"}, result.Values())
	require.False(t, result.Next())
	require.NoError(t, result.Error())
}
