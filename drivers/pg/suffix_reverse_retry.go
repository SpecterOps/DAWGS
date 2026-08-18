// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package pg

import (
	"encoding/json"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

const suffixReverseRetrySavepoint = "dawgs_suffix_reverse_retry_v1"

// SuffixReverseRetryLimits freezes the development candidate's public buffer
// boundary independently of SQL translation. Values must match the lowering
// metadata used to render the candidate statement.
type SuffixReverseRetryLimits struct {
	OutputRows  int64
	OutputBytes int64
}

// SuffixReverseRetryTransaction is a tool-only PostgreSQL execution surface.
// It is intentionally absent from graph.Transaction and cannot affect ordinary
// production queries without an explicit type assertion by repository tooling.
type SuffixReverseRetryTransaction interface {
	RawSuffixReverseRetry(candidateSQL, fallbackSQL string, candidateParameters, fallbackParameters map[string]any, limits SuffixReverseRetryLimits) graph.Result
}

// bufferedResult owns a completely drained candidate result. No database rows
// remain live when it is returned to a caller.
type bufferedResult struct {
	keys   []string
	rows   [][]any
	mapper graph.ValueMapper
	index  int
	err    error
}

func (s *bufferedResult) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *bufferedResult) Keys() []string { return s.keys }

func (s *bufferedResult) Values() []any {
	if s.index == 0 || s.index > len(s.rows) {
		return nil
	}
	return s.rows[s.index-1]
}

func (s *bufferedResult) Mapper() graph.ValueMapper { return s.mapper }

func (s *bufferedResult) Scan(targets ...any) error { return graph.ScanNextResult(s, targets...) }

func (s *bufferedResult) Error() error { return s.err }

func (s *bufferedResult) Close() {}

// suffixReverseRetryFallbackResult records completion only after the exact
// forward retry has drained without error. Closing early intentionally does
// not create a completion receipt: an interrupted retry is not evidence of a
// complete incumbent execution.
type suffixReverseRetryFallbackResult struct {
	graph.Result
	complete  func() error
	completed bool
	err       error
}

func (s *suffixReverseRetryFallbackResult) Next() bool {
	if s.err != nil || s.completed {
		return false
	}
	if s.Result.Next() {
		return true
	}
	if err := s.Result.Error(); err != nil {
		s.err = err
		return false
	}
	if err := s.complete(); err != nil {
		s.err = err
		return false
	}
	s.completed = true
	return false
}

func (s *suffixReverseRetryFallbackResult) Error() error {
	if s.err != nil {
		return s.err
	}
	return s.Result.Error()
}

// bufferGraphResult drains result and enforces both public candidate caps. The
// returned branch is empty only when the complete buffer is publishable.
func bufferGraphResult(result graph.Result, limits SuffixReverseRetryLimits) (*bufferedResult, string, error) {
	defer result.Close()
	buffered := &bufferedResult{mapper: result.Mapper()}
	var encodedBytes int64
	for result.Next() {
		values := append([]any(nil), result.Values()...)
		if buffered.keys == nil {
			buffered.keys = append([]string(nil), result.Keys()...)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, "forward_retry_output_encoding", nil
		}
		encodedBytes += int64(len(encoded))
		buffered.rows = append(buffered.rows, values)
		if int64(len(buffered.rows)) > limits.OutputRows {
			return nil, "forward_retry_output_rows", nil
		}
		if encodedBytes > limits.OutputBytes {
			return nil, "forward_retry_output_bytes", nil
		}
	}
	if err := result.Error(); err != nil {
		return nil, "", err
	}
	return buffered, "", nil
}

func (s *transaction) suffixReverseRetryExec(statement string, arguments ...any) error {
	_, err := s.driver().Exec(s.ctx, statement, arguments...)
	return err
}

func (s *transaction) suffixReverseRetryAbort(err error) graph.Result {
	_ = s.suffixReverseRetryExec("rollback to savepoint " + suffixReverseRetrySavepoint)
	_ = s.suffixReverseRetryExec("release savepoint " + suffixReverseRetrySavepoint)
	return graph.NewErrorResult(err)
}

// RawSuffixReverseRetry executes a reverse-only candidate and exact incumbent
// fallback in one stable snapshot. Candidate rows are fully buffered and
// validated before they can be observed.
func (s *transaction) RawSuffixReverseRetry(candidateSQL, fallbackSQL string, candidateParameters, fallbackParameters map[string]any, limits SuffixReverseRetryLimits) graph.Result {
	if s.tx == nil || !stableSnapshotIsolation(s.isolation) {
		return graph.NewErrorResult(fmt.Errorf("suffix reverse retry requires an explicit Repeatable Read or Serializable transaction"))
	}
	if limits.OutputRows <= 0 || limits.OutputBytes <= 0 {
		return graph.NewErrorResult(fmt.Errorf("suffix reverse retry requires positive output row and byte limits"))
	}
	if err := s.suffixReverseRetryExec("savepoint " + suffixReverseRetrySavepoint); err != nil {
		return graph.NewErrorResult(err)
	}
	if err := s.suffixReverseRetryExec("select set_config('dawgs.suffix_reverse_retry_status', '', true)"); err != nil {
		return s.suffixReverseRetryAbort(err)
	}

	candidate, bufferBranch, err := bufferGraphResult(s.Raw(candidateSQL, candidateParameters), limits)
	if err != nil {
		return s.suffixReverseRetryAbort(err)
	}
	var sqlBranch string
	if err := s.driver().QueryRow(s.ctx, "select current_setting('dawgs.suffix_reverse_retry_status', true)").Scan(&sqlBranch); err != nil {
		return s.suffixReverseRetryAbort(err)
	}
	branch := sqlBranch
	if bufferBranch != "" {
		branch = bufferBranch
	}
	if branch == "reverse_complete" {
		if err := s.suffixReverseRetryExec("release savepoint " + suffixReverseRetrySavepoint); err != nil {
			return graph.NewErrorResult(err)
		}
		return candidate
	}
	if branch != "forward_retry_suffix_overflow" && branch != "forward_retry_state_overflow" &&
		branch != "forward_retry_output_rows" && branch != "forward_retry_output_bytes" &&
		branch != "forward_retry_output_encoding" {
		return s.suffixReverseRetryAbort(fmt.Errorf("suffix reverse retry returned unknown or empty status %q", branch))
	}
	if err := s.suffixReverseRetryExec("rollback to savepoint " + suffixReverseRetrySavepoint); err != nil {
		return graph.NewErrorResult(err)
	}
	if err := s.suffixReverseRetryExec("release savepoint " + suffixReverseRetrySavepoint); err != nil {
		return graph.NewErrorResult(err)
	}
	if err := s.suffixReverseRetryExec(
		"select public.record_requested_traversal_runtime_attestation_v1($1, false, $2)",
		branch,
		"EXPANSION-STEPWISE-FORWARD",
	); err != nil {
		return graph.NewErrorResult(err)
	}
	return &suffixReverseRetryFallbackResult{
		Result: s.Raw(fallbackSQL, fallbackParameters),
		complete: func() error {
			return s.suffixReverseRetryExec(
				"select public.record_requested_traversal_runtime_attestation_v1($1, true, $2)",
				"exact_forward_retry_complete",
				"EXPANSION-STEPWISE-FORWARD",
			)
		},
	}
}
