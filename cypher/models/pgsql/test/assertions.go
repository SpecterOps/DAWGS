// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// ResultAssertion inspects a live query result. When called, Next() has not
// yet been advanced — the assertion is responsible for iteration.
type ResultAssertion func(t *testing.T, result graph.Result)

// bufferedResult implements graph.Result over an in-memory row buffer, enabling
// multiple assertion passes over the same result without re-executing the query.
type bufferedResult struct {
	rows    [][]any
	keys    []string
	mapper  graph.ValueMapper
	current int
	err     error
}

// newBufferedResult exhausts r into memory and returns a replayable result.
// The caller must not use r after this call.
func newBufferedResult(r graph.Result) *bufferedResult {
	br := &bufferedResult{mapper: r.Mapper(), current: -1}
	for r.Next() {
		if br.keys == nil {
			br.keys = r.Keys()
		}
		vals := r.Values()
		row := make([]any, len(vals))
		copy(row, vals)
		br.rows = append(br.rows, row)
	}
	br.err = r.Error()
	return br
}

func (b *bufferedResult) Reset()                    { b.current = -1 }
func (b *bufferedResult) Next() bool                { b.current++; return b.current < len(b.rows) }
func (b *bufferedResult) Keys() []string            { return b.keys }
func (b *bufferedResult) Mapper() graph.ValueMapper { return b.mapper }
func (b *bufferedResult) Error() error              { return b.err }
func (b *bufferedResult) Close()                    {}

func (b *bufferedResult) Values() []any {
	if b.current < 0 || b.current >= len(b.rows) {
		return nil
	}
	return b.rows[b.current]
}

func (b *bufferedResult) Scan(targets ...any) error {
	return graph.ScanNextResult(b, targets...)
}

// AssertNoError drains the result and asserts it carries no error.
func AssertNoError() ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		for result.Next() {
		}
		require.NoError(t, result.Error())
	}
}

// AssertEmpty asserts the result set contains zero rows.
func AssertEmpty() ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.Empty(t, br.rows, "expected empty result but got %d rows", len(br.rows))
	}
}

// AssertNonEmpty asserts the result set contains at least one row.
func AssertNonEmpty() ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.NotEmpty(t, br.rows, "expected non-empty result set")
	}
}

// AssertRowCount asserts the result set contains exactly n rows.
func AssertRowCount(n int) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.Len(t, br.rows, n, "unexpected row count")
	}
}

// AssertScalarString asserts the first column of the first row is the given string.
func AssertScalarString(expected string) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.NotEmpty(t, br.rows, "no rows returned; cannot assert scalar string")
		require.Equal(t, expected, br.rows[0][0], "scalar string mismatch")
	}
}

// AssertAtLeastInt64 asserts the first column of the first row is an int64
// greater than or equal to min. This is suitable for count/aggregate queries
// where the database may contain pre-existing rows alongside the fixture data.
func AssertAtLeastInt64(min int64) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.NotEmpty(t, br.rows, "no rows returned; cannot assert scalar int64")
		val, ok := br.rows[0][0].(int64)
		require.True(t, ok, "expected int64 scalar, got %T: %v", br.rows[0][0], br.rows[0][0])
		require.GreaterOrEqual(t, val, min, "scalar int64 below expected minimum")
	}
}

// AssertContainsNodeWithProp asserts that at least one row in the result
// contains a node (in any column) with the given string property value.
func AssertContainsNodeWithProp(key, val string) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.NotEmpty(t, br.rows, "no rows; cannot check node property %s", key)
		for _, row := range br.rows {
			for _, rawVal := range row {
				var node graph.Node
				if br.mapper.Map(rawVal, &node) {
					if s, err := node.Properties.Get(key).String(); err == nil && s == val {
						return
					}
				}
			}
		}
		t.Errorf("no result row contains a node with %s = %q", key, val)
	}
}

// AssertExactInt64 asserts the first column of the first row is exactly the given int64 value.
func AssertExactInt64(expected int64) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		require.NoError(t, br.err)
		require.NotEmpty(t, br.rows, "no rows returned; cannot assert scalar int64")
		val, ok := br.rows[0][0].(int64)
		require.True(t, ok, "expected int64 scalar, got %T: %v", br.rows[0][0], br.rows[0][0])
		require.Equal(t, expected, val, "scalar int64 mismatch")
	}
}

// AssertAll buffers the result once and runs each assertion against the same
// buffered data, resetting the cursor between assertions.
func AssertAll(assertions ...ResultAssertion) ResultAssertion {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		br := newBufferedResult(result)
		for _, a := range assertions {
			br.Reset()
			a(t, br)
		}
	}
}
