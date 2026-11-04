// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// suffixReverseRetryDatabase limits the tool-only retry behavior to one exact
// translated candidate statement. Every other database operation delegates
// unchanged to the underlying PostgreSQL database.
type suffixReverseRetryDatabase struct {
	graph.Database
	candidateSQL        string
	fallbackSQL         string
	candidateParameters map[string]any
	fallbackParameters  map[string]any
	limits              pg.SuffixReverseRetryLimits
}

func (s *suffixReverseRetryDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return s.Database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return delegate(&suffixReverseRetryTransaction{Transaction: tx, owner: s})
	}, options...)
}

type suffixReverseRetryTransaction struct {
	graph.Transaction
	owner *suffixReverseRetryDatabase
}

func (s *suffixReverseRetryTransaction) Raw(query string, parameters map[string]any) graph.Result {
	if query != s.owner.candidateSQL {
		return s.Transaction.Raw(query, parameters)
	}
	retry, ok := s.Transaction.(pg.SuffixReverseRetryTransaction)
	if !ok {
		return graph.NewErrorResult(fmt.Errorf("PostgreSQL transaction does not expose suffix reverse retry tooling"))
	}
	return retry.RawSuffixReverseRetry(
		s.owner.candidateSQL,
		s.owner.fallbackSQL,
		s.owner.candidateParameters,
		s.owner.fallbackParameters,
		s.owner.limits,
	)
}
