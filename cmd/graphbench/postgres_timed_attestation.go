// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// postgresTimedRuntimeDocument defines the serialized representation of postgres timed runtime.
type postgresTimedRuntimeDocument struct {
	// SchemaVersion identifies the schema version for schema version.
	SchemaVersion int `json:"schema_version"`
	// InvocationID identifies the invocation id.
	InvocationID string `json:"invocation_id"`
	// RequestedIdentity identifies the requested identity.
	RequestedIdentity string `json:"requested_identity"`
	// RuntimeIdentity identifies the runtime identity.
	RuntimeIdentity string `json:"runtime_identity"`
	// RuntimeBranch supplies the runtime branch input to the postgresTimedRuntimeDocument contract.
	RuntimeBranch string `json:"runtime_branch"`
	// FallbackExecuted supplies the fallback executed input to the postgresTimedRuntimeDocument contract.
	FallbackExecuted *bool `json:"fallback_executed"`
	// RecordCount records the number of record count.
	RecordCount int `json:"record_count"`
	// Events supplies the events input to the postgresTimedRuntimeDocument contract.
	Events []RuntimeReceiptEvent `json:"events"`
}

// postgresTimedReadAttestor arms a lightweight session-local receipt before
// each timed query and reads it after the duration has been recorded. A
// size-one pool is required so arming, execution, and reading cannot migrate.
type postgresTimedReadAttestor struct {
	// pool retains the pool while postgresTimedReadAttestor is assembled or evaluated.
	pool *pgxpool.Pool
	// requestedIdentity identifies the requested identity.
	requestedIdentity string
	// runID identifies the run id.
	runID string
	// activeInvocation retains the active invocation while postgresTimedReadAttestor is assembled or evaluated.
	activeInvocation string
}

// newPostgresTimedReadAttestor constructs postgres timed read attestor.
func newPostgresTimedReadAttestor(pool *pgxpool.Pool, poolSize int, requestedIdentity string) (*postgresTimedReadAttestor, error) {
	if pool == nil {
		return nil, fmt.Errorf("timed runtime attestation requires a PostgreSQL pool")
	}
	if poolSize != 1 {
		return nil, fmt.Errorf("timed runtime attestation requires pool size 1, got %d", poolSize)
	}
	if strings.TrimSpace(requestedIdentity) == "" {
		return nil, fmt.Errorf("timed runtime attestation requires a requested identity")
	}
	return &postgresTimedReadAttestor{
		pool:              pool,
		requestedIdentity: requestedIdentity,
		runID:             newRunUUID(),
	}, nil
}

// Begin supports benchmark evidence processing for begin.
func (s *postgresTimedReadAttestor) Begin(ctx context.Context, iteration int) error {
	if s.activeInvocation != "" {
		return fmt.Errorf("runtime attestation %q is still active", s.activeInvocation)
	}
	s.activeInvocation = fmt.Sprintf("%s-%d", s.runID, iteration)
	if _, err := s.pool.Exec(ctx, "select public.begin_traversal_runtime_attestation_v1($1, $2)", s.activeInvocation, s.requestedIdentity); err != nil {
		s.activeInvocation = ""
		return err
	}
	return nil
}

// Complete supports benchmark evidence processing for complete.
func (s *postgresTimedReadAttestor) Complete(ctx context.Context, _ int) (timedReadAttestation, error) {
	invocationID := s.activeInvocation
	if invocationID == "" {
		return timedReadAttestation{}, fmt.Errorf("no runtime attestation is active")
	}
	s.activeInvocation = ""
	var raw string
	readErr := s.pool.QueryRow(ctx, "select coalesce(public.read_traversal_runtime_attestation_v1($1)::text, '')", invocationID).Scan(&raw)
	_, clearErr := s.pool.Exec(ctx, "select public.clear_traversal_runtime_attestation_v1($1)", invocationID)
	if readErr != nil {
		return timedReadAttestation{}, readErr
	}
	if clearErr != nil {
		return timedReadAttestation{}, clearErr
	}
	if strings.TrimSpace(raw) == "" {
		return timedReadAttestation{}, fmt.Errorf("runtime invocation %q produced no receipt", invocationID)
	}
	var document postgresTimedRuntimeDocument
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return timedReadAttestation{}, fmt.Errorf("decode runtime receipt: %w", err)
	}
	if document.SchemaVersion != 2 || document.InvocationID != invocationID || document.RequestedIdentity != s.requestedIdentity {
		return timedReadAttestation{}, fmt.Errorf("runtime receipt identity does not match its armed invocation")
	}
	if document.RecordCount < 1 || len(document.Events) != document.RecordCount || document.RuntimeIdentity == "" || document.RuntimeBranch == "" || document.FallbackExecuted == nil {
		return timedReadAttestation{}, fmt.Errorf("runtime receipt is incomplete or has a broken event chain: %s", raw)
	}
	for idx, event := range document.Events {
		if event.Ordinal != idx+1 || event.RuntimeIdentity == "" || event.RuntimeBranch == "" {
			return timedReadAttestation{}, fmt.Errorf("runtime receipt event chain is not contiguous")
		}
		document.Events[idx].InvocationID = invocationID
	}
	return timedReadAttestation{
		InvocationID:      invocationID,
		RequestedIdentity: document.RequestedIdentity,
		RuntimeIdentity:   document.RuntimeIdentity,
		RuntimeBranch:     document.RuntimeBranch,
		FallbackExecuted:  document.FallbackExecuted,
		Events:            append([]RuntimeReceiptEvent(nil), document.Events...),
	}, nil
}
