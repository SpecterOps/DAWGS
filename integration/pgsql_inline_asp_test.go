// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

var (
	inlineASPNodeKind = graph.StringKind("InlineASPNode")
	inlineASPEdgeOne  = graph.StringKind("InlineASPEdgeOne")
	inlineASPEdgeTwo  = graph.StringKind("InlineASPEdgeTwo")
)

const inlineASPCypher = `
	MATCH p = allShortestPaths((s)-[:InlineASPEdgeOne|InlineASPEdgeTwo*1..4]->(e))
	WHERE id(s) = $start_id AND id(e) = $end_id
	RETURN p
`

// TestPostgreSQLInlineASPMatchesA1AndFallsBackWithoutPartialRows exercises the
// typed guarded statement at the real PostgreSQL boundary. A tiny state cap
// must select exact A1 and return the same complete relationship-distinct bag.
func TestPostgreSQLInlineASPMatchesA1AndFallsBackWithoutPartialRows(t *testing.T) {
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{inlineASPNodeKind},
		ExtraEdgeKinds:       graph.Kinds{inlineASPEdgeOne, inlineASPEdgeTwo},
	})

	var startID, endID, disconnectedID, deepStartID, deepEndID graph.ID
	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		left, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		end, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		startID, endID = start.ID, end.ID
		disconnected, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		disconnectedID = disconnected.ID
		deepStart, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		deepMiddleOne, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		deepMiddleTwo, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		deepEnd, err := tx.CreateNode(graph.NewProperties(), inlineASPNodeKind)
		if err != nil {
			return err
		}
		deepStartID, deepEndID = deepStart.ID, deepEnd.ID
		for _, edge := range []struct {
			start graph.ID
			end   graph.ID
			kind  graph.Kind
		}{
			{start.ID, left.ID, inlineASPEdgeOne},
			{left.ID, end.ID, inlineASPEdgeOne},
			{start.ID, right.ID, inlineASPEdgeTwo},
			{right.ID, end.ID, inlineASPEdgeTwo},
			{left.ID, left.ID, inlineASPEdgeOne},
			{left.ID, start.ID, inlineASPEdgeTwo},
			{deepStart.ID, deepMiddleOne.ID, inlineASPEdgeOne},
			{deepMiddleOne.ID, deepMiddleTwo.ID, inlineASPEdgeOne},
			{deepMiddleTwo.ID, deepEnd.ID, inlineASPEdgeOne},
		} {
			if _, err := tx.CreateRelationshipByIDs(edge.start, edge.end, edge.kind, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load inline ASP fixture: %v", err)
	}

	pgDriver, ok := session.DB.(*pg.Driver)
	if !ok {
		t.Fatalf("expected PostgreSQL driver, found %T", session.DB)
	}
	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("PostgreSQL default graph is not set")
	}
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), inlineASPCypher)
	if err != nil {
		t.Fatalf("parse inline ASP query: %v", err)
	}
	parameters := map[string]any{"start_id": int64(startID), "end_id": int64(endID)}

	a1, err := translate.Translate(session.Ctx, regularQuery, pgDriver.KindMapper(), parameters, defaultGraph.ID)
	if err != nil {
		t.Fatalf("translate A1: %v", err)
	}
	i1, err := translate.TranslateForTool(session.Ctx, regularQuery, pgDriver.KindMapper(), parameters, defaultGraph.ID,
		translate.ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG})
	if err != nil {
		t.Fatalf("translate I1: %v", err)
	}
	fallback, err := translate.TranslateWithProductionOptions(session.Ctx, regularQuery, pgDriver.KindMapper(), parameters, defaultGraph.ID,
		translate.ProductionOptions{
			ShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG,
			ShortestPathCaps: &translate.ProductionShortestPathCaps{
				StateLimit: 100, PredecessorLimit: 100, EnumerationLimit: 1, OutputBytesLimit: 1 << 20,
			},
			AuthorizedBucket: &translate.ProductionTraversalBucket{
				Direction: "outbound", ObservationMode: "all_paths", MinimumDepth: 1, MaximumDepth: 4,
				RelationshipKindCount: 2, UntypedRelationship: false,
			},
			SelectorVersion: "asp-i1-integration-fallback-v1",
		})
	if err != nil {
		t.Fatalf("translate I1 fallback: %v", err)
	}

	a1Rows := executeInlineASPTranslation(t, session, a1)
	i1Rows, candidateReceipt := executeInlineASPTranslationWithReceipt(t, session, i1, "inline-asp-candidate")
	fallbackRows, fallbackReceipt := executeInlineASPTranslationWithReceipt(t, session, fallback, "inline-asp-fallback")
	if len(a1Rows) != 2 {
		t.Fatalf("expected two relationship-distinct shortest paths, got %d: %v", len(a1Rows), a1Rows)
	}
	if fmt.Sprint(a1Rows) != fmt.Sprint(i1Rows) {
		t.Fatalf("inline I1 differs from A1: A1=%v I1=%v", a1Rows, i1Rows)
	}
	if !containsAll(candidateReceipt, "ASP-I1-U-DAG+MAT-M0", "inline_predecessor_dag", "false", "1") {
		t.Fatalf("candidate runtime receipt is incomplete: %s", candidateReceipt)
	}
	if fmt.Sprint(a1Rows) != fmt.Sprint(fallbackRows) {
		t.Fatalf("guarded fallback differs from A1: A1=%v fallback=%v", a1Rows, fallbackRows)
	}
	if !containsAll(fallbackReceipt, "ASP-A1-DAG", "exact_a1_fallback", "true", "1") {
		t.Fatalf("fallback runtime receipt is incomplete: %s", fallbackReceipt)
	}
	candidatePlan := explainInlineASPTranslation(t, session, i1)
	requireOrientationSubplanMetric(t, candidatePlan, "asp_i1_fallback_rows", "Actual Rows", 0)
	fallbackPlan := explainInlineASPTranslation(t, session, fallback)
	requireOrientationSubplanMetric(t, fallbackPlan, "asp_i1_candidate_rows", "Actual Rows", 0)

	for _, testCase := range []struct {
		name       string
		query      string
		parameters map[string]any
	}{
		{
			name: "inbound",
			query: `MATCH p = allShortestPaths((e)<-[:InlineASPEdgeOne|InlineASPEdgeTwo*1..4]-(s))
			        WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`,
			parameters: parameters,
		},
		{
			name:       "no path",
			query:      inlineASPCypher,
			parameters: map[string]any{"start_id": int64(startID), "end_id": int64(disconnectedID)},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			if err != nil {
				t.Fatalf("parse query: %v", err)
			}
			a1Translation, err := translate.Translate(session.Ctx, query, pgDriver.KindMapper(), testCase.parameters, defaultGraph.ID)
			if err != nil {
				t.Fatalf("translate A1: %v", err)
			}
			i1Translation, err := translate.TranslateForTool(session.Ctx, query, pgDriver.KindMapper(), testCase.parameters, defaultGraph.ID,
				translate.ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG})
			if err != nil {
				t.Fatalf("translate I1: %v", err)
			}
			expected := executeInlineASPTranslation(t, session, a1Translation)
			actual := executeInlineASPTranslation(t, session, i1Translation)
			if testCase.name == "no path" {
				var receipt string
				actual, receipt = executeInlineASPTranslationWithReceipt(t, session, i1Translation, "inline-asp-no-path")
				if !containsAll(receipt, "ASP-I1-U-DAG+MAT-M0", "inline_no_path", "false", "1") {
					t.Fatalf("no-path runtime receipt is incomplete: %s", receipt)
				}
			}
			if fmt.Sprint(expected) != fmt.Sprint(actual) {
				t.Fatalf("I1 differs from A1: A1=%v I1=%v", expected, actual)
			}
		})
	}

	t.Run("driver policy requires stable snapshot and rolls back immediately", func(t *testing.T) {
		policy := inlineASPTraversalPolicy(t, inlineASPCypher)
		if err := pgDriver.SetTraversalPolicy(policy); err != nil {
			t.Fatalf("set inline ASP policy: %v", err)
		}
		t.Cleanup(func() { _ = pgDriver.SetTraversalPolicy(pg.TraversalPolicy{}) })

		readCommittedRows, readCommittedReceipt := executeDriverCypherWithReceipt(t, session, inlineASPCypher, parameters,
			"inline-asp-policy-read-committed", optimize.ShortestPathExecutorASPA1DAG)
		if fmt.Sprint(a1Rows) != fmt.Sprint(readCommittedRows) || !containsAll(readCommittedReceipt, "ASP-A1-DAG") {
			t.Fatalf("read-committed policy did not preserve A1: rows=%v receipt=%s", readCommittedRows, readCommittedReceipt)
		}

		repeatableRows, repeatableReceipt := executeDriverCypherWithReceipt(t, session, inlineASPCypher, parameters,
			"inline-asp-policy-repeatable", optimize.ShortestPathExecutorASPI1DAG, pg.OptionSetTransactionIsolation(pgx.RepeatableRead))
		if fmt.Sprint(a1Rows) != fmt.Sprint(repeatableRows) || !containsAll(repeatableReceipt, "ASP-I1-U-DAG+MAT-M0", "inline_predecessor_dag") {
			t.Fatalf("repeatable-read policy did not execute I1: rows=%v receipt=%s", repeatableRows, repeatableReceipt)
		}

		if err := pgDriver.SetTraversalPolicy(pg.TraversalPolicy{Generation: policy.Generation + 1, DisableInlineASPDAG: true}); err != nil {
			t.Fatalf("activate inline ASP rollback: %v", err)
		}
		rollbackRows, rollbackReceipt := executeDriverCypherWithReceipt(t, session, inlineASPCypher, parameters,
			"inline-asp-policy-rollback", optimize.ShortestPathExecutorASPA1DAG, pg.OptionSetTransactionIsolation(pgx.RepeatableRead))
		if fmt.Sprint(a1Rows) != fmt.Sprint(rollbackRows) || !containsAll(rollbackReceipt, "ASP-A1-DAG") || strings.Contains(rollbackReceipt, "ASP-I1-U-DAG+MAT-M0") {
			t.Fatalf("rollback did not immediately restore A1: rows=%v receipt=%s", rollbackRows, rollbackReceipt)
		}
	})

	t.Run("canonical inline witness falls back to S4 before exposing rows", func(t *testing.T) {
		const shortestCypher = `MATCH p = shortestPath((s)-[:InlineASPEdgeOne*1..4]->(e))
			WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`
		query, err := frontend.ParseCypher(frontend.NewContext(), shortestCypher)
		if err != nil {
			t.Fatalf("parse canonical shortest query: %v", err)
		}
		deepParameters := map[string]any{"start_id": int64(deepStartID), "end_id": int64(deepEndID)}
		incumbent, err := translate.Translate(session.Ctx, query, pgDriver.KindMapper(), deepParameters, defaultGraph.ID)
		if err != nil {
			t.Fatalf("translate shortest incumbent: %v", err)
		}
		candidate, err := translate.TranslateWithProductionOptions(session.Ctx, query, pgDriver.KindMapper(), deepParameters, defaultGraph.ID,
			translate.ProductionOptions{
				ShortestPathExecutor: optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
				ShortestPathCaps: &translate.ProductionShortestPathCaps{
					StateLimit: 1, PredecessorLimit: 100, EnumerationLimit: 100, OutputBytesLimit: 1 << 20,
				},
				AuthorizedBucket: &translate.ProductionTraversalBucket{
					Direction: "outbound", ObservationMode: "one_path", MinimumDepth: 1, MaximumDepth: 4, RelationshipKindCount: 1,
				},
				SelectorVersion: "sp-i1-integration-fallback-v1",
			})
		if err != nil {
			t.Fatalf("translate canonical shortest candidate: %v", err)
		}
		expected := executeInlineASPTranslation(t, session, incumbent)
		actual, receipt := executeInlineASPTranslationWithReceipt(t, session, candidate, "sp-i1-s4-fallback", optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
		if fmt.Sprint(expected) != fmt.Sprint(actual) {
			t.Fatalf("canonical fallback differs from incumbent: incumbent=%v candidate=%v", expected, actual)
		}
		if !containsAll(receipt, "exact_s4_fallback", "SP-S4-C-WE+MAT-M0", "exact_relationship_trail_fallback", "SP-S3-U-E+MAT-M0", "2") {
			t.Fatalf("canonical fallback receipt does not contain the complete event chain: %s", receipt)
		}
	})
}

func inlineASPTraversalPolicy(t *testing.T, query string) pg.TraversalPolicy {
	t.Helper()
	queryDigest := pg.TraversalPolicyQuerySHA256(query)
	evidence := map[string]map[string]string{}
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"} {
		evidence[role] = map[string]string{"sha256": strings.Repeat("01", sha256.Size)}
	}
	raw, err := json.Marshal(map[string]any{
		"version": 2, "candidate": string(optimize.ShortestPathExecutorASPI1DAG), "selector_version": "asp-i1-driver-integration-v1",
		"source_commit": "integration", "source_sha256": strings.Repeat("0", 64),
		"binary_sha256": strings.Repeat("0", 64), "corpus_sha256": strings.Repeat("0", 64),
		"execution_boundary": "guarded_dual_arm", "fallback_executor": string(optimize.ShortestPathExecutorASPA1DAG),
		"caps": map[string]int64{"state_limit": 1000, "predecessor_limit": 1000, "enumeration_limit": 1000, "output_bytes_limit": 1 << 20},
		"buckets": []map[string]any{{
			"query_sha256": []string{queryDigest}, "qualification_split": []string{"training", "holdout"},
			"direction": "outbound", "observation_mode": "all_paths", "minimum_depth": 1, "maximum_depth": 4,
			"relationship_kind_count": 2, "untyped_relationship": false,
		}},
		"evidence": evidence,
	})
	if err != nil {
		t.Fatalf("encode inline ASP policy: %v", err)
	}
	digest := sha256.Sum256(raw)
	return pg.TraversalPolicy{
		Generation: 1, PromotionManifestSHA256: hex.EncodeToString(digest[:]), PromotionManifestJSON: raw,
		QuerySHA256Allowlist: []string{queryDigest}, ShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG,
	}
}

func explainInlineASPTranslation(t *testing.T, session *Session, translation translate.Result) any {
	t.Helper()
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render translated query: %v", err)
	}
	var plan any
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Raw("explain (analyze, timing off, summary off, format json) "+sqlQuery, translation.Parameters)
		defer result.Close()
		if !result.Next() {
			if err := result.Error(); err != nil {
				return err
			}
			return errors.New("PostgreSQL EXPLAIN returned no rows")
		}
		values := result.Values()
		if len(values) == 0 {
			return errors.New("PostgreSQL EXPLAIN returned an empty row")
		}
		parsed, err := normalizeExplainPlan(values[0])
		if err != nil {
			return err
		}
		plan = parsed
		return result.Error()
	}); err != nil {
		t.Fatalf("explain inline ASP query: %v", err)
	}
	return plan
}

func executeInlineASPTranslationWithReceipt(t *testing.T, session *Session, translation translate.Result, invocation string, requested ...optimize.ShortestPathExecutor) ([]string, string) {
	t.Helper()
	requestedIdentity := optimize.ShortestPathExecutorASPI1DAG
	if len(requested) > 0 {
		requestedIdentity = requested[0]
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render translated query: %v", err)
	}
	var rows []string
	var receipt string
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		arm := tx.Raw("select public.begin_traversal_runtime_attestation_v1(@invocation, @requested)", map[string]any{
			"invocation": invocation, "requested": string(requestedIdentity),
		})
		for arm.Next() {
		}
		if err := arm.Error(); err != nil {
			arm.Close()
			return err
		}
		arm.Close()

		result := tx.Raw(sqlQuery, translation.Parameters)
		for result.Next() {
			rows = append(rows, fmt.Sprint(result.Values()))
		}
		if err := result.Error(); err != nil {
			result.Close()
			return err
		}
		result.Close()

		read := tx.Raw(`select
			coalesce(document ->> 'runtime_identity', ''),
			coalesce(document ->> 'runtime_branch', ''),
			coalesce(document ->> 'fallback_executed', ''),
			coalesce(document ->> 'record_count', ''),
			coalesce(document ->> 'events', '')
			from (select public.read_traversal_runtime_attestation_v1(@invocation) document) receipt`, map[string]any{"invocation": invocation})
		if read.Next() {
			receipt = fmt.Sprint(read.Values())
		}
		if err := read.Error(); err != nil {
			read.Close()
			return err
		}
		read.Close()
		clear := tx.Raw("select public.clear_traversal_runtime_attestation_v1(@invocation)", map[string]any{"invocation": invocation})
		for clear.Next() {
		}
		err := clear.Error()
		clear.Close()
		return err
	}); err != nil {
		t.Fatalf("execute translated query with receipt: %v\nSQL: %s", err, sqlQuery)
	}
	sort.Strings(rows)
	return rows, receipt
}

func executeDriverCypherWithReceipt(t *testing.T, session *Session, cypher string, parameters map[string]any, invocation string,
	requested optimize.ShortestPathExecutor, options ...graph.TransactionOption) ([]string, string) {
	t.Helper()
	var rows []string
	var receipt string
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		arm := tx.Raw("select public.begin_traversal_runtime_attestation_v1(@invocation, @requested)", map[string]any{
			"invocation": invocation, "requested": string(requested),
		})
		for arm.Next() {
		}
		if err := arm.Error(); err != nil {
			arm.Close()
			return err
		}
		arm.Close()

		result := tx.Query(cypher, parameters)
		for result.Next() {
			rows = append(rows, fmt.Sprint(result.Values()))
		}
		if err := result.Error(); err != nil {
			result.Close()
			return err
		}
		result.Close()

		read := tx.Raw("select coalesce(public.read_traversal_runtime_attestation_v1(@invocation)::text, '')", map[string]any{"invocation": invocation})
		if read.Next() {
			values := read.Values()
			if len(values) > 0 {
				receipt = fmt.Sprint(values[0])
			}
		}
		if err := read.Error(); err != nil {
			read.Close()
			return err
		}
		read.Close()
		clear := tx.Raw("select public.clear_traversal_runtime_attestation_v1(@invocation)", map[string]any{"invocation": invocation})
		for clear.Next() {
		}
		err := clear.Error()
		clear.Close()
		return err
	}, append(options, pg.OptionInitializeTraversalRuntimeAttestation())...); err != nil {
		t.Fatalf("execute driver Cypher with receipt: %v", err)
	}
	sort.Strings(rows)
	return rows, receipt
}

func containsAll(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}

func executeInlineASPTranslation(t *testing.T, session *Session, translation translate.Result) []string {
	t.Helper()
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render translated query: %v", err)
	}
	var rows []string
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Raw(sqlQuery, translation.Parameters)
		defer result.Close()
		for result.Next() {
			rows = append(rows, fmt.Sprint(result.Values()))
		}
		return result.Error()
	}); err != nil {
		t.Fatalf("execute translated query: %v", err)
	}
	sort.Strings(rows)
	return rows
}
