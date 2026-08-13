// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

const orientationExecutionPlanCypher = `
	MATCH (root:ExpansionRoot)
	WHERE root.root_key = $root_key
	MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(:SuffixTerminal)
	RETURN path
`

var (
	orientationRootKind        = graph.StringKind("ExpansionRoot")
	orientationExpansionKind   = graph.StringKind("ExpansionNode")
	orientationSuffixHeadKind  = graph.StringKind("SuffixHead")
	orientationSuffixMidKind   = graph.StringKind("SuffixMiddle")
	orientationSuffixEndKind   = graph.StringKind("SuffixTerminal")
	orientationExpandEdge      = graph.StringKind("Expand")
	orientationSuffixEdgeOne   = graph.StringKind("EnterSuffix")
	orientationSuffixEdgeTwo   = graph.StringKind("ContinueSuffix")
	orientationSuffixEdgeThree = graph.StringKind("CompleteSuffix")
)

// TestPostgreSQLGuardedOrientationInactiveArmLoops proves the emitted
// marker-first LATERAL dependencies at the PostgreSQL execution boundary. The
// forward case must leave reverse recursion uninitialized; the reverse case
// must leave the exact materialized incumbent uninitialized.
func TestPostgreSQLGuardedOrientationInactiveArmLoops(t *testing.T) {
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds: graph.Kinds{
			orientationRootKind,
			orientationExpansionKind,
			orientationSuffixHeadKind,
			orientationSuffixMidKind,
			orientationSuffixEndKind,
		},
		ExtraEdgeKinds: graph.Kinds{
			orientationExpandEdge,
			orientationSuffixEdgeOne,
			orientationSuffixEdgeTwo,
			orientationSuffixEdgeThree,
		},
	})

	for _, testCase := range []struct {
		name                     string
		reverseDominates         bool
		expectedReverseLoops     int64
		expectedIncumbentLoops   int64
		expectedCandidateMarkers int64
		expectedIncumbentMarkers int64
	}{
		{
			name:                     "forward policy does not initialize reverse recursion",
			reverseDominates:         false,
			expectedReverseLoops:     0,
			expectedIncumbentLoops:   1,
			expectedCandidateMarkers: 0,
			expectedIncumbentMarkers: 1,
		},
		{
			name:                     "reverse policy does not initialize exact incumbent",
			reverseDominates:         true,
			expectedReverseLoops:     1,
			expectedIncumbentLoops:   0,
			expectedCandidateMarkers: 1,
			expectedIncumbentMarkers: 0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session.ClearGraph(t)
			loadOrientationExecutionFixture(t, session, testCase.reverseDominates)

			plan := explainGuardedOrientation(t, session)
			requireOrientationSubplanMetric(t, plan, "_orientation_executed_candidate", "Actual Rows", testCase.expectedCandidateMarkers)
			requireOrientationSubplanMetric(t, plan, "_orientation_executed_incumbent", "Actual Rows", testCase.expectedIncumbentMarkers)
			requireOrientationSubplanMetric(t, plan, "_orientation_reverse", "Actual Loops", testCase.expectedReverseLoops)
			requireOrientationSubplanMetric(t, plan, "_orientation_incumbent", "Actual Loops", testCase.expectedIncumbentLoops)
		})
	}
}

// TestPostgreSQLShadowOrientationAttestsEmptyIncumbent proves the shadow
// statement records its only executable arm even when that arm returns no
// rows. The marker must be outside the incumbent LATERAL boundary or an empty
// result would leave the timed receipt unprovable.
func TestPostgreSQLShadowOrientationAttestsEmptyIncumbent(t *testing.T) {
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds: graph.Kinds{
			orientationRootKind,
			orientationExpansionKind,
			orientationSuffixHeadKind,
			orientationSuffixMidKind,
			orientationSuffixEndKind,
		},
		ExtraEdgeKinds: graph.Kinds{
			orientationExpandEdge,
			orientationSuffixEdgeOne,
			orientationSuffixEdgeTwo,
			orientationSuffixEdgeThree,
		},
	})

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), orientationExecutionPlanCypher)
	if err != nil {
		t.Fatalf("parse shadow orientation query: %v", err)
	}
	pgDriver, ok := session.DB.(*pg.Driver)
	if !ok {
		t.Fatalf("expected PostgreSQL driver, found %T", session.DB)
	}
	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("PostgreSQL default graph is not set")
	}
	translation, err := translate.TranslateForTool(
		session.Ctx,
		regularQuery,
		pgDriver.KindMapper(),
		map[string]any{"root_key": "missing-orientation-plan-root"},
		defaultGraph.ID,
		translate.ToolOptions{EnableExpansionOrientationShadow: true},
	)
	if err != nil {
		t.Fatalf("translate shadow orientation query: %v", err)
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render shadow orientation query: %v", err)
	}

	const invocation = "shadow-orientation-empty-incumbent"
	var (
		rowCount int
		receipt  string
	)
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		arm := tx.Raw("select public.begin_traversal_runtime_attestation_v1(@invocation, @requested)", map[string]any{
			"invocation": invocation,
			"requested":  "EXPANSION-SUFFIX-SEEDED-REVERSE",
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
			rowCount++
		}
		if err := result.Error(); err != nil {
			result.Close()
			return err
		}
		result.Close()

		read := tx.Raw("select coalesce(public.read_traversal_runtime_attestation_v1(@invocation)::text, '')", map[string]any{"invocation": invocation})
		if read.Next() && len(read.Values()) > 0 {
			receipt = fmt.Sprint(read.Values()[0])
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
		t.Fatalf("execute empty shadow orientation query: %v\nSQL: %s", err, sqlQuery)
	}
	if rowCount != 0 {
		t.Fatalf("empty shadow incumbent returned %d rows", rowCount)
	}
	for _, fragment := range []string{`"runtime_identity": "EXPANSION-STEPWISE-FORWARD"`, `"runtime_branch": "shadow_incumbent"`, `"fallback_executed": false`, `"record_count": 1`} {
		if !strings.Contains(receipt, fragment) {
			t.Fatalf("empty shadow incumbent receipt lacks %q: %s", fragment, receipt)
		}
	}
}

// TestPostgreSQLGuardedOrientationFallbackReceipts proves both cap+1 fallback
// paths produce one truthful incumbent receipt. Probe overflow skips reverse
// recursion entirely; state overflow performs only the bounded reverse
// admission probe before executing the exact forward fallback.
func TestPostgreSQLGuardedOrientationFallbackReceipts(t *testing.T) {
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds: graph.Kinds{
			orientationRootKind,
			orientationExpansionKind,
			orientationSuffixHeadKind,
			orientationSuffixMidKind,
			orientationSuffixEndKind,
		},
		ExtraEdgeKinds: graph.Kinds{
			orientationExpandEdge,
			orientationSuffixEdgeOne,
			orientationSuffixEdgeTwo,
			orientationSuffixEdgeThree,
		},
	})

	for _, testCase := range []struct {
		name         string
		rootKey      string
		load         func(*testing.T, *Session)
		expectedRows int
	}{
		{
			name:         "probe overflow",
			rootKey:      "orientation-probe-overflow-root",
			load:         loadOrientationProbeOverflowFixture,
			expectedRows: 0,
		},
		{
			name:         "state overflow",
			rootKey:      "orientation-state-overflow-root",
			load:         loadOrientationStateOverflowFixture,
			expectedRows: 4096,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session.ClearGraph(t)
			testCase.load(t, session)
			rowCount, receipt := executeGuardedOrientationWithReceipt(t, session, testCase.rootKey)
			if rowCount != testCase.expectedRows {
				t.Fatalf("guarded orientation returned %d rows, want %d", rowCount, testCase.expectedRows)
			}
			for _, fragment := range []string{`"runtime_identity": "EXPANSION-STEPWISE-FORWARD"`, `"runtime_branch": "exact_forward_incumbent"`, `"fallback_executed": true`, `"record_count": 1`} {
				if !strings.Contains(receipt, fragment) {
					t.Fatalf("guarded orientation receipt lacks %q: %s", fragment, receipt)
				}
			}
		})
	}
}

func executeGuardedOrientationWithReceipt(t *testing.T, session *Session, rootKey string) (int, string) {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), orientationExecutionPlanCypher)
	if err != nil {
		t.Fatalf("parse guarded orientation query: %v", err)
	}
	pgDriver, ok := session.DB.(*pg.Driver)
	if !ok {
		t.Fatalf("expected PostgreSQL driver, found %T", session.DB)
	}
	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("PostgreSQL default graph is not set")
	}
	translation, err := translate.TranslateForTool(
		session.Ctx,
		regularQuery,
		pgDriver.KindMapper(),
		map[string]any{"root_key": rootKey},
		defaultGraph.ID,
		translate.ToolOptions{EnableExpansionOrientationTournament: true},
	)
	if err != nil {
		t.Fatalf("translate guarded orientation query: %v", err)
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render guarded orientation query: %v", err)
	}

	invocation := "guarded-" + rootKey
	var (
		rowCount int
		receipt  string
	)
	if err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		arm := tx.Raw("select public.begin_traversal_runtime_attestation_v1(@invocation, @requested)", map[string]any{
			"invocation": invocation,
			"requested":  "EXPANSION-SUFFIX-SEEDED-REVERSE",
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
			rowCount++
		}
		if err := result.Error(); err != nil {
			result.Close()
			return err
		}
		result.Close()

		read := tx.Raw("select coalesce(public.read_traversal_runtime_attestation_v1(@invocation)::text, '')", map[string]any{"invocation": invocation})
		if read.Next() && len(read.Values()) > 0 {
			receipt = fmt.Sprint(read.Values()[0])
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
		t.Fatalf("execute guarded orientation query: %v\nSQL: %s", err, sqlQuery)
	}
	return rowCount, receipt
}

func loadOrientationProbeOverflowFixture(t *testing.T, session *Session) {
	t.Helper()

	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.AsProperties(map[string]any{"root_key": "orientation-probe-overflow-root"}), orientationRootKind); err != nil {
			return err
		}
		for index := 0; index <= 512; index++ {
			if _, err := createOrientationSuffix(tx); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load orientation probe-overflow fixture: %v", err)
	}
}

func loadOrientationStateOverflowFixture(t *testing.T, session *Session) {
	t.Helper()

	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		root, err := tx.CreateNode(graph.AsProperties(map[string]any{"root_key": "orientation-state-overflow-root"}), orientationRootKind)
		if err != nil {
			return err
		}
		boundary, err := createOrientationSuffix(tx)
		if err != nil {
			return err
		}
		first, err := createOrientationNodes(tx, 16)
		if err != nil {
			return err
		}
		second, err := createOrientationNodes(tx, 32)
		if err != nil {
			return err
		}
		third, err := createOrientationNodes(tx, 8)
		if err != nil {
			return err
		}
		for _, node := range first {
			if _, err := tx.CreateRelationshipByIDs(root.ID, node.ID, orientationExpandEdge, graph.NewProperties()); err != nil {
				return err
			}
		}
		if err := connectOrientationLayers(tx, first, second); err != nil {
			return err
		}
		if err := connectOrientationLayers(tx, second, third); err != nil {
			return err
		}
		for _, node := range third {
			if _, err := tx.CreateRelationshipByIDs(node.ID, boundary.ID, orientationExpandEdge, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load orientation state-overflow fixture: %v", err)
	}
}

func createOrientationSuffix(tx graph.Transaction) (*graph.Node, error) {
	boundary, err := tx.CreateNode(graph.NewProperties(), orientationExpansionKind)
	if err != nil {
		return nil, err
	}
	head, err := tx.CreateNode(graph.NewProperties(), orientationSuffixHeadKind)
	if err != nil {
		return nil, err
	}
	middle, err := tx.CreateNode(graph.NewProperties(), orientationSuffixMidKind)
	if err != nil {
		return nil, err
	}
	terminal, err := tx.CreateNode(graph.NewProperties(), orientationSuffixEndKind)
	if err != nil {
		return nil, err
	}
	for _, edge := range []struct {
		start, end graph.ID
		kind       graph.Kind
	}{
		{boundary.ID, head.ID, orientationSuffixEdgeOne},
		{head.ID, middle.ID, orientationSuffixEdgeTwo},
		{middle.ID, terminal.ID, orientationSuffixEdgeThree},
	} {
		if _, err := tx.CreateRelationshipByIDs(edge.start, edge.end, edge.kind, graph.NewProperties()); err != nil {
			return nil, err
		}
	}
	return boundary, nil
}

func createOrientationNodes(tx graph.Transaction, count int) ([]*graph.Node, error) {
	nodes := make([]*graph.Node, 0, count)
	for index := 0; index < count; index++ {
		node, err := tx.CreateNode(graph.NewProperties(), orientationExpansionKind)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func connectOrientationLayers(tx graph.Transaction, left, right []*graph.Node) error {
	for _, start := range left {
		for _, end := range right {
			if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, orientationExpandEdge, graph.NewProperties()); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadOrientationExecutionFixture(t *testing.T, session *Session, reverseDominates bool) {
	t.Helper()

	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		root, err := tx.CreateNode(graph.AsProperties(map[string]any{"root_key": "orientation-plan-root"}), orientationRootKind)
		if err != nil {
			return err
		}

		addSuffix := func(connectRoot bool) error {
			boundary, err := tx.CreateNode(graph.NewProperties(), orientationExpansionKind)
			if err != nil {
				return err
			}
			head, err := tx.CreateNode(graph.NewProperties(), orientationSuffixHeadKind)
			if err != nil {
				return err
			}
			middle, err := tx.CreateNode(graph.NewProperties(), orientationSuffixMidKind)
			if err != nil {
				return err
			}
			terminal, err := tx.CreateNode(graph.NewProperties(), orientationSuffixEndKind)
			if err != nil {
				return err
			}
			if connectRoot {
				if _, err := tx.CreateRelationshipByIDs(root.ID, boundary.ID, orientationExpandEdge, graph.NewProperties()); err != nil {
					return err
				}
			}
			for _, edge := range []struct {
				start, end graph.ID
				kind       graph.Kind
			}{
				{boundary.ID, head.ID, orientationSuffixEdgeOne},
				{head.ID, middle.ID, orientationSuffixEdgeTwo},
				{middle.ID, terminal.ID, orientationSuffixEdgeThree},
			} {
				if _, err := tx.CreateRelationshipByIDs(edge.start, edge.end, edge.kind, graph.NewProperties()); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addSuffix(true); err != nil {
			return err
		}
		if reverseDominates {
			// One reverse seed but many typed forward neighbors makes reverse
			// strictly dominate orientation-probe-v1's 4:3 hysteresis rule.
			for index := 0; index < 24; index++ {
				decoy, err := tx.CreateNode(graph.NewProperties(), orientationExpansionKind)
				if err != nil {
					return err
				}
				if _, err := tx.CreateRelationshipByIDs(root.ID, decoy.ID, orientationExpandEdge, graph.NewProperties()); err != nil {
					return err
				}
			}
		} else {
			// Many disconnected suffix seeds overwhelm the one useful forward
			// neighbor, so the incumbent wins decisively.
			for index := 0; index < 20; index++ {
				if err := addSuffix(false); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load guarded orientation fixture: %v", err)
	}
}

func explainGuardedOrientation(t *testing.T, session *Session) any {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), orientationExecutionPlanCypher)
	if err != nil {
		t.Fatalf("parse guarded orientation query: %v", err)
	}
	pgDriver, ok := session.DB.(*pg.Driver)
	if !ok {
		t.Fatalf("expected PostgreSQL driver, found %T", session.DB)
	}
	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("PostgreSQL default graph is not set")
	}
	translation, err := translate.TranslateForTool(
		session.Ctx,
		regularQuery,
		pgDriver.KindMapper(),
		map[string]any{"root_key": "orientation-plan-root"},
		defaultGraph.ID,
		translate.ToolOptions{EnableExpansionOrientationTournament: true},
	)
	if err != nil {
		t.Fatalf("translate guarded orientation query: %v", err)
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("render guarded orientation query: %v", err)
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
		t.Fatalf("explain guarded orientation query: %v", err)
	}
	return plan
}

func requireOrientationSubplanMetric(t *testing.T, plan any, suffix, metric string, expected int64) {
	t.Helper()

	subplan, found := findOrientationSubplan(plan, suffix)
	if !found {
		t.Fatalf("PostgreSQL JSON plan has no subplan ending in %q", suffix)
	}
	actual, ok := postgresPlanInt64(subplan[metric])
	if !ok {
		t.Fatalf("orientation subplan %q has no numeric %s", subplan["Subplan Name"], metric)
	}
	if actual != expected {
		t.Fatalf("orientation subplan %q %s: got %d, want %d", subplan["Subplan Name"], metric, actual, expected)
	}
}

func findOrientationSubplan(value any, suffix string) (map[string]any, bool) {
	switch typed := value.(type) {
	case []any:
		for _, child := range typed {
			if found, ok := findOrientationSubplan(child, suffix); ok {
				return found, true
			}
		}
	case map[string]any:
		if name, ok := typed["Subplan Name"].(string); ok && strings.HasSuffix(name, suffix) {
			return typed, true
		}
		for _, child := range typed {
			if found, ok := findOrientationSubplan(child, suffix); ok {
				return found, true
			}
		}
	}
	return nil, false
}

func postgresPlanInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), typed == float64(int64(typed))
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	default:
		return 0, false
	}
}
