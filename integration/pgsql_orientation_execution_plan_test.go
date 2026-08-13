// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"errors"
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
