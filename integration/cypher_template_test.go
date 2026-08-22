// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
)

// cypherTemplateFile describes the ordinary and metamorphic query families loaded from one template JSON file.
type cypherTemplateFile struct {
	// Families contains independently asserted query-template families.
	Families []cypherTemplateFamily `json:"families,omitempty"`

	// Metamorphic contains families whose query variants must produce equivalent results.
	Metamorphic []cypherMetamorphicFamily `json:"metamorphic,omitempty"`

	// path records the source file for subtest naming and diagnostics.
	path string
}

// cypherTemplateFamily combines a fixture and query template with the variants asserted against it.
type cypherTemplateFamily struct {
	// Name identifies the family in test output.
	Name string `json:"name"`

	// Fixture is loaded transactionally for every variant.
	Fixture *opengraph.Graph `json:"fixture"`

	// Template is the Cypher source rendered with each variant's Vars.
	Template string `json:"template"`

	// Params supplies parameters shared by every variant.
	Params testutil.Params `json:"params,omitempty"`

	// NodeParams maps shared parameter names to fixture node identifiers.
	NodeParams map[string]string `json:"node_params,omitempty"`

	// NodeListParams maps shared parameter names to lists of fixture node identifiers.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`

	// Variants enumerates template substitutions and expected results.
	Variants []cypherTemplateVariant `json:"variants"`
}

// cypherTemplateVariant supplies one rendering and assertion for a query-template family.
type cypherTemplateVariant struct {
	// Name identifies the variant in test output.
	Name string `json:"name"`

	// Vars contains text substitutions applied to the Cypher template.
	Vars map[string]string `json:"vars,omitempty"`

	// Params augments or overrides family-level query parameters.
	Params testutil.Params `json:"params,omitempty"`

	// NodeParams augments or overrides family-level fixture-node parameters.
	NodeParams map[string]string `json:"node_params,omitempty"`

	// NodeListParams augments or overrides family-level fixture-node-list parameters.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`

	// Assert encodes the expected primary query result.
	Assert json.RawMessage `json:"assert"`

	// PostAssertions contains state checks run after the primary query drains.
	PostAssertions []stateAssertion `json:"post_assertions,omitempty"`
}

// cypherMetamorphicFamily describes queries that must agree under the selected comparison modes.
type cypherMetamorphicFamily struct {
	// Name identifies the family in test output.
	Name string `json:"name"`

	// Fixture is loaded once for the family's equivalence comparison.
	Fixture *opengraph.Graph `json:"fixture"`

	// Compare selects the result dimensions used to establish equivalence.
	Compare comparisonModes `json:"compare"`

	// Queries contains the query variants compared with the baseline.
	Queries []cypherMetamorphicQuery `json:"queries"`
}

// cypherMetamorphicQuery is one named Cypher statement and parameter set in an equivalence family.
type cypherMetamorphicQuery struct {
	// Name identifies the query in test output.
	Name string `json:"name"`

	// Cypher is the statement executed for this variant.
	Cypher string `json:"cypher"`

	// Params contains the statement's query parameters.
	Params testutil.Params `json:"params,omitempty"`
}

// TestCypherTemplates renders every template variant and verifies its query and post-state assertions against the shared fixture.
func TestCypherTemplates(t *testing.T) {
	templateFiles := loadCypherTemplateFiles(t)
	nodeKinds, edgeKinds := cypherTemplateKinds(templateFiles)

	db, ctx := SetupDBWithKindsNoGraphCleanup(t, nodeKinds, edgeKinds)
	ClearGraph(t, db, ctx)

	for _, templateFile := range templateFiles {
		fileName := strings.TrimSuffix(filepath.Base(templateFile.path), filepath.Ext(templateFile.path))
		t.Run(fileName, func(t *testing.T) {
			for _, family := range templateFile.Families {
				t.Run(family.Name, func(t *testing.T) {
					for _, variant := range family.Variants {
						t.Run(variant.Name, func(t *testing.T) {
							var (
								cypher = renderCypherTemplate(t, family.Template, variant.Vars)
								check  = parseAssertion(t, variant.Assert)
								tc     = testCase{
									Name:           variant.Name,
									Cypher:         cypher,
									Params:         mergeParams(family.Params, variant.Params),
									NodeParams:     mergeStringMap(family.NodeParams, variant.NodeParams),
									NodeListParams: mergeStringListMap(family.NodeListParams, variant.NodeListParams),
									Fixture:        family.Fixture,
									PostAssertions: variant.PostAssertions,
								}
							)

							runWithTemplateFixture(t, ctx, db, tc, check)
						})
					}
				})
			}

			for _, family := range templateFile.Metamorphic {
				t.Run(family.Name, func(t *testing.T) {
					runMetamorphicFamily(t, ctx, db, family)
				})
			}
		})
	}
}

// loadCypherTemplateFiles reads and decodes every JSON template file, preserving each source path for diagnostics.
func loadCypherTemplateFiles(t *testing.T) []cypherTemplateFile {
	t.Helper()

	paths, err := filepath.Glob("testdata/templates/*.json")
	if err != nil {
		t.Fatalf("failed to glob template files: %v", err)
	}

	if len(paths) == 0 {
		t.Skip("no template files found in testdata/templates/")
	}

	templateFiles := make([]cypherTemplateFile, 0, len(paths))
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read %s: %v", path, err)
		}

		var templateFile cypherTemplateFile
		if err := json.Unmarshal(raw, &templateFile); err != nil {
			t.Fatalf("failed to decode %s: %v", path, err)
		}
		templateFile.path = path
		templateFiles = append(templateFiles, templateFile)
	}

	return templateFiles
}

// cypherTemplateKinds collects the node and relationship kinds used by every
// inline template and metamorphic fixture.
func cypherTemplateKinds(templateFiles []cypherTemplateFile) (graph.Kinds, graph.Kinds) {
	var nodeKinds, edgeKinds graph.Kinds

	for _, templateFile := range templateFiles {
		for _, family := range templateFile.Families {
			if family.Fixture != nil {
				familyNodeKinds, familyEdgeKinds := family.Fixture.Kinds()
				nodeKinds = nodeKinds.Add(familyNodeKinds...)
				edgeKinds = edgeKinds.Add(familyEdgeKinds...)
			}
		}

		for _, family := range templateFile.Metamorphic {
			if family.Fixture != nil {
				familyNodeKinds, familyEdgeKinds := family.Fixture.Kinds()
				nodeKinds = nodeKinds.Add(familyNodeKinds...)
				edgeKinds = edgeKinds.Add(familyEdgeKinds...)
			}
		}
	}

	return nodeKinds, edgeKinds
}

// renderCypherTemplate replaces named placeholders and fails if any placeholder
// remains unresolved.
func renderCypherTemplate(t *testing.T, template string, vars map[string]string) string {
	t.Helper()

	rendered := template
	for name, value := range vars {
		rendered = strings.ReplaceAll(rendered, "{{"+name+"}}", value)
	}

	if strings.Contains(rendered, "{{") || strings.Contains(rendered, "}}") {
		t.Fatalf("template has unresolved placeholders: %s", rendered)
	}

	return rendered
}

// mergeParams returns a copy of base with overrides taking precedence.
func mergeParams(base, overrides map[string]any) map[string]any {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string]any, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}

	return merged
}

// mergeStringMap returns a copy of base with string overrides taking
// precedence.
func mergeStringMap(base, overrides map[string]string) map[string]string {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}

// mergeStringListMap returns a deep-enough copy of base with list overrides
// taking precedence.
func mergeStringListMap(base, overrides map[string][]string) map[string][]string {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string][]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = append([]string(nil), value...)
	}
	for key, value := range overrides {
		merged[key] = append([]string(nil), value...)
	}
	return merged
}

// runWithTemplateFixture executes a rendered case against its inline fixture,
// checks the query result and postconditions, and rolls the transaction back.
func runWithTemplateFixture(t *testing.T, ctx context.Context, db graph.Database, tc testCase, assertion caseAssertion) {
	t.Helper()

	if tc.Fixture == nil {
		t.Fatal("template cases must define an inline fixture")
	}

	queryErrorObserved := false
	session := &Session{
		DB:  db,
		Ctx: ctx,
	}
	err := session.WithRollbackFixture(t, tc.Fixture, false, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, idMap)
		result := tx.Query(tc.Cypher, params)
		assertion.checkResult(t, result, newAssertionContext(idMap))
		result.Close()
		if assertion.expectQueryError {
			queryErrorObserved = true
			return nil
		}

		return runStateAssertions(t, tx, idMap, tc.PostAssertions)
	})

	if assertion.expectQueryError && queryErrorObserved && err != nil {
		return
	}

	if err != nil {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// runMetamorphicFamily executes every query over one fixture and requires their
// selected comparison signatures to match the first query.
func runMetamorphicFamily(t *testing.T, ctx context.Context, db graph.Database, family cypherMetamorphicFamily) {
	t.Helper()

	if family.Fixture == nil {
		t.Fatal("metamorphic cases must define an inline fixture")
	}

	if len(family.Queries) < 2 {
		t.Fatal("metamorphic cases must define at least two queries")
	}

	session := &Session{
		DB:  db,
		Ctx: ctx,
	}
	err := session.WithRollbackFixture(t, family.Fixture, false, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		assertCtx := newAssertionContext(idMap)
		var baselineName string
		var baseline []string

		for _, query := range family.Queries {
			var (
				result    = tx.Query(query.Cypher, query.Params)
				collected = collectResult(t, result)
			)

			result.Close()

			signature := comparisonSignature(t, collected, assertCtx, family.Compare)
			if baseline == nil {
				baselineName = query.Name
				baseline = signature
				continue
			}

			if !reflect.DeepEqual(signature, baseline) {
				t.Fatalf(
					"metamorphic comparison %q mismatch for query %q against baseline %q:\n  got:  %v\n  want: %v",
					family.Compare,
					query.Name,
					baselineName,
					signature,
					baseline,
				)
			}
		}

		if baseline == nil {
			t.Fatal("all metamorphic queries were skipped")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// comparisonModes accepts either one comparison-mode string or a list in template JSON.
type comparisonModes []string

// UnmarshalJSON accepts either a single comparison mode or a list of modes.
func (s *comparisonModes) UnmarshalJSON(raw []byte) error {
	var mode string
	if err := json.Unmarshal(raw, &mode); err == nil {
		*s = comparisonModes{mode}
		return nil
	}

	var modes []string
	if err := json.Unmarshal(raw, &modes); err != nil {
		return err
	}

	*s = comparisonModes(modes)
	return nil
}

// String joins comparison modes for use in generated subtest names.
func (s comparisonModes) String() string {
	return strings.Join(s, ",")
}

// comparisonSignature computes each requested comparison mode for a collected
// result in declaration order.
func comparisonSignature(t *testing.T, result queryResult, ctx assertionContext, modes comparisonModes) []string {
	t.Helper()

	if len(modes) == 0 {
		t.Fatal("metamorphic comparison must specify at least one mode")
	}

	signature := make([]string, 0, len(modes))
	for _, mode := range modes {
		signature = append(signature, comparisonModeSignature(t, result, ctx, mode))
	}

	return signature
}

// comparisonModeSignature canonicalizes a collected result according to one
// supported metamorphic comparison mode.
func comparisonModeSignature(t *testing.T, result queryResult, ctx assertionContext, mode string) string {
	t.Helper()

	var signature []string
	switch mode {
	case "row_count":
		row := fmt.Sprintf("%d", len(result.rows))
		signature = []string{row}
	case "scalar_values":
		signature = sortedSignatures(firstScalarSignatures(t, result))
	case "ordered_scalar_values":
		signature = firstScalarSignatures(t, result)
	case "row_values":
		signature = sortedSignatures(rowScalarSignatures(result))
	case "ordered_row_values":
		signature = rowScalarSignatures(result)
	case "node_ids":
		signature = sortedSignatures(collectNodeIDs(t, result, ctx, false))
	case "node_id_set":
		signature = sortedSignatures(collectNodeIDs(t, result, ctx, true))
	case "path_node_ids":
		signatures := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			signatures = append(signatures, pathNodeIDSignature(t, path, ctx))
		}
		signature = sortedSignatures(signatures)
	case "path_edge_kinds":
		signatures := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			signatures = append(signatures, pathEdgeKindSignature(t, path))
		}
		signature = sortedSignatures(signatures)
	case "path_relationship_records":
		signatures := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			signatures = append(signatures, pathRelationshipRecordSignature(t, path, ctx))
		}
		signature = sortedSignatures(signatures)
	default:
		t.Fatalf("unknown metamorphic comparison mode %q", mode)
	}

	encoded, err := json.Marshal(signature)
	if err != nil {
		t.Fatalf("failed to encode metamorphic comparison signature: %v", err)
	}

	return mode + ":" + string(encoded)
}

// firstScalarSignatures returns the canonical signature of each row's first
// projected value.
func firstScalarSignatures(t *testing.T, result queryResult) []string {
	t.Helper()

	signatures := make([]string, 0, len(result.rows))
	for rowIdx, row := range result.rows {
		if len(row.values) == 0 {
			t.Fatalf("row %d has no values", rowIdx)
		}

		signatures = append(signatures, scalarSignature(row.values[0]))
	}

	return signatures
}

// rowScalarSignatures renders every result row into a deterministic scalar signature.
func rowScalarSignatures(result queryResult) []string {
	signatures := make([]string, 0, len(result.rows))
	for _, row := range result.rows {
		signatures = append(signatures, rowScalarSignature(row.values))
	}

	return signatures
}

// sortedSignatures returns a sorted copy without modifying its input.
func sortedSignatures(signatures []string) []string {
	sorted := append([]string(nil), signatures...)
	sort.Strings(sorted)
	return sorted
}
