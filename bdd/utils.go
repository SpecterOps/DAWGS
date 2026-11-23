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

package bdd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
	"github.com/google/go-cmp/cmp"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/integration"
	"github.com/specterops/dawgs/opengraph"
)

type graphSnapshot struct {
	NodesCount         int64
	RelationshipsCount int64
	PropertiesCount    int64
}

// dbContext owns the active database session and the state needed by BDD query assertions.
type dbContext struct {
	// db owns the active scenario's database connection.
	db graph.Database
	// openSession overrides session creation in lifecycle tests; nil selects the normal opener.
	openSession func(integration.Options) (*integration.Session, error)
	// testData lists graph fixtures available to the scenario setup steps.
	testData []string
	// beforeExecution records graph cardinalities before the query under test.
	beforeExecution graphSnapshot
	// afterExecution records graph cardinalities used to compare query side effects.
	afterExecution graphSnapshot
	// actualResult retains the executed query's result metadata for scenario assertions.
	actualResult graph.Result
	// rowCount records the number of rows consumed from the query result.
	rowCount int
	// actualRows stores formatted result values for scenario table comparisons.
	actualRows [][]string
}

// resetBeforeScenario clears persistent graph data and scenario-local result state.
func (s *dbContext) resetBeforeScenario(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
	s.db = nil
	s.beforeExecution = graphSnapshot{}
	s.actualResult = nil
	s.rowCount = 0
	s.actualRows = nil

	openSession := s.openSession
	if openSession == nil {
		openSession = integration.OpenSession
	}
	session, err := openSession(integration.Options{
		Schema: &graph.Schema{
			DefaultGraph: graph.Graph{
				Name: "dawgs-bdd",
			},
		},
	})
	if err != nil {
		return ctx, fmt.Errorf("failed to establish database connection error: %w", err)
	}

	s.db = session.DB

	if err := s.anEmptyGraph(ctx); err != nil {
		return ctx, fmt.Errorf("reset graph before scenario: %w", err)
	}
	return ctx, nil
}

// closeDBSession clears scenario data and closes the database, preserving both cleanup errors.
func (s *dbContext) closeDBSession(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
	if s.db == nil {
		return ctx, nil
	}
	db := s.db
	errEmptyGraph := s.anEmptyGraph(ctx)
	s.db = nil
	closeDbErr := db.Close(context.Background())
	if err := errors.Join(errEmptyGraph, closeDbErr); err != nil {
		return ctx, err
	}

	return ctx, nil
}

// anEmptyGraph removes all nodes and relationships from the graph.
func (s *dbContext) anEmptyGraph(ctx context.Context) error {
	err := s.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Nodes().Delete(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// theBinarytreeGraph load tck datasets
func (s *dbContext) theBinarytreeGraph(ctx context.Context, num int) error {
	fileName := fmt.Sprintf("testdata/binary-tree-%d.json", num)

	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open %v\n error: %w", fileName, err)
	}

	defer file.Close()

	_, err = opengraph.Load(ctx, s.db, file)
	if err != nil {
		return fmt.Errorf("failed to write graph data error: %w\n", err)
	}

	return nil
}

// loadBEDGraph loads BED dataset
func (s *dbContext) loadBEDGraph(ctx context.Context, num int) error {
	fileName := fmt.Sprintf("testdata/bed-%d.json", num)

	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open %v\n error: %w", fileName, err)
	}

	defer file.Close()

	_, err = opengraph.Load(ctx, s.db, file)
	if err != nil {
		return fmt.Errorf("failed to write graph data error: %w\n", err)
	}

	return nil
}

// anyGraph loads one of the configured graph fixtures into the scenario database.
func (s *dbContext) anyGraph(ctx context.Context) error {
	randomIndex := rand.Intn(len(s.testData))
	file, err := os.Open(s.testData[randomIndex])
	if err != nil {
		return fmt.Errorf("open graph fixture: %w", err)
	}
	defer file.Close()

	_, err = opengraph.Load(ctx, s.db, file)
	if err != nil {
		return err
	}

	return nil
}

// executingQuery runs a read query and records its rows for later comparison.
func (s *dbContext) executingQuery(ctx context.Context, input *godog.DocString) error {
	s.actualRows = nil

	before, err := captureGraphState(ctx, s.db)
	if err != nil {
		return err
	}
	s.beforeExecution = before

	if strings.Contains(strings.ToLower(input.Content), "create") {
		result, err := hasReturnClause(input.Content)
		if err != nil {
			return err
		}
		isMutationWithoutReturn := strings.Contains(strings.ToLower(input.Content), "create") &&
			!result
		err = s.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			var rowCount int64
			result := tx.Query(input.Content, nil)
			defer result.Close()

			if err = result.Error(); err != nil {
				return err
			}

			for result.Next() {
				// Mutations with no RETURN e.g CREATE ()-[:R]->() PG drivers generate SQL with SELECT 1; at the end of the statement which return one row;
				// Discard append any rows.
				if isMutationWithoutReturn {
					continue
				}
				var row []string

				rowCount++

				for _, value := range result.Values() {
					formatted, err := formatGraphValue(result.Mapper(), value)
					if err != nil {
						return fmt.Errorf("failed to format graph result: %w", err)
					}
					row = append(row, formatted)
				}
				s.actualRows = append(s.actualRows, row)
			}

			s.actualResult = result
			s.rowCount = int(rowCount)

			return nil
		})
		if err != nil {
			return err
		}
	} else {

		err = s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			var rowCount int64
			result := tx.Query(input.Content, nil)

			defer result.Close()

			for result.Next() {
				var row []string

				rowCount++

				for _, value := range result.Values() {
					formatted, err := formatGraphValue(result.Mapper(), value)
					if err != nil {
						return fmt.Errorf("failed to format graph result: %w", err)
					}
					row = append(row, formatted)
				}
				s.actualRows = append(s.actualRows, row)
			}

			s.actualResult = result
			s.rowCount = int(rowCount)
			if err := result.Error(); err != nil {
				return result.Error()
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	after, err := captureGraphState(ctx, s.db)
	if err != nil {
		return err
	}
	s.afterExecution = after

	return nil
}

// havingExecuted runs a write query to seed the graph for a scenario.
func (s *dbContext) havingExecuted(ctx context.Context, input *godog.DocString) error {
	err := s.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(input.Content, nil)

		defer result.Close()

		if result.Error() != nil {
			return result.Error()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// theResultShouldBeInAnyOrder compares recorded query rows with an expected table,
// ignoring row order.
func (s *dbContext) theResultShouldBeInAnyOrder(expectedTable *godog.Table) error {
	var expectedRows [][]string
	if len(expectedTable.Rows) > 1 {
		for _, value := range expectedTable.Rows[1:] {
			var row []string
			for _, cell := range value.Cells {
				row = append(row, formatString(cell.Value))
			}
			expectedRows = append(expectedRows, row)
		}
	}

	if s.rowCount != len(expectedRows) {
		return fmt.Errorf("Invalid row count expected %d actual %d", len(expectedRows), s.rowCount)
	}

	sortRows := func(rows [][]string) {
		// TODO normalize exptected actual rows by sorting kinds and their properties
		slices.SortFunc(rows, func(a, b []string) int {
			return slices.Compare(a, b)
		})
	}
	sortRows(expectedRows)
	sortRows(s.actualRows)
	for i := range expectedRows {
		if !slices.Equal(expectedRows[i], s.actualRows[i]) {
			return fmt.Errorf("Detected a drift expected %s, actual %s", strings.Join(expectedRows[i], ", "), strings.Join(s.actualRows[i], ", "))
		}
	}

	return nil
}

// theResultShouldBeEmpty rejects a scenario result that contains any rows.
func (s *dbContext) theResultShouldBeEmpty() error {

	if s.rowCount != 0 || len(s.actualRows) != 0 {
		return fmt.Errorf("The result set is not empty")
	}
	return nil
}

// formatGraphValue formats a supported graph value as a deterministic string.
func formatGraphValue(mapper graph.ValueMapper, value any) (string, error) {
	var path graph.Path
	if mapper.Map(value, &path) {
		return formatGraphPath(path)
	}

	var node graph.Node
	if mapper.Map(value, &node) {
		formatted, err := formatGraphResults([]graph.Node{node})
		if err != nil {
			return "", err
		}
		return formatted[0], nil
	}

	var relationship graph.Relationship
	if mapper.Map(value, &relationship) {
		return formatString(formatGraphRelationship(relationship)), nil
	}

	switch value := value.(type) {
	case string:
		return formatString(value), nil
	case int32:
		return formatString(fmt.Sprint(value)), nil
	case float64:
		return formatString(fmt.Sprint(value)), nil
	case int64:
		return formatString(fmt.Sprint(value)), nil
	case nil:
		return "null", nil
	default:
		return "", fmt.Errorf("unsupported returned value of type %T", value)
	}
}

// formatGraphPath formats a single path in traversal order for BDD comparisons.
func formatGraphPath(path graph.Path) (string, error) {
	if len(path.Nodes) != len(path.Edges)+1 {
		return "", fmt.Errorf("invalid graph path: %d nodes for %d relationships", len(path.Nodes), len(path.Edges))
	}

	var builder strings.Builder
	for index, node := range path.Nodes {
		if node == nil {
			return "", fmt.Errorf("invalid graph path: missing node at index %d", index)
		}
		if index > 0 {
			edge := path.Edges[index-1]
			if edge == nil || edge.Kind == nil {
				return "", fmt.Errorf("invalid graph path: missing relationship or relationship kind at index %d", index-1)
			}

			previousID := path.Nodes[index-1].ID
			var prefix, suffix string
			switch {
			case edge.StartID == previousID && edge.EndID == node.ID:
				prefix, suffix = "-", "->"
			case edge.EndID == previousID && edge.StartID == node.ID:
				prefix, suffix = "<-", "-"
			default:
				return "", fmt.Errorf("invalid graph path: relationship at index %d does not connect nodes %v and %v", index-1, previousID, node.ID)
			}
			builder.WriteString(prefix)
			builder.WriteString(formatGraphRelationship(*edge))
			builder.WriteString(suffix)
		}

		pathNode := *node
		pathNode.Kinds = node.Kinds.Copy()
		slices.SortFunc(pathNode.Kinds, func(left, right graph.Kind) int {
			return strings.Compare(left.String(), right.String())
		})
		if err := writeGraphNode(&builder, pathNode); err != nil {
			return "", err
		}
	}

	return formatString(builder.String()), nil
}

// formatGraphRelationship formats a graph relationship as a deterministic string.
func formatGraphRelationship(relationship graph.Relationship) string {
	var builder strings.Builder
	builder.WriteString("[:")
	builder.WriteString(relationship.Kind.String())

	if props := relationship.Properties.MapOrEmpty(); len(props) > 0 {
		keys := relationship.Properties.Keys(nil)
		slices.Sort(keys)
		builder.WriteString("{")
		for index, key := range keys {
			if index > 0 {
				builder.WriteString(", ")
			}
			value := relationship.Properties.Get(key)
			strValue, _ := value.String()
			builder.WriteString(key)
			builder.WriteString(": '")
			builder.WriteString(strValue)
			builder.WriteString("'")
		}
		builder.WriteString("}")
	}

	builder.WriteString("]")
	return builder.String()
}

// noSideEffects verifies that node, relationship, and property counts are unchanged.
func (s *dbContext) noSideEffects(ctx context.Context) error {
	after, err := captureGraphState(ctx, s.db)
	if err != nil {
		return err
	}
	if !cmp.Equal(s.beforeExecution, after) {
		return errors.New("Graph state drift detected")
	}
	return nil
}

// theSideEffectsShouldBe compares signed net changes with the scenario's expected effects.
func (s *dbContext) theSideEffectsShouldBe(expectedTable *godog.Table) error {
	deltas := map[string]int64{
		"nodes":         s.afterExecution.NodesCount - s.beforeExecution.NodesCount,
		"relationships": s.afterExecution.RelationshipsCount - s.beforeExecution.RelationshipsCount,
		"properties":    s.afterExecution.PropertiesCount - s.beforeExecution.PropertiesCount,
	}
	var errs []error
	for _, row := range expectedTable.Rows {
		if len(row.Cells) != 2 {
			return fmt.Errorf("side effect row must have 2 cells, got %d", len(row.Cells))
		}
		key := strings.TrimSpace(row.Cells[0].Value)
		var delta int64
		switch key {
		case "+nodes", "-nodes", "+relationships", "-relationships", "+properties", "-properties":
			delta = deltas[key[1:]]
		default:
			return fmt.Errorf("unsupported side effect %q", key)
		}
		expected, err := strconv.ParseInt(strings.TrimSpace(row.Cells[1].Value), 10, 64)
		if err != nil {
			return fmt.Errorf("side effect %s: invalid count: %w", key, err)
		}
		if expected < 0 {
			return fmt.Errorf("side effect %s: count must be nonnegative", key)
		}
		if key[0] == '-' {
			delta = -delta
		}
		actual := max(delta, 0)
		if actual != expected {
			errs = append(errs, fmt.Errorf("side effect %s: expected %d actual %d", key, expected, actual))
		}
	}
	return errors.Join(errs...)
}

// formatString normalizes a formatted graph value for comparison with feature data.
func formatString(s string) string {
	removeSpace := strings.ReplaceAll(s, " ", "")
	if strings.Contains(s, `"`) {
		return strings.ReplaceAll(removeSpace, `"`, `'`)
	}
	return removeSpace
}

// captureGraphState scans the graph once per entity type to count entities and
// non-null property entries. It measures net cardinalities, not value changes.
func captureGraphState(ctx context.Context, db graph.Database) (graphSnapshot, error) {
	var snapshot graphSnapshot
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Nodes().Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				snapshot.NodesCount++
				snapshot.PropertiesCount += countGraphProperties(node.Properties)
			}
			return cursor.Error()
		}); err != nil {
			return fmt.Errorf("capture nodes: %w", err)
		}
		if err := tx.Relationships().Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for relationship := range cursor.Chan() {
				snapshot.RelationshipsCount++
				snapshot.PropertiesCount += countGraphProperties(relationship.Properties)
			}
			return cursor.Error()
		}); err != nil {
			return fmt.Errorf("capture relationships: %w", err)
		}
		return nil
	})
	if err != nil {
		return graphSnapshot{}, err
	}
	return snapshot, nil
}

func countGraphProperties(properties *graph.Properties) int64 {
	var count int64
	for _, value := range properties.MapOrEmpty() {
		if value != nil {
			count++
		}
	}
	return count
}

// formatGraphResults formats graph nodes as deterministic strings for BDD comparisons.
func formatGraphResults(nodes []graph.Node) ([]string, error) {
	sb := strings.Builder{}
	for _, node := range nodes {
		if err := writeGraphNode(&sb, node); err != nil {
			return nil, err
		}
		sb.WriteByte('\n')
	}

	var result []string
	list := strings.Split(sb.String(), "\n")
	for _, item := range list {
		if item != "" {
			result = append(result, formatString(item))
		}
	}

	return result, nil
}

func writeGraphNode(builder *strings.Builder, node graph.Node) error {
	builder.WriteByte('(')
	for _, kind := range node.Kinds {
		builder.WriteByte(':')
		builder.WriteString(kind.String())
	}
	if len(node.Properties.MapOrEmpty()) > 0 {
		builder.WriteByte('{')
		for index, key := range node.Properties.Keys(nil) {
			if index > 0 {
				builder.WriteString(", ")
			}
			value, err := node.Properties.Get(key).String()
			if err != nil {
				return err
			}
			builder.WriteString(key)
			builder.WriteString(": '")
			builder.WriteString(value)
			builder.WriteByte('\'')
		}
		builder.WriteByte('}')
	}
	builder.WriteByte(')')

	return nil
}

func hasReturnClause(query string) (bool, error) {
	parsedQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return false, err
	}
	if parsedQuery.SingleQuery.SinglePartQuery.Return != nil {
		return true, nil
	}
	return false, nil
}
