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
	"os"
	"slices"
	"strings"

	"github.com/cucumber/godog"
	"github.com/google/go-cmp/cmp"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

type graphSnapshot struct {
	NodesCount         int64
	RelationshipsCount int64
}

type dbContext struct {
	db              graph.Database
	beforeExecution graphSnapshot
	actualResult    graph.Result
	rowCount        int
	actualRows      [][]string
}

// resetBeforeScenario clears persistent graph data and scenario-local result state.
func (c *dbContext) resetBeforeScenario(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
	if err := c.anEmptyGraph(ctx); err != nil {
		return ctx, fmt.Errorf("reset graph before scenario: %w", err)
	}

	c.beforeExecution = graphSnapshot{}
	c.actualResult = nil
	c.rowCount = 0
	c.actualRows = nil

	return ctx, nil
}

// anEmptyGraph removes all nodes and relationships from the graph.
func (c *dbContext) anEmptyGraph(ctx context.Context) error {
	err := c.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
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
func (c *dbContext) theBinarytreeGraph(ctx context.Context, num int) error {
	fileName := fmt.Sprintf("testdata/binary-tree-%d.json", num)

	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open %v\n error: %w", fileName, err)
	}

	defer file.Close()

	_, err = opengraph.Load(ctx, c.db, file)
	if err != nil {
		return fmt.Errorf("failed to write graph data error: %w\n", err)
	}

	return nil
}

// executingQuery runs a read query and records its rows and graph state for comparison.
func (c *dbContext) executingQuery(ctx context.Context, input *godog.DocString) error {
	c.actualRows = nil

	before, err := captureGraphState(ctx, c.db)
	if err != nil {
		return err
	}
	c.beforeExecution = before

	err = c.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
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
			c.actualRows = append(c.actualRows, row)
		}

		c.actualResult = result
		c.rowCount = int(rowCount)
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

// havingExecuted runs a write query to seed the graph for a scenario.
func (c *dbContext) havingExecuted(ctx context.Context, input *godog.DocString) error {
	err := c.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
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
func (c *dbContext) theResultShouldBeInAnyOrder(expectedTable *godog.Table) error {
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

	if c.rowCount != len(expectedRows) {
		return fmt.Errorf("Invalid row count expected %d actual %d", len(expectedRows), c.rowCount)
	}

	sortRows := func(rows [][]string) {
		// TODO normalize exptected actual rows by sorting kinds and their properties
		slices.SortFunc(rows, func(a, b []string) int {
			return slices.Compare(a, b)
		})
	}
	sortRows(expectedRows)
	sortRows(c.actualRows)
	for i := range expectedRows {
		if !slices.Equal(expectedRows[i], c.actualRows[i]) {
			return fmt.Errorf("Detected a drift expected %s, actual %s", strings.Join(expectedRows[i], ", "), strings.Join(c.actualRows[i], ", "))
		}
	}

	return nil
}

// formatGraphValue formats a supported graph value as a deterministic string.
func formatGraphValue(mapper graph.ValueMapper, value any) (string, error) {
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

	return "", fmt.Errorf("unsupported returned value of type %T", value)
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

// noSideEffects verifies that the query did not change graph node or relationship counts.
func (c *dbContext) noSideEffects(ctx context.Context) error {
	after, err := captureGraphState(ctx, c.db)
	if err != nil {
		return err
	}
	if !cmp.Equal(c.beforeExecution, after) {
		return errors.New("Graph state drift detected")
	}
	return nil
}

// formatString normalizes a formatted graph value for comparison with feature data.
func formatString(s string) string {
	removeSpace := strings.ReplaceAll(s, " ", "")
	if strings.Contains(s, `"`) {
		return strings.ReplaceAll(removeSpace, `"`, `'`)
	}
	return removeSpace
}

// captureGraphState returns the current node and relationship counts for the database.
func captureGraphState(ctx context.Context, db graph.Database) (graphSnapshot, error) {
	var err error
	var nodeCount int64
	var relationshipCount int64

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		nodeCount, err = tx.Nodes().Count()
		if err != nil {
			return err
		}
		relationshipCount, err = tx.Relationships().Count()
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return graphSnapshot{}, err
	}

	return graphSnapshot{
		NodesCount:         nodeCount,
		RelationshipsCount: relationshipCount,
	}, nil
}

// formatGraphResults formats graph nodes as deterministic strings for BDD comparisons.
func formatGraphResults(nodes []graph.Node) ([]string, error) {
	sb := strings.Builder{}
	for _, node := range nodes {
		for _, kind := range node.Kinds {
			sb.WriteString("(:")
			sb.WriteString(kind.String())
		}
		if len(node.Kinds) == 0 {
			sb.WriteString("(")
		}
		props := node.Properties.MapOrEmpty()
		if len(props) != 0 {
			// TODO sort node properties of feature files
			slices.Sort(node.Properties.Keys(nil))
			sb.WriteString("{")
			for index, key := range node.Properties.Keys(nil) {
				if index > 0 {
					sb.WriteString(", ")
				}
				value := node.Properties.Get(key)
				sb.WriteString(key)
				sb.WriteString(": ")
				strValue, _ := value.String()
				sb.WriteString("'")
				sb.WriteString(strValue)
				sb.WriteString("'")
			}
			sb.WriteString("}")
		}
		sb.WriteString(")\n")
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
