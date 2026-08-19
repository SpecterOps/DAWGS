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
	"fmt"
	"slices"
	"strings"

	"github.com/cucumber/godog"
	"github.com/specterops/dawgs/graph"
)

type dbContext struct {
	db         graph.Database
	rowCount   int
	actualRows []string
	nodes      []graph.Node
}

// anEmptyGraph deletes graph data
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

// executingQuery read cypher queries statement
func (c *dbContext) executingQuery(ctx context.Context, input *godog.DocString) error {
	err := c.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var (
			node     graph.Node
			nodes    []graph.Node
			rowCount int64
		)
		result := tx.Query(input.Content, nil)

		defer result.Close()

		for result.Next() {
			rowCount++

			for _, value := range result.Values() {
				mapper := result.Mapper()
				mapper.Map(value, &node)
				nodes = append(c.nodes, node)
			}

			// format graph nodes and their properties into a cypher query
			formatted, err := formatGraphResults(nodes)
			if err != nil {
				return fmt.Errorf("Failed to format graph results: %w", err)
			}
			c.actualRows = append(c.actualRows, formatted...)
		}

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

// havingExecuted seeds data
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

func (c *dbContext) theResultShouldBe(expectedTable *godog.Table) error {
	var expectedRows []string
	for _, value := range expectedTable.Rows {
		for _, cell := range value.Cells {
			if cell.Value != "n" {
				expectedRows = append(expectedRows, formatString(cell.Value))
			}
		}
	}

	if c.rowCount != len(expectedRows) {
		return fmt.Errorf("Invalid row count expected %d actual %d", c.rowCount, len(expectedTable.Rows))
	}

	for i := 0; i < len(expectedRows); i++ {
		// TODO normalize exptected actual rows by sorting kinds and their properties
		if formatString(expectedRows[i]) != formatString(c.actualRows[i]) {
			return fmt.Errorf("Detected a drift expected %s, actual %s", expectedRows[i], c.actualRows[i])
		}
	}

	return nil
}

func formatString(s string) string {
	removeSpace := strings.ReplaceAll(s, " ", "")
	if strings.Contains(s, `"`) {
		return strings.ReplaceAll(removeSpace, `"`, `'`)
	}
	return removeSpace
}

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
		if node.Properties.Len() != 0 {
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
		if node.Properties != nil {
			sb.WriteString(")\n")
		}
	}

	var result []string
	list := strings.Split(sb.String(), "\n")
	for _, item := range list {
		if item != "" {
			result = append(result, item)
		}
	}

	return result, nil
}
