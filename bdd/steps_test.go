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
//

//go:build bdd_integration

package bdd

import (
	"context"
	"log"
	"testing"

	"github.com/cucumber/godog"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/integration"
)

func InitializeTestSuite(ctxtestsuite *godog.TestSuiteContext, ctx context.Context, c *dbContext) {
	err := c.db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Nodes().Delete(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to clear database %v", err)
	}
}

func InitializeScenario(ctx *godog.ScenarioContext, dbCtx *dbContext) {
	ctx.Step(`^an empty graph$`, dbCtx.anEmptyGraph)
	ctx.Step(`^having executed:$`, dbCtx.havingExecuted)
	ctx.Step(`^executing query:$`, dbCtx.executingQuery)
	ctx.Step(`^the result should be:$`, dbCtx.theResultShouldBe)
}

func TestFeatures(t *testing.T) {
	backgroundCtx := context.Background()
	// establish database connection
	session := integration.Open(t, integration.Options{
		Schema: &graph.Schema{
			DefaultGraph: graph.Graph{
				Name: "dawgs-bdd",
			},
		},
	})

	dbCtx := &dbContext{
		db: session.DB,
	}
	suite := godog.TestSuite{
		Name: "OpenCypher-TCK",
		TestSuiteInitializer: func(ctx *godog.TestSuiteContext) {
			InitializeTestSuite(ctx, backgroundCtx, dbCtx)
		},
		ScenarioInitializer: func(ctx *godog.ScenarioContext) {
			InitializeScenario(ctx, dbCtx)
		},
		Options: &godog.Options{
			Format: "pretty",
			// TODO create env variable pointing to the TCK features via CI
			Paths:    []string{"features"},
			TestingT: t,
		},
	}

	if num := suite.Run(); num != 0 {
		t.Fatalf("TestSuite execution failed with status %d", num)
	}
}
