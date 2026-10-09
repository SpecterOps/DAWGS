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
	"os"
	"strings"
	"testing"

	"github.com/cucumber/godog"
	"github.com/davecgh/go-spew/spew"
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
	ctx.Before(dbCtx.resetBeforeScenario)
	ctx.Step(`^an empty graph$`, dbCtx.anEmptyGraph)
	ctx.Step(`^any graph$`, dbCtx.anyGraph)
	ctx.Step(`^the binary-tree-(\d+) graph$`, dbCtx.theBinarytreeGraph)
	ctx.Step(`^having executed:$`, dbCtx.havingExecuted)
	ctx.Step(`^executing query:$`, dbCtx.executingQuery)
	ctx.Step(`^the result should be, in any order:$`, dbCtx.theResultShouldBeInAnyOrder)
	ctx.Step(`^no side effects$`, dbCtx.noSideEffects)
	ctx.Step(`^the side effects should be:$`, dbCtx.theSideEffectsShouldBe)
	ctx.Step(`^the result should be empty$`, dbCtx.theResultShouldBeEmpty)
}

func TestFeatures(t *testing.T) {
	backgroundCtx := context.Background()
	if err := os.MkdirAll(bddReportDir, 0o755); err != nil {
		t.Fatalf("failed to create BDD report directory: %v", err)
	}
	// establish database connection
	session := integration.Open(t, integration.Options{
		Schema: &graph.Schema{
			DefaultGraph: graph.Graph{
				Name: "dawgs-bdd",
			},
		},
	})

	dbCtx := &dbContext{
		db:       session.DB,
		testData: []string{"testdata/binary-tree-1.json", "testdata/binary-tree-2.json"},
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
			Format:   "pretty,cucumber:" + bddJSONReportPath,
			Paths:    []string{"features"},
			TestingT: t,
		},
	}

	// delete staled reports
	_ = os.Remove(bddJSONReportPath)
	suite.Run()
	report, err := writeBDDHTMLReport(bddJSONReportPath, bddHTMLReportPath)
	if err != nil {
		t.Errorf("failed to write BDD HTML report: %v", err)
	}
	if strings.Contains(strings.ToLower(report.Folders[0].Name), "dawgs") && report.Folders[0].Failed > 0 {
		os.Exit(1)
	}
}

func TestMain(m *testing.M) {
	spew.Dump("here")
	num := m.Run()
	if num == 1 {
		os.Exit(0)
	}
}
