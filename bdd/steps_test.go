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
	"os"
	"testing"

	"github.com/cucumber/godog"
)

var (
	bddDawgsFailed   bool
	bddReportInvalid bool
)

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
	ctx.After(dbCtx.closeDBSession)
}

func TestFeatures(t *testing.T) {
	if err := os.MkdirAll(bddReportDir, 0o755); err != nil {
		t.Fatalf("failed to create BDD report directory: %v", err)
	}

	dbCtx := &dbContext{}

	suite := godog.TestSuite{
		Name: "OpenCypher-TCK",
		TestSuiteInitializer: func(_ *godog.TestSuiteContext) {
			dbCtx.testData = []string{"testdata/binary-tree-1.json", "testdata/binary-tree-2.json"}
		},
		ScenarioInitializer: func(ctx *godog.ScenarioContext) {
			InitializeScenario(ctx, dbCtx)
		},
		Options: &godog.Options{
			Format:      "pretty,cucumber:" + bddJSONReportPath,
			Paths:       []string{"features"},
			TestingT:    t,
			Concurrency: 1,
		},
	}

	// delete staled reports
	_ = os.Remove(bddJSONReportPath)
	suite.Run()
	report, err := writeBDDHTMLReport(bddJSONReportPath, bddHTMLReportPath)
	if err != nil {
		bddReportInvalid = true
		t.Errorf("failed to write BDD HTML report: %v", err)
		return
	}
	if len(report.Folders) == 0 {
		bddReportInvalid = true
		t.Errorf("BDD report is empty")
		return
	}
	bddDawgsFailed = hasFolderFailure(report, "dawgs")
}

func TestMain(m *testing.M) {
	m.Run()
	// Only DAWGS feature failures and invalid reports should fail the BDD command.
	if bddDawgsFailed || bddReportInvalid {
		os.Exit(1)
	}
	os.Exit(0)
}
