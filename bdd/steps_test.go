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

// InitializeScenario registers graph setup, query assertions and database cleanup for each BDD scenario.
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

// TestFeatures runs the TCK scenarios and evaluates enforced failures separately from compatibility reports.
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
			Strict:      true,
			Concurrency: 1,
		},
	}

	if err := os.Remove(bddJSONReportPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to remove stale BDD report: %v", err)
	}
	// Do not pass TestingT: report-only compatibility failures are evaluated
	// separately from the enforced scenarios below.
	status := suite.Run()
	report, err := writeBDDHTMLReport(bddJSONReportPath, bddHTMLReportPath)
	if err != nil {
		t.Errorf("failed to write BDD HTML report: %v", err)
		return
	}
	t.Logf("BDD gate: %d enforced, %d failed; compatibility reporting: %d scenarios, %d failed",
		report.Enforced, report.EnforcedFailed, report.Total-report.Enforced, report.Failed-report.EnforcedFailed)
	if err := checkBDDGate(report, status); err != nil {
		t.Error(err)
	}
}
