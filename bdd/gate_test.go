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
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/require"
)

func TestBDDGateWithGodog(t *testing.T) {
	for _, folder := range []string{"dawgs_features", "tck_features", "unclassified"} {
		for _, scenario := range []struct {
			name, tag, step string
			fails           bool
		}{
			{name: "passing enforced scenario", step: "a passing assertion"},
			{name: "failing enforced scenario", step: "a failing assertion", fails: true},
			{name: "undefined enforced scenario", step: "an undefined assertion", fails: true},
			{name: "report-only failure", tag: "@report-only", step: "a failing assertion"},
		} {
			t.Run(folder+"/"+scenario.name, func(t *testing.T) {
				dir := t.TempDir()
				featureDir := filepath.Join(dir, "features", folder)
				if folder == "unclassified" {
					featureDir = filepath.Join(dir, folder)
				}
				require.NoError(t, os.MkdirAll(featureDir, 0o755))
				feature := "Feature: Regression gate\n  Scenario: Passing baseline\n    Given a passing assertion\n\n  " + scenario.tag + "\n  Scenario: Candidate\n    Given " + scenario.step + "\n"
				require.NoError(t, os.WriteFile(filepath.Join(featureDir, "gate.feature"), []byte(feature), 0o600))
				reportPath := filepath.Join(dir, "results.json")
				suite := godog.TestSuite{
					Name: "gate-regression",
					ScenarioInitializer: func(ctx *godog.ScenarioContext) {
						ctx.Step(`^a passing assertion$`, func() error { return nil })
						ctx.Step(`^a failing assertion$`, func() error { return errors.New("controlled assertion failure") })
					},
					Options: &godog.Options{Paths: []string{featureDir}, Format: "cucumber:" + reportPath, Strict: true, Output: io.Discard},
				}
				status := suite.Run()
				report, err := writeBDDHTMLReport(reportPath, filepath.Join(dir, "report.html"))
				require.NoError(t, err)
				require.Equal(t, 2, report.Total)
				err = checkBDDGate(report, status)
				if scenario.fails {
					require.ErrorContains(t, err, "enforced scenarios failed")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestBDDCommandPreservesGoTestFailures(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestBDDOrdinaryFailureProbe$")
	cmd.Env = append(os.Environ(), "DAWGS_BDD_FAILURE_PROBE=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "ordinary Go-test failure must fail the BDD process: %s", output)
	require.Contains(t, string(output), "controlled ordinary Go-test failure")
}

func TestBDDOrdinaryFailureProbe(t *testing.T) {
	if os.Getenv("DAWGS_BDD_FAILURE_PROBE") != "1" {
		t.Skip("subprocess-only failure probe")
	}
	t.Fatal("controlled ordinary Go-test failure")
}
