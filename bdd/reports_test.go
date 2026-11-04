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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregateBDDReport(t *testing.T) {
	report := aggregateBDDReport([]cucumberFeature{
		{URI: "features/clauses/match/Match1.feature", Elements: []cucumberScenario{
			{Name: "passes", Steps: []cucumberStep{{Result: cucumberResult{Status: "passed"}}}},
			{Name: "fails", Steps: []cucumberStep{{Result: cucumberResult{Status: "failed", Error: "expected result"}}}},
		}},
		{URI: "features/clauses/return/Return1.feature", Elements: []cucumberScenario{
			{Name: "passes", Steps: []cucumberStep{{Result: cucumberResult{Status: "passed"}}}},
		}},
		{URI: "features/expressions/list/List1.feature", Elements: []cucumberScenario{
			{Name: "ignored", Steps: []cucumberStep{{Result: cucumberResult{Status: "failed"}}}},
		}},
	})
	if report.Total != 4 || report.Passed != 2 || report.Failed != 2 {
		t.Fatalf("unexpected totals: %+v", report)
	}
	if len(report.Folders) != 2 {
		t.Fatalf("unexpected folder results: %+v", report.Folders)
	}
}

func TestCheckBDDGate(t *testing.T) {
	tests := []struct {
		name      string
		report    bddReport
		status    int
		errorText string
	}{
		{name: "supported scenarios pass", report: bddReport{Total: 2, Enforced: 2}},
		{name: "report-only failure remains visible", report: bddReport{Total: 2, Failed: 1, Enforced: 1}, status: 1},
		{name: "upstream enforced failure fails gate", report: bddReport{Total: 2, Failed: 1, Enforced: 2, EnforcedFailed: 1}, status: 1, errorText: "1 of 2 enforced scenarios failed"},
		{name: "runner error fails even with passing report", report: bddReport{Total: 1, Enforced: 1}, status: 2, errorText: "runner failed with status 2"},
		{name: "empty report fails", errorText: "no enforced scenarios"},
		{name: "report-only suite cannot qualify gate", report: bddReport{Total: 2}, errorText: "no enforced scenarios"},
		{name: "runner failure missing from report", report: bddReport{Total: 1, Enforced: 1}, status: 1, errorText: "disagrees"},
		{name: "report failure missing from runner", report: bddReport{Total: 2, Failed: 1, Enforced: 1}, errorText: "disagrees"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkBDDGate(test.report, test.status)
			if test.errorText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.errorText)
			}
		})
	}
}

func TestAggregateBDDReportEnforcement(t *testing.T) {
	report := aggregateBDDReport([]cucumberFeature{
		{URI: "features/tck_features/Create.feature", Elements: []cucumberScenario{
			{Name: "supported upstream fails", Steps: []cucumberStep{{Result: cucumberResult{Status: "failed", Error: "regression"}}}},
			{Name: "report only", Tags: []cucumberTag{{Name: "@report-only"}}, Steps: []cucumberStep{{Result: cucumberResult{Status: "undefined"}}}},
		}},
		{URI: "features/dawgs_features/Match.feature", Elements: []cucumberScenario{
			{Name: "supported local passes", Steps: []cucumberStep{{Result: cucumberResult{Status: "passed"}}}},
		}},
	})
	require.Equal(t, 3, report.Total)
	require.Equal(t, 2, report.Failed)
	require.Equal(t, 2, report.Enforced)
	require.Equal(t, 1, report.EnforcedFailed)
	require.True(t, report.Folders[1].Features[0].Scenarios[0].Enforced)
	require.False(t, report.Folders[1].Features[0].Scenarios[1].Enforced)
	require.ErrorContains(t, checkBDDGate(report, 1), "enforced scenarios failed")
}

func TestTopLevelFolderFromURI(t *testing.T) {
	tests := []struct {
		uri    string
		folder string
		ok     bool
	}{
		{uri: "features/expressions/list/List1.feature", folder: "expressions", ok: true},
		{uri: "features/useCases/counting/Counting.feature", folder: "useCases", ok: true},
		{uri: "features/bdd_dawgs/matching.feature", folder: "bdd_dawgs", ok: true},
		{uri: "other/report.feature", ok: false},
	}
	for _, test := range tests {
		folder, ok := topLevelFolderFromURI(test.uri)
		if folder != test.folder || ok != test.ok {
			t.Errorf("topLevelFolderFromURI(%q) = %q, %v; want %q, %v", test.uri, folder, ok, test.folder, test.ok)
		}
	}
}

func TestFirstScenarioFailure(t *testing.T) {
	tests := []struct {
		scenario cucumberScenario
		expected string
	}{
		{
			scenario: cucumberScenario{
				Steps: []cucumberStep{
					{
						Name: "ScenarioA",
						Result: cucumberResult{
							Status: "failed",
						},
					},
				},
			},
			expected: "failed",
		},
		{
			scenario: cucumberScenario{
				Steps: []cucumberStep{
					{
						Name: "ScenarioA",
						Result: cucumberResult{
							Status: "failed",
							Error:  "SomeError",
						},
					},
				},
			},
			expected: "SomeError",
		},
		{
			scenario: cucumberScenario{
				Steps: []cucumberStep{
					{
						Name: "ScenarioA",
						Result: cucumberResult{
							Status: "passed",
						},
					},
				},
			},
			expected: "",
		},
	}

	for _, test := range tests {
		actual := firstScenarioFailure(test.scenario)
		require.Exactly(t, actual, test.expected)
	}
}

func TestWriteBDDHTMLReport(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "results.json")
	output := filepath.Join(dir, "report.html")
	require.NoError(t, os.WriteFile(input, []byte(`[{"uri":"features/tck_features/Create.feature","elements":[{"name":"enforced pass","steps":[{"result":{"status":"passed"}}]},{"name":"compatibility gap","tags":[{"name":"@report-only"}],"steps":[{"result":{"status":"failed","error_message":"<gap>"}}]}]}]`), 0o600))
	report, err := writeBDDHTMLReport(input, output)
	require.NoError(t, err)
	require.NoError(t, checkBDDGate(report, 1))
	html, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Contains(t, string(html), "1 scenarios, 0 failed")
	require.Contains(t, string(html), "enforced pass (enforced)")
	require.Contains(t, string(html), "compatibility gap (report-only)")
	require.Contains(t, string(html), "&lt;gap&gt;")
}

func TestWriteBDDHTMLReportErrors(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "results.json")
	output := filepath.Join(dir, "report.html")
	_, err := writeBDDHTMLReport(input, output)
	require.ErrorContains(t, err, "read Cucumber report")
	require.NoError(t, os.WriteFile(input, []byte(`invalid`), 0o600))
	_, err = writeBDDHTMLReport(input, output)
	require.ErrorContains(t, err, "parse Cucumber report")
	require.NoError(t, os.WriteFile(input, []byte(`[]`), 0o600))
	report, err := writeBDDHTMLReport(input, output)
	require.NoError(t, err)
	require.ErrorContains(t, checkBDDGate(report, 0), "no enforced scenarios")
	_, err = writeBDDHTMLReport(input, filepath.Join(input, "report.html"))
	require.ErrorContains(t, err, "create report directory")
	_, err = writeBDDHTMLReport(input, dir)
	require.ErrorContains(t, err, "create HTML report")
}

func TestFirstScenarioFailureRejectsMissingResults(t *testing.T) {
	require.Equal(t, "scenario has no steps", firstScenarioFailure(cucumberScenario{}))
	require.Equal(t, "step has no result status", firstScenarioFailure(cucumberScenario{Steps: []cucumberStep{{}}}))
}
