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
