// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package dawgs

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegressionCoverageManifestClosesEveryActiveID(t *testing.T) {
	raw, err := os.ReadFile("regression_coverage_manifest.md")
	require.NoError(t, err)

	rows := parseRegressionManifestRows(string(raw))
	activeFamilies := map[string]int{
		"LOGIC":  5,
		"REC":    8,
		"TRUST":  3,
		"PRUNE":  6,
		"HOP":    10,
		"SCAN":   8,
		"LOOKUP": 16,
		"WRITE":  8,
	}

	for family, count := range activeFamilies {
		for idx := 1; idx <= count; idx++ {
			id := fmt.Sprintf("%s-%02d", family, idx)
			cells, found := rows[id]
			require.True(t, found, "coverage manifest is missing active query form %s", id)
			for _, cell := range cells {
				status := strings.Fields(cell)
				if len(status) > 0 {
					require.NotContains(t, []string{"A", "P"}, status[0],
						"active query form %s retains an unclosed layer: %s", id, cell)
				}
			}
		}
	}

	futureCells, found := rows["FUTURE-01"]
	require.True(t, found, "coverage manifest is missing dormant query form FUTURE-01")
	require.Contains(t, futureCells, "A", "FUTURE-01 must retain absent activation-only layers")
	for _, cell := range futureCells {
		status := strings.Fields(cell)
		if len(status) > 0 {
			require.NotEqual(t, "C", status[0], "FUTURE-01 must remain outside production-complete coverage")
		}
	}
}

func parseRegressionManifestRows(manifest string) map[string][]string {
	rows := map[string][]string{}
	for _, line := range strings.Split(manifest, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}

		columns := strings.Split(line, "|")
		if len(columns) < 11 {
			continue
		}

		id := strings.Trim(strings.TrimSpace(columns[1]), "`")
		cells := make([]string, 0, len(columns)-3)
		for _, column := range columns[2 : len(columns)-1] {
			cells = append(cells, strings.TrimSpace(column))
		}
		rows[id] = cells
	}
	return rows
}
