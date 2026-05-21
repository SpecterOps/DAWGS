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

package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWriteJSONEmitsBaselineFriendlyReport(t *testing.T) {
	report := Report{
		Driver:     "pg",
		GitRef:     "abc123",
		Date:       "2026-05-14",
		Iterations: 3,
		Results: []Result{{
			Section:  "Traversal",
			Dataset:  "base",
			Label:    "depth 1",
			RowCount: 2,
			Stats: Stats{
				Median: 10 * time.Millisecond,
				P95:    20 * time.Millisecond,
				Max:    30 * time.Millisecond,
			},
		}},
	}

	var output bytes.Buffer
	if err := writeJSON(&output, report); err != nil {
		t.Fatalf("write JSON: %v", err)
	}

	text := output.String()
	for _, expected := range []string{
		`"driver": "pg"`,
		`"git_ref": "abc123"`,
		`"median": 10000000`,
		`"row_count": 2`,
		`"section": "Traversal"`,
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("JSON report missing %q:\n%s", expected, text)
		}
	}
}
