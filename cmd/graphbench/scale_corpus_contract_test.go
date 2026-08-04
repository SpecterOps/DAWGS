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

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var scaleCorpusRequiredIDs = []string{
	"REC-01", "REC-02", "REC-04", "REC-06", "REC-08",
	"TRUST-01", "TRUST-02",
	"PRUNE-01", "PRUNE-02", "PRUNE-03", "PRUNE-04",
	"HOP-01", "HOP-02", "HOP-03", "HOP-04", "HOP-05", "HOP-07", "HOP-09",
	"SCAN-01", "SCAN-02", "SCAN-03", "SCAN-04", "SCAN-05", "SCAN-07", "SCAN-08",
	"LOOKUP-02", "LOOKUP-04", "LOOKUP-05", "LOOKUP-09", "LOOKUP-11", "LOOKUP-13", "LOOKUP-15", "LOOKUP-16",
}

func scaleCorpusCaseID(name string) string {
	if separator := strings.IndexByte(name, '_'); separator >= 0 {
		return name[:separator]
	}
	return name
}

func scaleCorpusRequiredIDSet() map[string]struct{} {
	required := make(map[string]struct{}, len(scaleCorpusRequiredIDs))
	for _, id := range scaleCorpusRequiredIDs {
		required[id] = struct{}{}
	}
	return required
}

func TestScaleCorpusRequiredRepresentativesDeclareCardinality(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	required := scaleCorpusRequiredIDSet()
	covered := map[string]int{}
	for _, testCase := range corpus.Cases {
		id := scaleCorpusCaseID(testCase.Name)
		if _, isRequired := required[id]; !isRequired {
			continue
		}

		covered[id]++
		require.Contains(t, testCase.Tags, id, "%s must retain its stable query-form tag", testCase.Name)
		if testCase.WriteScenario == nil {
			require.NotNil(t, testCase.Expected.RowCount, "%s must declare expected row cardinality", testCase.Name)
		} else {
			require.NotNil(t, testCase.WriteScenario.ExpectedMatched, "%s must declare expected matched cardinality", testCase.Name)
			require.NotNil(t, testCase.WriteScenario.ExpectedAffected, "%s must declare expected affected cardinality", testCase.Name)
		}
	}

	for _, id := range scaleCorpusRequiredIDs {
		require.Positive(t, covered[id], "required scale corpus is missing %s", id)
	}
}

func TestScaleCorpusDistinguishesProjectionClasses(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	requiredClasses := map[string]bool{
		"projection-id-only":          false,
		"projection-shallow-ids-kind": false,
		"projection-full-hydration":   false,
	}
	for _, testCase := range corpus.Cases {
		for _, tag := range testCase.Tags {
			if _, required := requiredClasses[tag]; !required {
				continue
			}

			requiredClasses[tag] = true
			switch tag {
			case "projection-id-only":
				require.Equal(t, "id_set", testCase.Expected.ResultKind)
				require.False(t, testCase.Observes.Nodes)
				require.False(t, testCase.Observes.Relationships)
				require.False(t, testCase.Observes.Properties)
			case "projection-shallow-ids-kind":
				require.Equal(t, "shallow_ids_kind", testCase.Expected.ResultKind)
				require.False(t, testCase.Observes.Nodes)
				require.False(t, testCase.Observes.Relationships)
				require.False(t, testCase.Observes.Properties)
			case "projection-full-hydration":
				require.True(t, testCase.Observes.Nodes || testCase.Observes.Relationships)
				require.True(t, testCase.Observes.Properties)
			}
		}
	}

	for projectionClass, found := range requiredClasses {
		require.True(t, found, "scale corpus is missing %s", projectionClass)
	}
}
