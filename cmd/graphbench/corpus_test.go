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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadScaleCorpus(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	require.NotEmpty(t, corpus.Cases)

	for _, testCase := range corpus.Cases {
		require.NotEqual(t, "", testCase.Source)
		require.True(t, testCase.Supports(ModePostgresSQL), "postgres_sql should be part of the initial corpus for %s", testCase.Name)
		require.False(t, testCase.Supports(ExecutionMode("age")), "AGE is a reference design only for %s", testCase.Name)
	}
}

func TestScaleCorpusDatasets(t *testing.T) {
	corpus := ScaleCorpus{Cases: []ScaleCase{
		{Name: "a", Dataset: "base", Category: "counts", Cypher: "return 1", CandidateModes: []ExecutionMode{ModePostgresSQL}},
		{Name: "b", Dataset: "adcs_fanout", Category: "counts", Cypher: "return 1", CandidateModes: []ExecutionMode{ModePostgresSQL}},
		{Name: "c", Dataset: "base", Category: "counts", Cypher: "return 1", CandidateModes: []ExecutionMode{ModePostgresSQL}},
	}}

	require.Equal(t, []string{"adcs_fanout", "base"}, scaleCorpusDatasets(corpus))
}
