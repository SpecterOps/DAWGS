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

func TestRunLocalTraversalPlaceholders(t *testing.T) {
	records := runLocalTraversalPlaceholders(ScaleCorpus{Cases: []ScaleCase{
		{
			Name:           "supported",
			Dataset:        "base",
			Category:       "reachability",
			Cypher:         "MATCH (n) RETURN n",
			NodeParams:     map[string]string{"start_id": "n1"},
			CandidateModes: []ExecutionMode{ModePostgresSQL, ModeLocalTraversal},
		},
		{
			Name:           "unsupported",
			Dataset:        "base",
			Category:       "count",
			Cypher:         "MATCH (n) RETURN count(n)",
			CandidateModes: []ExecutionMode{ModePostgresSQL},
		},
	}})

	require.Len(t, records, 1)
	require.Equal(t, ModeLocalTraversal, records[0].ExecutionMode)
	require.Equal(t, StatusNotImplemented, records[0].Status)
	require.Equal(t, localTraversalUnavailableReason, records[0].FallbackReason)
	require.Equal(t, map[string]string{"start_id": "n1"}, records[0].NodeParams)
}
