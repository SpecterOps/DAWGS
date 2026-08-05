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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig(t *testing.T) {
	modes := []ExecutionMode{ModePostgresSQL, ModeNeo4j}

	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modesForRound(modes, 1))
	require.Equal(t, []ExecutionMode{ModeNeo4j, ModePostgresSQL}, modesForRound(modes, 2))
	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modes)
}

func TestParseConfigRequiresCompleteGateInputs(t *testing.T) {
	_, err := parseConfig([]string{"-gate-baseline", "baseline.jsonl"}, func(string) string { return "" })

	require.ErrorContains(t, err, "must be supplied together")
}
