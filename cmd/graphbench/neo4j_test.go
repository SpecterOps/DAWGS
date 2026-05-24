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
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNeo4jPlanDriverConfig(t *testing.T) {
	cfg, err := parseNeo4jPlanDriverConfig("neo4j://neo4j:secret@example.com:7687/neo4jdb?x=1")

	require.NoError(t, err)
	require.Equal(t, "neo4j://example.com:7687?x=1", cfg.Target)
	require.Equal(t, "neo4j", cfg.Username)
	require.Equal(t, "secret", cfg.Password)
	require.Equal(t, "neo4jdb", cfg.DatabaseName)
}

func TestNeo4jDatabaseNameRejectsNestedPath(t *testing.T) {
	for _, connStr := range []string{
		"neo4j://neo4j:secret@example.com:7687/a/b",
		"neo4j://neo4j:secret@example.com:7687/a%2Fb",
	} {
		parsed, err := url.Parse(connStr)
		require.NoError(t, err)

		_, err = neo4jDatabaseName(parsed)
		require.ErrorContains(t, err, "single database name")
	}
}

func TestNeo4jOperatorsAnnotatesOperators(t *testing.T) {
	operators := neo4jOperators(Neo4jPlanNode{
		Operator: "ProduceResults",
		Children: []Neo4jPlanNode{{
			Operator: "AllNodesScan",
		}},
	})

	require.Equal(t, []string{"ProduceResults@neo4j", "AllNodesScan@neo4j"}, operators)
}
