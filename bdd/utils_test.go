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

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestFormatGraphResults(t *testing.T) {
	nodes := []graph.Node{
		{
			ID:    1,
			Kinds: graph.Kinds{graph.StringKind("A")},
			Properties: &graph.Properties{
				Map: map[string]any{"name": "a"},
			},
		},
		{
			ID:    2,
			Kinds: graph.Kinds{graph.StringKind("B")},
			Properties: &graph.Properties{
				Map: map[string]any{"name": "b"},
			},
		},
		{
			ID: 3,
			Properties: &graph.Properties{
				Map: map[string]any{"name": "c"},
			},
		},
	}
	actualList, err := formatGraphResults(nodes)
	assert.Nil(t, err)

	expectedList := []string{"(:A{name: 'a'})", "(:B{name: 'b'})", "({name: 'c'})"}

	for i := range len(expectedList) {
		assert.Equal(t, actualList[i], expectedList[i])
	}
}
