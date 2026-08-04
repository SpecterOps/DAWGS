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

func TestPhase8DormantFormsStayOutOfPlanCorpus(t *testing.T) {
	suite, err := loadCorpus("../../integration/testdata")
	require.NoError(t, err)

	for _, group := range suite.caseGroups {
		for _, file := range group.files {
			for _, testCase := range file.Cases {
				requireNoDormantPlanQueryFormID(t, file.path+" case", testCase.Name)
			}
		}
	}

	for _, file := range suite.templateFiles {
		for _, family := range file.Families {
			requireNoDormantPlanQueryFormID(t, file.path+" family", family.Name)
			for _, variant := range family.Variants {
				requireNoDormantPlanQueryFormID(t, file.path+" variant", variant.Name)
			}
		}
		for _, family := range file.Metamorphic {
			requireNoDormantPlanQueryFormID(t, file.path+" metamorphic family", family.Name)
			for _, query := range family.Queries {
				requireNoDormantPlanQueryFormID(t, file.path+" metamorphic query", query.Name)
			}
		}
	}
}

func requireNoDormantPlanQueryFormID(t *testing.T, field, value string) {
	t.Helper()
	require.False(t, strings.Contains(strings.ToUpper(value), "FUTURE-"),
		"%s %q places a dormant query form in the active plan corpus", field, value)
}
