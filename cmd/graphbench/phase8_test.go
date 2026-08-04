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

func TestPhase8DormantFormsStayOutOfScaleCorpus(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	for _, testCase := range corpus.Cases {
		requireNoDormantQueryFormID(t, testCase.Source+" name", testCase.Name)
		for _, tag := range testCase.Tags {
			requireNoDormantQueryFormID(t, testCase.Source+" tag", tag)
		}
	}
}

func requireNoDormantQueryFormID(t *testing.T, field, value string) {
	t.Helper()
	require.False(t, strings.Contains(strings.ToUpper(value), "FUTURE-"),
		"%s %q places a dormant query form in the active scale corpus", field, value)
}
