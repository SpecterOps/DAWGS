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

const localTraversalUnavailableReason = "local traversal executor unavailable"

func runLocalTraversalPlaceholders(corpus ScaleCorpus) []CaseResult {
	records := make([]CaseResult, 0)
	for _, testCase := range corpus.Cases {
		if !testCase.Supports(ModeLocalTraversal) {
			continue
		}

		record := newCaseResult(testCase, ModeLocalTraversal, testCase.Params)
		record.Status = StatusNotImplemented
		record.FallbackReason = localTraversalUnavailableReason
		records = append(records, record)
	}

	return records
}
