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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// loadScaleCorpus loads all scale-case JSON files and rejects duplicate or invalid declarations.
func loadScaleCorpus(root string) (ScaleCorpus, error) {
	casePaths, err := filepath.Glob(filepath.Join(root, "cases", "*.json"))
	if err != nil {
		return ScaleCorpus{}, fmt.Errorf("glob scale cases: %w", err)
	}
	if len(casePaths) == 0 {
		return ScaleCorpus{}, fmt.Errorf("no scale case files found under %s", filepath.Join(root, "cases"))
	}

	sort.Strings(casePaths)

	var corpus ScaleCorpus
	for _, path := range casePaths {
		var file ScaleCaseFile
		if err := decodeJSONFile(path, &file); err != nil {
			return ScaleCorpus{}, err
		}

		source := filepath.ToSlash(path)
		for idx, testCase := range file.Cases {
			testCase.Source = source
			if err := validateScaleCase(testCase); err != nil {
				return ScaleCorpus{}, fmt.Errorf("%s case %d: %w", source, idx, err)
			}

			corpus.Cases = append(corpus.Cases, testCase)
		}
	}

	return corpus, nil
}

// validateScaleCase checks case identity, modes, parameters, expectations, and workload shape.
func validateScaleCase(testCase ScaleCase) error {
	if testCase.Name == "" {
		return fmt.Errorf("name is required")
	}
	if testCase.Dataset == "" {
		return fmt.Errorf("dataset is required")
	}
	if testCase.Category == "" {
		return fmt.Errorf("category is required")
	}
	if testCase.Cypher == "" {
		return fmt.Errorf("cypher is required")
	}
	if len(testCase.CandidateModes) == 0 {
		return fmt.Errorf("candidate_modes is required")
	}

	for _, mode := range testCase.CandidateModes {
		if !mode.Valid() {
			return fmt.Errorf("unsupported candidate mode %q", mode)
		}
	}
	for mode, reason := range testCase.UnsupportedModes {
		if !mode.Valid() {
			return fmt.Errorf("invalid unsupported mode %q", mode)
		}
		if reason == "" {
			return fmt.Errorf("unsupported mode %q requires a reason", mode)
		}
		if testCase.Supports(mode) {
			return fmt.Errorf("mode %q cannot be both candidate and unsupported", mode)
		}
	}
	if testCase.Shape.RelationshipKindCount < 0 {
		return fmt.Errorf("shape.relationship_kind_count must not be negative")
	}
	if tier := testCase.Shape.FixtureTier; tier != "" && tier != "normal" && tier != "envelope" && tier != "stress" {
		return fmt.Errorf("shape.fixture_tier must be normal, envelope, or stress")
	}
	if direction := testCase.Shape.Direction; direction != "" && direction != "outbound" && direction != "inbound" && direction != "directionless" && direction != "mirrored" {
		return fmt.Errorf("shape.direction must be outbound, inbound, directionless, or mirrored")
	}

	if len(testCase.Expected.IDRows) > 0 {
		if testCase.Expected.ResultKind != "id_rows" {
			return fmt.Errorf("expected.id_rows requires result_kind id_rows")
		}
		if testCase.Expected.RowCount == nil || int64(len(testCase.Expected.IDRows)) != *testCase.Expected.RowCount {
			return fmt.Errorf("expected.id_rows must contain exactly row_count rows")
		}
	}
	if len(testCase.Expected.PathRows) > 0 {
		if testCase.Expected.ResultKind != "path_set" {
			return fmt.Errorf("expected.path_rows requires result_kind path_set")
		}
		if testCase.Expected.RowCount == nil || int64(len(testCase.Expected.PathRows)) != *testCase.Expected.RowCount {
			return fmt.Errorf("expected.path_rows must contain exactly row_count rows")
		}
		for idx, path := range testCase.Expected.PathRows {
			if len(path.Nodes) != len(path.RelationshipKinds)+1 {
				return fmt.Errorf("expected.path_rows[%d] must have one more node than relationship kind", idx)
			}
		}
	}

	if testCase.WriteScenario != nil {
		if err := validateWriteScenario(*testCase.WriteScenario); err != nil {
			return err
		}
	}

	return nil
}

// validateWriteScenario checks mutation expectations and post-state query completeness.
func validateWriteScenario(scenario WriteScenario) error {
	if scenario.SelectionCypher == "" {
		return fmt.Errorf("write_scenario.selection_cypher is required")
	}
	if scenario.ExpectedMatched == nil {
		return fmt.Errorf("write_scenario.expected_matched is required")
	}
	if scenario.ExpectedAffected == nil {
		return fmt.Errorf("write_scenario.expected_affected is required")
	}
	if scenario.AffectedEntity != "node" && scenario.AffectedEntity != "relationship" {
		return fmt.Errorf("write_scenario.affected_entity must be node or relationship")
	}
	if len(scenario.PostState) == 0 {
		return fmt.Errorf("write_scenario.post_state is required")
	}

	for idx, postState := range scenario.PostState {
		if postState.Name == "" {
			return fmt.Errorf("write_scenario.post_state[%d].name is required", idx)
		}
		if postState.Cypher == "" {
			return fmt.Errorf("write_scenario.post_state[%d].cypher is required", idx)
		}
		if postState.Expected.RowCount == nil && postState.Expected.ScalarInt == nil {
			return fmt.Errorf("write_scenario.post_state[%d].expected requires row_count or scalar_int", idx)
		}
	}

	return nil
}

// decodeJSONFile reads a JSON file and decodes it into the supplied destination.
func decodeJSONFile(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}

	return nil
}

// scaleCorpusDatasets returns unique corpus dataset names in sorted order.
func scaleCorpusDatasets(corpus ScaleCorpus) []string {
	var (
		seen     = map[string]struct{}{}
		datasets = make([]string, 0)
	)

	for _, testCase := range corpus.Cases {
		if _, duplicate := seen[testCase.Dataset]; duplicate {
			continue
		}

		seen[testCase.Dataset] = struct{}{}
		datasets = append(datasets, testCase.Dataset)
	}

	sort.Strings(datasets)
	return datasets
}

// scaleCasesByDataset indexes scale cases by dataset while preserving corpus order.
func scaleCasesByDataset(corpus ScaleCorpus) map[string][]ScaleCase {
	grouped := map[string][]ScaleCase{}
	for _, testCase := range corpus.Cases {
		grouped[testCase.Dataset] = append(grouped[testCase.Dataset], testCase)
	}

	return grouped
}
