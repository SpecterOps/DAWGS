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

	return nil
}

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

func scaleCorpusDatasets(corpus ScaleCorpus) []string {
	seen := map[string]struct{}{}
	datasets := make([]string, 0)

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

func scaleCasesByDataset(corpus ScaleCorpus) map[string][]ScaleCase {
	grouped := map[string][]ScaleCase{}
	for _, testCase := range corpus.Cases {
		grouped[testCase.Dataset] = append(grouped[testCase.Dataset], testCase)
	}

	return grouped
}
