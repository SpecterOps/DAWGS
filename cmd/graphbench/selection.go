// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
)

const selectionManifestVersion = 1

type CorpusSelectors struct {
	Cases      []string `json:"cases,omitempty"`
	Datasets   []string `json:"datasets,omitempty"`
	Categories []string `json:"categories,omitempty"`
	Tags       []string `json:"tags,omitempty"`
}

type ResolvedCaseSelector struct {
	Dataset  string `json:"dataset"`
	Name     string `json:"name"`
	Category string `json:"category"`
}

type SelectionManifest struct {
	Version                  int                    `json:"version"`
	Requested                CorpusSelectors        `json:"requested"`
	Resolved                 []ResolvedCaseSelector `json:"resolved"`
	DiagnosticOnly           bool                   `json:"diagnostic_only"`
	FullDeclarationCount     int                    `json:"full_declaration_count"`
	SelectedDeclarationCount int                    `json:"selected_declaration_count"`
	OmittedDeclarationCount  int                    `json:"omitted_declaration_count"`
	DeclarationSHA256        string                 `json:"declaration_sha256"`
}

func selectScaleCorpus(corpus ScaleCorpus, selectors CorpusSelectors) (ScaleCorpus, SelectionManifest, error) {
	filtered := len(selectors.Cases)+len(selectors.Datasets)+len(selectors.Categories)+len(selectors.Tags) > 0
	manifest := SelectionManifest{
		Version:              selectionManifestVersion,
		Requested:            selectors,
		DiagnosticOnly:       filtered,
		FullDeclarationCount: len(corpus.DeclaredBackends()),
	}
	if err := validateCorpusSelectors(corpus, selectors); err != nil {
		return ScaleCorpus{}, SelectionManifest{}, err
	}

	selected := ScaleCorpus{}
	for _, testCase := range corpus.Cases {
		if matchesSelectors(testCase, selectors) {
			selected.Cases = append(selected.Cases, testCase)
			manifest.Resolved = append(manifest.Resolved, ResolvedCaseSelector{
				Dataset:  testCase.Dataset,
				Name:     testCase.Name,
				Category: testCase.Category,
			})
		}
	}
	if len(selected.Cases) == 0 {
		return ScaleCorpus{}, SelectionManifest{}, fmt.Errorf("selectors resolved to an empty corpus")
	}
	manifest.SelectedDeclarationCount = len(selected.DeclaredBackends())
	manifest.OmittedDeclarationCount = manifest.FullDeclarationCount - manifest.SelectedDeclarationCount
	manifest.DeclarationSHA256 = declarationSHA256(selected.DeclaredBackends())
	return selected, manifest, nil
}

func validateCorpusSelectors(corpus ScaleCorpus, selectors CorpusSelectors) error {
	caseMatches := map[string][]ScaleCase{}
	datasets := map[string]struct{}{}
	categories := map[string]struct{}{}
	tags := map[string]struct{}{}
	for _, testCase := range corpus.Cases {
		caseMatches[testCase.Name] = append(caseMatches[testCase.Name], testCase)
		datasets[testCase.Dataset] = struct{}{}
		categories[testCase.Category] = struct{}{}
		for _, tag := range testCase.Tags {
			tags[tag] = struct{}{}
		}
	}
	for _, name := range selectors.Cases {
		matches := caseMatches[name]
		if len(matches) == 0 {
			return fmt.Errorf("unknown case selector %q", name)
		}
		if len(matches) != 1 {
			return fmt.Errorf("ambiguous case selector %q resolves to %d cases", name, len(matches))
		}
	}
	for _, selector := range []struct {
		kind   string
		values []string
		known  map[string]struct{}
	}{
		{
			kind:   "dataset",
			values: selectors.Datasets,
			known:  datasets,
		},
		{
			kind:   "category",
			values: selectors.Categories,
			known:  categories,
		},
		{
			kind:   "tag",
			values: selectors.Tags,
			known:  tags,
		},
	} {
		for _, value := range selector.values {
			if _, found := selector.known[value]; !found {
				return fmt.Errorf("unknown %s selector %q", selector.kind, value)
			}
		}
	}
	return nil
}

func matchesSelectors(testCase ScaleCase, selectors CorpusSelectors) bool {
	if len(selectors.Cases) > 0 && !slices.Contains(selectors.Cases, testCase.Name) {
		return false
	}
	if len(selectors.Datasets) > 0 && !slices.Contains(selectors.Datasets, testCase.Dataset) {
		return false
	}
	if len(selectors.Categories) > 0 && !slices.Contains(selectors.Categories, testCase.Category) {
		return false
	}
	if len(selectors.Tags) > 0 {
		matched := false
		for _, tag := range selectors.Tags {
			matched = matched || slices.Contains(testCase.Tags, tag)
		}
		if !matched {
			return false
		}
	}
	return true
}

func selectionIdentity(records []CaseResult) (SelectionManifest, error) {
	var selected *SelectionManifest
	for _, record := range records {
		if record.Environment == nil || record.Environment.Selection == nil {
			return SelectionManifest{}, fmt.Errorf("%s/%s has no selection manifest", record.Dataset, record.Name)
		}
		if selected == nil {
			copy := *record.Environment.Selection
			selected = &copy
			continue
		}
		if selected.DeclarationSHA256 != record.Environment.Selection.DeclarationSHA256 || selected.DiagnosticOnly != record.Environment.Selection.DiagnosticOnly {
			return SelectionManifest{}, fmt.Errorf("artifact contains inconsistent selection manifests")
		}
	}
	if selected == nil {
		return SelectionManifest{}, fmt.Errorf("artifact contains no records")
	}
	return *selected, nil
}

func resolvedSelectionSHA256(resolved []ResolvedCaseSelector) string {
	items := append([]ResolvedCaseSelector(nil), resolved...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Dataset != items[j].Dataset {
			return items[i].Dataset < items[j].Dataset
		}
		return items[i].Name < items[j].Name
	})
	digest := sha256.New()
	for _, item := range items {
		fmt.Fprintf(digest, "%s\x00%s\x00%s\n", item.Dataset, item.Name, item.Category)
	}
	return hex.EncodeToString(digest.Sum(nil))
}
