package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

type corpus struct {
	caseGroups    map[string]*caseGroup
	datasetNames  []string
	templateFiles []templateFile
	nodeKinds     graph.Kinds
	edgeKinds     graph.Kinds
}

type caseGroup struct {
	dataset string
	files   []caseFile
}

type caseFile struct {
	path    string
	Dataset string      `json:"dataset"`
	Cases   []caseEntry `json:"cases"`
}

type caseEntry struct {
	Name    string           `json:"name"`
	Cypher  string           `json:"cypher"`
	Params  map[string]any   `json:"params,omitempty"`
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
}

type templateFile struct {
	path        string
	Families    []templateFamily    `json:"families,omitempty"`
	Metamorphic []metamorphicFamily `json:"metamorphic,omitempty"`
}

type templateFamily struct {
	Name     string            `json:"name"`
	Template string            `json:"template"`
	Params   map[string]any    `json:"params,omitempty"`
	Fixture  *opengraph.Graph  `json:"fixture,omitempty"`
	Variants []templateVariant `json:"variants"`
}

type templateVariant struct {
	Name   string            `json:"name"`
	Vars   map[string]string `json:"vars"`
	Params map[string]any    `json:"params,omitempty"`
}

type metamorphicFamily struct {
	Name    string             `json:"name"`
	Fixture *opengraph.Graph   `json:"fixture,omitempty"`
	Queries []metamorphicQuery `json:"queries"`
}

type metamorphicQuery struct {
	Name   string         `json:"name"`
	Cypher string         `json:"cypher"`
	Params map[string]any `json:"params,omitempty"`
}

func loadCorpus(datasetDir string) (corpus, error) {
	var loaded corpus
	loaded.caseGroups = map[string]*caseGroup{}

	if err := loaded.loadCaseFiles(datasetDir); err != nil {
		return corpus{}, err
	}
	if err := loaded.loadTemplateFiles(datasetDir); err != nil {
		return corpus{}, err
	}
	if err := loaded.loadDatasetKinds(datasetDir); err != nil {
		return corpus{}, err
	}

	sort.Strings(loaded.datasetNames)
	return loaded, nil
}

func (s *corpus) loadCaseFiles(datasetDir string) error {
	paths, err := filepath.Glob(filepath.Join(datasetDir, "cases", "*.json"))
	if err != nil {
		return fmt.Errorf("glob case files: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no case files found under %s", filepath.Join(datasetDir, "cases"))
	}
	sort.Strings(paths)

	for _, path := range paths {
		var file caseFile
		if err := decodeJSONFile(path, &file); err != nil {
			return err
		}
		file.path = filepath.ToSlash(path)

		dataset := file.Dataset
		if dataset == "" {
			dataset = "base"
		}
		if s.caseGroups[dataset] == nil {
			s.caseGroups[dataset] = &caseGroup{dataset: dataset}
			s.datasetNames = append(s.datasetNames, dataset)
		}
		s.caseGroups[dataset].files = append(s.caseGroups[dataset].files, file)

		for _, testCase := range file.Cases {
			s.addFixtureKinds(testCase.Fixture)
		}
	}

	return nil
}

func (s *corpus) loadTemplateFiles(datasetDir string) error {
	paths, err := filepath.Glob(filepath.Join(datasetDir, "templates", "*.json"))
	if err != nil {
		return fmt.Errorf("glob template files: %w", err)
	}
	sort.Strings(paths)

	for _, path := range paths {
		var file templateFile
		if err := decodeJSONFile(path, &file); err != nil {
			return err
		}
		file.path = filepath.ToSlash(path)
		s.templateFiles = append(s.templateFiles, file)

		for _, family := range file.Families {
			s.addFixtureKinds(family.Fixture)
		}
		for _, family := range file.Metamorphic {
			s.addFixtureKinds(family.Fixture)
		}
	}

	return nil
}

func (s *corpus) loadDatasetKinds(datasetDir string) error {
	for _, datasetName := range s.datasetNames {
		path := filepath.Join(datasetDir, datasetName+".json")
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open dataset %s: %w", datasetName, err)
		}

		doc, parseErr := opengraph.ParseDocument(f)
		closeErr := f.Close()
		if parseErr != nil {
			return fmt.Errorf("parse dataset %s: %w", datasetName, parseErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close dataset %s: %w", datasetName, closeErr)
		}

		nodeKinds, edgeKinds := doc.Graph.Kinds()
		s.nodeKinds = s.nodeKinds.Add(nodeKinds...)
		s.edgeKinds = s.edgeKinds.Add(edgeKinds...)
	}

	return nil
}

func (s *corpus) addFixtureKinds(fixture *opengraph.Graph) {
	if fixture == nil {
		return
	}

	nodeKinds, edgeKinds := fixture.Kinds()
	s.nodeKinds = s.nodeKinds.Add(nodeKinds...)
	s.edgeKinds = s.edgeKinds.Add(edgeKinds...)
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

func renderTemplate(template string, vars map[string]string) (string, error) {
	rendered := template
	for name, value := range vars {
		rendered = strings.ReplaceAll(rendered, "{{"+name+"}}", value)
	}
	if strings.Contains(rendered, "{{") || strings.Contains(rendered, "}}") {
		return "", fmt.Errorf("template has unresolved placeholders: %s", rendered)
	}
	return rendered, nil
}

func mergeParams(base, overrides map[string]any) map[string]any {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string]any, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}
