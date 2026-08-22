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
	"github.com/specterops/dawgs/testutil"
)

// corpus contains loaded corpus queries and their dataset definitions.
type corpus struct {
	// caseGroups indexes loaded corpus cases by dataset name.
	caseGroups map[string]*caseGroup
	// datasetNames lists fixture datasets in deterministic plan-capture order.
	datasetNames []string
	// templateFiles retains decoded template files for corpus expansion.
	templateFiles []templateFile
	// nodeKinds contains every node kind declared by loaded fixtures.
	nodeKinds graph.Kinds
	// edgeKinds contains every relationship kind declared by loaded fixtures.
	edgeKinds graph.Kinds
}

// caseGroup models a case-group entry in a scale-corpus JSON file.
type caseGroup struct {
	// dataset names the fixture shared by every case file in the group.
	dataset string
	// files retains source case files contributing to a dataset group.
	files []caseFile
}

// caseFile models the top-level groups in a scale-corpus case file.
type caseFile struct {
	// path retains the source path used in errors and provenance.
	path string
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Cases contains query cases declared by this source file.
	Cases []caseEntry `json:"cases"`
}

// caseEntry models one named query case and its parameter declarations.
type caseEntry struct {
	// Name identifies the query case within its dataset.
	Name string `json:"name"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// Fixture captures the fixture identity and cardinality contract.
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
}

// templateFile models template and metamorphic query families from a corpus template file.
type templateFile struct {
	// path retains the source path used in errors and provenance.
	path string
	// Families lists query-template families decoded from the file.
	Families []templateFamily `json:"families,omitempty"`
	// Metamorphic lists metamorphic query families decoded from the file.
	Metamorphic []metamorphicFamily `json:"metamorphic,omitempty"`
}

// templateFamily defines a base query and the variants rendered from it.
type templateFamily struct {
	// Name identifies the query-template family in expanded case names.
	Name string `json:"name"`
	// Template contains the Cypher template rendered for each variant.
	Template string `json:"template"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// Fixture captures the fixture identity and cardinality contract.
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
	// Variants lists substitutions rendered from the base query template.
	Variants []templateVariant `json:"variants"`
}

// templateVariant defines one named substitution set for a query template.
type templateVariant struct {
	// Name identifies this substitution set in the rendered case name.
	Name string `json:"name"`
	// Vars maps template placeholders to replacement text.
	Vars map[string]string `json:"vars"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
}

// metamorphicFamily groups semantically equivalent queries used for plan comparison.
type metamorphicFamily struct {
	// Name identifies the family of queries expected to remain semantically equivalent.
	Name string `json:"name"`
	// Fixture captures the fixture identity and cardinality contract.
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
	// Queries lists semantically equivalent queries in the metamorphic family.
	Queries []metamorphicQuery `json:"queries"`
}

// metamorphicQuery defines one named query in a metamorphic family.
type metamorphicQuery struct {
	// Name identifies one query variant within its metamorphic family.
	Name string `json:"name"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
}

// loadCorpus loads case, template, and dataset-kind declarations from a corpus directory.
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

// loadCaseFiles decodes case files and indexes them by dataset while retaining source paths.
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

// loadTemplateFiles renders template variants and metamorphic families into executable corpus cases.
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

// loadDatasetKinds loads fixture graphs and accumulates the node and relationship kinds they declare.
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

// addFixtureKinds unions a fixture's node and relationship kinds into the corpus kind sets.
func (s *corpus) addFixtureKinds(fixture *opengraph.Graph) {
	if fixture == nil {
		return
	}

	nodeKinds, edgeKinds := fixture.Kinds()
	s.nodeKinds = s.nodeKinds.Add(nodeKinds...)
	s.edgeKinds = s.edgeKinds.Add(edgeKinds...)
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

// renderTemplate substitutes every named placeholder and rejects any unresolved template markers.
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

// mergeParams returns a copied parameter map in which override values take precedence.
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

// mergeStringMap returns a copied string map in which override values take precedence.
func mergeStringMap(base, overrides map[string]string) map[string]string {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}

// mergeStringListMap returns a deep-enough copy of string-list parameters with overrides applied.
func mergeStringListMap(base, overrides map[string][]string) map[string][]string {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string][]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = append([]string(nil), value...)
	}
	for key, value := range overrides {
		merged[key] = append([]string(nil), value...)
	}
	return merged
}

// resolveFixtureParams replaces symbolic node keys and key lists with fixture database identifiers.
func resolveFixtureParams(
	params map[string]any,
	nodeParams map[string]string,
	nodeListParams map[string][]string,
	idMap opengraph.IDMap,
) (map[string]any, error) {
	resolved := make(map[string]any, len(params)+len(nodeParams)+len(nodeListParams))
	for name, value := range params {
		resolved[name] = value
	}

	for paramName, fixtureID := range nodeParams {
		id, found := idMap[fixtureID]
		if !found {
			return nil, fmt.Errorf("node parameter %q references unknown fixture ID %q", paramName, fixtureID)
		}
		resolved[paramName] = id.Int64()
	}

	for paramName, fixtureIDs := range nodeListParams {
		ids := make([]int64, len(fixtureIDs))
		for idx, fixtureID := range fixtureIDs {
			id, found := idMap[fixtureID]
			if !found {
				return nil, fmt.Errorf("node list parameter %q references unknown fixture ID %q", paramName, fixtureID)
			}
			ids[idx] = id.Int64()
		}
		resolved[paramName] = ids
	}

	if len(resolved) == 0 {
		return nil, nil
	}
	return resolved, nil
}
