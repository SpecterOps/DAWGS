package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

type fixtureKindCaseFile struct {
	Dataset string            `json:"dataset"`
	Cases   []fixtureKindCase `json:"cases"`
}

type fixtureKindCase struct {
	Name    string           `json:"name"`
	Cypher  string           `json:"cypher"`
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
}

type kindStringSet map[string]struct{}

type referencedKindSets struct {
	Node kindStringSet
	Edge kindStringSet
}

func TestFixtureBackedCasesSeedReferencedKinds(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "cases", "*.json"))
	if err != nil {
		t.Fatalf("failed to glob case files: %v", err)
	}

	datasetKindCache := map[string]referencedKindSets{}

	for _, path := range files {
		caseFile := readFixtureKindCaseFile(t, path)
		dataset := caseFile.Dataset
		if dataset == "" {
			dataset = "base"
		}

		datasetKinds, hasDatasetKinds := datasetKindCache[dataset]
		if !hasDatasetKinds {
			datasetKinds = readDatasetKindSets(t, dataset)
			datasetKindCache[dataset] = datasetKinds
		}

		for _, testCase := range caseFile.Cases {
			if testCase.Fixture == nil {
				continue
			}

			t.Run(fmt.Sprintf("%s/%s", filepath.Base(path), testCase.Name), func(t *testing.T) {
				referencedKinds := collectReferencedKinds(t, testCase.Cypher)
				seededKinds := datasetKinds.Clone()

				fixtureNodeKinds, fixtureEdgeKinds := testCase.Fixture.Kinds()
				seededKinds.Node.AddKinds(fixtureNodeKinds)
				seededKinds.Edge.AddKinds(fixtureEdgeKinds)

				if missingNodeKinds := referencedKinds.Node.MissingFrom(seededKinds.Node); len(missingNodeKinds) > 0 {
					t.Errorf("fixture does not seed referenced node kinds: %v", missingNodeKinds)
				}

				if missingEdgeKinds := referencedKinds.Edge.MissingFrom(seededKinds.Edge); len(missingEdgeKinds) > 0 {
					t.Errorf("fixture does not seed referenced edge kinds: %v", missingEdgeKinds)
				}
			})
		}
	}
}

func readFixtureKindCaseFile(t *testing.T, path string) fixtureKindCaseFile {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}

	var caseFile fixtureKindCaseFile
	if err := json.Unmarshal(raw, &caseFile); err != nil {
		t.Fatalf("failed to decode %s: %v", path, err)
	}

	return caseFile
}

func readDatasetKindSets(t *testing.T, dataset string) referencedKindSets {
	t.Helper()

	file, err := os.Open(datasetPath(dataset))
	if err != nil {
		t.Fatalf("failed to open dataset %q for kind scanning: %v", dataset, err)
	}
	defer file.Close()

	document, err := opengraph.ParseDocument(file)
	if err != nil {
		t.Fatalf("failed to parse dataset %q: %v", dataset, err)
	}

	nodeKinds, edgeKinds := document.Graph.Kinds()
	return newReferencedKindSets(nodeKinds, edgeKinds)
}

func collectReferencedKinds(t *testing.T, cypherQuery string) referencedKindSets {
	t.Helper()

	query, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		t.Fatalf("failed to parse Cypher: %v", err)
	}

	referencedKinds := newReferencedKindSets(nil, nil)
	if err := walk.Cypher(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		switch typedNode := node.(type) {
		case *cypher.NodePattern:
			referencedKinds.Node.AddKinds(typedNode.Kinds)

		case *cypher.RelationshipPattern:
			referencedKinds.Edge.AddKinds(typedNode.Kinds)

		}
	})); err != nil {
		t.Fatalf("failed to walk Cypher AST: %v", err)
	}

	return referencedKinds
}

func newReferencedKindSets(nodeKinds, edgeKinds graph.Kinds) referencedKindSets {
	return referencedKindSets{
		Node: newKindStringSet(nodeKinds),
		Edge: newKindStringSet(edgeKinds),
	}
}

func (s referencedKindSets) Clone() referencedKindSets {
	return referencedKindSets{
		Node: s.Node.Clone(),
		Edge: s.Edge.Clone(),
	}
}

func newKindStringSet(kinds graph.Kinds) kindStringSet {
	set := kindStringSet{}
	set.AddKinds(kinds)
	return set
}

func (s kindStringSet) AddKinds(kinds graph.Kinds) {
	for _, kind := range kinds {
		if kind != nil {
			s[kind.String()] = struct{}{}
		}
	}
}

func (s kindStringSet) Clone() kindStringSet {
	clone := make(kindStringSet, len(s))
	for kind := range s {
		clone[kind] = struct{}{}
	}

	return clone
}

func (s kindStringSet) MissingFrom(other kindStringSet) []string {
	var missing []string
	for kind := range s {
		if _, found := other[kind]; !found {
			missing = append(missing, kind)
		}
	}

	sort.Strings(missing)
	return missing
}
