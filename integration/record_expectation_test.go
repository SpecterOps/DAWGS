package integration

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// edgeExpectation describes the stable identity, kind, and optional properties required of a relationship result.
type edgeExpectation struct {
	// Start is the expected fixture ID of the relationship's start node.
	Start string `json:"start,omitempty"`

	// End is the expected fixture ID of the relationship's end node.
	End string `json:"end,omitempty"`

	// Kind is the expected relationship kind.
	Kind string `json:"kind,omitempty"`

	// Props contains the expected relationship property subset.
	Props map[string]any `json:"props,omitempty"`
}

// nodeExpectation describes the stable identity, kinds, and optional properties required of a node result.
type nodeExpectation struct {
	// ID is the expected fixture node identifier.
	ID string `json:"id"`

	// Kinds contains the expected node kinds independent of order.
	Kinds []string `json:"kinds,omitempty"`

	// Props contains the expected node property subset.
	Props map[string]any `json:"props,omitempty"`
}

// UnmarshalJSON rejects misspelled record fields instead of losing assertions.
func (s *nodeExpectation) UnmarshalJSON(raw []byte) error {
	type record nodeExpectation
	return decodeRecordExpectation(raw, (*record)(s))
}

func (s *edgeExpectation) UnmarshalJSON(raw []byte) error {
	type record edgeExpectation
	return decodeRecordExpectation(raw, (*record)(s))
}

func decodeRecordExpectation(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func TestRecordExpectationsRejectUnknownFields(t *testing.T) {
	for _, target := range []any{&nodeExpectation{}, &edgeExpectation{}} {
		require.NoError(t, json.Unmarshal([]byte(`{"props":{"score":2,"name":"2","enabled":true}}`), target))
		require.ErrorContains(t, json.Unmarshal([]byte(`{"properties":{"score":2}}`), target), `unknown field "properties"`)
		require.ErrorContains(t, json.Unmarshal([]byte(`{"props":{},"typo":true}`), target), `unknown field "typo"`)
	}
	var node nodeExpectation
	require.NoError(t, json.Unmarshal([]byte(`{"id":"a","kinds":["Input"],"props":{"score":2}}`), &node))
	require.Equal(t, "a", node.ID)
	require.Equal(t, []string{"Input"}, node.Kinds)
	require.Equal(t, float64(2), node.Props["score"])
	var edge edgeExpectation
	require.NoError(t, json.Unmarshal([]byte(`{"start":"a","end":"b","kind":"Link","props":{"flag":false}}`), &edge))
	require.Equal(t, "a", edge.Start)
	require.Equal(t, "b", edge.End)
	require.Equal(t, "Link", edge.Kind)
	require.Equal(t, false, edge.Props["flag"])
}

// Validate expectations without connecting to a database, including nested paths.
func TestCorpusRecordExpectations(t *testing.T) {
	for _, root := range []string{"testdata/cases", "testdata/templates"} {
		require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var doc any
			if err := json.Unmarshal(raw, &doc); err != nil {
				return err
			}
			t.Run(path, func(t *testing.T) { validateRecordExpectations(t, doc) })
			return nil
		}))
	}
}

func validateRecordExpectations(t *testing.T, doc any) {
	t.Helper()
	switch doc := doc.(type) {
	case map[string]any:
		for key, value := range doc {
			if key == "properties" || key == "props" {
				continue // Property names are application data, not assertion keys.
			}
			var target any
			switch key {
			case "node_records":
				target = &[]nodeExpectation{}
			case "relationship_records", "relationship_triples":
				target = &[]edgeExpectation{}
			case "path_relationship_records":
				target = &[][]edgeExpectation{}
			case "contains_edge":
				target = &edgeExpectation{}
			}
			if target != nil {
				raw, err := json.Marshal(value)
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(raw, target), key)
			} else {
				validateRecordExpectations(t, value)
			}
		}
	case []any:
		for _, value := range doc {
			validateRecordExpectations(t, value)
		}
	}
}
