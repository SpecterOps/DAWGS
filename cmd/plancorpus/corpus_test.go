package main

import (
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// TestLoadCorpus verifies that integration fixtures populate case groups, datasets, templates, and both node and edge kind catalogs.
func TestLoadCorpus(t *testing.T) {
	suite, err := loadCorpus(filepath.Join("..", "..", "integration", "testdata"))
	require.NoError(t, err)

	require.Contains(t, suite.caseGroups, "base")
	require.Contains(t, suite.datasetNames, "base")
	require.NotEmpty(t, suite.templateFiles)
	require.NotEmpty(t, suite.nodeKinds)
	require.NotEmpty(t, suite.edgeKinds)
}

// TestCorpusTemplatesParse verifies that every declared template variant renders without placeholders and parses as Cypher.
func TestCorpusTemplatesParse(t *testing.T) {
	suite, err := loadCorpus(filepath.Join("..", "..", "integration", "testdata"))
	require.NoError(t, err)

	for _, file := range suite.templateFiles {
		for _, family := range file.Families {
			for _, variant := range family.Variants {
				t.Run(family.Name+"/"+variant.Name, func(t *testing.T) {
					rendered, err := renderTemplate(family.Template, variant.Vars)
					require.NoError(t, err)
					_, err = frontend.ParseCypher(frontend.NewContext(), rendered)
					require.NoError(t, err)
				})
			}
		}
	}
}

// TestRenderTemplateRequiresAllPlaceholders verifies successful substitution and rejection when any template marker remains unresolved.
func TestRenderTemplateRequiresAllPlaceholders(t *testing.T) {
	rendered, err := renderTemplate("match ({{name}}) return {{name}}", map[string]string{"name": "n"})
	require.NoError(t, err)
	require.Equal(t, "match (n) return n", rendered)

	_, err = renderTemplate("match ({{name}}) return n", nil)
	require.ErrorContains(t, err, "unresolved placeholders")
}

// TestMergeParams verifies right-hand override precedence, retention of unrelated values, and a nil result for two absent maps.
func TestMergeParams(t *testing.T) {
	merged := mergeParams(map[string]any{"a": 1, "b": 2}, map[string]any{"b": 3})
	require.Equal(t, map[string]any{"a": 1, "b": 3}, merged)
	require.Nil(t, mergeParams(nil, nil))
}

// TestResolveFixtureParams verifies scalar/list key resolution to ordered int64 IDs and reports an unknown fixture key.
func TestResolveFixtureParams(t *testing.T) {
	params, err := resolveFixtureParams(
		map[string]any{"literal": "value"},
		map[string]string{"start_id": "start"},
		map[string][]string{"end_ids": {"end", "start"}},
		opengraph.IDMap{"start": graph.ID(11), "end": graph.ID(22)},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"literal":  "value",
		"start_id": int64(11),
		"end_ids":  []int64{22, 11},
	}, params)

	_, err = resolveFixtureParams(nil, map[string]string{"missing": "unknown"}, nil, opengraph.IDMap{})
	require.ErrorContains(t, err, "unknown fixture ID")
}
