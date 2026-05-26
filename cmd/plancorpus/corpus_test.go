package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCorpus(t *testing.T) {
	suite, err := loadCorpus(filepath.Join("..", "..", "integration", "testdata"))
	require.NoError(t, err)

	require.Contains(t, suite.caseGroups, "base")
	require.Contains(t, suite.datasetNames, "base")
	require.NotEmpty(t, suite.templateFiles)
	require.NotEmpty(t, suite.nodeKinds)
	require.NotEmpty(t, suite.edgeKinds)
}

func TestRenderTemplateRequiresAllPlaceholders(t *testing.T) {
	rendered, err := renderTemplate("match ({{name}}) return {{name}}", map[string]string{"name": "n"})
	require.NoError(t, err)
	require.Equal(t, "match (n) return n", rendered)

	_, err = renderTemplate("match ({{name}}) return n", nil)
	require.ErrorContains(t, err, "unresolved placeholders")
}

func TestMergeParams(t *testing.T) {
	merged := mergeParams(map[string]any{"a": 1, "b": 2}, map[string]any{"b": 3})
	require.Equal(t, map[string]any{"a": 1, "b": 3}, merged)
	require.Nil(t, mergeParams(nil, nil))
}
