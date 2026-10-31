//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLUndirectedSelfReferenceCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "undirected_self_reference_shapes")
}
