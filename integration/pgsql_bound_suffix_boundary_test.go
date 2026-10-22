//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLBoundSuffixBoundarySemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "bound_suffix_boundary_shapes")
}
