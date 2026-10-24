//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLExactExpansionLengthCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "exact_expansion_length_shapes")
}
