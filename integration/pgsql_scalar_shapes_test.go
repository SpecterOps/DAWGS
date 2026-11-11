//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLScalarShapeCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "scalar_shapes")
}
