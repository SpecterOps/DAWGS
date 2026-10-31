//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLPropertyArithmeticCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "property_arithmetic_shapes")
}
