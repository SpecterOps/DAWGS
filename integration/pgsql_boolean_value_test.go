//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLBooleanValueCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "boolean_value_shapes")
}
