//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLToIntegerCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "to_integer_shapes")
}
