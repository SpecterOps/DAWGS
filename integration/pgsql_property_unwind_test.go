//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLPropertyUnwindCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "property_unwind_shapes")
}
