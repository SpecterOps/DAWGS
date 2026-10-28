//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLLeadingOptionalCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "leading_optional_shapes")
}
