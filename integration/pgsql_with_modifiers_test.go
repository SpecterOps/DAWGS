//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLWithModifierCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "with_modifier_shapes")
}
