//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLOptionalRowIdentityCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "optional_row_identity_shapes")
}
