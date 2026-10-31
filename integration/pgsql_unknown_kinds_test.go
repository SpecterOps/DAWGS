//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLUnknownKindsCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "unknown_kind_shapes")
}
