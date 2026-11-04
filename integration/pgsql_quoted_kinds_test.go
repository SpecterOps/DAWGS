//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLQuotedKindsCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "quoted_kind_shapes")
}
