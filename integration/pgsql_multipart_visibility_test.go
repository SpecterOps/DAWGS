//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLMultipartWriteVisibilitySemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "multipart_write_visibility_shapes")
}
