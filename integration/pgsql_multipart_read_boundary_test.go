//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLMultipartReadBoundaryCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "multipart_read_boundary_shapes")
}
