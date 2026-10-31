//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLOpenTraversalCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "open_traversal_shapes")
}
