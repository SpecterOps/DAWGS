//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLUndirectedContinuationCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "undirected_continuation_shapes")
}
