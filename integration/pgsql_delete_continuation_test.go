//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLDeleteContinuationCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "delete_continuation_shapes")
}
