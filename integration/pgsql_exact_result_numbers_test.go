//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLExactResultNumberCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "exact_result_number_shapes")
}
