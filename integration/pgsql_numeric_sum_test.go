//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLNumericSumCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "numeric_sum_shapes")
}
