//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLTemporalParameterCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "temporal_parameter_shapes")
}
