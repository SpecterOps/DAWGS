//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLJSONResultStringCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "json_result_string_shapes")
}
