//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLUpdateRowStreamCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "update_row_stream_shapes")
}
