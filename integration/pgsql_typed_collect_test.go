//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLTypedCollectCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "typed_collect_shapes")
}
