//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLRawRewriteSemanticsCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "raw_rewrite_semantics_shapes")
}
