//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLOrderAliasCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "order_alias_shapes")
}

func TestPostgreSQLNodeOrderByCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "order_by_shapes")
}
