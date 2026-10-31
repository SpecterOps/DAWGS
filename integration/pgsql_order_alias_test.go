//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLOrderAliasCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "order_alias_shapes")
}
