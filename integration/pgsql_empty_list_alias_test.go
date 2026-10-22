//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLEmptyListAliasSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "empty_list_alias_shapes")
}
