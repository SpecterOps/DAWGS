//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLUpdateClauseOrderCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "update_clause_order_shapes")
}
