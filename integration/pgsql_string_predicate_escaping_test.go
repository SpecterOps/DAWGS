//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLStringPredicateEscapingCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "string_predicate_escaping_shapes")
}
