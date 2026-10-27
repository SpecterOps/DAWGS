//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLPredicateTypesCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "predicate_type_shapes")
}
