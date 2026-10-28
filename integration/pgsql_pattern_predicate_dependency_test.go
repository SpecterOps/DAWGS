//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLPatternPredicateDependencyCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "pattern_predicate_dependency_shapes")
}
