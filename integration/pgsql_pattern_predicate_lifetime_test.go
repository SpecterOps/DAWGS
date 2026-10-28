//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLPatternPredicateLifetimeCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "pattern_predicate_lifetime_shapes")
}
