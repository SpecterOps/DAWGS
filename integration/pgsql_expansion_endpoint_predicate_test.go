//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLExpansionEndpointPredicateCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "expansion_endpoint_predicate_shapes")
}
