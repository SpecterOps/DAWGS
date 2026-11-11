//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLBoundedShortestEndpointCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "bounded_shortest_endpoint_shapes")
}
