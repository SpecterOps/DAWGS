//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLReusedRelationshipCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "reused_relationship_shapes")
}
