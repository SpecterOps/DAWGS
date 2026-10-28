//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLCreatePropertyTypesCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "create_property_type_shapes")
}
