//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLWithPropertyConsumersCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "with_property_consumer_shapes")
}
