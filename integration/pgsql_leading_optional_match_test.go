//go:build manual_integration

package integration

import "testing"

func TestPostgreSQLLeadingOptionalMatchCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "leading_optional_match_shapes")
}
