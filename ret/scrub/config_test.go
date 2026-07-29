package scrub

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfigMatchesLegacyPolicy(t *testing.T) {
	config := DefaultConfig()

	require.True(t, config.Enabled)
	require.Equal(t, "example.invalid", config.Rules.FakeDomain)
	require.Equal(t, 17, config.Rules.TimestampShiftDays)
	require.Equal(t, "[REDACTED]", config.Rules.RedactionMarker)
	require.NoError(t, config.Validate())
}

func TestReadConfigPreservesLegacyFileShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retriever.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[scrub]
enabled = true
salt = "file-salt"
fake_domain = "scrub.example"
redaction_marker = "[X]"

[classifier]
long_text_threshold = 8
`), 0o600))

	config, err := ReadConfig(path)
	require.NoError(t, err)
	require.True(t, config.Enabled)
	require.Equal(t, "file-salt", config.Salt)
	require.Equal(t, "scrub.example", config.Rules.FakeDomain)
	require.Equal(t, "[X]", config.Rules.RedactionMarker)
	require.Equal(t, 8, config.Rules.Classifier.LongTextThreshold)
}

func TestConfigValidateRejectsInvalidValueShapePattern(t *testing.T) {
	config := DefaultConfig()
	config.Rules.Classifier.ValueShapePatterns = []ValueShapeConfig{{
		Name:    "invalid",
		Pattern: "[",
	}}

	require.Error(t, config.Validate())
}
