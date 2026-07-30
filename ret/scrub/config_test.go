package scrub

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfigMatchesLegacyPolicy(t *testing.T) {
	require.Equal(t, legacyDefaultConfig(), DefaultConfig())
	require.NoError(t, DefaultConfig().Validate())
}

func TestDefaultConfigReturnsIndependentMutableValues(t *testing.T) {
	first := DefaultConfig()
	second := DefaultConfig()

	first.Rules.GraphRules.DomainSIDReferenceKeys[0] = "changed"
	first.Rules.GraphRules.ObjectIDReferenceKeys[0] = "changed"
	first.Rules.GraphRules.SelfObjectIDAliasKeys[0] = "changed"
	first.Rules.GraphRules.DomainNameReferenceKeys[0] = "changed"
	first.Rules.Classifier.PreserveKeys[0] = "changed"
	first.Rules.Classifier.SensitiveKeyMarks[0] = "changed"
	first.Rules.Classifier.ValueShapePatterns[0].Name = "changed"

	require.Equal(t, legacyDefaultConfig(), second)
}

func TestExampleConfigMatchesDefaultPolicy(t *testing.T) {
	config, err := ReadConfig("example.toml")

	require.NoError(t, err)
	config.Salt = ""
	require.Equal(t, DefaultConfig(), config)
}

func TestReadConfigDecodesDirectPolicyShapeAndKeepsSaltRuntimeOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retriever.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
salt = "file-salt"
fake_domain = "scrub.example"
redaction_marker = "[X]"

[graph_rules]
domain_kind = "CustomDomain"

[classifier]
long_text_threshold = 8
`), 0o600))

	config, err := ReadConfig(path)
	require.NoError(t, err)
	require.Empty(t, config.Salt)
	require.Equal(t, "scrub.example", config.Rules.FakeDomain)
	require.Equal(t, "[X]", config.Rules.RedactionMarker)
	require.Equal(t, "CustomDomain", config.Rules.GraphRules.DomainKind)
	require.Equal(t, 8, config.Rules.Classifier.LongTextThreshold)
	require.Equal(t, legacyDefaultConfig().Rules.GraphRules.ObjectIDReferenceKeys, config.Rules.GraphRules.ObjectIDReferenceKeys)
}

func TestConfigValidateRejectsInvalidValueShapePattern(t *testing.T) {
	config := DefaultConfig()
	config.Rules.Classifier.ValueShapePatterns = []ValueShapeConfig{{
		Name:    "invalid",
		Pattern: "[",
	}}

	require.EqualError(t, config.Validate(), "compile value shape \"invalid\": error parsing regexp: missing closing ]: `[`")
}

func TestConfigValidationRejectsIncompleteOrNoncanonicalRules(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{
			name: "empty fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = ""
			},
			want: "fake domain must be non-empty",
		},
		{
			name: "blank fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = " \t"
			},
			want: "fake domain must be non-empty",
		},
		{
			name: "untrimmed fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = " example.invalid "
			},
			want: "fake domain must be trimmed",
		},
		{
			name: "uppercase fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = "Example.Invalid"
			},
			want: "fake domain must be lowercase",
		},
		{
			name: "leading dot in fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = ".example.invalid"
			},
			want: "fake domain must not have a leading or trailing dot",
		},
		{
			name: "trailing dot in fake domain",
			change: func(config *Config) {
				config.Rules.FakeDomain = "example.invalid."
			},
			want: "fake domain must not have a leading or trailing dot",
		},
		{
			name: "empty redaction marker",
			change: func(config *Config) {
				config.Rules.RedactionMarker = ""
			},
			want: "redaction marker must be non-empty",
		},
		{
			name: "blank redaction marker",
			change: func(config *Config) {
				config.Rules.RedactionMarker = " \t"
			},
			want: "redaction marker must be non-empty",
		},
		{
			name: "untrimmed redaction marker",
			change: func(config *Config) {
				config.Rules.RedactionMarker = " [REDACTED] "
			},
			want: "redaction marker must be trimmed",
		},
		{
			name: "zero long text threshold",
			change: func(config *Config) {
				config.Rules.Classifier.LongTextThreshold = 0
			},
			want: "long text threshold must be greater than zero",
		},
		{
			name: "negative long text threshold",
			change: func(config *Config) {
				config.Rules.Classifier.LongTextThreshold = -1
			},
			want: "long text threshold must be greater than zero",
		},
		{
			name: "zero timestamp shift",
			change: func(config *Config) {
				config.Rules.TimestampShiftDays = 0
			},
			want: "timestamp shift days must be non-zero",
		},
		{
			name: "missing value shape name",
			change: func(config *Config) {
				config.Rules.Classifier.ValueShapePatterns = []ValueShapeConfig{{
					Pattern: "^value$",
				}}
			},
			want: "value shape 0 name must be non-empty",
		},
		{
			name: "missing value shape pattern",
			change: func(config *Config) {
				config.Rules.Classifier.ValueShapePatterns = []ValueShapeConfig{{
					Name: "custom",
				}}
			},
			want: "value shape \"custom\" pattern must be non-empty",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			config := DefaultConfig()
			testCase.change(&config)

			require.EqualError(t, config.Validate(), testCase.want)
			scrubber, err := New(config)
			require.Nil(t, scrubber)
			require.EqualError(t, err, testCase.want)
		})
	}
}

func TestConfigValidateAllowsNoValueShapePatterns(t *testing.T) {
	config := DefaultConfig()
	config.Rules.Classifier.ValueShapePatterns = nil

	require.NoError(t, config.Validate())
}

func legacyDefaultConfig() Config {
	return Config{
		Rules: Rules{
			FakeDomain:         "example.invalid",
			TimestampShiftDays: 17,
			RedactionMarker:    "[REDACTED]",
			GraphRules: GraphRulesConfig{
				DomainKind:                  "Domain",
				ObjectIDKey:                 "objectid",
				DomainNameKey:               "domain",
				DomainSIDReferenceKeys:      []string{"domainsid", "domain_sid"},
				ObjectIDReferenceKeys:       []string{"objectid", "object_id", "sid", "owner_sid", "primarygroupid"},
				SelfObjectIDAliasKeys:       []string{"objectsid"},
				DomainNameReferenceKeys:     []string{"domain", "domain_name"},
				CaseInsensitiveDomainNames:  true,
				PreserveADSIDDomainPrefixes: true,
			},
			Classifier: ClassifierConfig{
				LongTextThreshold: 512,
				PreserveKeys:      []string{"objectid", "domainsid", "kind"},
				SensitiveKeyMarks: []string{
					"password",
					"secret",
					"token",
					"credential",
					"privatekey",
					"private_key",
					"apikey",
					"api_key",
					"email",
					"mail",
					"phone",
					"address",
					"name",
					"displayname",
					"samaccountname",
					"userprincipalname",
					"dns",
					"hostname",
				},
				ValueShapePatterns: []ValueShapeConfig{
					{Name: "email", Pattern: `(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`},
					{Name: "uuid", Pattern: `(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`},
					{Name: "domain_sid", Pattern: `^S-1-5-21-\d+-\d+-\d+$`},
					{Name: "object_sid", Pattern: `^(S-1-5-21-\d+-\d+-\d+)-(\d+)$`},
					{Name: "ipv4", Pattern: `^(\d{1,3}\.){3}\d{1,3}$`},
					{Name: "host", Pattern: `(?i)^[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+$`},
				},
			},
		},
	}
}
