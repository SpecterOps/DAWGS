// Package scrub applies the retriever's deterministic property-scrubbing policy.
package scrub

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config configures a Scrubber.
type Config struct {
	// Salt is runtime-only and is not decoded from or encoded into config files.
	Salt string `toml:"-" json:"-"`
	Rules
}

// Rules contains the policy rules applied while scrubbing properties.
type Rules struct {
	FakeDomain         string           `toml:"fake_domain"`
	TimestampShiftDays int              `toml:"timestamp_shift_days"`
	RedactionMarker    string           `toml:"redaction_marker"`
	GraphRules         GraphRulesConfig `toml:"graph_rules"`
	Classifier         ClassifierConfig `toml:"classifier"`
}

// GraphRulesConfig controls the reference-key portion of the policy.
type GraphRulesConfig struct {
	DomainKind                  string   `toml:"domain_kind"`
	ObjectIDKey                 string   `toml:"objectid_key"`
	DomainNameKey               string   `toml:"domain_name_key"`
	DomainSIDReferenceKeys      []string `toml:"domain_sid_reference_keys"`
	ObjectIDReferenceKeys       []string `toml:"objectid_reference_keys"`
	SelfObjectIDAliasKeys       []string `toml:"self_objectid_alias_keys"`
	DomainNameReferenceKeys     []string `toml:"domain_name_reference_keys"`
	CaseInsensitiveDomainNames  bool     `toml:"case_insensitive_domain_names"`
	PreserveADSIDDomainPrefixes bool     `toml:"preserve_ad_sid_domain_prefixes"`
}

// ClassifierConfig controls property and value classification.
type ClassifierConfig struct {
	LongTextThreshold  int                `toml:"long_text_threshold"`
	PreserveKeys       []string           `toml:"preserve_keys"`
	SensitiveKeyMarks  []string           `toml:"sensitive_key_markers"`
	ValueShapePatterns []ValueShapeConfig `toml:"value_shapes"`
}

// ValueShapeConfig identifies a named regular-expression value shape.
type ValueShapeConfig struct {
	Name    string `toml:"name"`
	Pattern string `toml:"pattern"`
}

// DefaultConfig returns the legacy scrub policy.
func DefaultConfig() Config {
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

// ReadConfig reads a TOML scrub policy. Missing values retain the defaults.
func ReadConfig(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read scrub config: %w", err)
	}

	config := DefaultConfig()
	if err := toml.Unmarshal(contents, &config); err != nil {
		return Config{}, fmt.Errorf("parse scrub config: %w", err)
	}

	return config, config.Validate()
}

// Validate verifies that the scrub policy is complete, canonical, and compilable.
func (s Config) Validate() error {
	if strings.TrimSpace(s.Rules.FakeDomain) == "" {
		return fmt.Errorf("fake domain must be non-empty")
	}
	if strings.TrimSpace(s.Rules.FakeDomain) != s.Rules.FakeDomain {
		return fmt.Errorf("fake domain must be trimmed")
	}
	if strings.ToLower(s.Rules.FakeDomain) != s.Rules.FakeDomain {
		return fmt.Errorf("fake domain must be lowercase")
	}
	if strings.HasPrefix(s.Rules.FakeDomain, ".") || strings.HasSuffix(s.Rules.FakeDomain, ".") {
		return fmt.Errorf("fake domain must not have a leading or trailing dot")
	}
	if strings.TrimSpace(s.Rules.RedactionMarker) == "" {
		return fmt.Errorf("redaction marker must be non-empty")
	}
	if strings.TrimSpace(s.Rules.RedactionMarker) != s.Rules.RedactionMarker {
		return fmt.Errorf("redaction marker must be trimmed")
	}
	if s.Rules.Classifier.LongTextThreshold <= 0 {
		return fmt.Errorf("long text threshold must be greater than zero")
	}
	if s.Rules.TimestampShiftDays == 0 {
		return fmt.Errorf("timestamp shift days must be non-zero")
	}

	for index, shape := range s.Rules.Classifier.ValueShapePatterns {
		if strings.TrimSpace(shape.Name) == "" {
			return fmt.Errorf("value shape %d name must be non-empty", index)
		}
		if strings.TrimSpace(shape.Pattern) == "" {
			return fmt.Errorf("value shape %q pattern must be non-empty", shape.Name)
		}
		if _, err := regexp.Compile(shape.Pattern); err != nil {
			return fmt.Errorf("compile value shape %q: %w", shape.Name, err)
		}
	}

	return nil
}

func cloneRules(rules Rules) Rules {
	return Rules{
		FakeDomain:         rules.FakeDomain,
		TimestampShiftDays: rules.TimestampShiftDays,
		RedactionMarker:    rules.RedactionMarker,
		GraphRules:         cloneGraphRules(rules.GraphRules),
		Classifier:         cloneClassifier(rules.Classifier),
	}
}

func cloneGraphRules(rules GraphRulesConfig) GraphRulesConfig {
	rules.DomainSIDReferenceKeys = append([]string(nil), rules.DomainSIDReferenceKeys...)
	rules.ObjectIDReferenceKeys = append([]string(nil), rules.ObjectIDReferenceKeys...)
	rules.SelfObjectIDAliasKeys = append([]string(nil), rules.SelfObjectIDAliasKeys...)
	rules.DomainNameReferenceKeys = append([]string(nil), rules.DomainNameReferenceKeys...)
	return rules
}

func cloneClassifier(classifier ClassifierConfig) ClassifierConfig {
	classifier.PreserveKeys = append([]string(nil), classifier.PreserveKeys...)
	classifier.SensitiveKeyMarks = append([]string(nil), classifier.SensitiveKeyMarks...)
	classifier.ValueShapePatterns = append([]ValueShapeConfig(nil), classifier.ValueShapePatterns...)
	return classifier
}
