// Package scrub applies the retriever's deterministic property-scrubbing policy.
package scrub

import (
	_ "embed"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config configures a Scrubber.
type Config struct {
	Enabled bool
	Salt    string
	Rules   Rules
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

//go:embed defaults.toml
var defaultConfigTOML []byte

// DefaultConfig returns the legacy scrub policy with scrubbing enabled.
func DefaultConfig() Config {
	config, err := decodeConfig(defaultConfigTOML, Config{Enabled: true})
	if err != nil {
		panic(fmt.Sprintf("parse embedded scrub defaults: %v", err))
	}

	return config
}

// ReadConfig reads a TOML configuration using the legacy [scrub] and
// [classifier] sections. Missing values retain the default policy values.
func ReadConfig(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read scrub config: %w", err)
	}

	config, err := decodeConfig(contents, DefaultConfig())
	if err != nil {
		return Config{}, fmt.Errorf("parse scrub config: %w", err)
	}

	return config, config.Validate()
}

// Validate verifies that configured value-shape patterns can be compiled.
func (s Config) Validate() error {
	for _, shape := range s.Rules.Classifier.ValueShapePatterns {
		if strings.TrimSpace(shape.Name) == "" || strings.TrimSpace(shape.Pattern) == "" {
			continue
		}
		if _, err := regexp.Compile(shape.Pattern); err != nil {
			return fmt.Errorf("compile value shape %q: %w", shape.Name, err)
		}
	}

	return nil
}

type configFile struct {
	Scrub struct {
		Enabled            bool             `toml:"enabled"`
		Salt               string           `toml:"salt"`
		FakeDomain         string           `toml:"fake_domain"`
		TimestampShiftDays int              `toml:"timestamp_shift_days"`
		RedactionMarker    string           `toml:"redaction_marker"`
		GraphRules         GraphRulesConfig `toml:"graph_rules"`
	} `toml:"scrub"`
	Classifier ClassifierConfig `toml:"classifier"`
}

func decodeConfig(contents []byte, base Config) (Config, error) {
	file := configFile{}
	file.Scrub.Enabled = base.Enabled
	file.Scrub.Salt = base.Salt
	file.Scrub.FakeDomain = base.Rules.FakeDomain
	file.Scrub.TimestampShiftDays = base.Rules.TimestampShiftDays
	file.Scrub.RedactionMarker = base.Rules.RedactionMarker
	file.Scrub.GraphRules = cloneGraphRules(base.Rules.GraphRules)
	file.Classifier = cloneClassifier(base.Rules.Classifier)
	if err := toml.Unmarshal(contents, &file); err != nil {
		return Config{}, err
	}

	return Config{
		Enabled: file.Scrub.Enabled,
		Salt:    file.Scrub.Salt,
		Rules: Rules{
			FakeDomain:         file.Scrub.FakeDomain,
			TimestampShiftDays: file.Scrub.TimestampShiftDays,
			RedactionMarker:    file.Scrub.RedactionMarker,
			GraphRules:         file.Scrub.GraphRules,
			Classifier:         file.Classifier,
		},
	}, nil
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
