package main

import (
	_ "embed"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

//go:embed defaults.toml
var defaultConfigTOML string

type Config struct {
	Database    DatabaseConfig    `toml:"database"`
	Exploration ExplorationConfig `toml:"exploration"`
	OpenGraph   OpenGraphConfig   `toml:"opengraph"`
	Scrub       ScrubConfig       `toml:"scrub"`
	Classifier  ClassifierConfig  `toml:"classifier"`
}

type DatabaseConfig struct {
	Driver     string `toml:"driver"`
	Connection string `toml:"connection"`
	Graph      string `toml:"graph"`
	AllGraphs  bool   `toml:"all_graphs"`
}

type ExplorationConfig struct {
	NodeSampleLimit         int `toml:"node_sample_limit"`
	RelationshipSampleLimit int `toml:"relationship_sample_limit"`
	HistogramLimit          int `toml:"histogram_limit"`
}

type OpenGraphConfig struct {
	Input    string `toml:"input"`
	Output   string `toml:"output"`
	Manifest string `toml:"manifest"`
	Plan     string `toml:"plan"`
}

type ScrubConfig struct {
	Mode               string           `toml:"mode"`
	Salt               string           `toml:"salt"`
	FakeDomain         string           `toml:"fake_domain"`
	TimestampShiftDays int              `toml:"timestamp_shift_days"`
	RedactionMarker    string           `toml:"redaction_marker"`
	GraphRules         GraphRulesConfig `toml:"graph_rules"`
}

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

type ClassifierConfig struct {
	LongTextThreshold             int                `toml:"long_text_threshold"`
	MaxDepth                      int                `toml:"max_depth"`
	IdentifierMinimumSamples      int                `toml:"identifier_minimum_samples"`
	IdentifierUniquenessThreshold float64            `toml:"identifier_uniqueness_threshold"`
	IdentifierMinimumAverageLen   float64            `toml:"identifier_minimum_average_length"`
	PreserveKeys                  []string           `toml:"preserve_keys"`
	SensitiveKeyMarks             []string           `toml:"sensitive_key_markers"`
	ValueShapePatterns            []ValueShapeConfig `toml:"value_shapes"`
}

type ValueShapeConfig struct {
	Name    string `toml:"name"`
	Pattern string `toml:"pattern"`
}

type rawConfig struct {
	Database    rawDatabaseConfig    `toml:"database"`
	Exploration rawExplorationConfig `toml:"exploration"`
	OpenGraph   rawOpenGraphConfig   `toml:"opengraph"`
	Scrub       rawScrubConfig       `toml:"scrub"`
	Classifier  rawClassifierConfig  `toml:"classifier"`
}

type rawDatabaseConfig struct {
	Driver     *string `toml:"driver"`
	Connection *string `toml:"connection"`
	Graph      *string `toml:"graph"`
	AllGraphs  *bool   `toml:"all_graphs"`
}

type rawExplorationConfig struct {
	NodeSampleLimit         *int `toml:"node_sample_limit"`
	RelationshipSampleLimit *int `toml:"relationship_sample_limit"`
	HistogramLimit          *int `toml:"histogram_limit"`
}

type rawOpenGraphConfig struct {
	Input    *string `toml:"input"`
	Output   *string `toml:"output"`
	Manifest *string `toml:"manifest"`
	Plan     *string `toml:"plan"`
}

type rawScrubConfig struct {
	Mode               *string             `toml:"mode"`
	Salt               *string             `toml:"salt"`
	FakeDomain         *string             `toml:"fake_domain"`
	TimestampShiftDays *int                `toml:"timestamp_shift_days"`
	RedactionMarker    *string             `toml:"redaction_marker"`
	GraphRules         rawGraphRulesConfig `toml:"graph_rules"`
}

type rawGraphRulesConfig struct {
	DomainKind                  *string  `toml:"domain_kind"`
	ObjectIDKey                 *string  `toml:"objectid_key"`
	DomainNameKey               *string  `toml:"domain_name_key"`
	DomainSIDReferenceKeys      []string `toml:"domain_sid_reference_keys"`
	ObjectIDReferenceKeys       []string `toml:"objectid_reference_keys"`
	SelfObjectIDAliasKeys       []string `toml:"self_objectid_alias_keys"`
	DomainNameReferenceKeys     []string `toml:"domain_name_reference_keys"`
	CaseInsensitiveDomainNames  *bool    `toml:"case_insensitive_domain_names"`
	PreserveADSIDDomainPrefixes *bool    `toml:"preserve_ad_sid_domain_prefixes"`
}

type rawClassifierConfig struct {
	LongTextThreshold             *int               `toml:"long_text_threshold"`
	MaxDepth                      *int               `toml:"max_depth"`
	IdentifierMinimumSamples      *int               `toml:"identifier_minimum_samples"`
	IdentifierUniquenessThreshold *float64           `toml:"identifier_uniqueness_threshold"`
	IdentifierMinimumAverageLen   *float64           `toml:"identifier_minimum_average_length"`
	PreserveKeys                  []string           `toml:"preserve_keys"`
	SensitiveKeyMarks             []string           `toml:"sensitive_key_markers"`
	ValueShapePatterns            []ValueShapeConfig `toml:"value_shapes"`
}

type Classifier struct {
	longTextThreshold             int
	maxDepth                      int
	identifierMinimumSamples      int
	identifierUniquenessThreshold float64
	identifierMinimumAverageLen   float64
	preserveKeys                  map[string]struct{}
	sensitiveKeyMarks             []string
	valueShapeRules               []valueShapeRule
}

type valueShapeRule struct {
	name    string
	pattern *regexp.Regexp
}

type ClassificationResult struct {
	Shapes  []string
	Reasons []string
}

func LoadConfig(path string) (Config, error) {
	var cfg Config

	defaults, err := parseRawConfig("embedded defaults.toml", []byte(defaultConfigTOML))
	if err != nil {
		return cfg, err
	}
	applyRawConfig(&cfg, defaults)

	if strings.TrimSpace(path) != "" {
		contents, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read graph scrubber config: %w", err)
		}

		override, err := parseRawConfig(path, contents)
		if err != nil {
			return cfg, err
		}
		applyRawConfig(&cfg, override)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func parseRawConfig(source string, contents []byte) (rawConfig, error) {
	var cfg rawConfig
	if err := toml.Unmarshal(contents, &cfg); err != nil {
		return cfg, fmt.Errorf("parse graph scrubber config %q: %w", source, err)
	}

	return cfg, nil
}

func applyRawConfig(target *Config, source rawConfig) {
	if source.Database.Driver != nil {
		target.Database.Driver = strings.TrimSpace(*source.Database.Driver)
	}
	if source.Database.Connection != nil {
		target.Database.Connection = strings.TrimSpace(*source.Database.Connection)
	}
	if source.Database.Graph != nil {
		target.Database.Graph = strings.TrimSpace(*source.Database.Graph)
		if target.Database.Graph != "" {
			target.Database.AllGraphs = false
		}
	}
	if source.Database.AllGraphs != nil {
		target.Database.AllGraphs = *source.Database.AllGraphs
	}

	if source.Exploration.NodeSampleLimit != nil {
		target.Exploration.NodeSampleLimit = *source.Exploration.NodeSampleLimit
	}
	if source.Exploration.RelationshipSampleLimit != nil {
		target.Exploration.RelationshipSampleLimit = *source.Exploration.RelationshipSampleLimit
	}
	if source.Exploration.HistogramLimit != nil {
		target.Exploration.HistogramLimit = *source.Exploration.HistogramLimit
	}

	if source.OpenGraph.Input != nil {
		target.OpenGraph.Input = strings.TrimSpace(*source.OpenGraph.Input)
	}
	if source.OpenGraph.Output != nil {
		target.OpenGraph.Output = strings.TrimSpace(*source.OpenGraph.Output)
	}
	if source.OpenGraph.Manifest != nil {
		target.OpenGraph.Manifest = strings.TrimSpace(*source.OpenGraph.Manifest)
	}
	if source.OpenGraph.Plan != nil {
		target.OpenGraph.Plan = strings.TrimSpace(*source.OpenGraph.Plan)
	}

	if source.Scrub.Mode != nil {
		target.Scrub.Mode = strings.TrimSpace(*source.Scrub.Mode)
	}
	if source.Scrub.Salt != nil {
		target.Scrub.Salt = strings.TrimSpace(*source.Scrub.Salt)
	}
	if source.Scrub.FakeDomain != nil {
		target.Scrub.FakeDomain = strings.Trim(strings.ToLower(strings.TrimSpace(*source.Scrub.FakeDomain)), ".")
	}
	if source.Scrub.TimestampShiftDays != nil {
		target.Scrub.TimestampShiftDays = *source.Scrub.TimestampShiftDays
	}
	if source.Scrub.RedactionMarker != nil {
		target.Scrub.RedactionMarker = strings.TrimSpace(*source.Scrub.RedactionMarker)
	}
	applyRawGraphRulesConfig(&target.Scrub.GraphRules, source.Scrub.GraphRules)

	if source.Classifier.LongTextThreshold != nil {
		target.Classifier.LongTextThreshold = *source.Classifier.LongTextThreshold
	}
	if source.Classifier.MaxDepth != nil {
		target.Classifier.MaxDepth = *source.Classifier.MaxDepth
	}
	if source.Classifier.IdentifierMinimumSamples != nil {
		target.Classifier.IdentifierMinimumSamples = *source.Classifier.IdentifierMinimumSamples
	}
	if source.Classifier.IdentifierUniquenessThreshold != nil {
		target.Classifier.IdentifierUniquenessThreshold = *source.Classifier.IdentifierUniquenessThreshold
	}
	if source.Classifier.IdentifierMinimumAverageLen != nil {
		target.Classifier.IdentifierMinimumAverageLen = *source.Classifier.IdentifierMinimumAverageLen
	}
	if source.Classifier.PreserveKeys != nil {
		target.Classifier.PreserveKeys = source.Classifier.PreserveKeys
	}
	if source.Classifier.SensitiveKeyMarks != nil {
		target.Classifier.SensitiveKeyMarks = source.Classifier.SensitiveKeyMarks
	}
	if source.Classifier.ValueShapePatterns != nil {
		target.Classifier.ValueShapePatterns = source.Classifier.ValueShapePatterns
	}
}

func (s Config) Validate() error {
	if s.Exploration.NodeSampleLimit < 0 {
		return fmt.Errorf("node_sample_limit must be >= 0")
	}
	if s.Exploration.RelationshipSampleLimit < 0 {
		return fmt.Errorf("relationship_sample_limit must be >= 0")
	}
	if s.Exploration.HistogramLimit < 0 {
		return fmt.Errorf("histogram_limit must be >= 0")
	}

	switch ScrubMode(strings.TrimSpace(s.Scrub.Mode)) {
	case ModeShapeOnly, ModeTradecraft, ModeResearchStable:
	case "":
		return fmt.Errorf("scrub mode is required")
	default:
		return fmt.Errorf("unsupported scrub mode %q", s.Scrub.Mode)
	}

	if strings.TrimSpace(s.Scrub.FakeDomain) == "" {
		return fmt.Errorf("fake_domain is required")
	}

	if strings.TrimSpace(s.Scrub.RedactionMarker) == "" {
		return fmt.Errorf("redaction_marker is required")
	}
	if strings.TrimSpace(s.Scrub.GraphRules.DomainKind) == "" {
		return fmt.Errorf("scrub.graph_rules.domain_kind is required")
	}
	if strings.TrimSpace(s.Scrub.GraphRules.ObjectIDKey) == "" {
		return fmt.Errorf("scrub.graph_rules.objectid_key is required")
	}
	if strings.TrimSpace(s.Scrub.GraphRules.DomainNameKey) == "" {
		return fmt.Errorf("scrub.graph_rules.domain_name_key is required")
	}

	if _, err := s.Classifier.Compile(); err != nil {
		return err
	}

	return nil
}

func applyRawGraphRulesConfig(target *GraphRulesConfig, source rawGraphRulesConfig) {
	if source.DomainKind != nil {
		target.DomainKind = strings.TrimSpace(*source.DomainKind)
	}
	if source.ObjectIDKey != nil {
		target.ObjectIDKey = strings.TrimSpace(*source.ObjectIDKey)
	}
	if source.DomainNameKey != nil {
		target.DomainNameKey = strings.TrimSpace(*source.DomainNameKey)
	}
	if source.DomainSIDReferenceKeys != nil {
		target.DomainSIDReferenceKeys = trimStringSlice(source.DomainSIDReferenceKeys)
	}
	if source.ObjectIDReferenceKeys != nil {
		target.ObjectIDReferenceKeys = trimStringSlice(source.ObjectIDReferenceKeys)
	}
	if source.SelfObjectIDAliasKeys != nil {
		target.SelfObjectIDAliasKeys = trimStringSlice(source.SelfObjectIDAliasKeys)
	}
	if source.DomainNameReferenceKeys != nil {
		target.DomainNameReferenceKeys = trimStringSlice(source.DomainNameReferenceKeys)
	}
	if source.CaseInsensitiveDomainNames != nil {
		target.CaseInsensitiveDomainNames = *source.CaseInsensitiveDomainNames
	}
	if source.PreserveADSIDDomainPrefixes != nil {
		target.PreserveADSIDDomainPrefixes = *source.PreserveADSIDDomainPrefixes
	}
}

func trimStringSlice(values []string) []string {
	trimmedValues := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			trimmedValues = append(trimmedValues, trimmed)
		}
	}
	return trimmedValues
}

func (s ClassifierConfig) Compile() (Classifier, error) {
	classifier := Classifier{
		longTextThreshold:             s.LongTextThreshold,
		maxDepth:                      s.MaxDepth,
		identifierMinimumSamples:      s.IdentifierMinimumSamples,
		identifierUniquenessThreshold: s.IdentifierUniquenessThreshold,
		identifierMinimumAverageLen:   s.IdentifierMinimumAverageLen,
		preserveKeys:                  map[string]struct{}{},
	}

	if classifier.longTextThreshold < 0 {
		return classifier, fmt.Errorf("long_text_threshold must be >= 0")
	}
	if classifier.maxDepth < 0 {
		return classifier, fmt.Errorf("max_depth must be >= 0")
	}
	if classifier.identifierMinimumSamples < 0 {
		return classifier, fmt.Errorf("identifier_minimum_samples must be >= 0")
	}
	if classifier.identifierUniquenessThreshold < 0 || classifier.identifierUniquenessThreshold > 1 {
		return classifier, fmt.Errorf("identifier_uniqueness_threshold must be between 0 and 1")
	}
	if classifier.identifierMinimumAverageLen < 0 {
		return classifier, fmt.Errorf("identifier_minimum_average_length must be >= 0")
	}

	for _, key := range s.PreserveKeys {
		if normalized := NormalizeSensitiveKey(key); normalized != "" {
			classifier.preserveKeys[normalized] = struct{}{}
		}
	}

	for _, marker := range s.SensitiveKeyMarks {
		if normalized := NormalizeSensitiveKey(marker); normalized != "" {
			classifier.sensitiveKeyMarks = append(classifier.sensitiveKeyMarks, normalized)
		}
	}

	for _, shape := range s.ValueShapePatterns {
		name := strings.TrimSpace(shape.Name)
		if name == "" {
			return classifier, fmt.Errorf("value shape name is required")
		}
		if strings.TrimSpace(shape.Pattern) == "" {
			return classifier, fmt.Errorf("value shape %q pattern is required", name)
		}

		compiledPattern, err := regexp.Compile(shape.Pattern)
		if err != nil {
			return classifier, fmt.Errorf("compile value shape %q: %w", name, err)
		}

		classifier.valueShapeRules = append(classifier.valueShapeRules, valueShapeRule{
			name:    name,
			pattern: compiledPattern,
		})
	}

	return classifier, nil
}

func (s Classifier) ClassifyProperty(key string, value any) ClassificationResult {
	if s.PreservedKey(key) {
		return ClassificationResult{
			Reasons: []string{"preserve_key"},
		}
	}

	result := s.classify(value, 0)
	if s.SensitiveKey(key) {
		result.Reasons = append(result.Reasons, "key_marker")
	}

	return result
}

func (s Classifier) ClassifyValue(value any) []string {
	return s.classify(value, 0).Shapes
}

func (s Classifier) classify(value any, depth int) ClassificationResult {
	if value == nil || depth > s.maxDepth {
		return ClassificationResult{}
	}

	switch typedValue := value.(type) {
	case string:
		return s.classifyString(typedValue)
	case []string:
		values := make([]any, 0, len(typedValue))
		for _, value := range typedValue {
			values = append(values, value)
		}
		return s.classifySlice(values, depth)
	case []any:
		return s.classifySlice(typedValue, depth)
	case map[string]any:
		return s.classifyMap(typedValue, depth)
	default:
		return ClassificationResult{}
	}
}

func (s Classifier) classifyString(value string) ClassificationResult {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ClassificationResult{}
	}

	result := ClassificationResult{
		Shapes: make([]string, 0, len(s.valueShapeRules)+1),
	}

	for _, rule := range s.valueShapeRules {
		if rule.pattern.MatchString(trimmed) {
			result.Shapes = append(result.Shapes, rule.name)
			result.Reasons = append(result.Reasons, "value_shape:"+rule.name)
		}
	}

	if s.longTextThreshold > 0 && len(trimmed) > s.longTextThreshold {
		result.Shapes = append(result.Shapes, "long_text")
		result.Reasons = append(result.Reasons, "value_shape:long_text")
	}

	return result
}

func (s Classifier) classifySlice(values []any, depth int) ClassificationResult {
	var result ClassificationResult
	for _, value := range values {
		nextResult := s.classify(value, depth+1)
		if len(nextResult.Shapes) > 0 || len(nextResult.Reasons) > 0 {
			nextResult.Reasons = append(nextResult.Reasons, "nested_value")
		}
		result.Merge(nextResult)
	}

	return result
}

func (s Classifier) classifyMap(values map[string]any, depth int) ClassificationResult {
	var result ClassificationResult
	for key, value := range values {
		if s.SensitiveKey(key) {
			result.Reasons = append(result.Reasons, "nested_key_marker")
		}

		nextResult := s.classify(value, depth+1)
		if len(nextResult.Shapes) > 0 || len(nextResult.Reasons) > 0 {
			nextResult.Reasons = append(nextResult.Reasons, "nested_value")
		}
		result.Merge(nextResult)
	}

	return result
}

func (s *ClassificationResult) Merge(nextResult ClassificationResult) {
	s.Shapes = append(s.Shapes, nextResult.Shapes...)
	s.Reasons = append(s.Reasons, nextResult.Reasons...)
}

func (s Classifier) SensitiveKey(key string) bool {
	if s.PreservedKey(key) {
		return false
	}

	normalized := NormalizeSensitiveKey(key)
	for _, marker := range s.sensitiveKeyMarks {
		if strings.Contains(normalized, marker) {
			return true
		}
	}

	return false
}

func (s Classifier) PreservedKey(key string) bool {
	_, preserved := s.preserveKeys[NormalizeSensitiveKey(key)]
	return preserved
}

func (s Classifier) IdentifierLike(stats PropertyStats) bool {
	if stats.PreservedKey || s.identifierMinimumSamples == 0 {
		return false
	}
	if stats.NonEmpty < s.identifierMinimumSamples {
		return false
	}
	if stats.AverageStringLength < s.identifierMinimumAverageLen {
		return false
	}

	return stats.UniqueRatio >= s.identifierUniquenessThreshold
}

func NormalizeSensitiveKey(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")
	return normalized
}
