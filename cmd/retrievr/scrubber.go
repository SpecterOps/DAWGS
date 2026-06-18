package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const scrubRulesVersion = "retrievr-scrub-v1"

type propertyAction string

const (
	actionPreserve       propertyAction = "preserve"
	actionPseudonymize   propertyAction = "pseudonymize"
	actionRedact         propertyAction = "redact"
	actionShiftTimestamp propertyAction = "shift_timestamp"
)

type propertyPlan struct {
	Key    string
	Action propertyAction
	Shape  string
}

type scrubberConfig struct {
	Salt               string           `toml:"salt"`
	FakeDomain         string           `toml:"fake_domain"`
	TimestampShiftDays int              `toml:"timestamp_shift_days"`
	RedactionMarker    string           `toml:"redaction_marker"`
	GraphRules         graphRulesConfig `toml:"graph_rules"`
	Classifier         classifierConfig `toml:"classifier"`
}

type scrubberFileConfig struct {
	Scrub      scrubberConfig   `toml:"scrub"`
	Classifier classifierConfig `toml:"classifier"`
}

type graphRulesConfig struct {
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

type classifierConfig struct {
	LongTextThreshold  int                `toml:"long_text_threshold"`
	PreserveKeys       []string           `toml:"preserve_keys"`
	SensitiveKeyMarks  []string           `toml:"sensitive_key_markers"`
	ValueShapePatterns []valueShapeConfig `toml:"value_shapes"`
}

type valueShapeConfig struct {
	Name    string `toml:"name"`
	Pattern string `toml:"pattern"`
}

type compiledShape struct {
	name    string
	pattern *regexp.Regexp
}

type scrubber struct {
	config           scrubberConfig
	preserveKeys     map[string]struct{}
	referenceKeys    map[string]struct{}
	shapeRules       []compiledShape
	identifierLookup map[string]string
}

var (
	emailPattern       = regexp.MustCompile(`(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)
	uuidPattern        = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	domainSIDPattern   = regexp.MustCompile(`^S-1-5-21-\d+-\d+-\d+$`)
	objectSIDPattern   = regexp.MustCompile(`^(S-1-5-21-\d+-\d+-\d+)-(\d+)$`)
	ipv4Pattern        = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	hostLikePattern    = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+$`)
	secretValuePattern = regexp.MustCompile(`(?i)(password|secret|token|private[_-]?key|credential|apikey|api[_-]?key)`)
)

func defaultScrubberConfig() scrubberConfig {
	return scrubberConfig{
		FakeDomain:         "example.invalid",
		TimestampShiftDays: 17,
		RedactionMarker:    "[REDACTED]",
		GraphRules: graphRulesConfig{
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
		Classifier: classifierConfig{
			LongTextThreshold: 512,
			PreserveKeys: []string{
				"objectid",
				"domainsid",
				"kind",
			},
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
			ValueShapePatterns: []valueShapeConfig{
				{
					Name:    "email",
					Pattern: emailPattern.String(),
				},
				{
					Name:    "uuid",
					Pattern: uuidPattern.String(),
				},
				{
					Name:    "domain_sid",
					Pattern: domainSIDPattern.String(),
				},
				{
					Name:    "object_sid",
					Pattern: objectSIDPattern.String(),
				},
				{
					Name:    "ipv4",
					Pattern: ipv4Pattern.String(),
				},
				{
					Name:    "host",
					Pattern: hostLikePattern.String(),
				},
			},
		},
	}
}

func newScrubber(configPath string, salt string) (*scrubber, error) {
	cfg := defaultScrubberConfig()
	if strings.TrimSpace(configPath) != "" {
		if contents, err := os.ReadFile(configPath); err != nil {
			return nil, fmt.Errorf("read scrub config: %w", err)
		} else {
			fileCfg := scrubberFileConfig{
				Scrub:      cfg,
				Classifier: cfg.Classifier,
			}
			if err := toml.Unmarshal(contents, &fileCfg); err != nil {
				return nil, fmt.Errorf("parse scrub config: %w", err)
			}
			cfg = fileCfg.Scrub
			cfg.Classifier = fileCfg.Classifier
		}
	}

	cfg.Salt = strings.TrimSpace(salt)
	cfg.FakeDomain = strings.Trim(strings.ToLower(strings.TrimSpace(cfg.FakeDomain)), ".")
	cfg.RedactionMarker = strings.TrimSpace(cfg.RedactionMarker)
	if cfg.RedactionMarker == "" {
		cfg.RedactionMarker = "[REDACTED]"
	}
	if cfg.Classifier.LongTextThreshold <= 0 {
		cfg.Classifier.LongTextThreshold = 512
	}
	if cfg.TimestampShiftDays == 0 {
		cfg.TimestampShiftDays = 17
	}

	preserveKeys := make(map[string]struct{}, len(cfg.Classifier.PreserveKeys))
	for _, key := range cfg.Classifier.PreserveKeys {
		preserveKeys[normalizeKey(key)] = struct{}{}
	}

	referenceKeys := map[string]struct{}{}
	for _, keys := range [][]string{
		cfg.GraphRules.DomainSIDReferenceKeys,
		cfg.GraphRules.ObjectIDReferenceKeys,
		cfg.GraphRules.SelfObjectIDAliasKeys,
		cfg.GraphRules.DomainNameReferenceKeys,
		{cfg.GraphRules.ObjectIDKey, cfg.GraphRules.DomainNameKey},
	} {
		for _, key := range keys {
			if normalized := normalizeKey(key); normalized != "" {
				referenceKeys[normalized] = struct{}{}
			}
		}
	}

	shapeRules := make([]compiledShape, 0, len(cfg.Classifier.ValueShapePatterns))
	for _, shape := range cfg.Classifier.ValueShapePatterns {
		if strings.TrimSpace(shape.Name) == "" || strings.TrimSpace(shape.Pattern) == "" {
			continue
		}
		if pattern, err := regexp.Compile(shape.Pattern); err != nil {
			return nil, fmt.Errorf("compile value shape %q: %w", shape.Name, err)
		} else {
			shapeRules = append(shapeRules, compiledShape{
				name:    shape.Name,
				pattern: pattern,
			})
		}
	}

	return &scrubber{
		config:           cfg,
		preserveKeys:     preserveKeys,
		referenceKeys:    referenceKeys,
		shapeRules:       shapeRules,
		identifierLookup: map[string]string{},
	}, nil
}

func (s *scrubber) metadata() scrubMetadata {
	return scrubMetadata{
		Mode:             scrubFull,
		RulesVersion:     scrubRulesVersion,
		SaltProvided:     strings.TrimSpace(s.config.Salt) != "",
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
}

func (s *scrubber) observeNode(properties map[string]any) {
	for key, value := range properties {
		normalized := normalizeKey(key)
		if _, ok := s.referenceKeys[normalized]; !ok {
			continue
		}
		s.observeIdentifier(value)
	}
}

func (s *scrubber) observeIdentifier(value any) {
	switch typed := value.(type) {
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed != "" {
			s.identifierLookup[trimmed] = s.pseudonymizeString(trimmed, s.classifyString(trimmed))
		}
	case []any:
		for _, item := range typed {
			s.observeIdentifier(item)
		}
	case []string:
		for _, item := range typed {
			s.observeIdentifier(item)
		}
	}
}

func (s *scrubber) scrubProperties(properties map[string]any) (map[string]any, map[string]int) {
	if properties == nil {
		return map[string]any{}, map[string]int{}
	}

	scrubbed := make(map[string]any, len(properties))
	actionCounts := map[string]int{}
	for key, value := range properties {
		nextValue, plan := s.scrubProperty(key, value)
		scrubbed[key] = nextValue
		actionCounts[string(plan.Action)] += 1
	}
	return scrubbed, actionCounts
}

func (s *scrubber) scrubProperty(key string, value any) (any, propertyPlan) {
	plan := s.planProperty(key, value)
	return s.scrubWithPlan(key, value, plan), plan
}

func (s *scrubber) planProperty(key string, value any) propertyPlan {
	normalizedKey := normalizeKey(key)
	plan := propertyPlan{
		Key:    key,
		Action: actionPreserve,
	}
	if _, ok := s.referenceKeys[normalizedKey]; ok && isStringLike(value) {
		plan.Action = actionPseudonymize
		plan.Shape = s.classifyValue(value)
		return plan
	}
	if _, ok := s.preserveKeys[normalizedKey]; ok {
		return plan
	}
	if isTimestampKey(normalizedKey) {
		plan.Action = actionShiftTimestamp
		return plan
	}
	if s.shouldRedact(normalizedKey, value) {
		plan.Action = actionRedact
		return plan
	}
	if shape := s.classifyValue(value); shape != "" {
		plan.Action = actionPseudonymize
		plan.Shape = shape
		return plan
	}
	if s.isSensitiveKey(normalizedKey) {
		plan.Action = actionPseudonymize
		return plan
	}
	return plan
}

func isStringLike(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		for _, item := range typed {
			if isStringLike(item) {
				return true
			}
		}
	case []string:
		return len(typed) > 0
	}
	return false
}

func (s *scrubber) scrubWithPlan(key string, value any, plan propertyPlan) any {
	switch plan.Action {
	case actionPreserve:
		return value
	case actionRedact:
		return s.redact(value)
	case actionShiftTimestamp:
		return s.shiftTimestamp(value)
	case actionPseudonymize:
		return s.pseudonymizeValue(key, value, plan.Shape)
	default:
		return value
	}
}

func (s *scrubber) shouldRedact(normalizedKey string, value any) bool {
	if secretValuePattern.MatchString(normalizedKey) {
		return true
	}
	switch typed := value.(type) {
	case string:
		return len(typed) > s.config.Classifier.LongTextThreshold
	case []any:
		for _, item := range typed {
			if s.shouldRedact(normalizedKey, item) {
				return true
			}
		}
	case []string:
		for _, item := range typed {
			if s.shouldRedact(normalizedKey, item) {
				return true
			}
		}
	}
	return false
}

func (s *scrubber) isSensitiveKey(normalizedKey string) bool {
	for _, marker := range s.config.Classifier.SensitiveKeyMarks {
		if marker = normalizeKey(marker); marker != "" && strings.Contains(normalizedKey, marker) {
			return true
		}
	}
	return false
}

func (s *scrubber) classifyValue(value any) string {
	switch typed := value.(type) {
	case string:
		return s.classifyString(typed)
	case []any:
		for _, item := range typed {
			if shape := s.classifyValue(item); shape != "" {
				return shape
			}
		}
	case []string:
		for _, item := range typed {
			if shape := s.classifyString(item); shape != "" {
				return shape
			}
		}
	}
	return ""
}

func (s *scrubber) classifyString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	for _, rule := range s.shapeRules {
		if rule.pattern.MatchString(trimmed) {
			return rule.name
		}
	}
	return ""
}

func (s *scrubber) redact(value any) any {
	switch typed := value.(type) {
	case []any:
		values := make([]any, len(typed))
		for idx := range values {
			values[idx] = s.config.RedactionMarker
		}
		return values
	case []string:
		values := make([]string, len(typed))
		for idx := range values {
			values[idx] = s.config.RedactionMarker
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typed))
		for key := range typed {
			values[key] = s.config.RedactionMarker
		}
		return values
	default:
		return s.config.RedactionMarker
	}
}

func (s *scrubber) shiftTimestamp(value any) any {
	shift := time.Duration(s.config.TimestampShiftDays) * 24 * time.Hour
	switch typed := value.(type) {
	case time.Time:
		return typed.Add(shift).UTC().Format(time.RFC3339Nano)
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, typed); err == nil {
			return parsed.Add(shift).UTC().Format(time.RFC3339Nano)
		}
		return s.pseudonymizeString(typed, "")
	case int:
		return typed + int(shift.Seconds())
	case int64:
		return typed + int64(shift.Seconds())
	case float64:
		return typed + shift.Seconds()
	case []any:
		values := make([]any, 0, len(typed))
		for _, item := range typed {
			values = append(values, s.shiftTimestamp(item))
		}
		return values
	case []string:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			shifted := s.shiftTimestamp(item)
			if stringValue, ok := shifted.(string); ok {
				values = append(values, stringValue)
			}
		}
		return values
	default:
		return value
	}
}

func (s *scrubber) pseudonymizeValue(key string, value any, shape string) any {
	normalizedKey := normalizeKey(key)
	switch typed := value.(type) {
	case string:
		if replacement, ok := s.identifierLookup[strings.TrimSpace(typed)]; ok {
			return replacement
		}
		if _, ok := s.referenceKeys[normalizedKey]; ok {
			return s.pseudonymizeString(typed, s.classifyString(typed))
		}
		return s.pseudonymizeString(typed, shape)
	case []any:
		values := make([]any, 0, len(typed))
		for _, item := range typed {
			values = append(values, s.pseudonymizeValue(key, item, s.classifyValue(item)))
		}
		return values
	case []string:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if replacement, ok := s.pseudonymizeValue(key, item, s.classifyString(item)).(string); ok {
				values = append(values, replacement)
			}
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typed))
		for nestedKey, nestedValue := range typed {
			values[nestedKey] = s.pseudonymizeValue(nestedKey, nestedValue, s.classifyValue(nestedValue))
		}
		return values
	default:
		return value
	}
}

func (s *scrubber) pseudonymizeString(value string, shape string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}

	digest := s.digest(trimmed)
	switch {
	case shape == "email" || emailPattern.MatchString(trimmed):
		return "user-" + digest[:12] + "@" + s.config.FakeDomain
	case shape == "uuid" || uuidPattern.MatchString(trimmed):
		return fmt.Sprintf("%s-%s-%s-%s-%s", digest[:8], digest[8:12], digest[12:16], digest[16:20], digest[20:32])
	case shape == "domain_sid" || domainSIDPattern.MatchString(trimmed):
		return s.fakeDomainSID(digest)
	case shape == "object_sid" || objectSIDPattern.MatchString(trimmed):
		matches := objectSIDPattern.FindStringSubmatch(trimmed)
		return s.pseudonymizeString(matches[1], "domain_sid") + "-" + matches[2]
	case shape == "ipv4" || ipv4Pattern.MatchString(trimmed):
		return fmt.Sprintf("10.%d.%d.%d", intFromHex(digest[0:2]), intFromHex(digest[2:4]), intFromHex(digest[4:6]))
	case shape == "host" || hostLikePattern.MatchString(trimmed):
		return "host-" + digest[:12] + "." + s.config.FakeDomain
	default:
		return "value-" + digest[:16]
	}
}

func (s *scrubber) fakeDomainSID(digest string) string {
	return fmt.Sprintf("S-1-5-21-%09d-%09d-%09d", intFromHex(digest[0:8])%1_000_000_000, intFromHex(digest[8:16])%1_000_000_000, intFromHex(digest[16:24])%1_000_000_000)
}

func (s *scrubber) digest(value string) string {
	mac := hmac.New(sha256.New, []byte(s.config.Salt))
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func intFromHex(value string) int {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return 0
	}
	result := 0
	for _, next := range decoded {
		result = result*256 + int(next)
	}
	return result
}

func normalizeKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, " ", "")
	return value
}

func isTimestampKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "time") ||
		strings.Contains(normalizedKey, "date") ||
		strings.HasSuffix(normalizedKey, "at") ||
		strings.Contains(normalizedKey, "created") ||
		strings.Contains(normalizedKey, "updated")
}
