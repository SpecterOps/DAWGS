package scrub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
)

const maxPropertyPlans = 4096

// ActionCounts reports the number of successful actions by action name.
type ActionCounts map[string]int64

type compiledShape struct {
	name    string
	pattern *regexp.Regexp
}

// Scrubber applies compiled, immutable rules to caller-owned property maps.
type Scrubber struct {
	enabled             bool
	rules               Rules
	salt                []byte
	preserveKeys        map[string]struct{}
	referenceKeys       map[string]struct{}
	sensitiveKeyMarkers []string
	shapeRules          []compiledShape
	propertyPlans       map[string]propertyPlan
	propertyPlansMu     sync.RWMutex
	rulesFingerprint    string
	saltFingerprint     string
}

// New compiles a scrub policy.
func New(config Config) (*Scrubber, error) {
	config.Salt = strings.TrimSpace(config.Salt)
	config.Rules = normalizeRules(cloneRules(config.Rules))
	if err := config.Validate(); err != nil {
		return nil, err
	}

	preserveKeys := make(map[string]struct{}, len(config.Rules.Classifier.PreserveKeys))
	for _, key := range config.Rules.Classifier.PreserveKeys {
		preserveKeys[normalizeKey(key)] = struct{}{}
	}
	referenceKeys := map[string]struct{}{}
	for _, keys := range [][]string{
		config.Rules.GraphRules.DomainSIDReferenceKeys,
		config.Rules.GraphRules.ObjectIDReferenceKeys,
		config.Rules.GraphRules.SelfObjectIDAliasKeys,
		config.Rules.GraphRules.DomainNameReferenceKeys,
		{config.Rules.GraphRules.ObjectIDKey, config.Rules.GraphRules.DomainNameKey},
	} {
		for _, key := range keys {
			if normalized := normalizeKey(key); normalized != "" {
				referenceKeys[normalized] = struct{}{}
			}
		}
	}
	shapeRules := make([]compiledShape, 0, len(config.Rules.Classifier.ValueShapePatterns))
	for _, shape := range config.Rules.Classifier.ValueShapePatterns {
		if strings.TrimSpace(shape.Name) == "" || strings.TrimSpace(shape.Pattern) == "" {
			continue
		}
		pattern, err := regexp.Compile(shape.Pattern)
		if err != nil {
			return nil, err
		}
		shapeRules = append(shapeRules, compiledShape{name: shape.Name, pattern: pattern})
	}
	sensitiveKeyMarkers := make([]string, 0, len(config.Rules.Classifier.SensitiveKeyMarks))
	for _, marker := range config.Rules.Classifier.SensitiveKeyMarks {
		if normalized := normalizeKey(marker); normalized != "" {
			sensitiveKeyMarkers = append(sensitiveKeyMarkers, normalized)
		}
	}

	rulesBytes, err := json.Marshal(config.Rules)
	if err != nil {
		return nil, err
	}
	rulesDigest := sha256.Sum256(rulesBytes)
	saltDigest := sha256.Sum256([]byte(config.Salt))

	return &Scrubber{
		enabled:             config.Enabled,
		rules:               config.Rules,
		salt:                append([]byte(nil), config.Salt...),
		preserveKeys:        preserveKeys,
		referenceKeys:       referenceKeys,
		sensitiveKeyMarkers: sensitiveKeyMarkers,
		shapeRules:          shapeRules,
		propertyPlans:       map[string]propertyPlan{},
		rulesFingerprint:    hex.EncodeToString(rulesDigest[:]),
		saltFingerprint:     hex.EncodeToString(saltDigest[:]),
	}, nil
}

func normalizeRules(rules Rules) Rules {
	rules.FakeDomain = strings.Trim(strings.ToLower(strings.TrimSpace(rules.FakeDomain)), ".")
	rules.RedactionMarker = strings.TrimSpace(rules.RedactionMarker)
	if rules.RedactionMarker == "" {
		rules.RedactionMarker = "[REDACTED]"
	}
	if rules.Classifier.LongTextThreshold <= 0 {
		rules.Classifier.LongTextThreshold = 512
	}
	if rules.TimestampShiftDays == 0 {
		rules.TimestampShiftDays = 17
	}
	return rules
}

// Scrub mutates properties in place and returns action counts.
func (s *Scrubber) Scrub(properties map[string]any) ActionCounts {
	counts := ActionCounts{}
	if s == nil || !s.enabled {
		return counts
	}
	s.scrubMap(properties, counts)
	return counts
}

func (s *Scrubber) scrubMap(properties map[string]any, counts ActionCounts) {
	for key, value := range properties {
		action := s.planProperty(key, value)
		properties[key] = s.scrubWithAction(key, value, action)
		counts[string(action)]++
		if action == actionPreserve {
			s.scrubNested(value, counts)
		}
	}
}

func (s *Scrubber) scrubNested(value any, counts ActionCounts) {
	switch typed := value.(type) {
	case map[string]any:
		s.scrubMap(typed, counts)
	case []any:
		for _, item := range typed {
			s.scrubNested(item, counts)
		}
	}
}

func (s *Scrubber) planKey(key string) propertyPlan {
	normalized := normalizeKey(key)
	s.propertyPlansMu.RLock()
	plan, found := s.propertyPlans[normalized]
	s.propertyPlansMu.RUnlock()
	if found {
		return plan
	}

	_, plan.reference = s.referenceKeys[normalized]
	_, plan.preserve = s.preserveKeys[normalized]
	plan.normalized = normalized
	plan.timestamp = isTimestampKey(normalized)
	plan.freeText = isFreeTextKey(normalized)
	plan.path = isPathKey(normalized)
	plan.script = isScriptKey(normalized)
	plan.sensitive = s.isSensitiveKey(normalized)
	plan.semantic = isSemanticOrgKey(normalized)

	s.propertyPlansMu.Lock()
	if existing, ok := s.propertyPlans[normalized]; ok {
		plan = existing
	} else if len(s.propertyPlans) < maxPropertyPlans {
		s.propertyPlans[normalized] = plan
	}
	s.propertyPlansMu.Unlock()
	return plan
}

func (s *Scrubber) isSensitiveKey(normalizedKey string) bool {
	for _, marker := range s.sensitiveKeyMarkers {
		if strings.Contains(normalizedKey, marker) {
			return true
		}
	}
	return false
}

func (s *Scrubber) digest(value string) string {
	mac := hmac.New(sha256.New, s.salt)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

// RulesFingerprint returns the lowercase SHA-256 fingerprint of canonical rules.
func (s *Scrubber) RulesFingerprint() string {
	if s == nil {
		return ""
	}
	return s.rulesFingerprint
}

// SaltFingerprint returns the lowercase SHA-256 fingerprint of the configured salt.
func (s *Scrubber) SaltFingerprint() string {
	if s == nil {
		return ""
	}
	return s.saltFingerprint
}
