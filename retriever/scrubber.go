package retriever

import (
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	scrubRulesVersion     = "retriever-scrub-v2"
	maxScrubPropertyPlans = 4096
)

type PropertyAction string

const (
	actionPreserve       PropertyAction = "preserve"
	actionPseudonymize   PropertyAction = "pseudonymize"
	actionRedact         PropertyAction = "redact"
	actionShiftTimestamp PropertyAction = "shift_timestamp"
)

type PropertyPlan struct {
	Key    string
	Action PropertyAction
	Shape  string
}

type ScrubberConfig struct {
	Salt               string           `toml:"salt"`
	FakeDomain         string           `toml:"fake_domain"`
	TimestampShiftDays int              `toml:"timestamp_shift_days"`
	RedactionMarker    string           `toml:"redaction_marker"`
	GraphRules         GraphRulesConfig `toml:"graph_rules"`
	Classifier         ClassifierConfig `toml:"classifier"`
}

type ScrubberFileConfig struct {
	Scrub      ScrubberConfig   `toml:"scrub"`
	Classifier ClassifierConfig `toml:"classifier"`
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
	LongTextThreshold  int                `toml:"long_text_threshold"`
	PreserveKeys       []string           `toml:"preserve_keys"`
	SensitiveKeyMarks  []string           `toml:"sensitive_key_markers"`
	ValueShapePatterns []ValueShapeConfig `toml:"value_shapes"`
}

type ValueShapeConfig struct {
	Name    string `toml:"name"`
	Pattern string `toml:"pattern"`
}

type compiledShape struct {
	name    string
	pattern *regexp.Regexp
}

type scrubber struct {
	config              ScrubberConfig
	preserveKeys        map[string]struct{}
	referenceKeys       map[string]struct{}
	sensitiveKeyMarkers []string
	shapeRules          []compiledShape
	propertyPlans       map[string]propertyKeyPlan
	propertyPlansMu     sync.RWMutex
	mac                 hash.Hash
	digestBytes         [sha256.Size]byte
	digestHex           [sha256.Size * 2]byte
}

type propertyKeyPlan struct {
	normalized string
	reference  bool
	preserve   bool
	timestamp  bool
	freeText   bool
	path       bool
	script     bool
	sensitive  bool
	semantic   bool
}

type scrubActionCounts struct {
	preserve     int
	pseudonymize int
	redact       int
	shift        int
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

//go:embed defaults.toml
var defaultScrubberConfigTOML []byte

func DefaultScrubberConfig() ScrubberConfig {
	cfg, err := decodeScrubberFileConfig(defaultScrubberConfigTOML, ScrubberConfig{})
	if err != nil {
		panic(fmt.Sprintf("parse embedded scrub defaults: %v", err))
	}

	return cfg
}

func defaultScrubberConfig() ScrubberConfig {
	return DefaultScrubberConfig()
}

func ReadScrubberConfig(reader io.Reader, base ScrubberConfig) (ScrubberConfig, error) {
	contents, err := io.ReadAll(reader)
	if err != nil {
		return ScrubberConfig{}, fmt.Errorf("read scrub config: %w", err)
	}

	return decodeScrubberFileConfig(contents, base)
}

func decodeScrubberFileConfig(contents []byte, base ScrubberConfig) (ScrubberConfig, error) {
	fileCfg := ScrubberFileConfig{
		Scrub:      base,
		Classifier: base.Classifier,
	}
	if err := toml.Unmarshal(contents, &fileCfg); err != nil {
		return ScrubberConfig{}, err
	}

	cfg := fileCfg.Scrub
	cfg.Classifier = fileCfg.Classifier

	return cfg, nil
}

func newScrubber(configReader io.Reader, salt string) (*scrubber, error) {
	cfg := defaultScrubberConfig()
	if configReader != nil {
		if decoded, err := ReadScrubberConfig(configReader, cfg); err != nil {
			return nil, fmt.Errorf("parse scrub config: %w", err)
		} else {
			cfg = decoded
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

	sensitiveKeyMarkers := make([]string, 0, len(cfg.Classifier.SensitiveKeyMarks))
	for _, marker := range cfg.Classifier.SensitiveKeyMarks {
		if normalized := normalizeKey(marker); normalized != "" {
			sensitiveKeyMarkers = append(sensitiveKeyMarkers, normalized)
		}
	}

	saltBytes := []byte(cfg.Salt)

	return &scrubber{
		config:              cfg,
		preserveKeys:        preserveKeys,
		referenceKeys:       referenceKeys,
		sensitiveKeyMarkers: sensitiveKeyMarkers,
		shapeRules:          shapeRules,
		propertyPlans:       map[string]propertyKeyPlan{},
		mac:                 hmac.New(sha256.New, saltBytes),
	}, nil
}

func (s *scrubber) forGraph() *scrubber {
	saltBytes := []byte(s.config.Salt)
	return &scrubber{
		config:              s.config,
		preserveKeys:        s.preserveKeys,
		referenceKeys:       s.referenceKeys,
		sensitiveKeyMarkers: s.sensitiveKeyMarkers,
		shapeRules:          s.shapeRules,
		propertyPlans:       map[string]propertyKeyPlan{},
		mac:                 hmac.New(sha256.New, saltBytes),
	}
}

func (s *scrubber) metadata() ScrubMetadata {
	return ScrubMetadata{
		Mode:             ScrubFull,
		RulesVersion:     scrubRulesVersion,
		SaltProvided:     strings.TrimSpace(s.config.Salt) != "",
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
}

func (s *scrubber) scrubProperties(properties map[string]any) (map[string]any, map[string]int) {
	scrubbed, counts := s.scrubPropertiesWithCounts(properties)
	return scrubbed, counts.mapValue()
}

func (s *scrubber) scrubPropertiesWithCounts(properties map[string]any) (map[string]any, scrubActionCounts) {
	if properties == nil {
		return map[string]any{}, scrubActionCounts{}
	}

	scrubbed := make(map[string]any, len(properties))
	var actionCounts scrubActionCounts
	for key, value := range properties {
		nextValue, plan := s.scrubProperty(key, value)
		scrubbed[key] = nextValue
		actionCounts.add(plan.Action)
	}

	return scrubbed, actionCounts
}

func (s *scrubber) scrubProperty(key string, value any) (any, PropertyPlan) {
	plan := s.planProperty(key, value)
	return s.scrubWithPlan(key, value, plan), plan
}

func (s *scrubber) planProperty(key string, value any) PropertyPlan {
	keyPlan := s.planKey(key)
	plan := PropertyPlan{
		Key:    key,
		Action: actionPreserve,
	}

	if keyPlan.reference && isStringLike(value) {
		plan.Action = actionPseudonymize
		plan.Shape = s.classifyValue(value)

		return plan
	}

	if keyPlan.preserve {
		return plan
	}

	if keyPlan.timestamp {
		plan.Action = actionShiftTimestamp

		return plan
	}

	if keyPlan.freeText {
		plan.Action = actionRedact

		return plan
	}

	if keyPlan.path {
		plan.Action = actionPseudonymize
		plan.Shape = s.classifyValue(value)

		return plan
	}

	if keyPlan.script {
		plan.Action = actionPseudonymize
		plan.Shape = s.classifyValue(value)

		return plan
	}

	if s.shouldRedact(keyPlan.normalized, value) {
		plan.Action = actionRedact

		return plan
	}

	if shape := s.classifyValue(value); shape != "" {
		plan.Action = actionPseudonymize
		plan.Shape = shape

		return plan
	}

	if keyPlan.sensitive {
		plan.Action = actionPseudonymize

		return plan
	}

	if keyPlan.semantic || isStringLike(value) {
		plan.Action = actionPseudonymize
		plan.Shape = s.classifyValue(value)

		return plan
	}

	return plan
}

func (s *scrubber) planKey(key string) propertyKeyPlan {
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
	} else if len(s.propertyPlans) < maxScrubPropertyPlans {
		s.propertyPlans[normalized] = plan
	}
	s.propertyPlansMu.Unlock()

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

func (s *scrubber) scrubWithPlan(key string, value any, plan PropertyPlan) any {
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
	for _, marker := range s.sensitiveKeyMarkers {
		if strings.Contains(normalizedKey, marker) {
			return true
		}
	}

	return false
}

func isFreeTextKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "description") ||
		strings.Contains(normalizedKey, "comment") ||
		strings.Contains(normalizedKey, "note") ||
		normalizedKey == "info"
}

func isPathKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "path") ||
		strings.Contains(normalizedKey, "directory") ||
		strings.Contains(normalizedKey, "homedir") ||
		strings.Contains(normalizedKey, "folder")
}

func isScriptKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "script")
}

func isSemanticOrgKey(normalizedKey string) bool {
	switch normalizedKey {
	case "title", "department", "division", "company", "organization", "office", "location":
		return true
	default:
		return false
	}
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
		if len(matches) == 3 {
			return s.pseudonymizeString(matches[1], "domain_sid") + "-" + matches[2]
		}

		return "value-" + digest[:16]

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
	s.mac.Reset()
	_, _ = io.WriteString(s.mac, value)
	digest := s.mac.Sum(s.digestBytes[:0])
	hex.Encode(s.digestHex[:], digest)
	result := string(s.digestHex[:])
	clear(s.digestBytes[:])
	clear(s.digestHex[:])

	return result
}

func (s *scrubActionCounts) add(action PropertyAction) {
	switch action {
	case actionPreserve:
		s.preserve++
	case actionPseudonymize:
		s.pseudonymize++
	case actionRedact:
		s.redact++
	case actionShiftTimestamp:
		s.shift++
	}
}

func (s *scrubActionCounts) addCounts(other scrubActionCounts) {
	s.preserve += other.preserve
	s.pseudonymize += other.pseudonymize
	s.redact += other.redact
	s.shift += other.shift
}

func (s scrubActionCounts) mapValue() map[string]int {
	result := map[string]int{}
	for action, count := range map[PropertyAction]int{
		actionPreserve:       s.preserve,
		actionPseudonymize:   s.pseudonymize,
		actionRedact:         s.redact,
		actionShiftTimestamp: s.shift,
	} {
		if count > 0 {
			result[string(action)] = count
		}
	}

	return result
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
		strings.Contains(normalizedKey, "created") ||
		strings.Contains(normalizedKey, "updated") ||
		strings.Contains(normalizedKey, "deleted") ||
		strings.Contains(normalizedKey, "modified") ||
		strings.HasSuffix(normalizedKey, "seenat")
}
