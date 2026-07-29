package scrub

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type propertyAction string

const (
	actionPreserve       propertyAction = "preserve"
	actionPseudonymize   propertyAction = "pseudonymize"
	actionRedact         propertyAction = "redact"
	actionShiftTimestamp propertyAction = "shift_timestamp"
)

type propertyPlan struct {
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

var (
	emailPattern       = regexp.MustCompile(`(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)
	uuidPattern        = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	domainSIDPattern   = regexp.MustCompile(`^S-1-5-21-\d+-\d+-\d+$`)
	objectSIDPattern   = regexp.MustCompile(`^(S-1-5-21-\d+-\d+-\d+)-(\d+)$`)
	ipv4Pattern        = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	hostLikePattern    = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+$`)
	secretValuePattern = regexp.MustCompile(`(?i)(password|secret|token|private[_-]?key|credential|apikey|api[_-]?key)`)
)

func (s *Scrubber) planProperty(key string, value any) propertyAction {
	plan := s.planKey(key)

	if plan.reference && isStringLike(value) {
		return actionPseudonymize
	}
	if plan.preserve {
		return actionPreserve
	}
	if plan.timestamp {
		return actionShiftTimestamp
	}
	if plan.freeText {
		return actionRedact
	}
	if plan.path || plan.script {
		return actionPseudonymize
	}
	if s.shouldRedact(plan.normalized, value) {
		return actionRedact
	}
	if s.classifyValue(value) != "" {
		return actionPseudonymize
	}
	if plan.sensitive || plan.semantic || isStringLike(value) {
		return actionPseudonymize
	}

	return actionPreserve
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

func (s *Scrubber) scrubWithAction(key string, value any, action propertyAction) any {
	switch action {
	case actionRedact:
		return s.redact(value)
	case actionShiftTimestamp:
		return s.shiftTimestamp(value)
	case actionPseudonymize:
		return s.pseudonymizeValue(key, value, s.classifyValue(value))
	default:
		return value
	}
}

func (s *Scrubber) shouldRedact(normalizedKey string, value any) bool {
	if secretValuePattern.MatchString(normalizedKey) {
		return true
	}
	switch typed := value.(type) {
	case string:
		return len(typed) > s.rules.Classifier.LongTextThreshold
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

func (s *Scrubber) redact(value any) any {
	switch typed := value.(type) {
	case []any:
		values := make([]any, len(typed))
		for index := range values {
			values[index] = s.rules.RedactionMarker
		}
		return values
	case []string:
		values := make([]string, len(typed))
		for index := range values {
			values[index] = s.rules.RedactionMarker
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typed))
		for key := range typed {
			values[key] = s.rules.RedactionMarker
		}
		return values
	default:
		return s.rules.RedactionMarker
	}
}

func (s *Scrubber) shiftTimestamp(value any) any {
	shift := time.Duration(s.rules.TimestampShiftDays) * 24 * time.Hour
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
			if shifted, ok := s.shiftTimestamp(item).(string); ok {
				values = append(values, shifted)
			}
		}
		return values
	default:
		return value
	}
}

func (s *Scrubber) pseudonymizeValue(key string, value any, shape string) any {
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

func (s *Scrubber) classifyValue(value any) string {
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

func (s *Scrubber) classifyString(value string) string {
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

func (s *Scrubber) pseudonymizeString(value string, shape string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	digest := s.digest(trimmed)
	switch {
	case shape == "email" || emailPattern.MatchString(trimmed):
		return "user-" + digest[:12] + "@" + s.rules.FakeDomain
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
		return "host-" + digest[:12] + "." + s.rules.FakeDomain
	default:
		return "value-" + digest[:16]
	}
}

func (s *Scrubber) fakeDomainSID(digest string) string {
	return fmt.Sprintf("S-1-5-21-%09d-%09d-%09d", intFromHex(digest[0:8])%1_000_000_000, intFromHex(digest[8:16])%1_000_000_000, intFromHex(digest[16:24])%1_000_000_000)
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
	return strings.ReplaceAll(value, " ", "")
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

func isFreeTextKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "description") || strings.Contains(normalizedKey, "comment") || strings.Contains(normalizedKey, "note") || normalizedKey == "info"
}

func isPathKey(normalizedKey string) bool {
	return strings.Contains(normalizedKey, "path") || strings.Contains(normalizedKey, "directory") || strings.Contains(normalizedKey, "homedir") || strings.Contains(normalizedKey, "folder")
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
