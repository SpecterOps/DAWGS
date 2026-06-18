package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ScrubMode string

const (
	ModeShapeOnly      ScrubMode = "shape_only"
	ModeTradecraft     ScrubMode = "tradecraft"
	ModeResearchStable ScrubMode = "research_stable"
)

type PropertyAction string

const (
	ActionPreserve       PropertyAction = "preserve"
	ActionPseudonymize   PropertyAction = "pseudonymize"
	ActionRedact         PropertyAction = "redact"
	ActionShiftTimestamp PropertyAction = "shift_timestamp"
)

type PropertyPlan struct {
	Key     string         `json:"key"`
	Action  PropertyAction `json:"action"`
	Shapes  []string       `json:"shapes,omitempty"`
	Reasons []string       `json:"reasons,omitempty"`
}

type Scrubber struct {
	config     ScrubConfig
	classifier Classifier
}

var (
	downlevelLogonPattern = regexp.MustCompile(`^[^\\/\s]+\\([^\\/\s]+)$`)
	spnPattern            = regexp.MustCompile(`(?i)^([a-z][a-z0-9+.-]*)/[^\s/:]+(:\d+)?(/[^\s]+)?$`)
	domainSIDPattern      = regexp.MustCompile(`^S-1-5-21-\d+-\d+-\d+$`)
	adObjectSIDPattern    = regexp.MustCompile(`^(S-1-5-21-\d+-\d+-\d+)-(\d+)$`)
	fakeSIDPattern        = regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}-1000$`)
	fakeDomainSIDPattern  = regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}$`)
)

func NewScrubber(cfg Config) (Scrubber, error) {
	if err := cfg.Validate(); err != nil {
		return Scrubber{}, err
	}
	if ScrubMode(cfg.Scrub.Mode) != ModeShapeOnly {
		return Scrubber{}, fmt.Errorf("scrub mode %q is recognized but not implemented yet", cfg.Scrub.Mode)
	}
	if strings.TrimSpace(cfg.Scrub.Salt) == "" {
		return Scrubber{}, fmt.Errorf("scrub salt is required")
	}

	classifier, err := cfg.Classifier.Compile()
	if err != nil {
		return Scrubber{}, err
	}

	cfg.Scrub.FakeDomain = strings.Trim(strings.ToLower(strings.TrimSpace(cfg.Scrub.FakeDomain)), ".")
	cfg.Scrub.RedactionMarker = strings.TrimSpace(cfg.Scrub.RedactionMarker)

	return Scrubber{
		config:     cfg.Scrub,
		classifier: classifier,
	}, nil
}

func (s Scrubber) PlanProperty(key string, value any) PropertyPlan {
	plan := PropertyPlan{
		Key:    key,
		Action: ActionPreserve,
	}

	if s.classifier.PreservedKey(key) {
		plan.Reasons = appendUnique(plan.Reasons, "preserve_key")
		return plan
	}

	if s.alreadyScrubbed(value) {
		plan.Reasons = appendUnique(plan.Reasons, "already_scrubbed")
		return plan
	}

	classification := s.classifier.ClassifyProperty(key, value)
	plan.Shapes = uniqueStrings(classification.Shapes)
	plan.Reasons = uniqueStrings(classification.Reasons)

	if shouldShiftTimestamp(key, value) {
		plan.Action = ActionShiftTimestamp
		plan.Reasons = appendUnique(plan.Reasons, "timestamp_key")
		return plan
	}

	if shouldRedact(key, plan.Shapes) {
		plan.Action = ActionRedact
		plan.Reasons = appendUnique(plan.Reasons, "redaction_rule")
		return plan
	}

	if s.classifier.SensitiveKey(key) || len(plan.Shapes) > 0 {
		plan.Action = ActionPseudonymize
		if s.classifier.SensitiveKey(key) {
			plan.Reasons = appendUnique(plan.Reasons, "key_marker")
		}
		if len(plan.Shapes) > 0 {
			plan.Reasons = appendUnique(plan.Reasons, "value_shape")
		}
	}

	return plan
}

func (s Scrubber) alreadyScrubbed(value any) bool {
	switch typedValue := value.(type) {
	case string:
		return s.alreadyScrubbedString(typedValue)
	case []string:
		if len(typedValue) == 0 {
			return false
		}
		for _, value := range typedValue {
			if !s.alreadyScrubbedString(value) {
				return false
			}
		}
		return true
	case []any:
		if len(typedValue) == 0 {
			return false
		}
		for _, value := range typedValue {
			if !s.alreadyScrubbed(value) {
				return false
			}
		}
		return true
	case map[string]any:
		if len(typedValue) == 0 {
			return false
		}
		for _, value := range typedValue {
			if !s.alreadyScrubbed(value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (s Scrubber) alreadyScrubbedString(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if trimmed == s.config.RedactionMarker {
		return true
	}
	if s.config.FakeDomain != "" && strings.Contains(strings.ToLower(trimmed), s.config.FakeDomain) {
		return true
	}
	if strings.HasPrefix(trimmed, "principal-") || strings.HasPrefix(trimmed, "id-") {
		return true
	}
	if strings.HasPrefix(trimmed, `C:\Scrubbed\`) {
		return true
	}
	return fakeSIDPattern.MatchString(trimmed)
}

func (s Scrubber) ScrubProperty(key string, value any) (any, PropertyPlan) {
	plan := s.PlanProperty(key, value)
	return s.scrubWithPlan(key, value, plan), plan
}

func (s Scrubber) ScrubProperties(properties map[string]any) (map[string]any, []PropertyPlan) {
	if properties == nil {
		return map[string]any{}, nil
	}

	scrubbed := make(map[string]any, len(properties))
	plans := make([]PropertyPlan, 0, len(properties))
	for key, value := range properties {
		nextValue, plan := s.ScrubProperty(key, value)
		scrubbed[key] = nextValue
		plans = append(plans, plan)
	}

	return scrubbed, plans
}

func (s Scrubber) scrubWithPlan(key string, value any, plan PropertyPlan) any {
	switch plan.Action {
	case ActionPreserve:
		return value
	case ActionRedact:
		return s.redact(value)
	case ActionShiftTimestamp:
		return s.shiftTimestamp(value)
	case ActionPseudonymize:
		return s.pseudonymize(key, value, plan.Shapes)
	default:
		return value
	}
}

func (s Scrubber) redact(value any) any {
	switch typedValue := value.(type) {
	case []any:
		values := make([]any, 0, len(typedValue))
		for range typedValue {
			values = append(values, s.config.RedactionMarker)
		}
		return values
	case []string:
		values := make([]string, 0, len(typedValue))
		for range typedValue {
			values = append(values, s.config.RedactionMarker)
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typedValue))
		for key := range typedValue {
			values[key] = s.config.RedactionMarker
		}
		return values
	default:
		return s.config.RedactionMarker
	}
}

func (s Scrubber) shiftTimestamp(value any) any {
	switch typedValue := value.(type) {
	case string:
		if shifted, ok := s.shiftTimestampString(typedValue); ok {
			return shifted
		}
	case []any:
		values := make([]any, 0, len(typedValue))
		for _, value := range typedValue {
			values = append(values, s.shiftTimestamp(value))
		}
		return values
	case []string:
		values := make([]string, 0, len(typedValue))
		for _, value := range typedValue {
			if shifted, ok := s.shiftTimestampString(value); ok {
				values = append(values, shifted)
			} else {
				values = append(values, value)
			}
		}
		return values
	}

	return value
}

func (s Scrubber) shiftTimestampString(value string) (string, bool) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
			return parsed.AddDate(0, 0, s.config.TimestampShiftDays).Format(layout), true
		}
	}

	return value, false
}

func (s Scrubber) pseudonymize(key string, value any, shapes []string) any {
	switch typedValue := value.(type) {
	case string:
		return s.pseudonymizeString(key, typedValue, shapes)
	case []any:
		values := make([]any, 0, len(typedValue))
		for _, value := range typedValue {
			values = append(values, s.pseudonymize(key, value, shapes))
		}
		return values
	case []string:
		values := make([]string, 0, len(typedValue))
		for _, value := range typedValue {
			values = append(values, s.pseudonymizeString(key, value, shapes))
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typedValue))
		for nestedKey, nestedValue := range typedValue {
			nextValue, _ := s.ScrubProperty(nestedKey, nestedValue)
			values[nestedKey] = nextValue
		}
		return values
	default:
		return s.token("opaque:"+NormalizeSensitiveKey(key), fmt.Sprintf("%v", value), 16)
	}
}

func (s Scrubber) pseudonymizeString(key, value string, shapes []string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}

	shapeSet := stringSet(shapes)
	if shouldUseOpaqueIdentifierReplacement(key) {
		return "id-" + s.token("opaque:"+NormalizeSensitiveKey(key), trimmed, 16)
	}

	switch {
	case shapeSet["email"] || shapeSet["embedded_email"]:
		return "user-" + s.token("email", trimmed, 12) + "@" + s.fakeHost("mail", trimmed)
	case shapeSet["hostname"]:
		return s.fakeHost("host", trimmed)
	case shapeSet["hostname_trailing_dot"]:
		return s.fakeHost("host", trimmed) + "."
	case shapeSet["sid"] || shapeSet["embedded_sid"]:
		return s.fakeSID(trimmed)
	case shapeSet["guid"] || shapeSet["embedded_guid"] || shapeSet["compact_guid"]:
		return s.fakeGUID("guid", trimmed)
	case shapeSet["distinguished_name"]:
		return s.fakeDistinguishedName(trimmed)
	case shapeSet["url"] || shapeSet["uri"] || shapeSet["ldap_url"] || shapeSet["embedded_uri"]:
		return s.fakeURL(trimmed)
	case shapeSet["spn"]:
		return s.fakeSPN(trimmed)
	case shapeSet["downlevel_logon_name"]:
		if matches := downlevelLogonPattern.FindStringSubmatch(trimmed); len(matches) == 2 {
			return "SCRUBBED\\" + s.fakePrincipal(matches[1])
		}
	case shapeSet["windows_path"]:
		return `C:\Scrubbed\` + s.token("windows-path", trimmed, 12)
	case shapeSet["unc_path"]:
		return `\\` + s.fakeHost("files", trimmed) + `\share-` + s.token("unc-share", trimmed, 8)
	case shapeSet["azure_resource_id"]:
		return "/subscriptions/" + s.fakeGUID("azure-subscription", trimmed) + "/resourceGroups/rg-" + s.token("azure-rg", trimmed, 8) + "/providers/Microsoft.Scrubbed/resources/res-" + s.token("azure-resource", trimmed, 8)
	case shapeSet["aws_arn"]:
		return "arn:aws:iam::000000000000:role/scrubbed-" + s.token("aws-arn", trimmed, 12)
	case shapeSet["ipv4"]:
		return s.fakeIPv4(trimmed)
	case shapeSet["ipv4_cidr"]:
		return s.fakeIPv4(trimmed) + "/24"
	case shapeSet["ipv6"]:
		return "2001:db8::" + s.token("ipv6", trimmed, 4)
	case shapeSet["mac_address"]:
		return s.fakeMAC(trimmed)
	case shapeSet["phone_number"]:
		return "+1555" + s.decimalToken("phone", trimmed, 7)
	}

	if s.classifier.SensitiveKey(key) {
		return s.classifySensitiveKeyString(key, trimmed)
	}

	return "id-" + s.token("opaque:"+NormalizeSensitiveKey(key), trimmed, 16)
}

func shouldUseOpaqueIdentifierReplacement(key string) bool {
	normalizedKey := NormalizeSensitiveKey(key)
	for _, marker := range []string{
		"objectid",
		"appid",
		"applicationid",
		"clientid",
		"deviceid",
		"onpremid",
		"roledefinitionid",
		"serviceprincipalid",
		"subscriptionid",
		"templateid",
		"tenantid",
		"userid",
	} {
		if strings.Contains(normalizedKey, marker) {
			return true
		}
	}
	return false
}

func (s Scrubber) classifySensitiveKeyString(key, value string) string {
	normalizedKey := NormalizeSensitiveKey(key)
	switch {
	case strings.Contains(normalizedKey, "domain") || strings.Contains(normalizedKey, "dns") || strings.Contains(normalizedKey, "fqdn") || strings.Contains(normalizedKey, "host"):
		return s.fakeHost("host", value)
	case strings.Contains(normalizedKey, "sid"):
		return s.fakeSID(value)
	case strings.Contains(normalizedKey, "guid"):
		return s.fakeGUID("guid", value)
	case strings.Contains(normalizedKey, "url") || strings.Contains(normalizedKey, "endpoint"):
		return "https://" + s.fakeHost("endpoint", value)
	case strings.Contains(normalizedKey, "name") || strings.Contains(normalizedKey, "principal") || strings.Contains(normalizedKey, "user") || strings.Contains(normalizedKey, "owner"):
		return s.fakePrincipal(value)
	default:
		return "id-" + s.token("opaque:"+normalizedKey, value, 16)
	}
}

func (s Scrubber) fakeHost(prefix, value string) string {
	return prefix + "-" + s.token(prefix+":host", value, 12) + "." + s.config.FakeDomain
}

func (s Scrubber) fakePrincipal(value string) string {
	return "principal-" + s.token("principal", value, 12)
}

func (s Scrubber) fakeSID(value string) string {
	token := s.decimalToken("sid", value, 27)
	return "S-1-5-21-" + token[0:9] + "-" + token[9:18] + "-" + token[18:27] + "-1000"
}

func (s Scrubber) fakeDomainSID(value string) string {
	token := s.decimalToken("domain-sid", value, 27)
	return "S-1-5-21-" + token[0:9] + "-" + token[9:18] + "-" + token[18:27]
}

func (s Scrubber) fakeGUID(namespace, value string) string {
	token := s.token(namespace, value, 32)
	return token[0:8] + "-" + token[8:12] + "-" + token[12:16] + "-" + token[16:20] + "-" + token[20:32]
}

func (s Scrubber) fakeDistinguishedName(value string) string {
	parts := strings.Split(s.config.FakeDomain, ".")
	dcParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			dcParts = append(dcParts, "DC="+part)
		}
	}
	return "CN=" + s.fakePrincipal(value) + ",OU=scrubbed," + strings.Join(dcParts, ",")
}

func (s Scrubber) fakeURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		return "https://" + s.fakeHost("host", value) + "/resource/" + s.token("url-path", value, 12)
	}

	scheme := parsed.Scheme
	if strings.EqualFold(scheme, "ldap") || strings.EqualFold(scheme, "ldaps") {
		return scheme + "://" + s.fakeHost("ldap", value)
	}

	return scheme + "://" + s.fakeHost("host", value) + "/resource/" + s.token("url-path", value, 12)
}

func (s Scrubber) fakeSPN(value string) string {
	if matches := spnPattern.FindStringSubmatch(value); len(matches) == 4 {
		return matches[1] + "/" + s.fakeHost("svc", value) + matches[2] + matches[3]
	}
	return "svc/" + s.fakeHost("svc", value)
}

func (s Scrubber) fakeIPv4(value string) string {
	token := s.decimalToken("ipv4", value, 3)
	lastOctet, _ := strconv.Atoi(token)
	return fmt.Sprintf("198.51.100.%d", (lastOctet%254)+1)
}

func (s Scrubber) fakeMAC(value string) string {
	token := s.token("mac", value, 10)
	return "02:00:" + token[0:2] + ":" + token[2:4] + ":" + token[4:6] + ":" + token[6:8]
}

func (s Scrubber) token(namespace, value string, length int) string {
	mac := hmac.New(sha256.New, []byte(s.config.Salt))
	mac.Write([]byte(namespace))
	mac.Write([]byte{0})
	mac.Write([]byte(value))
	token := hex.EncodeToString(mac.Sum(nil))
	if length <= 0 || length > len(token) {
		return token
	}
	return token[:length]
}

func (s Scrubber) decimalToken(namespace, value string, length int) string {
	hexToken := s.token(namespace, value, length*2)
	var builder strings.Builder
	for idx := 0; builder.Len() < length && idx+2 <= len(hexToken); idx += 2 {
		nextByte, _ := strconv.ParseUint(hexToken[idx:idx+2], 16, 8)
		builder.WriteByte(byte('0' + (nextByte % 10)))
	}
	return builder.String()
}

func shouldShiftTimestamp(key string, value any) bool {
	normalizedKey := NormalizeSensitiveKey(key)
	if !(strings.Contains(normalizedKey, "timestamp") || strings.HasSuffix(normalizedKey, "time") || strings.HasSuffix(normalizedKey, "date")) {
		return false
	}

	switch typedValue := value.(type) {
	case string:
		return strings.TrimSpace(typedValue) != ""
	case []string:
		return len(typedValue) > 0
	case []any:
		return len(typedValue) > 0
	default:
		return false
	}
}

func shouldRedact(key string, shapes []string) bool {
	normalizedKey := NormalizeSensitiveKey(key)
	if strings.Contains(normalizedKey, "description") || strings.Contains(normalizedKey, "note") {
		return true
	}

	for _, shape := range shapes {
		if shape == "long_text" || shape == "pem_header" {
			return true
		}
	}

	return false
}

func uniqueStrings(values []string) []string {
	var unique []string
	seen := map[string]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			unique = append(unique, value)
		}
	}
	return unique
}

func appendUnique(values []string, next string) []string {
	for _, value := range values {
		if value == next {
			return values
		}
	}
	return append(values, next)
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}
