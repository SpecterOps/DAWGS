package retriever

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestScrubberPseudonymizesSensitiveValues(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"name":     "Alice",
		"email":    "alice@example.com",
		"password": "super-secret",
	})

	for key, raw := range map[string]string{
		"name":     "Alice",
		"email":    "alice@example.com",
		"password": "super-secret",
	} {
		if scrubbed[key] == raw {
			t.Fatalf("expected %s to be scrubbed", key)
		}
	}

	if counts[string(actionPseudonymize)] == 0 {
		t.Fatalf("expected pseudonymize action count")
	}

	if counts[string(actionRedact)] == 0 {
		t.Fatalf("expected redact action count")
	}
}

func TestScrubberDeterministic(t *testing.T) {
	left, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new left scrubber: %v", err)
	}

	right, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new right scrubber: %v", err)
	}

	leftValue, _ := left.scrubProperty("email", "alice@example.com")
	rightValue, _ := right.scrubProperty("email", "alice@example.com")

	if leftValue != rightValue {
		t.Fatalf("expected deterministic scrub output, got %q and %q", leftValue, rightValue)
	}
}

func TestScrubberConfigFileShape(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "retriever.toml")
	writeTestFile(t, configPath, []byte(`
[scrub]
fake_domain = "scrub.example"
redaction_marker = "[X]"

[classifier]
long_text_threshold = 8
`))

	configFile, err := os.Open(configPath)
	if err != nil {
		t.Fatalf("open scrub config: %v", err)
	}
	defer configFile.Close()

	scrubber, err := newScrubber(configFile, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	if scrubber.config.FakeDomain != "scrub.example" {
		t.Fatalf("fake domain = %q", scrubber.config.FakeDomain)
	}

	if scrubber.config.RedactionMarker != "[X]" {
		t.Fatalf("redaction marker = %q", scrubber.config.RedactionMarker)
	}

	if scrubber.config.Classifier.LongTextThreshold != 8 {
		t.Fatalf("long text threshold = %d", scrubber.config.Classifier.LongTextThreshold)
	}
}

func TestScrubberShapeSpecificPseudonyms(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	cases := []struct {
		name    string
		value   string
		shape   string
		pattern *regexp.Regexp
	}{
		{
			name:    "email",
			value:   "alice@example.com",
			shape:   "email",
			pattern: regexp.MustCompile(`^user-[0-9a-f]{12}@example\.invalid$`),
		},
		{
			name:    "uuid",
			value:   "00112233-4455-6677-8899-aabbccddeeff",
			shape:   "uuid",
			pattern: regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`),
		},
		{
			name:    "domain sid",
			value:   "S-1-5-21-1-2-3",
			shape:   "domain_sid",
			pattern: regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}$`),
		},
		{
			name:    "object sid",
			value:   "S-1-5-21-1-2-3-500",
			shape:   "object_sid",
			pattern: regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}-500$`),
		},
		{
			name:    "object sid shape fallback",
			value:   "not-a-sid",
			shape:   "object_sid",
			pattern: regexp.MustCompile(`^value-[0-9a-f]{16}$`),
		},
		{
			name:    "ipv4",
			value:   "192.0.2.10",
			shape:   "ipv4",
			pattern: regexp.MustCompile(`^10\.\d+\.\d+\.\d+$`),
		},
		{
			name:    "host",
			value:   "server.example.com",
			shape:   "host",
			pattern: regexp.MustCompile(`^host-[0-9a-f]{12}\.example\.invalid$`),
		},
		{
			name:    "generic",
			value:   "Alice",
			shape:   "",
			pattern: regexp.MustCompile(`^value-[0-9a-f]{16}$`),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := scrubber.pseudonymizeString(testCase.value, testCase.shape)
			if got == testCase.value {
				t.Fatalf("value was not pseudonymized")
			}

			if !testCase.pattern.MatchString(got) {
				t.Fatalf("pseudonym %q did not match %s", got, testCase.pattern)
			}
		})
	}
}

func TestScrubberTimestampAndRedactionBranches(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	shifted := scrubber.shiftTimestamp("2026-01-01T00:00:00Z")
	if shifted != "2026-01-18T00:00:00Z" {
		t.Fatalf("shifted timestamp = %v", shifted)
	}

	shiftedTime := scrubber.shiftTimestamp(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if shiftedTime != "2026-01-18T00:00:00Z" {
		t.Fatalf("shifted time.Time = %v", shiftedTime)
	}

	shiftedSlice := scrubber.shiftTimestamp([]string{"2026-01-01T00:00:00Z"})
	if values, ok := shiftedSlice.([]string); !ok || len(values) != 1 || values[0] != "2026-01-18T00:00:00Z" {
		t.Fatalf("shifted slice = %#v", shiftedSlice)
	}

	redacted := scrubber.redact(map[string]any{"a": "secret", "b": "secret"})
	redactedMap, ok := redacted.(map[string]any)

	if !ok || redactedMap["a"] != scrubber.config.RedactionMarker || redactedMap["b"] != scrubber.config.RedactionMarker {
		t.Fatalf("redacted map = %#v", redacted)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"description": strings.Repeat("x", scrubber.config.Classifier.LongTextThreshold+1),
		"email_map":   map[string]any{"primary": "alice@example.com"},
		"seen_at":     "2026-01-01T00:00:00Z",
	})
	if counts[string(actionRedact)] == 0 || counts[string(actionShiftTimestamp)] == 0 || counts[string(actionPseudonymize)] == 0 {
		t.Fatalf("expected redact, shift, and pseudonymize counts, got %+v", counts)
	}

	if scrubbed["description"] != scrubber.config.RedactionMarker {
		t.Fatalf("long text was not redacted: %#v", scrubbed["description"])
	}

	if scrubbed["seen_at"] != "2026-01-18T00:00:00Z" {
		t.Fatalf("timestamp property was not shifted: %#v", scrubbed["seen_at"])
	}
}

func TestScrubberTimestampKeyHeuristics(t *testing.T) {
	timestampKeys := []string{
		"timestamp",
		"created_at",
		"updated_at",
		"deleted_at",
		"modified_at",
		"seen_at",
	}

	for _, key := range timestampKeys {
		if !isTimestampKey(normalizeKey(key)) {
			t.Fatalf("expected %q to be a timestamp key", key)
		}
	}

	nonTimestampKeys := []string{
		"format",
		"seat",
		"heat",
		"coat",
		"float",
	}

	for _, key := range nonTimestampKeys {
		if isTimestampKey(normalizeKey(key)) {
			t.Fatalf("expected %q not to be a timestamp key", key)
		}
	}

	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	if plan := scrubber.planProperty("format", "json"); plan.Action == actionShiftTimestamp {
		t.Fatalf("format was planned as timestamp: %+v", plan)
	}
}

func TestScrubberRedactsFreeTextFields(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"description": "Work item ABC123 service owner Example Person for Example Division",
		"comments":    "Read only access to placeholder application resource",
		"info":        "Example location operations notes",
	})

	for key, value := range scrubbed {
		if value != scrubber.config.RedactionMarker {
			t.Fatalf("expected %s to be redacted, got %#v", key, value)
		}
	}

	if counts[string(actionRedact)] != 3 {
		t.Fatalf("redact count = %d, want 3", counts[string(actionRedact)])
	}
}

func TestScrubberPseudonymizesPathAndScriptFields(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"homedirectory": `\\fileserver01\share\account123`,
		"profilepath":   `\\profilehost01\profiles\group\account456`,
		"logonscript":   `startup\login.bat`,
	})

	if counts[string(actionPseudonymize)] != 3 {
		t.Fatalf("pseudonymize count = %d, want 3", counts[string(actionPseudonymize)])
	}

	assertScrubbedStringDoesNotContain(t, scrubbed["homedirectory"], "fileserver01", "share", "account123")
	assertScrubbedStringDoesNotContain(t, scrubbed["profilepath"], "profilehost01", "profiles", "account456")
	assertScrubbedStringDoesNotContain(t, scrubbed["logonscript"], "startup", "login")

	home, ok := scrubbed["homedirectory"].(string)

	if !ok || !strings.HasPrefix(home, "value-") {
		t.Fatalf("home directory was not pseudonymized as generic value: %#v", scrubbed["homedirectory"])
	}

	script, ok := scrubbed["logonscript"].(string)

	if !ok || !strings.HasPrefix(script, "value-") {
		t.Fatalf("logon script was not pseudonymized as generic value: %#v", scrubbed["logonscript"])
	}
}

func TestScrubberPseudonymizesUnknownStringsInFullScrub(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"business_justification": "Read only access to placeholder application for example organization",
		"enabled":                true,
		"risk_score":             42,
	})

	if counts[string(actionPseudonymize)] != 1 || counts[string(actionPreserve)] != 2 {
		t.Fatalf("unexpected action counts: %+v", counts)
	}

	assertScrubbedStringDoesNotContain(t, scrubbed["business_justification"], "placeholder", "application", "organization")

	if got, ok := scrubbed["business_justification"].(string); !ok || !strings.HasPrefix(got, "value-") {
		t.Fatalf("unknown string was not pseudonymized as generic value: %#v", scrubbed["business_justification"])
	}

	if scrubbed["enabled"] != true || scrubbed["risk_score"] != 42 {
		t.Fatalf("safe scalar values were not preserved: %#v", scrubbed)
	}
}

func TestScrubberPseudonymizesTicketLikeValues(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	scrubbed, counts := scrubber.scrubProperties(map[string]any{
		"request_id": "WORKITEM-12345",
	})

	if counts[string(actionPseudonymize)] != 1 {
		t.Fatalf("expected pseudonymize count, got %+v", counts)
	}

	if got, ok := scrubbed["request_id"].(string); !ok || !strings.HasPrefix(got, "value-") {
		t.Fatalf("ticket was not pseudonymized as generic value: %#v", scrubbed["request_id"])
	}
}

func TestScrubberRegistryFreeOutputParityAndReferenceConsistency(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	type parityGraph struct {
		nodes []map[string]any
		edges []map[string]any
	}

	graphs := []parityGraph{
		{
			nodes: []map[string]any{
				{
					"objectid":           "S-1-5-21-1-2-3-500",
					"owner_sid":          "S-1-5-21-1-2-3-501",
					"unrelated_repeat":   "S-1-5-21-1-2-3-500",
					"domain_sid_history": []any{"S-1-5-21-1-2-3", "", 42},
					"nested":             map[string]any{"email": "alice@example.com", "values": []any{"alpha", "beta"}},
					"description":        "free text",
					"created_at":         "2026-01-01T00:00:00Z",
					"enabled":            true,
				},
				{
					"objectid":  "S-1-5-21-1-2-3-501",
					"owner_sid": " S-1-5-21-1-2-3-500 ",
					"name":      "ALICE",
					"empty":     "",
				},
			},
			edges: []map[string]any{{
				"owner_sid": "S-1-5-21-1-2-3-500",
				"path":      `\\server\share\alice`,
				"password":  "secret",
			}},
		},
		{
			nodes: []map[string]any{{
				"objectid":  "S-1-5-21-9-8-7-1000",
				"owner_sid": "S-1-5-21-1-2-3-500",
			}},
			edges: []map[string]any{{
				"owner_sid": "S-1-5-21-9-8-7-1000",
				"office":    "North",
			}},
		},
	}

	legacyLookup := map[string]string{}
	for _, nextGraph := range graphs {
		for _, properties := range nextGraph.nodes {
			legacyObserveNode(scrubber, legacyLookup, properties)
		}
	}

	var legacyRecords, registryFreeRecords []any
	for _, nextGraph := range graphs {
		for _, properties := range append(append([]map[string]any{}, nextGraph.nodes...), nextGraph.edges...) {
			legacyProperties, legacyCounts := legacyScrubProperties(scrubber, legacyLookup, properties)
			registryFreeProperties, registryFreeCounts := scrubber.scrubProperties(properties)
			legacyRecords = append(legacyRecords, []any{legacyProperties, legacyCounts})
			registryFreeRecords = append(registryFreeRecords, []any{registryFreeProperties, registryFreeCounts})
		}
	}

	legacyJSON, err := json.Marshal(legacyRecords)
	if err != nil {
		t.Fatalf("marshal legacy records: %v", err)
	}
	registryFreeJSON, err := json.Marshal(registryFreeRecords)
	if err != nil {
		t.Fatalf("marshal registry-free records: %v", err)
	}
	if string(legacyJSON) != string(registryFreeJSON) {
		t.Fatalf("registry-free scrub output changed decoded JSONL records\nlegacy: %s\ncurrent: %s", legacyJSON, registryFreeJSON)
	}

	first := registryFreeRecords[0].([]any)[0].(map[string]any)
	second := registryFreeRecords[1].([]any)[0].(map[string]any)
	if first["objectid"] != first["unrelated_repeat"] || first["objectid"] != second["owner_sid"] {
		t.Fatalf("equal source identifiers did not map consistently: %#v %#v", first, second)
	}
	if first["objectid"] == first["owner_sid"] {
		t.Fatalf("unequal source identifiers unexpectedly collided: %#v", first)
	}
}

func legacyObserveNode(scrubber *scrubber, lookup map[string]string, properties map[string]any) {
	for key, value := range properties {
		if _, ok := scrubber.referenceKeys[normalizeKey(key)]; !ok {
			continue
		}
		legacyObserveIdentifier(scrubber, lookup, value)
	}
}

func legacyObserveIdentifier(scrubber *scrubber, lookup map[string]string, value any) {
	switch typed := value.(type) {
	case string:
		if trimmed := strings.TrimSpace(typed); trimmed != "" {
			lookup[trimmed] = scrubber.pseudonymizeString(trimmed, scrubber.classifyString(trimmed))
		}
	case []any:
		for _, item := range typed {
			legacyObserveIdentifier(scrubber, lookup, item)
		}
	case []string:
		for _, item := range typed {
			legacyObserveIdentifier(scrubber, lookup, item)
		}
	}
}

func legacyScrubProperties(scrubber *scrubber, lookup map[string]string, properties map[string]any) (map[string]any, map[string]int) {
	scrubbed := make(map[string]any, len(properties))
	actionCounts := map[string]int{}
	for key, value := range properties {
		plan := scrubber.planProperty(key, value)
		if plan.Action == actionPseudonymize {
			scrubbed[key] = legacyPseudonymizeValue(scrubber, lookup, key, value, plan.Shape)
		} else {
			scrubbed[key] = scrubber.scrubWithPlan(key, value, plan)
		}
		actionCounts[string(plan.Action)]++
	}

	return scrubbed, actionCounts
}

func legacyPseudonymizeValue(scrubber *scrubber, lookup map[string]string, key string, value any, shape string) any {
	switch typed := value.(type) {
	case string:
		if replacement, ok := lookup[strings.TrimSpace(typed)]; ok {
			return replacement
		}
		if _, ok := scrubber.referenceKeys[normalizeKey(key)]; ok {
			return scrubber.pseudonymizeString(typed, scrubber.classifyString(typed))
		}
		return scrubber.pseudonymizeString(typed, shape)
	case []any:
		values := make([]any, 0, len(typed))
		for _, item := range typed {
			values = append(values, legacyPseudonymizeValue(scrubber, lookup, key, item, scrubber.classifyValue(item)))
		}
		return values
	case []string:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if replacement, ok := legacyPseudonymizeValue(scrubber, lookup, key, item, scrubber.classifyString(item)).(string); ok {
				values = append(values, replacement)
			}
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typed))
		for nestedKey, nestedValue := range typed {
			values[nestedKey] = legacyPseudonymizeValue(scrubber, lookup, nestedKey, nestedValue, scrubber.classifyValue(nestedValue))
		}
		return values
	default:
		return value
	}
}

func assertScrubbedStringDoesNotContain(t *testing.T, value any, forbidden ...string) {
	t.Helper()
	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("expected scrubbed string, got %#v", value)
	}

	for _, next := range forbidden {
		if strings.Contains(stringValue, next) {
			t.Fatalf("scrubbed value %q still contains %q", stringValue, next)
		}
	}
}

func TestScrubberPropertyPlanCacheIsBounded(t *testing.T) {
	scrubber, err := newScrubber(nil, "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}
	for index := 0; index < maxScrubPropertyPlans+100; index++ {
		scrubber.planKey("unique-property-" + strconv.Itoa(index))
	}
	if len(scrubber.propertyPlans) != maxScrubPropertyPlans {
		t.Fatalf("property plan cache size = %d, want %d", len(scrubber.propertyPlans), maxScrubPropertyPlans)
	}
}

func writeTestFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
