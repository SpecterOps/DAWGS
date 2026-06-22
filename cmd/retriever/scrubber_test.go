package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestScrubberPseudonymizesSensitiveValues(t *testing.T) {
	scrubber, err := newScrubber("", "test-salt")
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
	left, err := newScrubber("", "test-salt")
	if err != nil {
		t.Fatalf("new left scrubber: %v", err)
	}
	right, err := newScrubber("", "test-salt")
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

	scrubber, err := newScrubber(configPath, "test-salt")
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
	scrubber, err := newScrubber("", "test-salt")
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
	scrubber, err := newScrubber("", "test-salt")
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

func TestScrubberIdentifierRegistryConsistency(t *testing.T) {
	scrubber, err := newScrubber("", "test-salt")
	if err != nil {
		t.Fatalf("new scrubber: %v", err)
	}

	sourceSID := "S-1-5-21-1-2-3-500"
	scrubber.observeNode(map[string]any{"objectid": sourceSID})
	scrubbed, _ := scrubber.scrubProperties(map[string]any{
		"objectid":  sourceSID,
		"owner_sid": sourceSID,
	})
	if scrubbed["objectid"] != scrubbed["owner_sid"] {
		t.Fatalf("registry rewrite mismatch: objectid=%#v owner_sid=%#v", scrubbed["objectid"], scrubbed["owner_sid"])
	}
	if scrubbed["objectid"] == sourceSID {
		t.Fatalf("identifier was not scrubbed")
	}
}

func writeTestFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
