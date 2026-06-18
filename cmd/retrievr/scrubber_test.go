package main

import (
	"os"
	"path/filepath"
	"testing"
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
	configPath := filepath.Join(t.TempDir(), "retrievr.toml")
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

func writeTestFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
