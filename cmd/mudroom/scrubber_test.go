package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestClassifyValueDetectsSensitiveShapes(t *testing.T) {
	classifier := testClassifier(t)

	tests := []struct {
		name   string
		value  any
		shapes []string
	}{
		{name: "email", value: "alice@example.com", shapes: []string{"email"}},
		{name: "sid", value: "S-1-5-21-100-200-300-500", shapes: []string{"sid"}},
		{name: "guid", value: "01234567-89ab-cdef-0123-456789abcdef", shapes: []string{"guid"}},
		{name: "hostname", value: "host.example.com", shapes: []string{"hostname"}},
		{name: "unc path", value: `\\host.example.com\share\folder`, shapes: []string{"unc_path"}},
		{name: "azure resource id", value: "/subscriptions/01234567-89ab-cdef-0123-456789abcdef/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm01", shapes: []string{"azure_resource_id"}},
		{name: "long text", value: "this is a long free-text description that should be treated as unsafe to preserve verbatim because it can contain organization-specific details", shapes: []string{"long_text"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifier.ClassifyValue(tt.value)
			for _, expected := range tt.shapes {
				if !contains(got, expected) {
					t.Fatalf("expected shape %q in %v", expected, got)
				}
			}
		})
	}
}

func TestScrubPropertyPseudonymizesEmailDeterministically(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")

	firstValue, plan := scrubber.ScrubProperty("userprincipalname", "alice@corp.example")
	secondValue, _ := testScrubber(t, "unit-test-salt").ScrubProperty("userprincipalname", "alice@corp.example")
	thirdValue, _ := testScrubber(t, "different-salt").ScrubProperty("userprincipalname", "alice@corp.example")

	firstString, ok := firstValue.(string)
	if !ok {
		t.Fatalf("expected string replacement, got %T", firstValue)
	}
	if firstString == "alice@corp.example" {
		t.Fatal("expected email to be replaced")
	}
	if !strings.Contains(firstString, "@mail-") || !strings.HasSuffix(firstString, ".example.invalid") {
		t.Fatalf("expected fake email domain, got %q", firstString)
	}
	if firstValue != secondValue {
		t.Fatalf("expected stable pseudonym for same salt, got %q and %q", firstValue, secondValue)
	}
	if firstValue == thirdValue {
		t.Fatalf("expected different pseudonym for different salt, got %q", firstValue)
	}
	if plan.Action != ActionPseudonymize || !contains(plan.Shapes, "email") || !contains(plan.Reasons, "value_shape") {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

func TestScrubPropertyPreservesConfiguredStructuralValues(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")

	value, plan := scrubber.ScrubProperty("operatingsystemversion", "10.0.19044")
	if value != "10.0.19044" {
		t.Fatalf("expected OS version to be preserved, got %q", value)
	}
	if plan.Action != ActionPreserve || !contains(plan.Reasons, "preserve_key") {
		t.Fatalf("unexpected preserve plan: %+v", plan)
	}
}

func TestScrubPropertyRedactsFreeTextAndShiftsTimestamps(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")

	value, plan := scrubber.ScrubProperty("description", "This system belongs to Example Corp and the CEO uses it for testing.")
	if value != "[REDACTED]" || plan.Action != ActionRedact {
		t.Fatalf("expected redaction, got value=%q plan=%+v", value, plan)
	}

	value, plan = scrubber.ScrubProperty("customTimestamp", "2026-05-10T12:00:00Z")
	if value != "2026-06-16T12:00:00Z" || plan.Action != ActionShiftTimestamp {
		t.Fatalf("expected shifted timestamp, got value=%q plan=%+v", value, plan)
	}
}

func TestScrubPropertiesHandlesNestedSensitiveValues(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")

	scrubbed, plans := scrubber.ScrubProperties(map[string]any{
		"metadata": map[string]any{
			"ownerSid": "S-1-5-21-100-200-300-500",
			"note":     "belongs to Example Corp",
		},
	})

	metadata, ok := scrubbed["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map, got %T", scrubbed["metadata"])
	}
	if metadata["ownerSid"] == "S-1-5-21-100-200-300-500" {
		t.Fatalf("expected nested SID to be replaced: %+v", metadata)
	}
	if !strings.HasPrefix(metadata["ownerSid"].(string), "S-1-5-21-") {
		t.Fatalf("expected fake SID shape, got %q", metadata["ownerSid"])
	}
	if metadata["note"] != "[REDACTED]" {
		t.Fatalf("expected nested note to be redacted, got %+v", metadata)
	}
	if len(plans) != 1 || plans[0].Action != ActionPseudonymize {
		t.Fatalf("unexpected top-level plans: %+v", plans)
	}
}

func TestPropertyPlanDoesNotIncludeRawValues(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")
	_, plan := scrubber.ScrubProperty("userprincipalname", "secret-user@example.com")

	contents, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "secret-user@example.com") {
		t.Fatalf("plan leaked raw value: %s", contents)
	}
}

func testClassifier(t *testing.T) Classifier {
	t.Helper()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}

	classifier, err := cfg.Classifier.Compile()
	if err != nil {
		t.Fatal(err)
	}

	return classifier
}

func testScrubber(t *testing.T, salt string) Scrubber {
	t.Helper()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scrub.Salt = salt

	scrubber, err := NewScrubber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return scrubber
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
