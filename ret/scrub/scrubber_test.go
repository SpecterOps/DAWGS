package scrub

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestScrubber(t *testing.T) *Scrubber {
	t.Helper()
	config := DefaultConfig()
	config.Salt = "test-salt"
	scrubber, err := New(config)
	require.NoError(t, err)
	return scrubber
}

func TestScrubPseudonymizesSensitiveValues(t *testing.T) {
	scrubber := newTestScrubber(t)
	properties := map[string]any{
		"name":     "Alice",
		"email":    "alice@example.com",
		"password": "super-secret",
	}

	counts := scrubber.Scrub(properties)

	for key, raw := range map[string]string{
		"name":     "Alice",
		"email":    "alice@example.com",
		"password": "super-secret",
	} {
		require.NotEqual(t, raw, properties[key], key)
	}
	require.EqualValues(t, 2, counts["pseudonymize"])
	require.EqualValues(t, 1, counts["redact"])
}

func TestScrubIsDeterministic(t *testing.T) {
	left := newTestScrubber(t)
	right := newTestScrubber(t)
	leftProperties := map[string]any{"email": "alice@example.com"}
	rightProperties := map[string]any{"email": "alice@example.com"}

	left.Scrub(leftProperties)
	right.Scrub(rightProperties)

	require.Equal(t, leftProperties, rightProperties)
}

func TestScrubShapeSpecificPseudonyms(t *testing.T) {
	scrubber := newTestScrubber(t)
	cases := []struct {
		name    string
		key     string
		value   string
		pattern *regexp.Regexp
	}{
		{"email", "email", "alice@example.com", regexp.MustCompile(`^user-[0-9a-f]{12}@example\.invalid$`)},
		{"uuid", "value", "00112233-4455-6677-8899-aabbccddeeff", regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)},
		{"domain sid", "value", "S-1-5-21-1-2-3", regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}$`)},
		{"object sid", "value", "S-1-5-21-1-2-3-500", regexp.MustCompile(`^S-1-5-21-\d{9}-\d{9}-\d{9}-500$`)},
		{"ipv4", "value", "192.0.2.10", regexp.MustCompile(`^10\.\d+\.\d+\.\d+$`)},
		{"host", "value", "server.example.com", regexp.MustCompile(`^host-[0-9a-f]{12}\.example\.invalid$`)},
		{"generic", "value", "Alice", regexp.MustCompile(`^value-[0-9a-f]{16}$`)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			properties := map[string]any{testCase.key: testCase.value}
			scrubber.Scrub(properties)
			got, ok := properties[testCase.key].(string)
			require.True(t, ok)
			require.NotEqual(t, testCase.value, got)
			require.Regexp(t, testCase.pattern, got)
		})
	}
}

func TestScrubObjectSIDShapeFallbackUsesGenericPseudonym(t *testing.T) {
	config := DefaultConfig()
	config.Salt = "test-salt"
	config.Rules.Classifier.ValueShapePatterns = []ValueShapeConfig{{
		Name:    "object_sid",
		Pattern: "^not-a-sid$",
	}}
	scrubber, err := New(config)
	require.NoError(t, err)
	properties := map[string]any{"value": "not-a-sid"}

	scrubber.Scrub(properties)

	require.Regexp(t, `^value-[0-9a-f]{16}$`, properties["value"])
}

func TestScrubTimestampAndRedactionBranches(t *testing.T) {
	scrubber := newTestScrubber(t)
	properties := map[string]any{
		"description":  strings.Repeat("x", DefaultConfig().Rules.Classifier.LongTextThreshold+1),
		"email_map":    map[string]any{"primary": "alice@example.com"},
		"seen_at":      "2026-01-01T00:00:00Z",
		"created_unix": 1767225600,
		"updated_time": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"deleted_at":   []string{"2026-01-01T00:00:00Z"},
	}

	counts := scrubber.Scrub(properties)

	require.Equal(t, "[REDACTED]", properties["description"])
	require.Equal(t, "2026-01-18T00:00:00Z", properties["seen_at"])
	require.Equal(t, 1768694400, properties["created_unix"])
	require.Equal(t, "2026-01-18T00:00:00Z", properties["updated_time"])
	require.Equal(t, []string{"2026-01-18T00:00:00Z"}, properties["deleted_at"])
	require.EqualValues(t, 1, counts["redact"])
	require.EqualValues(t, 4, counts["shift_timestamp"])
	require.EqualValues(t, 1, counts["pseudonymize"])
}

func TestScrubTimestampKeyHeuristics(t *testing.T) {
	scrubber := newTestScrubber(t)
	timestampKeys := []string{
		"timestamp",
		"created_at",
		"updated_at",
		"deleted_at",
		"modified_at",
		"seen_at",
	}
	properties := make(map[string]any, len(timestampKeys))
	for _, key := range timestampKeys {
		require.True(t, isTimestampKey(normalizeKey(key)), key)
		require.Equal(t, actionShiftTimestamp, scrubber.planProperty(key, "2026-01-01T00:00:00Z"), key)
		properties[key] = "2026-01-01T00:00:00Z"
	}

	counts := scrubber.Scrub(properties)

	require.EqualValues(t, len(timestampKeys), counts["shift_timestamp"])
	for key, value := range properties {
		require.Equal(t, "2026-01-18T00:00:00Z", value, key)
	}

	nonTimestampKeys := []string{
		"format",
		"seat",
		"heat",
		"coat",
		"float",
	}
	nonTimestampProperties := make(map[string]any, len(nonTimestampKeys))
	for _, key := range nonTimestampKeys {
		require.False(t, isTimestampKey(normalizeKey(key)), key)
		require.NotEqual(t, actionShiftTimestamp, scrubber.planProperty(key, "json"), key)
		nonTimestampProperties[key] = "json"
	}

	counts = scrubber.Scrub(nonTimestampProperties)

	require.Zero(t, counts["shift_timestamp"])
}

func TestScrubRedactsFreeTextFields(t *testing.T) {
	scrubber := newTestScrubber(t)
	properties := map[string]any{
		"description": "Work item ABC123 service owner Example Person for Example Division",
		"comments":    "Read only access to placeholder application resource",
		"info":        "Example location operations notes",
	}

	counts := scrubber.Scrub(properties)

	for key, value := range properties {
		require.Equal(t, "[REDACTED]", value, key)
	}
	require.EqualValues(t, 3, counts["redact"])
}

func TestScrubPseudonymizesPathAndScriptFields(t *testing.T) {
	scrubber := newTestScrubber(t)
	properties := map[string]any{
		"homedirectory": `\\fileserver01\share\account123`,
		"profilepath":   `\\profilehost01\profiles\group\account456`,
		"logonscript":   `startup\login.bat`,
	}

	counts := scrubber.Scrub(properties)

	require.EqualValues(t, 3, counts["pseudonymize"])
	for key, forbidden := range map[string][]string{
		"homedirectory": {"fileserver01", "share", "account123"},
		"profilepath":   {"profilehost01", "profiles", "account456"},
		"logonscript":   {"startup", "login"},
	} {
		got, ok := properties[key].(string)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(got, "value-"))
		for _, value := range forbidden {
			require.NotContains(t, got, value)
		}
	}
}

func TestScrubPseudonymizesUnknownStringsAndPreservesSafeScalars(t *testing.T) {
	scrubber := newTestScrubber(t)
	properties := map[string]any{
		"business_justification": "Read only access to placeholder application for example organization",
		"enabled":                true,
		"risk_score":             42,
	}

	counts := scrubber.Scrub(properties)

	require.EqualValues(t, 1, counts["pseudonymize"])
	require.EqualValues(t, 2, counts["preserve"])
	require.Regexp(t, `^value-[0-9a-f]{16}$`, properties["business_justification"])
	require.Equal(t, true, properties["enabled"])
	require.Equal(t, 42, properties["risk_score"])
}

func TestScrubPseudonymizesTicketLikeValues(t *testing.T) {
	properties := map[string]any{"request_id": "WORKITEM-12345"}
	counts := newTestScrubber(t).Scrub(properties)

	require.EqualValues(t, 1, counts["pseudonymize"])
	require.Regexp(t, `^value-[0-9a-f]{16}$`, properties["request_id"])
}

func TestScrubRecordFixturePreservesReferenceConsistencyAndActionCounts(t *testing.T) {
	scrubber := newTestScrubber(t)
	nested := map[string]any{
		"email":  "alice@example.com",
		"values": []any{"alpha", "beta"},
	}
	records := []struct {
		name       string
		properties map[string]any
		want       ActionCounts
	}{
		{
			name: "first node",
			properties: map[string]any{
				"objectid":           "S-1-5-21-1-2-3-500",
				"owner_sid":          "S-1-5-21-1-2-3-501",
				"unrelated_repeat":   "S-1-5-21-1-2-3-500",
				"domain_sid_history": []any{"S-1-5-21-1-2-3", "", 42},
				"nested":             nested,
				"description":        "free text",
				"created_at":         "2026-01-01T00:00:00Z",
				"enabled":            true,
			},
			want: ActionCounts{"pseudonymize": 6, "preserve": 2, "redact": 1, "shift_timestamp": 1},
		},
		{
			name: "second node",
			properties: map[string]any{
				"objectid":  "S-1-5-21-1-2-3-501",
				"owner_sid": " S-1-5-21-1-2-3-500 ",
				"name":      "ALICE",
				"empty":     "",
			},
			want: ActionCounts{"pseudonymize": 3, "preserve": 1},
		},
		{
			name: "first edge",
			properties: map[string]any{
				"owner_sid": "S-1-5-21-1-2-3-500",
				"path":      `\\server\share\alice`,
				"password":  "secret",
			},
			want: ActionCounts{"pseudonymize": 2, "redact": 1},
		},
		{
			name: "third node",
			properties: map[string]any{
				"objectid":  "S-1-5-21-9-8-7-1000",
				"owner_sid": "S-1-5-21-1-2-3-500",
			},
			want: ActionCounts{"pseudonymize": 2},
		},
		{
			name: "second edge",
			properties: map[string]any{
				"owner_sid": "S-1-5-21-9-8-7-1000",
				"office":    "North",
			},
			want: ActionCounts{"pseudonymize": 2},
		},
	}

	for _, record := range records {
		t.Run(record.name, func(t *testing.T) {
			require.Equal(t, record.want, scrubber.Scrub(record.properties))
		})
	}

	first := records[0].properties
	second := records[1].properties
	third := records[3].properties

	require.Equal(t, first["objectid"], first["unrelated_repeat"])
	require.Equal(t, first["objectid"], second["owner_sid"])
	require.NotEqual(t, first["objectid"], first["owner_sid"])
	require.NotEqual(t, first["objectid"], third["objectid"])

	history, ok := first["domain_sid_history"].([]any)
	require.True(t, ok)
	require.Len(t, history, 3)
	require.Regexp(t, `^S-1-5-21-\d{9}-\d{9}-\d{9}$`, history[0])
	require.Equal(t, "", history[1])
	require.Equal(t, 42, history[2])

	// A preserved top-level map remains caller-owned and is recursively scrubbed.
	require.Regexp(t, `^user-[0-9a-f]{12}@example\.invalid$`, nested["email"])
	values, ok := nested["values"].([]any)
	require.True(t, ok)
	require.Len(t, values, 2)
	for _, value := range values {
		require.Regexp(t, `^value-[0-9a-f]{16}$`, value)
	}
}

func TestScrubMutatesNestedValueBecauseInputCopyIsShallow(t *testing.T) {
	nested := map[string]any{"password": "secret"}
	properties := map[string]any{"nested": nested}

	newTestScrubber(t).Scrub(properties)

	require.Equal(t, "[REDACTED]", nested["password"])
}

func TestScrubDisabledDoesNotMutate(t *testing.T) {
	config := DefaultConfig()
	config.Enabled = false
	scrubber, err := New(config)
	require.NoError(t, err)
	properties := map[string]any{"password": "secret"}

	counts := scrubber.Scrub(properties)

	require.Empty(t, counts)
	require.Equal(t, "secret", properties["password"])
}

func TestScrubPropertyPlanCacheIsBounded(t *testing.T) {
	scrubber := newTestScrubber(t)
	for index := 0; index < maxPropertyPlans+100; index++ {
		scrubber.planKey("unique-property-" + strconv.Itoa(index))
	}

	require.Len(t, scrubber.propertyPlans, maxPropertyPlans)
}

func TestFingerprintsAreStableAndSaltIsNotExposed(t *testing.T) {
	config := DefaultConfig()
	config.Salt = "private"
	first, err := New(config)
	require.NoError(t, err)
	second, err := New(config)
	require.NoError(t, err)

	require.Equal(t, first.RulesFingerprint(), second.RulesFingerprint())
	require.Equal(t, first.SaltFingerprint(), second.SaltFingerprint())
	require.NotContains(t, first.SaltFingerprint(), "private")
	require.Regexp(t, `^[0-9a-f]{64}$`, first.RulesFingerprint())
	require.Regexp(t, `^[0-9a-f]{64}$`, first.SaltFingerprint())
}

func TestNewTrimsSaltLikeLegacyPolicy(t *testing.T) {
	trimmedConfig := DefaultConfig()
	trimmedConfig.Salt = "test-salt"
	trimmed, err := New(trimmedConfig)
	require.NoError(t, err)
	spacedConfig := DefaultConfig()
	spacedConfig.Salt = " test-salt "
	spaced, err := New(spacedConfig)
	require.NoError(t, err)
	trimmedProperties := map[string]any{"email": "alice@example.com"}
	spacedProperties := map[string]any{"email": "alice@example.com"}

	trimmed.Scrub(trimmedProperties)
	spaced.Scrub(spacedProperties)

	require.Equal(t, trimmedProperties, spacedProperties)
	require.Equal(t, trimmed.SaltFingerprint(), spaced.SaltFingerprint())
}
