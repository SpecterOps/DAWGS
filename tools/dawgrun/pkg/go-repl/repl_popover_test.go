package repl

import "testing"

func TestCompletionPopoverMode(t *testing.T) {
	r := &Repl{}

	t.Setenv("REPL_COMPLETION_POPOVER_MODE", "")
	t.Setenv("ZELLIJ", "")
	if mode := r.completionPopoverMode(); mode != "overlay" {
		t.Fatalf("expected overlay mode by default, got %q", mode)
	}

	t.Setenv("ZELLIJ", "1")
	if mode := r.completionPopoverMode(); mode != "overlay" {
		t.Fatalf("expected overlay mode in zellij, got %q", mode)
	}

	t.Setenv("REPL_COMPLETION_POPOVER_MODE", "insert")
	if mode := r.completionPopoverMode(); mode != "insert" {
		t.Fatalf("expected insert mode override, got %q", mode)
	}

	t.Setenv("REPL_COMPLETION_POPOVER_MODE", "overlay")
	if mode := r.completionPopoverMode(); mode != "overlay" {
		t.Fatalf("expected overlay mode override, got %q", mode)
	}
}

func TestClampCompletionLines(t *testing.T) {
	testCases := []struct {
		name   string
		lines  []string
		max    int
		expect []string
	}{
		{name: "no rows", lines: []string{"a", "b"}, max: 0, expect: nil},
		{name: "fits", lines: []string{"a", "b"}, max: 2, expect: []string{"a", "b"}},
		{name: "single row summary", lines: []string{"a", "b", "c"}, max: 1, expect: []string{"3 matches"}},
		{name: "truncated with remainder", lines: []string{"a", "b", "c", "d"}, max: 2, expect: []string{"a", "... 3 more"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampCompletionLines(tc.lines, tc.max)
			if len(got) != len(tc.expect) {
				t.Fatalf("expected %d lines, got %d (%v)", len(tc.expect), len(got), got)
			}

			for i := range tc.expect {
				if got[i] != tc.expect[i] {
					t.Fatalf("expected line[%d]=%q, got %q", i, tc.expect[i], got[i])
				}
			}
		})
	}
}

func TestIsCursorQueryResponse(t *testing.T) {
	testCases := []struct {
		name   string
		bytes  []byte
		expect bool
	}{
		{name: "standard response", bytes: []byte{27, 91, 50, 48, 59, 49, 82}, expect: true},
		{name: "response with prefix text", bytes: []byte{'x', 'y', 27, 91, 49, 59, 49, 82}, expect: true},
		{name: "arrow left", bytes: []byte{27, 91, 68}, expect: false},
		{name: "regular character", bytes: []byte{'q'}, expect: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCursorQueryResponse(tc.bytes); got != tc.expect {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
		})
	}
}

func TestTruncateCompletionLine(t *testing.T) {
	testCases := []struct {
		name   string
		line   string
		width  int
		expect string
	}{
		{name: "fits", line: "hello", width: 8, expect: "hello"},
		{name: "small width", line: "hello", width: 2, expect: "he"},
		{name: "ellipsis", line: "completion-candidate", width: 10, expect: "complet..."},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateCompletionLine(tc.line, tc.width); got != tc.expect {
				t.Fatalf("expected %q, got %q", tc.expect, got)
			}
		})
	}
}
