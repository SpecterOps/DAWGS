package metrics

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// failingHTMLWriter simulates an output failure after template execution begins.
type failingHTMLWriter struct {
	// err is the write failure returned to the report renderer.
	err error
}

// Write rejects each write with the configured error.
func (s failingHTMLWriter) Write(data []byte) (int, error) {
	return 0, s.err
}

// TestWriteHTMLPropagatesWriterError checks that rendering retains output error identity.
func TestWriteHTMLPropagatesWriterError(t *testing.T) {
	writeErr := errors.New("report output unavailable")
	err := WriteHTML(failingHTMLWriter{
		err: writeErr,
	}, Report{}, HTMLReportOptions{})
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected wrapped output error, got %v", err)
	}
}

// TestWriteHTMLConcurrentReports checks shared-template execution remains deterministic and escaped.
func TestWriteHTMLConcurrentReports(t *testing.T) {
	report := Report{
		Records: []FunctionMetric{{
			Package:    "sample",
			Function:   "<function>&",
			File:       "sample.go",
			Statements: 1,
		}},
	}
	var expected bytes.Buffer
	if err := WriteHTML(&expected, report, HTMLReportOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(expected.String(), "&lt;function&gt;&amp;") {
		t.Fatal("function name was not HTML escaped")
	}

	for range 8 {
		t.Run("render", func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			if err := WriteHTML(&output, report, HTMLReportOptions{}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expected.Bytes(), output.Bytes()) {
				t.Fatal("concurrent report differs from the initial render")
			}
		})
	}
}

// TestWriteHTMLSeverityBoundaries checks that report pills retain their published CRAP bands.
func TestWriteHTMLSeverityBoundaries(t *testing.T) {
	for _, test := range []struct {
		// name identifies the score boundary in subtest output.
		name string
		// score is the function's CRAP score at or immediately below a band boundary.
		score float64
		// pill is the expected rendered CSS class and human-readable severity label.
		pill string
	}{
		{
			name:  "below watch",
			score: 29.9,
			pill:  `<span class="pill ok">OK</span>`,
		},
		{
			name:  "watch",
			score: 30,
			pill:  `<span class="pill watch">Watch</span>`,
		},
		{
			name:  "high",
			score: 60,
			pill:  `<span class="pill high">High</span>`,
		},
		{
			name:  "critical",
			score: 100,
			pill:  `<span class="pill critical">Critical</span>`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := Report{
				Records: []FunctionMetric{{
					Function: "boundary",
					CRAP:     test.score,
				}},
			}
			var output bytes.Buffer
			if err := WriteHTML(&output, report, HTMLReportOptions{}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), test.pill) {
				t.Fatalf("report missing severity pill %q", test.pill)
			}
		})
	}
}
