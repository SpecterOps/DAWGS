package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

func WriteMarkdown(file io.Writer, nextReport Report) {
	fmt.Fprintf(file, "# PII Exploration Report\n\n")
	fmt.Fprintf(file, "- Generated: %s\n", nextReport.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(file, "- Driver: %s\n", nextReport.Driver)
	if nextReport.Graph != "" {
		fmt.Fprintf(file, "- Graph: %s\n", nextReport.Graph)
	}
	fmt.Fprintf(file, "- Nodes: %d\n", nextReport.NodeCount)
	fmt.Fprintf(file, "- Relationships: %d\n", nextReport.RelationshipCount)
	fmt.Fprintf(file, "- Node property sample limit: %d\n", nextReport.NodePropertySampleLimit)
	fmt.Fprintf(file, "- Relationship property sample limit: %d\n", nextReport.RelationshipPropertySampleLimit)
	fmt.Fprintf(file, "- Histogram limit: %d\n", nextReport.HistogramLimit)
	fmt.Fprintf(file, "- Out degree: min=%d avg=%.2f max=%d\n", nextReport.OutDegree.Min, nextReport.OutDegree.Average, nextReport.OutDegree.Max)
	fmt.Fprintf(file, "- In degree: min=%d avg=%.2f max=%d\n\n", nextReport.InDegree.Min, nextReport.InDegree.Average, nextReport.InDegree.Max)

	WriteHistogram(file, "Node Kinds", nextReport.NodeKindHistogram, nextReport.HistogramLimit)
	WriteHistogram(file, "Relationship Kinds", nextReport.RelationshipKindHistogram, nextReport.HistogramLimit)
	WritePropertyTable(file, "Node Properties", nextReport.NodeProperties)
	WritePropertyTable(file, "Relationship Properties", nextReport.RelationshipProperties)

	if len(nextReport.Notes) > 0 {
		fmt.Fprintf(file, "## Notes\n\n")
		for _, note := range nextReport.Notes {
			fmt.Fprintf(file, "- %s\n", note)
		}
		fmt.Fprintln(file)
	}
}

func WriteHistogram(file io.Writer, title string, histogram map[string]int, limit int) {
	fmt.Fprintf(file, "## %s\n\n", title)

	entries := SortedEntries(histogram)
	if len(entries) == 0 {
		fmt.Fprintln(file, "No values observed.")
		fmt.Fprintln(file)
		return
	}

	fmt.Fprintln(file, "| Value | Count |")
	fmt.Fprintln(file, "| --- | ---: |")

	for idx, entry := range entries {
		if limit > 0 && idx >= limit {
			break
		}
		fmt.Fprintf(file, "| `%s` | %d |\n", entry.Key, entry.Value)
	}

	fmt.Fprintln(file)
}

func WritePropertyTable(file io.Writer, title string, stats map[string]PropertyStats) {
	fmt.Fprintf(file, "## %s\n\n", title)

	entries := SortedPropertyEntries(stats)
	if len(entries) == 0 {
		fmt.Fprintln(file, "No sampled properties observed.")
		fmt.Fprintln(file)
		return
	}

	fmt.Fprintln(file, "| Key | Samples | Unique | Unique Ratio | Sensitive Key | Preserved Key | Identifier Like | Types | Shapes | Reasons |")
	fmt.Fprintln(file, "| --- | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |")

	for _, entry := range entries {
		nextStats := entry.Value
		fmt.Fprintf(
			file,
			"| `%s` | %d | %d | %.2f | %t | %t | %t | %s | %s | %s |\n",
			entry.Key,
			nextStats.Samples,
			nextStats.Unique,
			nextStats.UniqueRatio,
			nextStats.SensitiveKey,
			nextStats.PreservedKey,
			nextStats.IdentifierLike,
			InlineMap(nextStats.Types),
			InlineMap(nextStats.Shapes),
			InlineMap(nextStats.Reasons),
		)
	}

	fmt.Fprintln(file)
}

type IntEntry struct {
	Key   string
	Value int
}

func SortedEntries(values map[string]int) []IntEntry {
	entries := make([]IntEntry, 0, len(values))
	for key, value := range values {
		entries = append(entries, IntEntry{Key: key, Value: value})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Value == entries[j].Value {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].Value > entries[j].Value
	})

	return entries
}

type PropertyEntry struct {
	Key   string
	Value PropertyStats
}

func SortedPropertyEntries(values map[string]PropertyStats) []PropertyEntry {
	entries := make([]PropertyEntry, 0, len(values))
	for key, value := range values {
		entries = append(entries, PropertyEntry{Key: key, Value: value})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Value.SensitiveKey == entries[j].Value.SensitiveKey {
			if entries[i].Value.Samples == entries[j].Value.Samples {
				return entries[i].Key < entries[j].Key
			}
			return entries[i].Value.Samples > entries[j].Value.Samples
		}
		return entries[i].Value.SensitiveKey
	})

	return entries
}

func InlineMap(values map[string]int) string {
	if len(values) == 0 {
		return ""
	}

	entries := SortedEntries(values)
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, fmt.Sprintf("`%s:%d`", entry.Key, entry.Value))
	}

	return strings.Join(parts, ", ")
}
