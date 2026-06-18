package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type PropertyStats struct {
	Samples               int            `json:"samples"`
	NonEmpty              int            `json:"non_empty"`
	Unique                int            `json:"unique"`
	StringSamples         int            `json:"string_samples"`
	UniqueRatio           float64        `json:"unique_ratio"`
	NonEmptyRatio         float64        `json:"non_empty_ratio"`
	AverageStringLength   float64        `json:"average_string_length"`
	Types                 map[string]int `json:"types"`
	Shapes                map[string]int `json:"shapes"`
	Reasons               map[string]int `json:"reasons"`
	SensitiveKey          bool           `json:"sensitive_key"`
	PreservedKey          bool           `json:"preserved_key"`
	IdentifierLike        bool           `json:"identifier_like"`
	seen                  map[string]struct{}
	totalStringValueBytes int
}

type DegreeStats struct {
	Min     uint64  `json:"min"`
	Max     uint64  `json:"max"`
	Average float64 `json:"average"`
}

type Report struct {
	GeneratedAt                     time.Time                `json:"generated_at"`
	Driver                          string                   `json:"driver"`
	Graph                           string                   `json:"graph"`
	NodeCount                       int64                    `json:"node_count"`
	RelationshipCount               int64                    `json:"relationship_count"`
	NodePropertySampleLimit         int                      `json:"node_property_sample_limit"`
	RelationshipPropertySampleLimit int                      `json:"relationship_property_sample_limit"`
	HistogramLimit                  int                      `json:"histogram_limit"`
	NodeKindHistogram               map[string]int           `json:"node_kind_histogram"`
	RelationshipKindHistogram       map[string]int           `json:"relationship_kind_histogram"`
	NodeProperties                  map[string]PropertyStats `json:"node_properties"`
	RelationshipProperties          map[string]PropertyStats `json:"relationship_properties"`
	OutDegree                       DegreeStats              `json:"out_degree"`
	InDegree                        DegreeStats              `json:"in_degree"`
	Notes                           []string                 `json:"notes"`
}

func RecordProperty(stats map[string]PropertyStats, key string, value any, classifier Classifier) {
	nextStats := stats[key]
	if nextStats.Types == nil {
		nextStats.Types = map[string]int{}
		nextStats.Shapes = map[string]int{}
		nextStats.Reasons = map[string]int{}
		nextStats.seen = map[string]struct{}{}
		nextStats.SensitiveKey = classifier.SensitiveKey(key)
		nextStats.PreservedKey = classifier.PreservedKey(key)
	}

	nextStats.Samples += 1
	typeName := fmt.Sprintf("%T", value)
	nextStats.Types[typeName] += 1

	if value != nil && fmt.Sprintf("%v", value) != "" {
		nextStats.NonEmpty += 1
		nextStats.seen[fmt.Sprintf("%T:%v", value, value)] = struct{}{}
	}
	if strValue, ok := value.(string); ok {
		nextStats.StringSamples += 1
		nextStats.totalStringValueBytes += len(strings.TrimSpace(strValue))
	}

	classification := classifier.ClassifyProperty(key, value)
	for _, shape := range classification.Shapes {
		nextStats.Shapes[shape] += 1
	}
	for _, reason := range classification.Reasons {
		nextStats.Reasons[reason] += 1
	}

	stats[key] = nextStats
}

func FinalizePropertyStats(stats map[string]PropertyStats, classifier Classifier) {
	for key, nextStats := range stats {
		nextStats.Unique = len(nextStats.seen)
		nextStats.seen = nil
		if nextStats.Samples > 0 {
			nextStats.NonEmptyRatio = float64(nextStats.NonEmpty) / float64(nextStats.Samples)
		}
		if nextStats.NonEmpty > 0 {
			nextStats.UniqueRatio = float64(nextStats.Unique) / float64(nextStats.NonEmpty)
		}
		if nextStats.StringSamples > 0 {
			nextStats.AverageStringLength = float64(nextStats.totalStringValueBytes) / float64(nextStats.StringSamples)
		}
		nextStats.totalStringValueBytes = 0

		if nextStats.Reasons == nil {
			nextStats.Reasons = map[string]int{}
		}
		if nextStats.Shapes == nil {
			nextStats.Shapes = map[string]int{}
		}
		if classifier.IdentifierLike(nextStats) {
			nextStats.IdentifierLike = true
			nextStats.Shapes["identifier_like"] += 1
			nextStats.Reasons["high_uniqueness"] += 1
		}
		stats[key] = nextStats
	}
}

func SummarizeDegrees(degrees map[uint64]uint64) DegreeStats {
	if len(degrees) == 0 {
		return DegreeStats{}
	}

	minDegree := uint64(math.MaxUint64)
	maxDegree := uint64(0)
	total := uint64(0)

	for _, degree := range degrees {
		if degree < minDegree {
			minDegree = degree
		}
		if degree > maxDegree {
			maxDegree = degree
		}
		total += degree
	}

	return DegreeStats{
		Min:     minDegree,
		Max:     maxDegree,
		Average: float64(total) / float64(len(degrees)),
	}
}

func BuildNotes(nextReport Report) []string {
	var notes []string

	if len(nextReport.NodeKindHistogram) > 0 {
		notes = append(notes, "Node kind histograms should be preserved by the scrubber.")
	}

	if len(nextReport.RelationshipKindHistogram) > 0 {
		notes = append(notes, "Relationship kind histograms should be preserved by the scrubber.")
	}

	if len(SensitiveProperties(nextReport.NodeProperties)) > 0 || len(SensitiveProperties(nextReport.RelationshipProperties)) > 0 {
		notes = append(notes, "Sensitive-looking property keys or value shapes are present; pseudonymization rules are required.")
	}

	if nextReport.OutDegree.Max > 0 || nextReport.InDegree.Max > 0 {
		notes = append(notes, "Degree summaries confirm that topology must be preserved exactly for shape and tradecraft analysis.")
	}

	return notes
}

func SensitiveProperties(stats map[string]PropertyStats) []string {
	var keys []string
	for key, nextStats := range stats {
		if nextStats.SensitiveKey || nextStats.IdentifierLike || len(nextStats.Shapes) > 0 {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys
}
