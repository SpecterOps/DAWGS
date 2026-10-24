package translate

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestRelationshipUniquenessAcrossMatchSteps(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query                        string
				inequalities, overlaps, exclusions int
			}{
				{"comma fixed", `MATCH (a)-[r:MemberOf]->(x), (a)-[s:MemberOf]->(y) RETURN r, s`, 1, 0, 0},
				{"anonymous retained", `MATCH (a)-[:MemberOf]->(), (a)-[:MemberOf]->() RETURN count(*)`, 1, 0, 0},
				{"node part between", `MATCH (a)-[r:MemberOf]->(), (n), (a)-[s:MemberOf]->() RETURN r, s`, 1, 0, 0},
				{"three parts", `MATCH (a)-[r:MemberOf]->(), (a)-[s:MemberOf]->(), (a)-[t:MemberOf]->() RETURN r, s, t`, 3, 0, 0},
				{"separate MATCH", `MATCH (a)-[r:MemberOf]->() MATCH (a)-[s:MemberOf]->() RETURN r, s`, 0, 0, 0},
				{"WITH boundary", `MATCH (a)-[r:MemberOf]->() WITH a, r MATCH (a)-[s:MemberOf]->() RETURN r, s`, 0, 0, 0},
				{"consecutive expansions", `MATCH (a)-[:MemberOf*1..2]->()-[:MemberOf*1..2]->() RETURN count(*)`, 0, 1, 0},
				{"three expansions", `MATCH (a)-[:MemberOf*0..2]->()-[:MemberOf*0..2]->()-[:MemberOf*0..2]->() RETURN count(*)`, 0, 3, 0},
				{"comma expansions", `MATCH (a)-[:MemberOf*0..2]->(), (a)-[:MemberOf*0..2]->() RETURN count(*)`, 0, 1, 0},
				{"fixed then expansion", `MATCH (a)-[r:MemberOf]->(), (a)-[:MemberOf*0..2]->() RETURN r`, 0, 0, 1},
				{"expansion then fixed", `MATCH (a)-[:MemberOf*0..2]->(), (a)-[r:MemberOf]->() RETURN r`, 0, 0, 1},
				{"separate expansions", `MATCH (a)-[:MemberOf*1..2]->(b) MATCH (b)-[:MemberOf*1..2]->() RETURN count(*)`, 0, 0, 0},
				{"optional scope", `MATCH (a)-[r:MemberOf]->() OPTIONAL MATCH (a)-[s:MemberOf]->(), (a)-[t:MemberOf]->() RETURN r, s, t`, 1, 0, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					require.Equal(t, tc.inequalities, strings.Count(formatted.Statement, " != ")-strings.Count(formatted.Statement, " != all ("), formatted.Statement)
					require.Equal(t, tc.overlaps, strings.Count(formatted.Statement, " && "), formatted.Statement)
					// Internal recursive checks compare a scanned edge with its own
					// path. Cross-step checks reference a carried edge or path binding.
					exclusions := regexp.MustCompile(`(?:e\d+\.id != all \(s\d+\.ep\d+\)|\(s\d+\.e\d+\)\.id != all \(s\d+\.path\))`)
					require.Len(t, exclusions.FindAllString(formatted.Statement, -1), tc.exclusions, formatted.Statement)
				})
			}
		})
	}
}

func BenchmarkRelationshipUniquenessCompilation(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"single_fixed_control", `MATCH (a)-[:MemberOf]->(b) RETURN count(*)`},
		{"comma_fixed", `MATCH (a)-[:MemberOf]->(x), (a)-[:MemberOf]->(y) RETURN count(*)`},
		{"consecutive_expansions", `MATCH (a)-[:MemberOf*1..2]->(y)-[:MemberOf*1..2]->(z) RETURN count(*)`},
		{"comma_expansions", `MATCH (a)-[:MemberOf*1..2]->(x), (a)-[:MemberOf*1..2]->(y) RETURN count(*)`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mapper := optimizerSafetyKindMapper()
			b.ReportAllocs()
			for b.Loop() {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				if err != nil {
					b.Fatal(err)
				}
				translation, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: OptimizerEnabled})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := Translated(translation); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
