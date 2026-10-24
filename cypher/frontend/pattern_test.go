package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/stretchr/testify/require"
)

func TestParseRelationshipRange(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		source    string
		canonical string
		start     *int64
		end       *int64
	}{
		{source: "*", canonical: "*"},
		{source: "*..", canonical: "*"},
		{source: "*0", canonical: "*0..0", start: rangeBound(0), end: rangeBound(0)},
		{source: "*1", canonical: "*1..1", start: rangeBound(1), end: rangeBound(1)},
		{source: "*2", canonical: "*2..2", start: rangeBound(2), end: rangeBound(2)},
		{source: "*15", canonical: "*15..15", start: rangeBound(15), end: rangeBound(15)},
		{source: "*16", canonical: "*16..16", start: rangeBound(16), end: rangeBound(16)},
		{source: "*32", canonical: "*32..32", start: rangeBound(32), end: rangeBound(32)},
		{source: "*9223372036854775807", canonical: "*9223372036854775807..9223372036854775807", start: rangeBound(9223372036854775807), end: rangeBound(9223372036854775807)},
		{source: "*..0", canonical: "*..0", end: rangeBound(0)},
		{source: "*..16", canonical: "*..16", end: rangeBound(16)},
		{source: "*0..0", canonical: "*0..0", start: rangeBound(0), end: rangeBound(0)},
		{source: "*2..16", canonical: "*2..16", start: rangeBound(2), end: rangeBound(16)},
		{source: "*16..16", canonical: "*16..16", start: rangeBound(16), end: rangeBound(16)},
		{source: "*16..", canonical: "*16..", start: rangeBound(16)},
		{source: "* 16 ", canonical: "*16..16", start: rangeBound(16), end: rangeBound(16)},
		{source: "* 16 .. 17 ", canonical: "*16..17", start: rangeBound(16), end: rangeBound(17)},
		{source: "* /* exact */ 16", canonical: "*16..16", start: rangeBound(16), end: rangeBound(16)},
	} {
		t.Run(testCase.source, func(t *testing.T) {
			t.Parallel()
			query, err := frontend.ParseCypher(frontend.NewContext(), fmt.Sprintf("MATCH p = (a)-[:R%s]->(b) RETURN p", testCase.source))
			require.NoError(t, err)

			var ranges []*cypher.PatternRange
			require.NoError(t, walk.CypherStructural(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
				if relationship, ok := node.(*cypher.RelationshipPattern); ok {
					ranges = append(ranges, relationship.Range)
				}
			})))
			require.Len(t, ranges, 1)
			require.Equal(t, cypher.NewPatternRange(testCase.start, testCase.end), ranges[0])

			formatted, err := format.RegularQuery(query, false)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("match p = (a)-[:R%s]->(b) return p", testCase.canonical), formatted)
			reparsed, err := frontend.ParseCypher(frontend.NewContext(), formatted)
			require.NoError(t, err)
			require.Equal(t, query, reparsed)
		})
	}
}

func TestParseRelationshipRangeRejectsOverflow(t *testing.T) {
	for _, source := range []string{"*9223372036854775808", "*9223372036854775808..", "*..9223372036854775808"} {
		t.Run(source, func(t *testing.T) {
			_, err := frontend.ParseCypher(frontend.NewContext(), fmt.Sprintf("MATCH (a)-[:R%s]->(b) RETURN b", source))
			require.ErrorContains(t, err, "failed parsing range literal")
		})
	}
}

func rangeBound(value int64) *int64 {
	return &value
}
