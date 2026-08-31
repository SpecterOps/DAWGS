package query

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	cypherFormat "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// FuzzParameterNamerAndRewriterAgree protects the positional provenance used
// by PostgreSQL builder-query caching. The non-mutating naming pass and the
// rewriting pass must assign identical symbols and values in structural order.
func FuzzParameterNamerAndRewriterAgree(f *testing.F) {
	f.Add(int64(101), int64(202), int64(303))
	f.Add(int64(-1), int64(0), int64(1))

	f.Fuzz(func(t *testing.T, first, second, third int64) {
		builder := NewBuilder(nil)
		builder.Apply(
			Where(And(
				Equals(NodeProperty("first"), first),
				Or(
					GreaterThan(NodeProperty("second"), second),
					LessThan(NodeProperty("third"), third),
				),
			)),
			Returning(Node()),
		)
		regularQuery, err := builder.Build(false)
		if err != nil {
			t.Fatal(err)
		}

		namer := NewParameterNamerWithPrefix("fuzz_p")
		if err := walk.Cypher(regularQuery, namer); err != nil {
			t.Fatal(err)
		}
		namedSource, err := cypherFormat.RegularQueryWithParameterSequence(regularQuery, false, namer.Symbols)
		if err != nil {
			t.Fatal(err)
		}

		owned := cypher.Copy(regularQuery)
		rewriter := NewParameterRewriterWithPrefix("fuzz_p")
		if err := walk.Cypher(owned, rewriter); err != nil {
			t.Fatal(err)
		}
		rewrittenSource, err := cypherFormat.RegularQuery(owned, false)
		if err != nil {
			t.Fatal(err)
		}

		if namedSource != rewrittenSource {
			t.Fatalf("parameter naming diverged:\nnamed: %s\nrewritten: %s", namedSource, rewrittenSource)
		}
		if !reflect.DeepEqual(namer.Parameters, rewriter.Parameters) {
			t.Fatalf("parameter values diverged: named=%v rewritten=%v", namer.Parameters, rewriter.Parameters)
		}

		originalSource, err := cypherFormat.RegularQuery(regularQuery, false)
		if err != nil {
			t.Fatal(err)
		}
		if originalSource == rewrittenSource {
			t.Fatal("parameter rewrite unexpectedly mutated the builder-owned query")
		}
	})
}
