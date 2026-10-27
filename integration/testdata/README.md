# Integration Corpus

Files under `cases/` execute one Cypher query per case. Files under `templates/`
share a fixture and query template across variants. Fixture-backed cases run in
a write transaction that is always rolled back.

Mutation cases use `assert: "no_error"` (or another primary result assertion)
and one or more `post_assertions`. The primary mutation result is fully drained
and checked before post-state queries run in the same transaction. Each
post-state entry contains `cypher`, optional `params`, and `assert`.

`exact_result_number_shapes.json` covers large integer CREATE and SET results,
carried property projections, both signed 64-bit limits, and post-mutation reads.
It uses typed `exact_int` expectations to avoid rounding expected values through
JSON-to-float conversion. PostgreSQL runs every variant in both compiler modes.
`TestExactResultNumbers` additionally checks actual Go types in committed scalar,
map, entity, collection and path results on both backends. PostgreSQL wire-codec
tests cover JSON/JSONB arrays, nullable composites and explicit range errors.

`json_result_string_shapes.json` covers JSON-like strings returned by CREATE,
node/relationship SET and WITH continuation. It checks exact string contents and
complete post-state, including unchanged fixture entities. PostgreSQL executes
every variant in both compiler modes. `TestJSONResultStrings` additionally checks
the actual Go types through `Values` and `Scan` for committed properties, carried
scalars, entities, paths and nested parameter values on both backends. PostgreSQL
wire tests cover JSON and JSONB in text and binary formats alongside real structured
JSON, text and bytea controls.

The assertion vocabulary includes exact fixture-backed state checks:

- `node_id_set` for exact surviving node IDs;
- `node_records` for exact node IDs, kinds, and complete property maps;
- `relationship_triples` for exact directed start/end/kind triples;
- `relationship_records` for exact triples and complete property maps;
- `exact_int` and `row_count` for counts.

Use `ordered_column_values` to check scalar values alongside returned entities,
for example `{"ordered_column_values": {"total": [2, 1]}}`. Combine it with
`ordered_node_ids` and `keys` to verify ranked node/count pairs and aliases.

`bound_expansion_endpoint_shapes.json` checks expansions that reuse a bound
terminal: false and genuine cycles, zero hops, repeated bindings through
`WITH`, per-row endpoint correlation, and mutation selection. Assertions check
complete returned node records and ordered path node IDs. Direction coverage
includes outbound/inbound expansions and undirected fixed prefixes; fully
undirected variable expansions are currently unsupported by the translator.

`exact_expansion_length_shapes.json` checks `*k` against `*k..k` at zero, one,
two, 15, 16 and 17 hops. It covers exact paths, inbound traversal, fixed prefixes
and suffixes, bound endpoints, duplicate rows, OPTIONAL MATCH, spaced ranges and
explicit non-exact range controls. Mutations check complete node/relationship
state, unchanged decoys, WITH continuation and CREATE cardinality with and without
RETURN. PostgreSQL executes every variant with both compiler modes. Shortest-path
live controls use exact one-hop bounds; SQL tests additionally check normalized
bounds and parameterized harness statements at larger depths.

`node_representation_shapes.json` checks carried node IDs, aliases, inline node
and relationship map expressions, and exact mutation selection.
`leading_multipart_shapes.json` checks leading/attached UNWIND and CREATE across
WITH, including empty inputs, properties, relationship creation, and post-state.
`correlated_suffix_shapes.json` checks carried suffix predicates on boundaries,
nodes, and relationships, with ordered path nodes and duplicate row multiplicity.
PostgreSQL tests also execute these corpora with each compiler mode explicitly.

`bound_suffix_boundary_shapes.json` checks suffixes whose expansion boundary is
already bound, including root reuse, aliases, cycles, zero hops, and mutation
selection. `empty_list_alias_shapes.json` checks empty lists materialized through
WITH and their zero-row mutation effects. Inline-map function references are
covered in `node_representation_shapes.json`.

`multipart_write_visibility_shapes.json` keeps CREATE followed by MATCH, SET,
or DELETE in the same primary Cypher query. Post-assertions additionally check
exact created values/counts and unchanged decoys. Separate post-assertion queries
alone cannot establish visibility between clauses inside the primary query.
It also checks constant projection cardinality, entity aliases versus captured
scalar values, and filters after ordering/pagination but before later mutations.

`update_row_stream_shapes.json` checks fan-out, repeated mutation targets,
duplicate input values, nullable node and relationship targets, per-row assignment
expressions, aliases, and empty inputs. It asserts the complete node and relationship
post-state as well as returned row multiplicity. `update_row_stream_inline.json`
checks continuation through a later WITH mutation. PostgreSQL also executes the
template variants with each compiler mode explicitly.

`update_clause_order_shapes.json` checks final property and kind intent for SET/REMOVE
in both orders, alternating and repeated changes, overlapping label lists, mixed node
and relationship targets, fan-out, null targets, empty input, and continuation through
WITH. Each variant checks complete node and relationship post-state, including decoys.
PostgreSQL runs the same variants with each compiler mode explicitly. These constant-value
assignments do not establish sequential evaluation of expressions that read earlier writes.

`boolean_value_shapes.json` checks conjunctions in `RETURN`, `WITH`, function arguments and node/relationship
assignments. It covers mixed Boolean operators, separate projections, parameters, missing properties, explicit
WHERE filtering and continuation after writes. Mutation variants assert complete node and relationship post-state,
including untouched decoys. PostgreSQL runs every variant with each compiler mode explicitly.

`scalar_shapes.json` includes literal null predicate projections and WHERE filters: `null is null`, negated forms,
non-null literals, bound-node identity and WITH/UNWIND-bound null values, alongside its property stored-null coverage.
Literal-false predicates must empty their matches instead of being dropped. Repeated `NOT` prefixes must preserve
their parity: double negation matches the positive predicate in WHERE, WITH WHERE, projections, string-prefix
predicates and null predicates, while triple negation behaves as a single negation. Its float-literal family checks
that integral float literals keep floating-point semantics on both backends: `3.0 / 2.0` returns `1.5`,
`toString(1.0)` returns `"1.0"`, float lists and property equality keep element values, and the integer `3 / 2`
control still returns `1`. PostgreSQL runs every variant with each compiler mode explicitly.

`expansion_endpoint_predicate_shapes.json` checks predicates comparing expansion endpoints before a fixed suffix,
including disjunction, conjunction, negation, equality, missing properties, parameters, zero hops, fixed prefixes,
inbound expansion, bound suffix endpoints, duplicate input rows and exact path multiplicity. Mutation variants check
complete node and relationship records, including direction, kind, root identity and property decoys. PostgreSQL runs
every variant with each compiler mode explicitly. The undirected suffix case restricts its terminal kind to isolate
predicate retention from the separately tracked undirected endpoint-reuse behavior.

`optional_row_identity_shapes.json` checks repeated OPTIONAL MATCH inputs, fan-out, no matches, mixed source rows,
leading and pending UNWIND, empty inputs, WITH aliases, DISTINCT/aggregation, null scalar and entity bindings,
chained optionals, multiple pattern parts, uncorrelated matches, WHERE filters, bound endpoints, variable-length
and zero-hop paths, incoming paths and wildcard projection. Mutation cases check exact node/relationship state,
staged pagination and continuation, and CREATE cardinality with and without RETURN. PostgreSQL executes every
variant with each compiler mode explicitly. Read-only WITH pagination remains the separate DAWGS-040 finding;
the pagination case uses the supported staged-write boundary.

`undirected_continuation_shapes.json` checks paired endpoint orientations after fixed and variable-length prefixes,
including incoming and undirected prefixes, opposite-direction parallel edges, self-loops, bound endpoints, paths,
duplicate inputs, aliases, optional matches, property/kind filters and consecutive undirected hops. Mutations check
complete node/relationship state, untouched decoys and CREATE cardinality with and without RETURN. PostgreSQL executes
every variant with each compiler mode explicitly. The fixture uses distinct directed triples supported by both backends.

`reused_relationship_shapes.json` checks fixed relationship identity across MATCH clauses, WITH aliases,
OPTIONAL MATCH and pattern predicates. It covers bound/unbound endpoints, directed and undirected roots,
fixed continuations, self-loops, paths, duplicate inputs, null relationships, local property/kind constraints,
wildcard continuation and reuse after expansion. Mutations check complete node/relationship post-state and
CREATE cardinality with and without RETURN, including untouched decoys. PostgreSQL executes each variant
with both compiler modes. Relationship-list reuse is a separate finding.

`undirected_self_reference_shapes.json` checks that `(x)-[r]-(x)` selects only genuine self-loops, including local
constraints on either occurrence, relationship/path identities, duplicate and empty independent inputs, unrelated
prior matches, aliases, OPTIONAL MATCH, pattern predicates and fixed continuations. Mutation variants check complete
node/relationship state, unchanged incident-edge and kind/property/missing-property decoys, WITH continuation and
CREATE cardinality with and without RETURN. PostgreSQL executes every variant with both compiler modes.

`relationship_uniqueness_shapes.json` checks relationship uniqueness across comma-separated patterns and consecutive
expansions, including mixed fixed/variable steps, node-only intervening parts, incoming/undirected fixed patterns,
self-loops, zero hops, exact path multisets, duplicate inputs, OPTIONAL MATCH nulls and separate clause/predicate scopes.
Mutations check complete node/relationship records and CREATE cardinality with and without RETURN, including untouched
direction, kind, property and identity decoys. PostgreSQL executes every variant with both compiler modes. Expansion
variants use relationship kinds; inline maps on variable-length relationships remain a separate translation limitation.

Every new reconciliation or post-processing mutation fixture must contain a
positive match and applicable decoys for direction, kind, property, fixture ID,
missing/null property state, and relationship property. Reuse
`NewReconciliationFixture`, `FixtureNames`, and `FixtureKinds` from the
`integration` package for deterministic Go integration cases and large
cardinality lists.

`cache_kind_identity_shapes.json` provides shared read/mutation controls for matching
all versus any requested kinds, including exact stored properties on nonmatches.
The PostgreSQL cache regression reuses its fixture with builder-only empty predicates
and exclusive/overlapping kind ASTs. It populates the cache in both orders and repeats
requests with new mutation values to check returned IDs and complete stored properties.

Tagged datetime parameters decode to `time.Time`:

```json
{
  "params": {
    "threshold": {
      "$type": "datetime",
      "value": "2026-01-02T03:04:05Z"
    }
  }
}
```

Raw Cypher may instead use an explicit conversion such as
`datetime($threshold)`. Legacy query-builder cases must pass `time.Time`
directly.

Large string-list parameters use the same tagged parameter decoder without a
large handwritten JSON array:

```json
{
  "params": {
    "object_ids": {
      "$type": "string_list",
      "prefix": "missing",
      "count": 1000,
      "include": ["target-id"]
    }
  }
}
```

Fixture-backed cases and template variants can bind database IDs without
hard-coding them. `node_params` maps a query parameter to one fixture node ID;
`node_list_params` maps a parameter to an ordered list of fixture node IDs:

```json
{
  "node_params": {"start_id": "start"},
  "node_list_params": {"end_ids": ["end-a", "end-b"]}
}
```

The integration runner and `cmd/plancorpus` resolve these fields after loading
the fixture, so semantic execution and plan capture use the same ID-anchored
query shape.
