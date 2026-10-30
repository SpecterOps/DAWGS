# Integration Corpus

Files under `cases/` execute one Cypher query per case. Files under `templates/`
share a fixture and query template across variants. Fixture-backed cases run in
a write transaction that is always rolled back.

Template files may declare top-level `node_kinds` and `edge_kinds` arrays for
kinds used only by queries or post-state checks. These declarations register
schema kinds without inserting fixture entities. Both integration runners and
`cmd/plancorpus` merge them with fixture-derived kinds; a corpus must not depend
on kinds left behind by another test or an earlier run.

Fixtures use `properties`, `start_id` and `end_id`. Record assertions instead use
`props`, `start` and `end`: `node_records` contains `{id, kinds, props}` and
`relationship_records` contains `{start, end, kind, props}`. Unknown record fields
are rejected, and the unit suite audits every case/template record expectation.

`create_property_type_shapes.json` checks numeric, Boolean, string and missing
property copies into node and relationship CREATE maps. Direct properties, WITH
aliases, ordering and pagination run with and without RETURN. Pagination selects
the middle of three numerically ordered inputs with `SKIP 1 LIMIT 1`;
postconditions check copied values, unchanged fixture records and exact creation
counts.
PostgreSQL executes the corpus in both optimizer modes.

List indexing and slicing are explicitly unsupported by the shared DAWGS parser and PostgreSQL
text compiler. Parser negative fixtures and a templated unit matrix cover read,
SET, CREATE and DELETE contexts. Driver-scoped live tests check PostgreSQL
rejection before execution and unchanged committed state in both compiler modes;
Neo4j native indexing and slicing have separate driver-scoped controls. These differing support
boundaries do not use backend-specific assertions in the shared corpus.

`CREATE UNIQUE` and `LOAD CSV` are rejected explicitly by the shared parser and
PostgreSQL compiler rather than silently omitted. Negative fixtures and templated
parser regressions cover reads, WITH boundaries and mutations with and without
RETURN. PostgreSQL driver controls commit after consuming compilation errors and
verify that preceding SET/CREATE statements and target node/relationship state
remain unchanged in both compiler modes. Neo4j controls preserve native text and
parameters and check native legacy-syntax/missing-file errors. These differing
support boundaries stay driver-scoped.

Standalone `CALL` is likewise rejected explicitly by the shared parser and
PostgreSQL compiler, including implicit invocations and `YIELD`. Parser negative
fixtures and a templated syntax matrix cover read and write procedure names;
PostgreSQL driver controls verify repeated errors and unchanged committed graph
state. Neo4j driver controls execute native standalone procedures, including a
parameterized invocation. Procedure support boundaries stay driver-scoped.

Shortest-path expressions are rejected explicitly by the shared parser and
PostgreSQL compiler. A templated parser matrix and negative corpus cover RETURN,
WITH, WHERE, SET, CREATE and DELETE, including nested scalar consumers. Separate
driver controls check unchanged committed PostgreSQL state in both compiler
modes and native Neo4j expression execution. Shortest-path MATCH patterns retain
their supported syntax. These support boundaries stay driver-scoped.

Mutation cases use `assert: "no_error"` (or another primary result assertion)
and one or more `post_assertions`. The primary mutation result is fully drained
and checked before post-state queries run in the same transaction. Each
post-state entry contains `cypher`, optional `params`, and `assert`.

`delete_node_shapes.json` checks plain deletion of isolated nodes, explicit
relationship targets, DETACH cleanup of both directions and self-loops, mixed
plain/detached targets, duplicate input rows, null and missing targets, and WITH
boundaries. Every variant checks complete surviving node and relationship
records. PostgreSQL runs every variant in both compiler modes. Separate committed
fixtures check connected-node rejection, commit and rollback, and physical edge
counts so endpoint joins cannot hide orphaned storage. Driver-scoped schema
tests check parent/partition cleanup, repeat upgrades and overlapping IDs across
graphs; a forced hash-join control checks that deletion guards see only matched
input rows.

`delete_continuation_shapes.json` extends those controls with RETURN counts,
constant and captured-scalar projections, duplicate/null/empty input streams,
grouping, DISTINCT and pagination. SET/CREATE-to-DELETE and alias controls verify
complete surviving records. PostgreSQL runs every variant in both compiler modes;
committed fixtures check same-part SET/DELETE success, rejection and rollback.

`to_integer_shapes.json` covers numeric truncation toward zero for literals,
parameters, node/relationship properties, WITH aliases and UNWIND inputs. Cases
include native float arrays, array parameters, alias continuations, integer and
empty inputs, and UNWIND-to-CREATE mutations with complete fixture post-state.
Controls include nulls, exact large integers and signed 64-bit limits supplied as
strings.
SET, filter and CREATE variants check complete fixture state and creation counts
with and without RETURN. `TestPostgreSQLToIntegerCompilerModes` runs the shared
corpus in both compiler modes. `TestToInteger` checks actual Go integer types
from native parameters and committed properties, including floating parameters
at the signed 64-bit boundaries. These live checks require running databases;
their current execution status is recorded in the active variance plan.

`numeric_sum_shapes.json` checks exact large integer SUM results, DISTINCT,
grouping, empty/all-null inputs, float and literal controls, relationship properties,
WITH aliases and arithmetic after aggregation. Node/relationship SET and CREATE
variants check complete fixture post-state, unchanged direction/kind/role/missing
decoys, exact large stored totals and CREATE cardinality with and without RETURN.
Numeric fixture properties are assigned from exact decimal strings in Cypher so
fixture JSON decoding cannot turn integers into Neo4j floats. PostgreSQL executes
all variants in both compiler modes. `TestNumericSum` independently checks committed
properties, concrete Go result types, both signed 64-bit limits and invalid operands
on both backends. AVG controls cover ordinary values and its empty-input null;
large-integer averaging remains a separate precision limitation. PostgreSQL-scoped
schema tests check repeat assertion, SQL/JSON nulls, float spelling and overflow.

`typed_collect_shapes.json` checks typed property collections, DISTINCT, null and
empty inputs, grouping, WITH aliases, nested lists, membership, head/tail, list
concatenation and UNWIND consumers. Node/relationship SET and CREATE variants
check complete fixture state, unchanged direction/kind/role/missing-property
decoys and created values/cardinality with and without RETURN. PostgreSQL runs
every variant in both compiler modes. `TestTypedCollect` independently checks
committed node and relationship inputs and actual Go element types, including
both signed 64-bit limits, mixed values, Booleans and JSON-looking strings.

`exact_result_number_shapes.json` covers large integer CREATE and SET results,
carried property projections, both signed 64-bit limits, and post-mutation reads.
It uses typed `exact_int` expectations to avoid rounding expected values through
JSON-to-float conversion. PostgreSQL runs every variant in both compiler modes.
`TestExactResultNumbers` additionally checks actual Go types in committed scalar,
map, entity, collection and path results on both backends. PostgreSQL wire-codec
tests cover JSON/JSONB arrays, nullable composites and explicit range errors.

`predicate_type_shapes.json` checks string predicates and ordered comparisons
without converting non-string values to text or numeric strings to numbers.
It covers null results, negation, both operand positions, property pairs, numeric,
string, Boolean and list ordering, parameters and WITH aliases. Mutation variants
check complete node/relationship state, unchanged direction/kind/property/missing
decoys and CREATE cardinality with and without RETURN. PostgreSQL runs every
variant in both compiler modes. `TestPropertyPredicateTypes` also checks actual
Boolean/nil Go results and committed exact integers through signed-64-bit limits.
Schema tests cover nested and nullable list comparisons. Assignment fixtures use
`coalesce(predicate, false)` to keep the separate stored-null behavior out of
these backend-equivalent assertions.

`string_predicate_escaping_shapes.json` covers literal `%`, `_` and backslashes
in STARTS WITH, ENDS WITH and CONTAINS after case conversion, nested functions,
coalesce, toString and WITH aliases. Controls cover direct properties, parameters,
literal left operands, negation, empty strings and missing properties. SET and
CREATE variants run with and without RETURN and check complete node/relationship
state, unchanged direction/kind/role/missing-property decoys and creation counts.
`TestPostgreSQLStringPredicateEscapingCompilerModes` runs the shared corpus in
both compiler modes. Live execution requires running databases; its status is
recorded in the active variance plan.

`with_modifier_shapes.json` covers ordering, SKIP and LIMIT on read-only WITH
boundaries, entity/scalar aliases, nested ordering expressions, hidden sort keys,
DISTINCT, aggregation, literal projections, duplicate/empty inputs, consecutive
pages and independent RETURN modifiers. WITH WHERE cases distinguish filtering
after pagination from filtering the input. SET and CREATE variants run with and
without RETURN and check complete node/relationship post-state, unchanged
direction/kind/property/root/missing-property decoys and created values/counts.
`TestPostgreSQLWithModifierCompilerModes` runs all 44 shared variants in both
compiler modes. Live execution requires running databases; its current status is
recorded in the active variance plan.

`json_result_string_shapes.json` covers JSON-like strings returned by CREATE,
node/relationship SET and WITH continuation. It checks exact string contents and
complete post-state, including unchanged fixture entities. PostgreSQL executes
every variant in both compiler modes. `TestJSONResultStrings` additionally checks
the actual Go types through `Values` and `Scan` for committed properties, carried
scalars, entities, paths and nested parameter values on both backends. PostgreSQL
wire tests cover JSON and JSONB in text and binary formats alongside real structured
JSON, text and bytea controls.

`property_arithmetic_shapes.json` checks all six numeric operators, exact large
integers, integer/float division, binary floating remainder, strings, lists,
missing values, parentheses, chained operations, WITH-carried operands, comparisons
and complete node/relationship mutation state. Numeric fixture properties are
assigned in Cypher before the tested expression so JSON fixture decoding cannot
silently turn integer inputs into Neo4j floats. PostgreSQL executes all variants
in both compiler modes. `TestPropertyArithmetic` also checks committed typed
properties, actual Go result types and errors for invalid arithmetic on both
backends. PostgreSQL schema tests check repeat assertion, nulls, overflow and
floating remainder against Go's `math.Mod`.

`entity_membership_shapes.json` checks node and relationship membership through
ID-only and full-entity collections, WITH aliases, DISTINCT, duplicate inputs,
numeric ID decoys, negation and nullable/empty collections. SET and CREATE variants
check complete fixture state and created values/counts with and without RETURN.
PostgreSQL runs every variant in both compiler modes. Its separate
`TestPostgreSQLEntityMembershipOverlappingIDs` commits an explicit node/relationship
ID overlap and checks exact stored state after rollback-scoped mutations, so
independent ID sequences cannot hide cross-type identity comparisons.

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
variant with each compiler mode explicitly. Read-only WITH pagination has dedicated
coverage in `with_modifier_shapes.json`; this corpus's pagination case uses the
staged-write boundary.

`leading_optional_shapes.json` checks the single implicit input row of a leading
OPTIONAL MATCH, empty graphs, rejected WHERE and pattern predicates, fixed and
variable paths, wildcard/path bindings, comma-separated patterns, aggregation,
WITH/UNWIND and later mandatory/optional matches. Its 53 shared variants include
SET and CREATE with and without RETURN, exact fixture node/relationship records,
unchanged direction/kind/property/source/missing-property decoys and creation
cardinality/values. `TestPostgreSQLLeadingOptionalCompilerModes` runs every
variant in both compiler modes. Live acceptance requires running databases;
its current status is recorded in the active variance plan.

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

Tagged datetime parameters decode to `time.Time`, normalized to a UTC fixed offset
without losing the instant or nanosecond precision. This avoids named-zone and
offset tie-breaking in Neo4j comparisons and supports its Bolt datetime encoding:

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

`with_property_consumer_shapes.json` checks string contents and escaping across
WITH, repeated and rebound aliases, case conversion, split, size, head/tail, heterogeneous coalesce,
equality/null checks and Boolean consumers. Node and relationship mutations check
complete stored state, all applicable decoys, continuation and CREATE with and
without RETURN. PostgreSQL executes every variant in both compiler modes.
`TestWithPropertyConsumers` additionally checks committed properties and actual Go
result types, exact large integers, numeric-string/date conversion and invalid
runtime types on both backends. The split variant uses nonempty inputs; the
separate empty-string split finding remains open.

`pattern_predicate_lifetime_shapes.json` checks MATCH WHERE pattern predicates
followed by another MATCH, including relationship-kind and direction decoys,
negation, multi-hop predicates, repeated predicates, compound Boolean filters,
parameterized relationship properties, independent matches, OPTIONAL MATCH and
WITH. SET and CREATE run with and without RETURN; postconditions check all
fixture node/relationship records and created labels, values and counts.
`TestPostgreSQLPatternPredicateLifetimeCompilerModes` runs the same 22 variants
in both compiler modes. Every fixture transaction is rolled back.

`pattern_predicate_dependency_shapes.json` checks dependency-safe MATCH ordering
for directed, incoming, undirected, negated and compound pattern predicates,
parameterized and missing endpoints, multiple earlier bindings, WITH boundaries,
counts, and exact SET, REMOVE and CREATE selection with complete node/relationship
post-state, including writes without RETURN.
PostgreSQL runs the same variants in both compiler modes.

`multipart_read_boundary_shapes.json` covers WITH → MATCH → WITH input-frame
selection, aliases, carried properties and relationships, fixed and variable-length
paths, independent inputs and carried predicates, both bound endpoints,
directions, duplicate/empty inputs, DISTINCT,
aggregation, pagination and literal projections. Carried predicates also preserve
zero-hop and duplicate-input CREATE correlation. SET and CREATE cases compare
complete original node/relationship records plus exact created labels and names,
including writes without RETURN. PostgreSQL executes all variants in both compiler
modes; the shared corpus has identical assertions on both backends.

`order_alias_shapes.json` checks nested RETURN aliases in arithmetic, casts and
supported functions, alias/input collisions, quoted identifiers, multiple sort
keys, DISTINCT and aggregates, duplicate/empty/null inputs and pagination.
Ordered assertions distinguish SQL failures from silent order changes. SET and
CREATE variants check complete original node records, creation values/cardinality
and empty relationship state, with and without RETURN. PostgreSQL runs the same
variants in both compiler modes.

`property_unwind_shapes.json` checks node/relationship property lists, WITH aliases,
nested collection expansion, exact duplicate cardinality, arithmetic/text/Boolean
consumers, pagination, scalar inputs, empty/missing inputs and OPTIONAL MATCH nulls.
SET and CREATE variants run with and without RETURN and check complete fixture
state, unchanged direction/kind/property/missing-property decoys and created
values/counts. Direct UNWIND-to-SET checks pending input rows and empty mutations.
PostgreSQL executes all 46 variants in both compiler modes. `TestPropertyUnwind`
checks committed node/relationship properties and actual Go element types,
including both signed 64-bit limits, JSON-looking strings, scalar and empty inputs.

`unknown_kind_shapes.json` leaves its unknown kind undeclared and checks empty
node/relationship matches, mixed alternatives, label conjunctions, bounded
expansions, negation, type-name comparisons, OPTIONAL MATCH and null predicates.
SET, DELETE and CREATE-from-empty-input variants check complete fixture state.
PostgreSQL runs all variants in both compiler modes; driver-scoped checks verify
that matches leave kind metadata unchanged and recompile after registration.

`relationship_kind_conjunction_shapes.json` checks disjoint, overlapping, repeated
and nested relationship-kind filters, with OR and NOT controls. Its 21 shared
read/SET variants assert complete node and relationship state, including direction,
endpoint, kind and missing-property decoys. `TestBuilderRelationshipKindConjunction`
renders the corresponding legacy builder ASTs and reuses every fixture and
assertion on both backends; PostgreSQL runs both compiler modes.
