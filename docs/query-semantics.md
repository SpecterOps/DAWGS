# Query semantics and compatibility

Backend behavior, supported query forms, and compatibility notes. For the broader syntax reference, see [Cypher syntax support](../cypher/Cypher%20Syntax%20Support.md).

[Documentation index](README.md) · [Project overview](../README.md)

## Builder filters and Neo4j rewrites

Legacy query-builder relationship-kind filters preserve Boolean scope: conjunctive
filters intersect their allowed kinds, disjoint filters match no relationships, and
OR, XOR and NOT keep their predicates in WHERE. This also prevents mutations from
affecting relationships selected only by an incorrect union of kinds.
Legacy builder Boolean filters also preserve XOR grouping inside AND, including
negated groups. PostgreSQL groups both XOR comparison operands and the lowered
Boolean result, so read and mutation predicates retain the same selection.

Neo4j raw-query rewrites for pattern-map parameters and temporal comparisons preserve
qualified function names, conjunctive label predicates (`n:A:B`) and floating-point
literal types. Builder filters that match any kind retain OR semantics. The shared
`raw_rewrite_semantics_shapes` corpus checks reads and mutation targets on both
backends; a Neo4j control executes the native `point.distance` function. Vendor
`CYPHER` prefixes and index hints do not yet have a rewrite-fidelity guarantee.

## Unsupported import and legacy creation clauses

The DAWGS Cypher parser rejects `CREATE UNIQUE` and `LOAD CSV` with located
`CREATE UNIQUE is not supported` and `LOAD CSV is not supported` errors.
PostgreSQL text compilation returns these errors before executing any part of
the query, including preceding writes. Check parse errors before formatting an
AST. Neo4j text queries pass through to the native server, which determines
syntax support and CSV access. Full shared support remains pending.

## Procedure calls

The DAWGS Cypher parser rejects standalone `CALL` with
`standalone CALL is not supported`, including implicit invocations and `YIELD`.
PostgreSQL text compilation returns this error before execution. Check parse
errors before formatting an AST. Neo4j text queries retain native procedure
execution through the driver. Full shared-parser procedure support remains
pending. In-query `CALL` also returns a located `in-query CALL is not supported`
error, including calls with arguments, `YIELD` and filters. PostgreSQL rejects
the whole query before executing preceding writes. Native Neo4j in-query
procedure execution remains available.

## Shortest-path expressions

The DAWGS Cypher parser rejects `shortestPath(...)` and `allShortestPaths(...)`
in expression positions with `shortest-path expressions are not supported`.
PostgreSQL returns this error before execution, including in write queries.
Use the supported MATCH pattern form, such as
`MATCH p = shortestPath((a)-[:R*1..3]->(b)) RETURN p`.
Neo4j text queries retain native expression execution through the driver.

## List indexing and slicing support

The DAWGS Cypher parser rejects list indexing (`xs[0]`, including negative,
parameterized and chained indexes) with `list indexing is not supported`.
List slicing (`xs[0..2]`, including omitted, negative and parameterized bounds)
returns `list slicing is not supported`.
PostgreSQL text queries return these errors before execution, including in write
queries. Check parse errors before formatting an AST. Full indexing and slicing
semantics remain pending. Neo4j text queries retain native indexing and slicing
when the driver passes the original query through after a declined rewrite.

PostgreSQL result-column aliases preserve case and decoded Cypher spelling in
`Result.Keys()`: `AS objectId` returns `objectId`, and ``AS `object id` ``
returns `object id`. Backtick escapes and embedded double quotes are translated
to SQL identifier quoting, including aliases carried through WITH and ORDER BY.
Callers that relied on lowercase folding of mixed-case aliases must use their
requested spelling when looking up result columns. The shared
`result_alias_shapes` templates cover reads and SET/CREATE post-state; direct
result-key checks run on both backends and in both PostgreSQL compiler modes.

## PostgreSQL JSON result numbers

PostgreSQL JSON/JSONB query results decode integer tokens as exact signed 64-bit `int64`
values, and decimal/exponent tokens as `float64`. This applies recursively to maps,
lists, node/relationship properties and paths, including direct pgx reads through DAWGS
pools. Callers inspecting untyped results must accept `int64` where they previously
received `float64`. Out-of-range integer tokens and floating-point overflow stop row
iteration and appear in `Result.Error()`; check it after draining results. These rules
apply to the JSON spelling returned by PostgreSQL; JSONB can normalize exponent
spellings before returning them.

JSON string values retain their exact contents, including strings such as `[1, 2]`,
`{"k":"v"}` and `"q"`. Reading a string through `Result.Values()` or scanning it into
a Go string does not parse its contents as another JSON value. Actual JSON objects
and arrays continue to decode to maps and lists.

Arithmetic between PostgreSQL properties uses their stored JSON types. Numeric
values support `+`, `-`, `*`, `/`, `%` and `^`; integer operations retain signed
64-bit precision and integer division truncates toward zero. String addition
concatenates, list addition appends, and missing/null operands yield null.
Unsupported operand types and integer overflow return query errors. Chained
arithmetic and arithmetic on properties carried through WITH use the same rules.
Known literal arithmetic and property/literal expressions retain their native
SQL path. Run ordinary schema assertion when upgrading to install the
`cypher_arithmetic(jsonb, jsonb, text)` helper.

Integer versus float dispatch follows the stored JSON number spelling. Fractional
spellings and floating results retain float behavior; JSON serialization or JSONB
normalization that has already erased an integral float's fractional spelling
cannot be reversed. Use explicit `toFloat` operands when that distinction matters
for existing values. Runtime type dispatch adds per-row cost; local measurements
and the remaining WITH alias-filter boundary are recorded in
[the variance ledger](../variance_findings_priority_complete.md#dawgs-016-runtime-property-arithmetic--2026-10-26).

## PostgreSQL WITH ordering and pagination

`WITH` applies `ORDER BY`, `SKIP` and `LIMIT` at its own query boundary, including
read-only queries. For example, `MATCH (n:User) WITH n ORDER BY n.score DESC
SKIP 1 LIMIT 2 RETURN n.score` selects the second and third highest scores.
Entity and scalar aliases resolve against that boundary's input. `WITH ... WHERE`
filters the paginated output, and a later `WITH` or `RETURN` keeps its own
ordering and pagination. Pagination counts retain the existing literal-only
contract. The shared read and mutation regressions and their live validation
status are recorded in [the active variance plan](../variance_findings_priority_complete.md#dawgs-040-with-ordering-and-pagination--2026-10-27).

## PostgreSQL pattern predicate dependencies

MATCH reordering preserves the node and relationship bindings referenced by
WHERE pattern predicates, including negated and compound predicates. A more
selective MATCH waits for any earlier bindings its predicate consumes; unresolved
bindings keep the clause pinned. Shared regressions cover returned rows and
complete mutation state on Neo4j and in both PostgreSQL optimizer modes. See the
[validation ledger](../variance_findings_priority_complete.md#dawgs-035-pattern-predicate-dependencies-and-mutation-boundary--2026-10-28).

## PostgreSQL temporal parameters

Go `time.Time` values whose location is `time.Local` map to PostgreSQL
`timestamp without time zone`. Values with `time.UTC` or another explicit
location map to `timestamp with time zone`, even when that location shares
the local zone's name or offset. Use `value.UTC()` for an explicit UTC instant
and `value.In(time.Local)` for a local timestamp. Parameter types also separate
translation cache entries, and cache hits bind the current timestamp value.

## Neo4j temporal parameters

Neo4j query parameters also accept Go timestamps parsed from numeric RFC3339
offsets. The driver preserves their instant, offset and nanosecond precision,
including inside parameter maps and lists. Named timezones retain Neo4j's native
comparison behavior: equal instants with different timezone representations can
compare differently. Tagged `datetime` values in the shared test corpus are
canonical UTC instants with a numeric offset so their assertions are portable.

## Optional matches

Leading `OPTIONAL MATCH` clauses preserve one null-filled row when no pattern
matches, including through subsequent optional clauses and mutations. PostgreSQL
uses the same optional-join path for leading clauses and clauses with prior input.

## PostgreSQL typed predicates

String predicates (`STARTS WITH`, `ENDS WITH`, `CONTAINS` and regex matching)
require string values on both sides. Numeric, Boolean and list properties yield
null instead of matching their text representation. Missing properties and
negated predicates preserve null as well. Use an explicit `toString` conversion
when a textual comparison is intended.

`STARTS WITH`, `ENDS WITH` and `CONTAINS` treat `%`, `_` and backslashes
as literal characters, including after functions such as `toLower`, `toUpper`,
`coalesce` and `toString`, and through WITH aliases. PostgreSQL escapes literal
LIKE patterns before adding prefix/suffix wildcards; the `toLower` index
prefilter uses the same escaped pattern. Parameter and property operands keep
their existing runtime escaping. No schema migration is required. Live validation
status is recorded in
[the active variance plan](../variance_findings_priority_complete.md#dawgs-014-string-predicate-literal-escaping--2026-10-27).

Ordered comparisons (`<`, `<=`, `>` and `>=`) retain dynamic property types:
numbers compare numerically, strings compare by Unicode order, Booleans compare
false before true, and lists compare lexicographically. Incompatible types and
null operands yield null; numeric strings are not numbers. These rules also
apply to properties carried through WITH and to mutation filters. Existing native
temporal comparisons keep their cast path. Run ordinary schema assertion when
upgrading to install `cypher_compare(jsonb, jsonb)`.

Positive string filters retain indexable text lookups. Runtime ordered comparison
adds per-row dispatch cost; local measurements and remaining representation
boundaries are recorded in
[the variance ledger](../variance_findings_priority_complete.md#dawgs-018-typed-predicates--2026-10-26).

## PostgreSQL properties carried through WITH

Properties keep their stored value types across `WITH` and when copied into node
or relationship `CREATE` maps, including after ordering and pagination. Numeric
and Boolean values stay numeric and Boolean; strings retain their contents.
`toString` extracts string
contents without JSON quotes or extra escaping; case conversion, split (including
a carried delimiter), size, head/tail, scalar casts, comparisons and Boolean predicates
consume the carried value. `size` counts Unicode characters in strings and elements
in lists. `coalesce` preserves the first non-null value's type, including when its
fallback has a different type. Missing properties and stored JSON nulls both behave
as null in these consumers. Node and relationship mutations use the same rules.

Run ordinary schema assertion when upgrading to install `cypher_text(jsonb, text)`,
`cypher_size(jsonb)` and `cypher_list(jsonb)`. Runtime text conversion accepts
strings; explicit `toString` additionally accepts numbers and Booleans. Unsupported values return a
query error. Direct property case conversion retains its functional-index path;
native literal, typed-scalar and direct property-array coalesce paths remain available.

This does not change empty-string split behavior, list UNWIND/collection support, or temporal storage
representation. JSON cannot recover temporal or integral-float type information
already lost during serialization. Helper costs and validation are recorded in
[the variance ledger](../variance_findings_priority_complete.md#dawgs-052-with-property-consumers--2026-10-26).

## PostgreSQL property-list UNWIND

`UNWIND` expands node and relationship property arrays without converting their
numbers, Booleans or strings to text. WITH aliases and nested collected lists
retain element types; each UNWIND expands one list level. Empty, missing and
null inputs produce no rows, while a non-list property value produces one row.
Duplicate elements retain their row multiplicity, including through SET and
CREATE with or without RETURN. Native SQL-array inputs keep their existing path.

No schema migration is required. The dynamic path materializes a SQL JSONB array
for each input value; this change does not introduce an execution budget or
qualify large-list performance. Stored JSON cannot recover type information
already lost during serialization. Validation is recorded in the
[completion ledger](../variance_findings_priority_complete.md#dawgs-042-property-list-unwind--2026-10-29).

## PostgreSQL integer conversion

`toInteger` truncates numeric inputs toward zero: `1.9` becomes `1`, `-1.5`
becomes `-1`, and fractions between `-1` and `1` become `0`. This applies to
literals, parameters, properties, WITH aliases, native-array `UNWIND` bindings
and mutation expressions. Integer
inputs retain their exact signed 64-bit value, including values above 2^53.
No schema migration is required. This repair covers numeric rounding; invalid
strings and out-of-range conversion behavior remain separate compatibility limits.
Live validation status is recorded in
[the active variance plan](../variance_findings_priority_complete.md#dawgs-015-integer-truncation--2026-10-27).

## PostgreSQL numeric aggregates

`sum` preserves signed 64-bit integer precision, including values above 2^53.
Floating-point inputs produce floating-point sums; mixed numeric inputs use
floating-point arithmetic. Null and missing values do not contribute, and empty
or all-null input returns integer zero. An empty grouped input produces no groups.
`sum(DISTINCT ...)` counts each distinct numeric value once. These rules also apply
to relationship properties, WITH aliases, and aggregate values used in later writes.
Non-numeric inputs and integer overflow return query errors.

Run ordinary schema assertion when upgrading to install `cypher_sum(jsonb)` and
`cypher_sum_transition(jsonb, jsonb)`. The aggregate retains one numeric accumulator
per group and uses runtime type dispatch. Its measured cost and the existing JSON
limitations on integral float representation are recorded in
[the variance ledger](../variance_findings_priority_complete.md#dawgs-053-numeric-sum--2026-10-27).
`avg` retains its existing floating-point input conversion and null result on empty
input; this change does not provide exact integer averaging.

## PostgreSQL collections

`collect` preserves stored property value types, including exact signed 64-bit integers,
Booleans, strings and nested lists. Missing and null values are excluded; empty
or all-null input returns an empty list. DISTINCT retains value types as well.
Values keep their types through WITH and UNWIND, so collected numbers support
arithmetic and collected Booleans support Boolean expressions. Numeric-looking
and JSON-looking strings remain strings.

Property collections use JSONB elements internally. Native scalar and entity
collections retain their existing representations. As with other JSON-backed
values, an integral float's type cannot be recovered after storage has erased its
fractional spelling. Validation and collection costs are recorded in
[the variance ledger](../variance_findings_priority_complete.md#dawgs-054-typed-collections--2026-10-27).

Entity membership preserves node and relationship identity types through `collect`
and `WITH`. A relationship never matches a collected node with the same numeric ID,
and a numeric ID never matches an entity. Membership of bound entities uses their
identity, including when a collection retains full entities. Negation and nullable/empty
lists keep Cypher's membership semantics in both PostgreSQL compiler modes.
See [the membership regression evidence](../variance_findings_priority_complete.md#dawgs-056-entity-membership-types--2026-10-27).

## Aggregate traversal counts

Aggregate traversal counts use ordinary traversal when the second `MATCH` labels the source or reuses it as the
terminal. The shared `aggregate_traversal_count_shapes` integration templates verify source labels, cycle counts,
ranking, and mutation effects; PostgreSQL coverage executes these cases with the optimizer enabled and disabled.

## Traversal bounds and endpoint identity

Variable-length traversals that reuse a node bound by a fixed prefix retain that node's identity at every returned
depth, including zero hops. Such terminals use ordinary traversal. The shared `bound_expansion_endpoint_shapes`
templates check node identities, ordered path endpoints, and mutation selection in both PostgreSQL optimizer modes.

Relationship expansions written as `*k` normalize to the exact range `*k..k`, including zero and explicit lengths
above 15. Formatting preserves that exact range, and PostgreSQL uses the requested upper bound. Whitespace and
comments within ranges are accepted. The shared `exact_expansion_length_shapes` templates check exact lengths,
paths and mutation selection on both backends and in both PostgreSQL compiler modes. Open-ended PostgreSQL
traversals preserve an absent maximum, including paths beyond 15 hops. Ordinary
traversals exhaust relationship-unique trails; open shortest searches use a
budgeted breadth-first workspace. Explicit Cypher bounds remain inclusive semantic
limits. Open ranges do not select executors that require a finite upper bound.

Run ordinary schema assertion before using the upgraded compiler: it installs the
versioned open-search harnesses and admission helper. Open traversal admission
allows 100,000 candidate trail states and 64 MiB of cumulative estimated trail
state (128 bytes plus 8 bytes per relationship per admitted candidate). These are
execution budgets, not a maximum depth or a measurement of PostgreSQL process
memory. Exhaustion raises SQLSTATE `54000`; results remain provisional until the
stream is drained/closed and its error checked. A failed write cannot commit,
including preceding multipart stages. Context cancellation remains available.
Operators may set positive PostgreSQL session/transaction settings
`dawgs.traversal_state_limit` and `dawgs.traversal_byte_limit`; accounting is fresh
for each driver query and shared by its multipart stages. Increasing budgets can
increase execution time and temporary storage substantially. See the
[open-ended traversal policy](open_ended_traversal_policy.md).

This changes successful results and mutation targets: queries can now reach
entities beyond depth 15. Add an explicit `*1..15` bound where that was intended.
Generic undirected shortest-path execution remains outside the existing support
boundary; this repair preserves supported traversal directions.

Bounded PostgreSQL `shortestPath` calls with singleton IDs use the current
endpoint arrays throughout search, including bounds above 64. Their generated
fragments cannot read generic endpoint-filter tables, and the bidirectional
harness verifies endpoint IDs before returning or stopping. A prior query on a
pooled connection cannot supply this call's terminal filter. Seeded self-endpoint
checks first establish that the root belongs to the filtered seed relation, so
unrelated edge rows cannot raise an endpoint error before the seed join.
Install the updated
SQL functions through ordinary schema assertion when deploying the compiler;
existing graph data and temporary-table layouts need no migration. The shared
`bounded_shortest_endpoint_shapes` corpus checks paths, scalar lengths, endpoint
predicates and mutation targets on both backends and in both PostgreSQL compiler
modes. Driver regressions cover fresh connections, filter-table reuse, changing
parameters across leases, and complete 128-edge witnesses.
Separate PostgreSQL issues with bounded all-shortest zero-hop results and
`shortestPath` followed by `SET ... RETURN p` remain recorded in the
[validation ledger](../variance_findings_priority_complete.md#bounded-shortest-path-workspace-repair--2026-10-30).

## Query boundaries, matching, and mutations

Predicates comparing variable-length traversal endpoints remain active when a fixed hop follows the expansion.
The shared `expansion_endpoint_predicate_shapes` templates check filters, path multiplicity, zero hops, inbound
traversals and exact node/relationship mutation selection on both backends and in both PostgreSQL compiler modes.

Carried nodes retain the representation materialized before `WITH`, and inline property maps retain the fields their
expressions read, including function arguments such as `id(source)` and `labels(source)`. `UNWIND` accepts empty lists
carried through `WITH` aliases, including chained aliases. Leading `UNWIND` and `CREATE` clauses can feed multipart queries.

Execute multipart writes in `WriteTransaction`. PostgreSQL executes each query part after the first write as a separate
statement in that transaction, materializing its carried rows in temporary relations. Later `MATCH`, `SET`, and `DELETE`
query parts therefore see earlier writes. Constant projections preserve incoming row counts, filters run before the
next part's mutations, and entity aliases reflect updates while captured scalar values retain their values.
Result closure releases the temporary relations; transaction completion also
drops them. These executions bypass SQL translation retention and temporary-relation statement caching. Translation
tools must use `translate.TranslatedStatements` and execute every returned statement in the same explicit transaction;
the single-statement renderer rejects staged writes. The shared `node_representation_shapes`, `leading_multipart_shapes`,
`empty_list_alias_shapes`, and `multipart_write_visibility_shapes` templates cover these behaviors.

PostgreSQL `OPTIONAL MATCH` preserves the identity of each incoming row, including repeated values and pending
`UNWIND` rows. Matches join back to their own input row; an unmatched row is retained once. PostgreSQL numbers
the input in a materialized relation and removes that internal identity after the join. The shared
`optional_row_identity_shapes` templates check fan-out, chained optional clauses, null bindings, aggregation,
wildcards, variable-length paths and downstream writes on both backends and in both PostgreSQL compiler modes.

A leading `OPTIONAL MATCH` starts with one implicit input row. If its complete pattern or WHERE predicate finds no
match, it returns one row with null bindings; successful matches return their normal rows without an extra null row.
Those rows remain available to aggregation, subsequent clauses and writes. The shared `leading_optional_shapes`
templates cover empty graphs, rejected predicates, paths, chained optionals and complete SET/CREATE post-state.
Multipart OPTIONAL inputs read the preceding materialized boundary, preserving
pending UNWIND rows without referencing the enclosing WITH CTE from within itself.
Their live acceptance status is recorded in `variance_findings_priority.md`.

Pattern predicates in MATCH WHERE retain their relationship kinds and correlated input across later MATCH
clauses. Deferred filters are evaluated before the next MATCH changes the input frame, including before an
OPTIONAL MATCH. The shared `pattern_predicate_lifetime_shapes` templates cover directed, undirected,
negated and multi-hop predicates, repeated predicates, fan-out, and SET/CREATE post-state in both
PostgreSQL compiler modes and Neo4j. Aggregate traversal-count optimization still explicitly rejects
pattern predicates under the separate DAWGS-030 containment.

Fixed relationship patterns that reuse a bound relationship retain its identity across MATCH clauses, WITH aliases,
OPTIONAL MATCH and pattern predicates. Each SQL edge scan correlates its ID to the carried relationship; unrelated
edges cannot multiply the input rows or downstream writes. The shared `reused_relationship_shapes` templates check
endpoint and path identities, duplicate rows, local property/kind constraints and complete mutation post-state on
both backends and in both PostgreSQL compiler modes. Relationship-list reuse remains a separate support boundary.

Within an ordinary MATCH clause, relationships remain unique across comma-separated patterns and consecutive
variable-length segments. PostgreSQL compares fixed relationship IDs and rejects overlap between segment edge-ID
arrays, retaining intermediate IDs until later patterns have checked them. Duplicate input rows and valid paths
retain their multiplicity. Separate MATCH clauses, WITH boundaries, and pattern predicates have their own uniqueness
scope. The shared `relationship_uniqueness_shapes` templates check exact paths, counts, optional nulls and stored
mutation state on both backends and in both PostgreSQL compiler modes. PostgreSQL fan-out tests additionally check
closed-form counts and report diagnostic EXPLAIN timings.

Undirected fixed continuations pair the near and far endpoints for each relationship orientation. A non-loop edge
therefore reaches its opposite endpoint; a self-loop still returns the same node once. The shared
`undirected_continuation_shapes` templates check both orientations, duplicate paths, carried bindings, expansion
prefixes and exact downstream write state on both backends and in both PostgreSQL compiler modes.

An undirected pattern with a repeated endpoint, `(x)-[r]-(x)`, matches only self-loops and returns each matching
relationship once per input row. A newly introduced `x` remains local to that pattern even when unrelated input rows
already exist. The shared `undirected_self_reference_shapes` templates check loop identities, duplicate and empty inputs,
optional matches, returned paths and exact SET/CREATE state on both backends and in both PostgreSQL compiler modes.

Read-only multipart queries consume the preceding WITH output at each MATCH and export only the next WITH's
bindings. Traversals never read their own future output boundary. Literal WITH projections preserve duplicate
and empty input streams. Expansion seed predicates involving an input row also filter completed paths for that row.
The shared `multipart_read_boundary_shapes` templates check fixed and variable-length
paths, directions, aliases, bound endpoints, aggregation, pagination and complete SET/CREATE post-state on Neo4j
and in both PostgreSQL compiler modes.

`ORDER BY` resolves projection aliases inside arithmetic and supported function expressions.
User aliases such as `i0`, `n0` and `s0` retain their Cypher meaning even when they
match generated SQL names. DISTINCT ordering carries computed sort keys through
an internal projection without exposing extra result columns; pagination follows
deduplication. The shared `order_alias_shapes` templates cover ordered results,
collisions, duplicate/null inputs and exact paginated SET/CREATE state on both
backends and in both PostgreSQL compiler modes.

PostgreSQL SET and REMOVE preserve the incoming row stream when several matches update the same node or relationship.
Later update targets receive every input row, and null targets from OPTIONAL MATCH leave those rows available to later
updates and RETURN. Entity aliases observe updated values while captured scalars retain their values. The shared
`update_row_stream_shapes` templates check row counts, returned values, and exact node and relationship post-state in
both PostgreSQL optimizer modes and on Neo4j. Property and kind additions/removals on each mutation target retain
source order, including alternating SET/REMOVE of the same key or label. The `update_clause_order_shapes` templates
check exact final state, untouched decoys, fan-out and null targets. Assignment expressions retain their existing
input-frame evaluation; expressions that depend on earlier writes should use a `WITH` boundary between clauses.

Cypher plain `DELETE` rejects a node with remaining incident relationships; explicitly deleting all of those
relationships in the same query part permits the node deletion. `DETACH DELETE` removes inbound, outbound and
self-loop relationships together with the node. PostgreSQL cleanup stays within the selected graph, including when
explicit node IDs overlap between graphs. Schema assertion upgrades cleanup triggers on existing node partitions;
new partitions receive them on creation. Run `AssertSchema` when upgrading an existing database before using the
updated compiler. The Go node-deletion and batch APIs retain their cascading behavior.
The shared `delete_node_shapes` templates check complete surviving node and relationship state on both backends and
in both PostgreSQL compiler modes. Committed-fixture checks additionally verify rollback, commit errors and physical
PostgreSQL edge storage. DELETE preserves the incoming rows for RETURN, including counts, captured scalars,
null targets and repeated matches. Capture property values before deleting an entity when those values are needed
later. The shared `delete_continuation_shapes` templates cover grouping, pagination and complete stored state.
When a PostgreSQL query part creates or updates entities before DELETE, execution materializes its input rows at a
statement boundary in the same transaction, so deletion observes the earlier writes. Failures roll back those writes;
this path uses the same temporary-relation lifecycle as multipart mutations. Compiler consumers must use
`TranslatedStatements` to execute staged results, as with multipart writes.

PostgreSQL keeps `AND` operands within expressions in `RETURN`, `WITH`, function arguments and `SET`, preserving input
rows. At `WHERE`, outer conjunctions become filter predicates. The shared `boolean_value_shapes` templates check
Boolean values, missing properties, node/relationship assignments and exact stored state on both backends and in both
PostgreSQL compiler modes.

## Batch updates

Neo4j batch upserts quote kind and identity-property names, including spaces,
keywords, Unicode and embedded backticks. Repeated relationship upserts retain
the same identity and update their existing entities.

Batch identity keys distinguish entity kinds, property names and values, and relationship endpoints. `NodeUpdate.Key`,
`RelationshipUpdate.Key`, and `Properties.HashInto` produce opaque identities whose encoding is not a stable storage
format. Key generation leaves caller-owned identity-property slices in their original order.

Neo4j `Batch.UpdateNodes` applies tracked property deletions alongside property and kind additions and removals.
Different nodes in the same batch retain their own deletion sets. PostgreSQL `Batch.UpdateNodeBy` applies tracked
kind removals after resolving the upserted node IDs, preserving other kinds and properties.

PostgreSQL batch operations can commit flushed work before the batch delegate returns. A later delegate error does
not undo completed flushes. Identity-based node upsert flushes containing kind removals apply the upsert and removals
in one explicit transaction; other flushes keep their existing execution paths.

## Traversal optimizer boundaries

Suffix reverse policies keep predicates correlated with carried values or other suffix bindings on the ordinary
executor; forced reverse diagnostics reject those shapes. `correlated_suffix_shapes` checks exact paths, duplicate rows,
and mutation selection. Additional constraints on an already-bound root use ordinary traversal as well. A bound expansion
boundary, including a repeated root, also retains ordinary traversal; `bound_suffix_boundary_shapes` verifies this across
zero-hop paths, genuine cycles, aliases, and mutation selection.

The PostgreSQL aggregate traversal count lowering rejects `MATCH ... WHERE` clauses carrying pattern predicates with a
normal translation error; those predicates cannot be expressed against the lowering's recursive CTEs, and declining the
optimization would reach generic multi-match translation, which does not yet translate the same shape correctly. Plain
shapes and property-only predicates keep their existing lowering. This containment is PostgreSQL-scoped: Neo4j executes
pattern predicates natively.
