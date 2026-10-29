# Cypher to PgSQL Translation Test Cases

## Test Framework

The translation test cases are loaded from an `embedded.FS` instance in the `translator_test.go` file.

Each `case:` is treated as a subtest via: `t.Run(...)` so that users can run a single query translation case if
desired.

## Format

A test case is presented to the test framework as a SQL query that is preceded by a comment with the `case:` tag. The
end of the query is determined by matching a closing ';' character.

```postgresql
-- This is a regular comment and will be ignored.
-- case: match (n) return n
with n1 as (select n1.* from node n1)
select *
from n1;

-- Matching for the `case:` tag is case-insensitive.
-- CASE: match (n) return n
with n1 as (select n1.* from node n1)
select * -- Comments may follow query content.
-- Comments may also interleave with queries.
from n1;
```

### Parameter Matching

Test cases may specify parameter matchers for further assertion:

```postgresql
-- case: match (s) where s.name = $myParam return s
-- cypher_params: {"myParam": "123"}
-- pgsql_params: {"pi0": "123"}
with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = @pi0::text)
select s0.n0 as s
from s0;
```

`create_property_types.sql` checks JSON value preservation in CREATE maps for
direct and parenthesized node/relationship properties and WITH aliases, including
paginated inputs. Native UNWIND bindings retain their element types in both
compiler modes; existing ordering and predicate fixtures reflect that type information.

`delete.sql` distinguishes plain node deletion from DETACH deletion and checks
explicit relationship targets, multiple targets, mixed plain/detached targets,
nullable optional bindings and WITH inputs. Plain node deletion materializes its
input and checks for incident relationships outside the complete deletion target
sets. Staged DELETE/read continuations run in the shared integration templates.

`with_modifiers.sql` covers read-only WITH ordering, SKIP and LIMIT, entity and
scalar aliases, nested alias expressions, DISTINCT, aggregation, input predicates
and WITH WHERE after pagination. It also checks literal row-stream projections,
consecutive pages, SET and CREATE without RETURN. Existing staged-write modifier
tests remain controls; read-only boundaries reuse their ordering and pagination
renderer.

`optional_row_identity.sql` covers OPTIONAL MATCH correlation for duplicate inputs,
pending/leading UNWIND, chained optionals, variable-length expansion, DISTINCT,
updates and CREATE without RETURN. Related existing optional cases in `nodes.sql`,
`multipart.sql` and `update.sql` also retain the materialized input row identity.

`leading_optional.sql` covers the one-row input seed for leading OPTIONAL MATCH,
WHERE and pattern-predicate placement, directions, variable/zero-hop paths,
wildcard path bindings, comma-separated patterns, chaining, aggregation,
WITH/UNWIND, SET and CREATE without RETURN. Multipart seed controls cover leading
and preceding WITH boundaries, aliases and pending UNWIND rows without referring
to the enclosing CTE as an input. The shared integration corpus also
checks exact mutation state and staged SET-to-CREATE continuations.

`reused_relationship.sql` covers identity-constrained scans of reused fixed
relationships, WITH aliases, directions, bound endpoints, paths, continuations,
pattern predicates, OPTIONAL MATCH updates and CREATE without RETURN. The related
property-constrained pattern predicate in `stepwise_traversal.sql` keeps its
property condition inside the correlated predicate.

`undirected_continuation.sql` covers paired near/far endpoint orientations after
outbound, inbound, undirected and variable-length prefixes, returned paths,
consecutive undirected steps, updates and CREATE without RETURN. Pairing is
applied before frame binding so scalar IDs and composite nodes stay qualified.

`relationship_uniqueness.sql` covers comma-separated fixed and variable-length
patterns, consecutive expansions, retained anonymous relationship state, node-only
intervening parts, OPTIONAL MATCH, updates and CREATE without RETURN. Existing
cases in `pattern_expansion.sql` and `stepwise_traversal.sql` also enforce
cross-segment disjointness and cross-part fixed relationship inequality.

`undirected_self_reference.sql` covers repeated endpoints constrained to both ends
of a self-loop, constraints on the right occurrence, paths, counts, duplicate and
unrelated inputs, OPTIONAL MATCH, bound endpoints, SET and CREATE without RETURN.
New local endpoints retain unrelated input rows without referencing a node from
the previous frame.

`exact_expansion_length.sql` covers normalized `*k` bounds, zero hops, short
fixed-step lowering, ordinary traversal above 15 hops, incoming traversal, paths,
SET and CREATE without RETURN. Shortest/all-shortest fixtures check that the
harness receives the normalized maximum; compiler tests compare complete SQL and
parameters for the exact shorthand and explicit range in both optimizer modes.

`null_predicates.sql` covers literal and entity `IS NULL`/`IS NOT NULL`
projections and WHERE predicates, WITH/UNWIND-bound null values and the
unchanged property-lookup null rewrites that distinguish missing keys from
stored JSON nulls.

`negation_parity.sql` covers repeated `NOT` prefixes in WHERE filters,
conjunctions, WITH WHERE continuations, projections and SET assignments. Even
negation counts cancel to the positive predicate, odd counts negate exactly
once, and an explicitly parenthesized `not (not …)` retains both operators.
Double negation also drops the negated string-predicate coalesce rewrite while
the property null rewrite keeps its dedicated lowering.

`float_literals.sql` covers integral floating-point literals in projections,
division, `toString`, list literals, membership lists and property equality.
Integral values keep a fractional suffix (`3.0`, never `3`) so PostgreSQL
evaluates them as numeric literals and float arithmetic never silently becomes
integer arithmetic; the integer division control pins `3 / 2` as integer
arithmetic. Exponent spellings (`1e3`) canonicalize to decimal float form.

`property_arithmetic.sql` covers runtime type dispatch for all six numeric
operators, chained expressions, WHERE comparisons, WITH aliases, node/relationship
SET and CREATE without RETURN. Three existing `nodes.sql` cases also dispatch
property addition at runtime rather than assuming string concatenation.

`predicate_types.sql` covers runtime type preservation for string predicates,
negation, regex operands, ordered comparisons in either operand position,
property pairs, lists, WITH-carried properties, node/relationship SET and CREATE
without RETURN. Existing string-predicate fixtures now include a nullable type
guard; positive filters retain bare text LIKE conditions for index use. Dynamic
ordered comparisons call `cypher_compare` instead of casting extracted text or
using JSONB's cross-type total ordering. Known temporal comparisons retain their
native casts.

`string_predicate_escaping.sql` checks literal LIKE escaping after functions and
WITH aliases, negation, literal left operands, node/relationship SET and CREATE
without RETURN. Both the ILIKE prefilter and exact lower-case LIKE comparison use
the escaped pattern. Parameter, direct property and regex controls retain their
existing paths. Existing coalesce examples in `nodes.sql`, `pattern_binding.sql`
and `shortest_paths.sql` now escape literal underscores, including SQL embedded
in traversal-harness parameters.

`with_property_consumers.sql` covers typed property aliases through WITH, text
conversion without JSON quoting, case conversion, both split operands, size,
coalesce, equality/null checks, scalar conversion and node/relationship mutations
including CREATE without RETURN. Existing golden cases now retain dynamic
coalesce values and call the runtime text/size helpers where needed.

`numeric_sum.sql` covers typed runtime SUM, DISTINCT, grouped and empty inputs,
literal/UNWIND operands, carried properties, arithmetic after aggregation and
node/relationship mutations including CREATE without RETURN. Related existing
`scalar_aggregation.sql` cases now retain integer SUM types and empty-input zero;
AVG keeps its existing floating-point conversion.

`typed_collect.sql` covers JSONB property aggregation, DISTINCT, null and Boolean
literals, nested collection aliases, typed UNWIND arithmetic/Boolean expressions,
list concatenation, membership, head and node/relationship mutations including
CREATE without RETURN. Related existing aggregation, multipart, pattern-binding
and UNWIND cases preserve property types instead of extracting text.

`entity_membership.sql` covers typed ID-only collections, full node/relationship
collections, WITH aliases, numeric ID decoys, negation, optional null targets and
SET/CREATE without RETURN. Same-type tests extract entity identity; cross-type
tests preserve null and empty-list semantics without comparing numeric IDs.
Direct aggregate entity-expression controls preserve their grouping scope.

`to_integer.sql` covers truncation toward zero for numeric literals, native float
UNWIND bindings, WITH scalars, node/relationship properties, mutation filters,
SET and CREATE without RETURN. Exact integer, integer-string and null controls retain their existing
casts. Related `nodes.sql`, `scalar_aggregation.sql` and
`with_property_consumers.sql` cases now truncate JSONB numbers through exact
numeric values before casting to int8.

`pattern_predicate_lifetime.sql` retains correlated predicates and relationship
kinds across subsequent MATCH clauses. It covers negation, undirected and
multi-hop predicates, repeated predicates, OPTIONAL MATCH, WITH, SET and CREATE
without RETURN. Predicate futures are resolved once; any pending filter is
consumed before a later MATCH changes its correlation frame.

`multipart_read_boundaries.sql` covers fixed, incoming, undirected, both-bound and
variable-length traversals between WITH boundaries, aliases, consecutive
boundaries, literal row preservation, predicates, pagination and downstream SET
and CREATE without RETURN. Input-dependent seed predicates also filter completed
paths, including zero-hop states and duplicate-input CREATE. Traversal frames skip the enclosing part's future
output while keeping the scope stack available for boundary unwinding.
