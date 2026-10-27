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

`optional_row_identity.sql` covers OPTIONAL MATCH correlation for duplicate inputs,
pending/leading UNWIND, chained optionals, variable-length expansion, DISTINCT,
updates and CREATE without RETURN. Related existing optional cases in `nodes.sql`,
`multipart.sql` and `update.sql` also retain the materialized input row identity.

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
