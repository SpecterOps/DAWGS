# Cypher Optimization Pass Memory

## Purpose

The PostgreSQL translator currently lowers Cypher traversal parts mostly in source order. That is simple and predictable, but it can produce expensive SQL for multi-part path queries where a later pattern contains more selective anchors or where returned path payloads are carried through unrelated expansions.

This note captures a conservative plan for introducing a PostgreSQL-specific pre-translation optimization phase. The goal is not to require users to reauthor valid Cypher to get acceptable runtime behavior.

## Motivating Query Shape

```cypher
MATCH (n:Group)
WHERE n.objectid = 'S-1-5-21-2643190041-1319121918-239771340-513'
MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d)
WHERE ct.authenticationenabled = true
AND ct.requiresmanagerapproval = false
AND ct.enrolleesuppliessubject = true
AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
RETURN p1, p2
```

The current PostgreSQL shape can preserve too much intermediate state. In particular, because `p1` is returned, path-related state from `p1` may be carried through the `p2` expansion before `p2` has been filtered. Neo4j's planner is more flexible: it can reorder pattern evaluation, use endpoint-aware expansion, and materialize path values late.

## Architectural Decisions

The first optimizer effort is intentionally PostgreSQL-specific. The optimizer should avoid painting the project into a backend-neutral corner, but PostgreSQL is the only Cypher translation target that currently needs this work.

- Ship optimizer rules directly once they are covered. Do not add a user-facing feature flag surface for optimizer behavior.
- Optimize only read-only `MATCH` and `WHERE` groups inside a single query part for the first milestone.
- Treat `WITH`, `RETURN`, aggregation, `DISTINCT`, `ORDER BY`, `LIMIT`, `UNWIND`, `OPTIONAL MATCH`, writes, and procedure calls as semantic barriers.
- Allow the optimizer to build a new ordered logical plan inside eligible regions.
- Represent path variables as late-materialized recipes throughout the optimized PostgreSQL logical model.
- Use deterministic heuristics for early reordering. Defer schema statistics and cost-based planning.
- Accept more complex SQL when it materially improves runtime conditions. The database is responsible for executing the improved plan shape.
- Defer broad benchmark and real-world query set definition until after the basic framework and first optimizer rules are in place.

## Safety Constraints

Keep the first implementation deliberately conservative.

- Preserve Cypher row semantics, path relationship uniqueness, variable binding rules, and zero-length expansion behavior.
- Keep each optimization rule individually named and testable.
- If a rule cannot prove a rewrite is safe, keep the original logical order for that part of the plan.
- Require translation-shape tests, PostgreSQL integration equivalence, and Neo4j equivalence coverage for optimizer behavior.

## Sequenced Plan

### Phase 1: Define Optimizer Boundaries

Document the Cypher regions eligible for optimization and the barriers that terminate an optimization region. The initial eligible region should be a read-only sequence of `MATCH` and `WHERE` clauses within one query part.

Add diagnostics that can print the logical pattern parts, bindings, predicates, path variables, and final projection dependencies before translation.

### Phase 2: Build The Safety Net

Add translation-shape coverage for the motivating ADCS query. The first tests should capture the current expensive SQL shape so improvements can be measured.

Add smaller focused cases for:

- multiple `MATCH` clauses sharing variables
- returned path variables used only at final projection
- variable-length expansion followed by a fixed suffix
- repeated bound variables such as `(ca)` and `(d)`
- zero-length expansion with `*0..`

Validate optimizer behavior with all three test classes:

- translation-shape tests
- PostgreSQL integration equivalence tests
- Neo4j integration equivalence tests

Neo4j tests should assert result shape and semantics, not exact Neo4j plan shape.

### Phase 3: Introduce A No-Op Optimizer Skeleton

Insert a PostgreSQL-specific pre-translation logical optimization pass between parsing/semantic modeling and PostgreSQL rendering.

The initial pass should return the same logical model it receives. This keeps the integration point small and gives tests a stable hook before behavioral rules are added.

Suggested rule names:

- `PredicateAttachment`
- `ProjectionPruning`
- `LatePathMaterialization`
- `ExpandIntoDetection`
- `ConservativePatternReordering`
- `VariableExpansionTerminalPushdown`

### Phase 4: Attach Predicates To Their Bindings

Move eligible `WHERE` predicates as close as possible to the bindings they reference.

For the motivating query, the `ct.*` predicates should be owned by the `ct:CertTemplate` binding. This does not need to reorder pattern matching at first; it makes predicate dependencies explicit so later rules can apply filters earlier.

### Phase 5: Prune Intermediate Projections And Paths

Compute a narrower carry set for each operation:

- bindings needed by the next operation
- bindings needed by predicates
- bindings needed as join keys
- bindings needed later only to construct returned values

The translator should not carry every visible binding through every later expansion just because it appears in the final `RETURN`.

This should be the first real runtime-focused optimization rule. It directly addresses the reported query shape, creates the liveness information required by later rules, and is lower risk than traversal reordering or suffix pushdown.

### Phase 6: Materialize Paths Late

Represent returned paths internally as recipes over node and relationship bindings rather than as fully materialized values throughout every step.

For the motivating query, the optimizer should be able to continue from a narrow frame after `p1`, such as distinct `(n, ca, d)`, evaluate and filter `p2`, then join back to the full `p1` rows and materialize `p1` and `p2` at the final projection.

This is the first high-value optimization target because it reduces row width and delays the `p1 x p2` multiplication without changing the user's Cypher.

### Phase 7: Detect Expand-Into Opportunities

When both endpoints of a relationship or variable-length segment are known, lower that segment as a constrained connectivity/path problem instead of an open expansion.

This mirrors Neo4j's `Expand(Into)` and `VarLengthExpand(Into)` behavior and is especially relevant when separate `MATCH` clauses bind endpoints that are reused later.

### Phase 8: Add Deterministic Pattern Reordering

After projection pruning and late materialization are stable, allow limited reordering inside a single read-only optimization region.

Start with obvious anchors:

- node label plus equality property
- fixed relationship type scans
- already-bound endpoints
- selective labels or properties only when deterministic local information is available

Do not begin with a general cost-based planner or schema-statistics dependency. Prefer deterministic rewrites with clear safety checks.

### Phase 9: Push Terminal Constraints Into Variable Expansions

For variable-length expansions followed by fixed suffixes, add terminal or suffix constraints as semi-joins or correlated existence checks.

For the motivating query, this means avoiding emission of `MemberOf*0..` endpoints that cannot reach an eligible `CertTemplate` published to the already-bound `ca`, and avoiding `RootCA` endpoints that cannot connect back to the already-bound `d`.

### Phase 10: Measure Each Rule Locally

Every optimization rule should include:

- unit or translation tests for the logical rewrite
- PostgreSQL integration result-equivalence coverage
- Neo4j integration result-equivalence coverage
- SQL shape assertions for representative queries
- before and after `EXPLAIN` comparison on synthetic fanout data

The synthetic data should include many `p1` paths to the same `(n, ca, d)`, many membership paths from `n`, and only a small number of eligible certificate template and root CA paths.

Broader benchmark suites and real-world query collections are deferred until after the basic optimizer framework and first rules are implemented.

## Recommended First Milestone

Implement phases 1 through 6 first.

That milestone establishes the PostgreSQL optimizer framework, test bar, predicate ownership, projection and path pruning, and late path materialization. It should improve the reported query shape without taking on endpoint-aware expansion, suffix semi-joins, schema statistics, or a full cost-based planner.

## Quality Review Follow-Up Plan

The first optimizer milestone introduced the PostgreSQL optimizer hook, predicate attachment diagnostics, projection pruning, and late path materialization. Before moving on to endpoint-aware expansion or pattern reordering, close the review gaps in this order:

### Step 1: Preserve The Optional-Match Barrier

Keep projection pruning and late path materialization scoped to plain `MATCH` translation until optional path semantics have dedicated coverage. `OPTIONAL MATCH` already acts as an optimization-region barrier in the analysis pass; translator-side lowering should respect the same boundary.

### Step 2: Assert Path Semantics, Not Only SQL Shape

Expand integration coverage for optimized path returns so tests assert path node order, relationship order, and path length for mixed fixed-hop and variable-length paths. Include `relationships(p)` on paths that are eligible for late materialization.

### Step 3: Harden Direct Relationship References

Add focused translation tests proving direct relationship references keep edge composites when used in returned values, predicates, relationship properties, `type(r)`, and endpoint functions such as `startNode(r)`.

### Step 4: Document Performance Measurement Needs

Keep the current shape tests as guardrails, but add an explicit measurement task for high-fanout ADCS-style data before expanding the optimizer into endpoint-aware expansion, suffix semi-joins, or deterministic reordering.
