# Cypher Translation Support V4 Plan

This plan organizes implementation work for the next CySQL translation completeness targets:
list indexing and slicing, CASE expressions, existential subqueries, recursive pattern predicates,
list and pattern comprehensions, and UNION / UNION ALL.

## Phase 0: Baseline

Update the support matrix so it reflects current translator behavior. The existing support document is stale in places:
CREATE, UNWIND, quantifiers, labels(), head(), and tail() have translator and test coverage now, despite older notes
listing some of them as unsupported or defective.

Add negative tests for the exact currently-missing constructs before implementing each feature. This keeps every feature
with a clear before/after signal.

## 1. List Indexing And Slicing

Target forms:

```cypher
nodes(p)[0]
relationships(p)[-1]
list[1]
list[1..3]
list[..3]
list[1..]
```

Implementation:

- Add Cypher AST nodes such as `ListIndexExpression` and `ListSliceExpression`.
- Implement `oC_ListOperatorExpression` handling in `frontend/expression.go`.
- Add copy, format, and walk support.
- Lower to existing PgSQL `ArrayIndex` and `ArraySlice`.
- Define Cypher-to-Postgres index semantics explicitly. Cypher is zero-based; PostgreSQL arrays are one-based.
- Handle negative indexes with `cardinality(array) + index + 1`.
- Normalize open-ended slices and test empty/null behavior against Neo4j.

Tests:

- Translation cases for literals, parameters, `nodes(p)`, `relationships(p)`, and `collect(...)`.
- Integration cases for first, last, middle, out-of-range, null list, and empty list.

## 2. CASE Expressions

Target forms:

```cypher
CASE WHEN cond THEN x ELSE y END
CASE expr WHEN value THEN x ELSE y END
```

Implementation:

- Remove the frontend unsupported-rule rejection for `oC_CaseExpression`.
- Add AST nodes: `CaseExpression` and `CaseAlternative`.
- Add visitors for `oC_CaseExpression` and `oC_CaseAlternative`.
- Reuse the existing `pgsql.Case` model and formatter.
- Add type inference for CASE result type, likely by finding a common supertype across `THEN` and `ELSE`.
- Ensure aggregate grouping logic can traverse CASE expressions.

Tests:

- Scalar CASE in `RETURN`, `WHERE`, and `WITH`.
- CASE over properties, labels, nulls, parameters, and aggregates.
- CASE inside `ORDER BY` and grouped aggregation.

## 3. Existential Subqueries

Target forms:

```cypher
EXISTS { MATCH ... }
EXISTS { (n)-[:R]->() }
NOT EXISTS { ... }
```

Implementation:

- Remove the frontend unsupported-rule rejection for `oC_ExistentialSubquery`.
- Add an AST node `ExistentialSubquery` with either a `RegularQuery` or `Pattern + Where`.
- For pattern-only forms, translate through existing pattern predicate machinery.
- For query forms, compile to `pgsql.ExistsExpression` with a correlated subquery.
- Reuse the current scope for outer references, but isolate subquery-local bindings.
- Start read-only: support `MATCH`, `WHERE`, and `RETURN` first. Defer updates inside existential subqueries unless
  backend semantics require a hard rejection.

Tests:

- Correlated and uncorrelated exists.
- Nested `EXISTS`.
- `NOT EXISTS`.
- Optional matches inside exists.
- Equivalence with current pattern predicates where applicable.

## 4. Recursive Pattern Predicates

Current blocker: pattern predicates reject traversal steps with expansion.

Implementation:

- Reuse variable-length traversal translation, but render it under an `EXISTS` subquery instead of a top-level match
  frame.
- Start with single expansion predicates:

```cypher
WHERE (n)-[:R*1..3]->(m)
```

- Then support anonymous endpoint and endpoint predicates:

```cypher
WHERE (n)-[:R*]->(:Kind {prop: 1})
```

- Ensure pattern predicate frames do not become visible to outer projection or path rendering.
- Add optimizer safety checks so recursive predicate lowering does not accidentally use path materialization meant for
  returned paths.

Tests:

- Positive and negated recursive predicates.
- Bound source, bound target, both bound, and anonymous target.
- Recursive predicate combined with normal match expansion in the same query.

## 5. List Comprehensions

Target forms:

```cypher
[x IN list WHERE pred | expr]
[x IN list | expr]
[x IN list WHERE pred]
```

Implementation:

- Add an AST node `ListComprehension` using the existing `FilterExpression` shape plus an optional projection
  expression.
- Translate with a correlated `SELECT array_agg(...) FROM unnest(...)`.
- Define the comprehension variable in an isolated scope frame.
- Use `coalesce(array_agg(...), array[]::<type>[])` to preserve list-return shape.
- Reuse quantifier `IDInCollection` handling where practical, without over-coupling list comprehensions to boolean
  quantifiers.

Tests:

- Literal lists, property arrays, parameters, and `collect(...)`.
- Projection omitted vs. projection provided.
- Predicates using outer variables.
- Empty input and null input semantics.

## 6. Pattern Comprehensions

Target forms:

```cypher
[(a)-->(b) | b.name]
[p = (a)-[*]->(b) WHERE b.enabled | p]
```

Implementation:

- Add an AST node `PatternComprehension` with optional path variable, pattern, optional where, and projection
  expression.
- Lower to a correlated subquery that runs pattern translation and aggregates the projected expression.
- Start with fixed-length patterns only.
- Add variable-length/path support after recursive pattern predicates are solid.
- Use the same scope isolation rules as existential subqueries.

Tests:

- Fixed relationship pattern returning nodes/properties.
- Correlated outer variable source.
- Optional path binding.
- Empty result returns empty list.
- Later: variable-length pattern comprehension.

## 7. UNION / UNION ALL

This should land last because it touches query shape and result contracts.

Implementation:

- Remove the frontend unsupported-rule rejection for `oC_Union`.
- Extend `RegularQuery` AST to represent multiple `SingleQuery` branches plus `ALL` flags.
- Translate each branch independently with its own scope and parameter namespace.
- Validate projection compatibility: same column count and compatible aliases/types.
- Lower to existing PgSQL `SetOperation` with `UNION` / `UNION ALL`.
- Decide alias source. Cypher generally uses the first branch's return names.
- Block branch-local updates initially unless existing update semantics are clearly safe.

Tests:

- `UNION` distinct vs. `UNION ALL`.
- Matching projection aliases and mismatched aliases.
- Parameters in both branches.
- Branch-level vs. final `ORDER BY`, `SKIP`, and `LIMIT`, if the grammar allows the form.

## Suggested Order

1. List indexing and slicing.
2. CASE expressions.
3. Existential subqueries, pattern-only first.
4. Recursive pattern predicates.
5. List comprehensions.
6. Pattern comprehensions.
7. UNION / UNION ALL.

This order builds reusable machinery before the harder features need it: array indexing helps path/list expressions,
CASE exercises scalar AST plumbing, existential subqueries establish correlated subquery scope, and that scope model then
carries comprehensions and recursive pattern predicates.
