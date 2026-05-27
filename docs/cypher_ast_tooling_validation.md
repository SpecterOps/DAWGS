# Cypher AST Tooling Validation

Validation date: 2026-05-27.

This records the validation pass for the Cypher AST tooling test-hardening work.

## Review Remediation Preflight

- Branch: `main`, 21 commits ahead of `upstream/main`.
- Baseline: `upstream/main` resolves locally at `9fe779703362543ce2ef6a46fd93f4c040ac1ac0`.
- Existing untracked files left untouched during preflight: `review.md` and `docs/cypher_support_v4.md`.
- Integration validation will be run separately for the Neo4j and PostgreSQL connection strings provided for this
  remediation pass.

## XOR Traversal Audit

`walk.Cypher` consumers in `cypher/models/pgsql/translate`, `cypher/models/pgsql/optimize`, `query/builder.go`, and
`query/neo4j` were audited for the newly reachable `*cypher.ExclusiveDisjunction` node. The PostgreSQL translator
needed an explicit XOR translation path; it now lowers XOR expression-list joins to PostgreSQL boolean inequality.
Reference and source collectors operate on descendant variables and tolerate the newly visible operand sub-trees.

## Walker Benchmark Comparison

Benchmarks were captured with:

```bash
go test -run '^$' -bench 'BenchmarkCypher' -benchmem -count=10 ./cypher/models/walk
```

`upstream/main` does not have `walk.CypherStructural`, so the upstream worktree used the branch benchmark file with the
structural benchmark omitted. The comparable semantic walker results were:

| Benchmark | `upstream/main` | `HEAD` | Delta |
| --- | ---: | ---: | ---: |
| `CypherWalkLargeProjection-20` | 69.72 us/op | 83.54 us/op | +19.83% |
| `CypherWalkLargeMapLiteral-20` | 43.28 ns/op | 141014 ns/op | +325680.29% |

The map-literal number is not an apples-to-apples traversal comparison: `HEAD` visits map items and values, while
`upstream/main` treats the same map literal as a near-leaf. The projection benchmark remains slower after replacing the
post-dequeue reflective nil check with cursor-constructor nil filtering and a leaf fast path in `Generic`; allocations
are effectively flat at 74.55 KiB/op on `upstream/main` vs. 74.41 KiB/op on `HEAD`.

The branch-only structural benchmark measured:

| Benchmark | `HEAD` |
| --- | ---: |
| `CypherStructuralWalkLongPattern-20` | 70.19 us/op, 49.02 KiB/op, 1288 allocs/op |

## Walker Coverage Comparison

Package-local coverage was captured with:

```bash
go test -covermode=count -coverprofile=.coverage/walk-head.cover ./cypher/models/walk
go test -covermode=count -coverprofile=.coverage/walk-upstream.cover ./cypher/models/walk
```

| Revision | Coverage |
| --- | ---: |
| `upstream/main` | 53.2% |
| `HEAD` | 81.3% |

`HEAD` does not lower `cypher/models/walk` package coverage.

## PR Description Notes

Behavior changes to call out:

- `cypher.MapLiteral.Keys()` now returns keys in ascending lexical order. It previously returned descending order.
- `walk.Cypher` now traverses `*cypher.ExclusiveDisjunction`; translator and collector visitors now see XOR operand
  sub-trees.
- `walk.Generic` treats nil roots and nil optional branches as skipped traversal inputs instead of reporting a cursor
  negotiation error.
- `cancelableVisitorHandler.SetError` now accumulates repeated errors with `errors.Join` in a left-associated chain
  instead of storing a flat slice before joining.

New exported APIs:

- `walk.CypherStructural`
- `walk.NewSimpleVisitorWithOrder`
- `walk.OrderInfix`
- `walk.OrderPostfix`
- `cypher.MapLiteral.ForEachItem`

`README.md` has build/test/metric workflow guidance but no walker API summary, so no README API update was needed.

## Commands

- `go test ./cypher/models/walk ./cypher/models/cypher ./cypher/models/cypher/format`
  - Result: pass.
- `go test -covermode=count -coverpkg=./cypher/models/walk,./cypher/models/cypher,./cypher/models/cypher/format -coverprofile=/tmp/cypher_ast_tooling_validation.cover ./cypher/models/walk ./cypher/models/cypher ./cypher/models/cypher/format`
  - Result: pass.
  - Package coverage: `walk` 27.1%, `cypher` 18.4%, `format` 28.4%.
- `make test`
  - Result: pass.
  - Wrote `.coverage/coverage.txt`.
- `make format`
  - Initial result: fail in the local environment with `xargs: goimports: Permission denied` because the
    wrapper-managed Go bin directory was not on `PATH`.
- `PATH="/home/zinic/codex/config/go/bin:$PATH" make format`
  - Result: pass.
  - No file changes after formatting.
- `CONNECTION_STRING=<neo4j connection string> make test_neo4j`
  - Result: pass.
- `CONNECTION_STRING=<postgres connection string> make test_pg`
  - Result: pass.

## CRAP Snapshot

CRAP was calculated from the focused coverage profile for the altered Cypher AST tooling paths.

| CRAP | Complexity | Coverage | Function |
| ---: | ---: | ---: | --- |
| 18.00 | 18 | 100.0% | `cypher/models/walk/walk.go:189 Generic` |
| 14.00 | 14 | 100.0% | `cypher/models/walk/walk_cypher.go:66 newCypherStructuralValueWalkCursor` |
| 11.00 | 11 | 100.0% | `cypher/models/walk/walk_cypher.go:540 newCypherUpdatingWalkCursor` |
| 11.00 | 11 | 100.0% | `cypher/models/walk/walk_cypher.go:478 newCypherClauseWalkCursor` |
| 10.00 | 10 | 100.0% | `cypher/models/walk/walk_cypher.go:267 newCypherPredicateWalkCursor` |
| 10.00 | 10 | 100.0% | `cypher/models/walk/walk_cypher.go:217 newCypherValueWalkCursor` |
| 9.02 | 9 | 94.1% | `cypher/models/walk/walk_cypher.go:175 newCypherWalkCursor` |
| 9.00 | 9 | 100.0% | `cypher/models/cypher/format/format.go:326 (Emitter).formatMapLiteral` |
| 8.03 | 8 | 92.3% | `cypher/models/walk/walk_cypher.go:591 newCypherPatternWalkCursor` |
| 8.00 | 8 | 100.0% | `cypher/models/walk/walk_cypher.go:347 newCypherProjectionWalkCursor` |
| 7.00 | 7 | 100.0% | `cypher/models/walk/walk_cypher.go:401 newCypherStatementWalkCursor` |
| 6.00 | 6 | 100.0% | `cypher/models/walk/walk_cypher.go:316 newCypherOperatorWalkCursor` |
| 6.00 | 6 | 100.0% | `cypher/models/walk/walk_cypher.go:143 newCypherStructuralPatternWalkCursor` |
| 6.00 | 2 | 0.0% | `cypher/models/cypher/model.go:1001 (*ListLiteral).Keys` |
| 4.00 | 4 | 100.0% | `cypher/models/walk/walk_cypher.go:458 newCypherSinglePartQueryWalkCursor` |
| 4.00 | 4 | 100.0% | `cypher/models/walk/walk_cypher.go:438 newCypherMultiPartQueryPartWalkCursor` |
| 3.00 | 3 | 100.0% | `cypher/models/walk/walk_cypher.go:390 newCypherQueryWalkCursor` |
| 3.00 | 3 | 100.0% | `cypher/models/walk/walk_cypher.go:55 newCypherStructuralWalkCursor` |
| 3.00 | 3 | 100.0% | `cypher/models/cypher/model.go:949 (MapLiteral).ForEachItem` |
| 2.00 | 2 | 100.0% | `cypher/models/walk/walk_cypher.go:204 newCypherLeafWalkCursor` |
| 2.00 | 2 | 100.0% | `cypher/models/cypher/model.go:959 (MapLiteral).Keys` |
| 1.00 | 1 | 100.0% | `cypher/models/cypher/model.go:935 (MapLiteral).Items` |

`(*ListLiteral).Keys` is included in the snapshot because it matched the measured function-name set, but it was not part
of this change sequence.
