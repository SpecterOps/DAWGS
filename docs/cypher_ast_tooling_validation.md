# Cypher AST Tooling Validation

Validation date: 2026-05-27.

This records the validation pass for the Cypher AST tooling test-hardening work.

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
  - Result: fail in the local environment with `xargs: goimports: Permission denied`.
  - Touched Go files were formatted with `gofmt`.
- `make test_all`
  - Result: not run because `CONNECTION_STRING` was unset. Integration validation requires that variable.

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
