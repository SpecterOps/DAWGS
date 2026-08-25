# Fixed-suffix cardinality metadata audit

Status: **no hard pre-translation bound is currently available**.

This audit asks whether production translation can directly select
`EXPANSION-SUFFIX-SEEDED-REVERSE` only when it can prove both physical suffix
rows and reverse states are at most 512, without executing the retired runtime
probe/fallback design.

## Existing inputs

- The public translator receives the Cypher AST, kind mapper, parameters, and
  graph ID. It has no database connection or graph-cardinality provider.
- Graph schema metadata describes names, kinds, indexes, and constraints. It
  does not contain degree, suffix-row, path, or reverse-state bounds.
- The PostgreSQL `graph` catalog contains only graph ID and name. Partition
  models contain table names, indexes, and constraints.
- `OptimizeStorage` reads approximate live/dead tuple counts for vacuum
  decisions. These counts are database-storage statistics, not per-root or
  per-kind hard bounds.
- PostgreSQL planner statistics and `pg_class.reltuples` are estimates. They
  are neither correctness-grade upper bounds nor available to the optimizer
  before SQL emission.
- Translation caching is keyed by query text, graph ID, and parameter types.
  A selector dependent on parameter values or mutable graph cardinality would
  require new invalidation and cache-identity rules.

## Finding

Suffix rows and reverse states depend on the selected root, relationship kinds,
query depth, physical trail multiplicity, and current graph contents. Global
node/edge counts or planner estimates cannot prove either 512 ceiling. No
existing schema constraint establishes these limits, and no maintained
per-graph or per-root synopsis supplies conservative upper bounds.

Therefore the S511/S512 wins do not currently support automatic production
dispatch. Production must continue selecting `EXPANSION-STEPWISE-FORWARD` for
this family. A future attempt would require a new proof-bearing metadata/API
contract plus mutation-safe maintenance, cache invalidation, and independent
qualification; that work is outside this completed audit.
