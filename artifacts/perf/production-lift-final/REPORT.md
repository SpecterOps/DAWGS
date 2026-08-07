# Production lift final report

Date: 2026-08-07

## Outcome

`sp-static-v2` is active in the public PostgreSQL translator. It selects
`SP-S3-U-D` for its qualified distance-only envelope and
`SP-S3-U-E+MAT-M0` for its qualified one-path envelope. Every failed
eligibility fact retains `SP-S0` with a specific diagnostic. ADCS remains on
`ADCS-INCUMBENT-STEPWISE`; A3 is tool-only because the measured crossover has
no safe static query-shape selector.

The A3 suffix emitter also preserves the recursive terminal boundary's label
and property predicates in the materialized suffix. Forced shortest and A3
translation both fail closed if the selected emitter is not recorded as
applied.

## Production-boundary confirmation

The immediate predecessor and candidate executables ran through the public
Cypher/driver boundary in ten alternating, independently reloaded rounds. Each
case retained 20 untimed warmups and 50 warm samples per arm per round. Cold
diagnostics were not included. Exact observations, fixture checksums, row
counts, PostgreSQL settings, relation sizes, within-arm SQL fingerprints, and
normalized within-arm plan shapes matched.

| Case | p50 ratio, 95% interval | p50 saving, 95% interval | p95 ratio, 95% interval | p95 saving, 95% interval |
|---|---:|---:|---:|---:|
| D2 distance | 0.0843 [0.0431, 0.1515] | 4.897 ms [3.668, 5.933] | 0.1366 [0.1255, 0.1424] | 6.609 ms [6.495, 6.866] |
| D2 path | 0.1304 [0.0717, 0.2098] | 4.871 ms [4.208, 6.087] | 0.1602 [0.1554, 0.1719] | 7.152 ms [7.037, 7.260] |
| D16 distance | 0.0266 [0.0164, 0.0474] | 20.484 ms [17.844, 25.027] | 0.0299 [0.0285, 0.0318] | 40.468 ms [39.271, 41.157] |
| D16 path | 0.0269 [0.0209, 0.0433] | 21.270 ms [18.723, 30.780] | 0.0429 [0.0417, 0.0448] | 33.854 ms [33.366, 34.230] |
| D32 path | 0.0162 [0.0143, 0.0206] | 62.311 ms [48.809, 71.670] | 0.0201 [0.0188, 0.0207] | 82.890 ms [80.828, 84.535] |

Forced raw-SQL captures are diagnostic/reference evidence only and are not used
for this production materiality claim.

## Cumulative live corpus

Five complete rounds produced 935/935 `ok` records: all 94 workload
declarations and 187 supported backend declarations per round. Every
backend/case retained 150 warm samples. The run covered 465 PostgreSQL and 470
Neo4j records and enforced unsupported-mode declarations instead of taking an
intersection after execution.

Selected warm-only PostgreSQL/Neo4j median ratios after activation:

| Case | PostgreSQL median | Neo4j median | PG / Neo4j |
|---|---:|---:|---:|
| D2 distance | 0.214 ms | 1.001 ms | 0.214 |
| D2 path | 0.375 ms | 0.978 ms | 0.383 |
| D16 distance | 0.278 ms | 0.987 ms | 0.281 |
| D16 path | 0.504 ms | 1.061 ms | 0.476 |
| D32 distance | 0.750 ms | 0.927 ms | 0.809 |
| D32 path | 1.091 ms | 1.004 ms | 1.087 |
| D64 distance | 1.338 ms | 0.993 ms | 1.348 |
| D64 path | 1.727 ms | 1.047 ms | 1.649 |
| Typed edge count | 0.058 ms | 0.610 ms | 0.094 |
| HOP-05 sparse thousand endpoints | 1.928 ms | 1.378 ms | 1.399 |

`allShortestPaths` is outside the selector envelope and retains `SP-S0`; its
D4 diamond case remains 12.11x slower than Neo4j by median. Likewise, legacy
base shortest forms that do not satisfy the static envelope retain the
incumbent.

## Semantic, planner, resource, and lifecycle qualification

- All 25 generated shortest cases passed their declared live backend modes (49
  records), covering depth 0-64, fanout through 1,000, inbound/outbound,
  disconnected endpoints, cycles, parallel-edge ties, and self-loops.
- Equal-length path ties are compared exactly across backends only when the
  corpus declares `expected.path_rows`; otherwise each backend must retain a
  stable valid path and exact row count without inventing a Cypher tie-break.
- PostgreSQL `auto`, `force_custom_plan`, and `force_generic_plan` passed D16
  distance and path execution.
- Reachable plans have positive recursive/hydration work and no local/temp
  buffers, temp files/bytes, or read-only WAL. Missing endpoints execute zero
  recursive edge-search loops.
- Half/full/twice-pool concurrency uses a two-connection pool and 25 operations
  per worker at concurrency 1, 2, and 4.
- D64 distance, D64 path, and A3 cancellation returned SQLSTATE `57014` in
  1.1-1.2 ms, below the asserted 250 ms ceiling; rollback and same-PID reuse
  passed.
- D64 distance/path each completed 10,000 warm operations. Distance p50/p95/p99
  was 1.230/1.764/2.232 ms; path was 1.581/2.248/2.721 ms. Both p99 values are
  gated. Aggregate parse-cache state was 20,044 hits, two misses, no bypasses,
  evictions, coalesced misses, or pending entries.
- Unit, PostgreSQL integration, Neo4j integration, and focused race suites
  passed.

## ADCS disposition and current gaps

Native A3 wins the sparse D16/F1000 tier but regresses high reverse fan-in. The
required suffix density and reverse fan-in are data properties, not bounded
static query facts. No bounded same-snapshot runtime probe/fallback passed, so
automatic A3 is permanently closed for this plan. The remaining production
residual is visible in the cumulative corpus: sparse ADCS endpoint/path medians
are roughly 54-66x Neo4j. Reopening this work requires a new runtime-selector
program with holdouts, regret/overflow limits, and exact fallback—not a hidden
extension of `sp-static-v2`.

The live database exposes the repository's fixed 21-partition schema. The
release run exercised active child partitions and verified physical fixture
counts, but did not rebuild the external PostgreSQL service at 1/8/32/128
partition counts. This is retained as deployment-matrix evidence to collect in
environments that actually ship those alternate schemas; it does not alter the
query selector, whose emitted SQL is graph-ID parameter stable.

`make format` cannot complete in this environment because `goimports` is not
installed. All changed Go files were formatted with `gofmt`, and
`git diff --check` is clean.

## Artifact manifest

The raw files are retained under `.coverage/`; reconstructible bundle checksum
files bind executable, source patch, corpus declaration, manifest, and JSONL.

| Artifact | SHA-256 |
|---|---|
| predecessor executable | `dfc9be838e639211fcad41745cf7a6b1631f0b0b614bab890e2a53e7ab97e68a` |
| confirmation predecessor JSONL | `1a12b7c015f32482742e7c833703f4eaf1ec50c4eb8518b4d5bdeb13464d14fe` |
| confirmation candidate JSONL | `94eebb69ac0340b43aae23e39dd89b5996a3776f6c2eb09a60d85cc269ad9053` |
| confirmation report | `fabbd4749a672edbe7a70f2c5baa1ad589413d5a34e0aa56ab9165f256cdf98c` |
| all-shortest semantic JSONL | `2eb5d14d713ce819e12a110f98925dd0d4c73f0a60c701582f2b7d9c2a1c9da0` |
| custom-plan JSONL | `dcfedb13d7d1f28878eb8f413ea80b90c3e59eb6e4892ef89c2e6c257122c934` |
| generic-plan JSONL | `9e971d72a99cb723fc23770e92d04b7c3806d238a54e6fc1ffd206f5211b31a6` |
| cumulative corpus JSONL | `b3a0e81e603ff6424ae87a26b1745b61b90d02bfa25df7bbb85037035e42c0d6` |
| 10k soak JSONL | `20cffd5b6f20ac08a1ed6f4a707a61b77aecaaa9f050816c68caef41378cd41d` |
| semantic bundle checksums | `d77ba96c51fbe602e993d4465c8ec8672e467b21a37dbf33b7e9879135b23e9e` |
| cumulative bundle checksums | `3e74447b3a381be7733cf0de18f52a03215d165bd6fef6d798804089d0e659d0` |
| soak bundle checksums | `b99110df23ccc1746822b8ad32ed2dd6cb2de8da5a713185929c3059008afa06` |
