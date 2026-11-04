# PostgreSQL V2 PR draft

Retained branch delivery draft, including historical benchmark and validation
records. "Current" and "latest" below refer to the recorded capture revisions;
this document does not certify the present branch tip. Maintained setup and
runtime guidance lives in the [documentation index](../README.md).

## Description

Delivers PostgreSQL driver v2 in `drivers/pg`, combining connection-local runtime state with broader CySQL correctness coverage. It also repairs shared graph APIs, Neo4j query handling, and import behavior.

- **PostgreSQL runtime:** adds bounded translation caches per physical connection, shared immutable shortest-path templates, generation-based invalidation, configurable pool limits, statement warm-up, and query-text-free diagnostics. Shared AST and compiler caches remain in place. `pg.NewPool` still returns the caller-owned `*pgxpool.Pool` passed to `pg.NewDriver` or `dawgs.Open`.
- **Kind metadata:** replaces per-query catalog reloads and wrapper dictionary copies with immutable shared snapshots and private transaction overlays. Known names and result IDs resolve in memory; name misses use deduplicated targeted lookups on the owned connection. Unknown result IDs retain their mapping error and signal a coalesced reload at a later idle boundary. Registrations publish only after commit, destructive refreshes invalidate both translation layers, and stable transactions preserve their catalog view. First readers share initialization; schema assertion creates storage before servicing pending refreshes. Actual metadata lookup, refresh and registration work is included in SQL-generation diagnostics.
- **Traversal and query semantics:** expands variable-length and shortest-path lowering while preserving direction, path order, relationship uniqueness, duplicate input rows, OPTIONAL MATCH cardinality, and multipart boundaries. Unregistered kinds are treated as absent in MATCH without registering them. WITH ordering/pagination, UNWIND, typed collections, arithmetic, aggregates, temporal values, Boolean predicates, and result aliases receive regression coverage.
- **Writes and schema:** preserves SET/REMOVE order, batch mutation intent, and read-after-write visibility across multipart queries. Plain DELETE checks incident relationships; detach cleanup stays within the owning graph. Graph selection looks up existing graphs, explicit schema assertion reconciles all declared graphs, and transactional kind registration uses the leased connection and publishes new IDs only after commit.
- **Neo4j and shared APIs:** fixes quoted identifiers in batch upserts, conjunctive relationship-kind filters, Boolean grouping, and raw-query rewrite fidelity. Repairs batch identity collisions, reachability-cache completeness, bitmap/container locking and iteration, and parallel-fetch cancellation.
- **Import handling:** preserves exact integers and JSON string contents, recursively scrubs nested fields, and validates collection fragment paths and symlinks.
- **Regression and qualification tooling:** adds shared backend-equivalent query and mutation fixtures, translation goldens, driver-specific schema/codec tests, generated-case CI checks, and enforced BDD gates. GraphBench and plan-corpus tooling add reproducible captures, portable evidence bundles, plan comparisons, and statistical/resource checks. PostgreSQL-specific kind-cache tests cover publication races, rollback, initialization, refresh failures and snapshot adoption. The README is condensed into a project overview with detailed guides under `docs/`; commit-message maintenance scripts and tests are included.

Manifest-backed traversal canaries and topology routing remain default-off and require separate qualification. The benchmark results below use default runtime policies; capture revisions are identified separately.

### Compatibility and upgrade notes

- Requires Go 1.27.3. Dependency updates are included.
- Use `pg.NewPool` or `pg.NewPoolWithRuntimeConfig` to install the connection lifecycle hooks. Defaults are 64 retained translations per live connection, 128 shared shortest-path templates, and a 5–50 connection pool; cache capacities and pool bounds are configurable.
- Cache diagnostics change shape: `Driver.TranslationCacheStats()` now returns runtime `pg.Stats`. Migrate consumers of the previous compiler-cache counters to `Driver.CompilationCacheStats()` and `pg.CompilationCacheStats`.
- Run `AssertSchema` when upgrading to install the SQL runtime helpers, topology metadata, and graph-scoped cleanup triggers. Schema assertion performs DDL and resets pooled type metadata; reset or recreate pools after out-of-band schema changes affecting generated SQL or types.
- Kind metadata has no polling or automatic freshness bound. After committing external kind deletions or catalog resets, call `driver.RefreshKinds(ctx)` on every affected driver. An unknown result kind ID signals later recovery without retrying the failed query. `GetKindIDsByKind` still returns an independently mutable copy.
- PostgreSQL JSON/JSONB results and OpenGraph imports now preserve integer tokens as `int64`, including nested values. Untyped consumers that expected `float64` must handle `int64`; after draining query results, check `Result.Error()` for decoding/iteration errors. Result-column aliases preserve their requested case and spelling.
- Unsupported parser constructs return explicit errors before PostgreSQL executes preceding writes: standalone/in-query CALL, LOAD CSV, CREATE UNIQUE, list indexing/slicing, and expression-position shortest paths. The MATCH-pattern shortest-path form remains supported. Neo4j retains native handling where queries pass through to its server. The v2 query builder also rejects empty kind lists in predicates and kind mutations.
- Direct translator users must supply the graph ID to `translate.NewExpansionBuilder`. Consumers rendering staged multipart writes must execute every statement from `translate.TranslatedStatements` in the same explicit transaction. Treat batch identity keys and property hashes as opaque runtime values.

## Latest recorded benchmarks

The latest PostgreSQL captures compare `upstream/main` [2d3c62b7](https://github.com/SpecterOps/DAWGS/commit/2d3c62b74370b74d830a3d30fc77679263b019dd), the original PR [4790a2c8](https://github.com/SpecterOps/DAWGS/commit/4790a2c82a43ea166640e92401d3601e42b91744), a diagnostic build removing the two eager reload calls, and the P1 kind-cache implementation. Results were recorded on 2026-11-02; upstream was rechecked during that capture and was unchanged.

The current changeset is [fae5140f](https://github.com/SpecterOps/DAWGS/commit/fae5140f3a8bf9a5d30e320a7ffc31d4ab2858a9). The captures used an implementation working tree based on `4790a2c8`; its recorded source hashes differ from the current changeset, which also includes subsequent initialization, snapshot-adoption and query-boundary fixes. These are the latest available implementation measurements, not a benchmark of the exact current commit.

### PostgreSQL: warmed kind-cache comparison

- Same Linux/amd64 host, Intel Core i9-12900HK, Go 1.27.3 and PostgreSQL 18.6. Identical external harness source and physical fixture IDs across builds; three nodes, two edges and a 715-kind catalog.
- Four balanced rounds, 100 warm-ups and 200 timed requests per case/build: **800 observations per row and build**. Initial pool connections were leased and queried before capture; pool bounds remained 5–50. Rows and decoded kinds were verified, and SQL tracing used an additional untimed request.
- Main comparisons are limited to scope-independent scalar queries and identical graph-scoped Raw SQL. Graph counts, entities and paths have no main comparator because graph scoping differs. No compilation, CPU profiling or other live test suites ran during timing.

Cells show **p50 / p95**. Warm scalar latency fell from 446.42µs to 33.06µs; fluent and Cypher node-count medians fell from 488.31µs and 464.15µs to 29.53µs and 19.90µs. Every final warm implementation trace contained **zero kind-catalog statements**, versus one full 715-row reload per builder/Cypher request in the original PR.

| Query | upstream/main | PR | No-refresh diagnostic | P1 implementation |
|---|---:|---:|---:|---:|
| Cypher scalar | 29.95µs / 44.60µs | 446.42µs / 752.66µs | 38.04µs / 117.00µs | 33.06µs / 41.41µs |
| Fluent node count | — | 488.31µs / 784.81µs | 37.86µs / 117.85µs | 29.53µs / 69.13µs |
| Cypher node count | — | 464.15µs / 777.17µs | 47.53µs / 91.04µs | 19.90µs / 57.51µs |
| Kind-filtered node projection | — | 502.67µs / 842.24µs | 35.16µs / 121.08µs | 33.55µs / 118.71µs |
| Decoded entity | — | 479.77µs / 818.92µs | 80.96µs / 130.71µs | 34.01µs / 119.91µs |
| Decoded shortest path | — | 6.122ms / 8.266ms | 6.605ms / 8.562ms | 5.846ms / 7.604ms |
| Raw decoded entities | 46.73µs / 138.16µs | 54.04µs / 179.65µs | 71.65µs / 203.60µs | 44.34µs / 128.56µs |
| Raw scalar | 29.07µs / 49.07µs | 21.01µs / 91.47µs | 31.55µs / 53.88µs | 29.07µs / 38.10µs |
| Raw node count | 36.31µs / 51.17µs | 40.15µs / 74.97µs | 39.77µs / 72.85µs | 36.33µs / 96.00µs |

Scalar client allocation fell from approximately 30,284 bytes/request in the no-refresh diagnostic to 2,922 bytes/request; decoded-entity allocation fell from 37,566 to 10,075 bytes/request. These totals include all client work. Real absent-name probes issued one targeted lookup per request (141.10µs / 284.79µs), and signaled-refresh scalar probes issued one full reload (450.21µs / 830.97µs).

Component benchmarks used five repetitions with `-benchmem -benchtime=200ms -cpu=1`. Wrapper construction used a nil physical connection; commit bookkeeping excluded database I/O. Medians follow.

| Seed kinds | Wrapper ns/op | Wrapper B/op / allocs/op | Overlay lookup ns/op | Unchanged commit bookkeeping ns/op |
|---:|---:|---:|---:|---:|
| 32 | 135.1 | 416 / 2 | 21.90 | 4.116 |
| 715 | 145.2 | 416 / 2 | 21.84 | 4.090 |
| 7,150 | 180.1 | 416 / 2 | 22.52 | 4.143 |

Overlay lookups and unchanged commit bookkeeping allocated zero bytes. Dictionary copies remain on publication or explicitly requested owned copies. A separate control warmed all 50 pool connections before measuring empty transactions: implementation throughput was 653k–861k requests/sec versus 147k–167k for the diagnostic, with approximately 666–696 versus 27,966–27,988 client bytes/request.

These are descriptive local measurements on small fixtures. Identical-binary controls showed substantial host variation, including scalar p50 of 12.88–34.17µs. The final implementation scalar median remained 3.11µs above main; Raw-count p95 was 96.00µs versus 51.17µs on main and 72.85µs in the diagnostic. Earlier cohorts reversed that tail difference, so precise latency equivalence and elimination of tail risk are not established. Production-scale performance and default-off traversal policies require separate qualification. Cold optimizer overhead remains unresolved.

### Neo4j: matched-query follow-up

The follow-up review used identical parameterized statements, binding types and fixed physical IDs on main and the original PR, with Neo4j Community 4.4.44. It predates the PostgreSQL-only P1 fix.

| Scenario | upstream/main p50 | PR p50 | Change |
|---|---:|---:|---:|
| Base all-shortest | 1.342ms | 1.485ms | +10.6% |
| Diamond all-shortest | 1.233ms | 1.183ms | −4.0% |
| Disconnected all-shortest | 1.127ms | 1.128ms | Approximately unchanged |

The earlier committed-harness comparison used literal endpoints on main and parameters on the PR; parameterized `Query` parsing adds 0.33–0.42ms on both revisions. Earlier diamond/disconnected regressions did not reproduce with matched inputs. The base offset remains unresolved, and identical-binary controls produced apparent median changes of −10.6% to +1.9%. These observations do not establish a repeatable Neo4j implementation regression; batch-write performance was outside the read measurements.

Local capture protocols, raw samples, source hashes, allocation summaries and validation logs are retained under `.coverage/p1/` and `.coverage/pr125/regression-review/`. These ignored artifacts are available in the measurement workspace and are not part of the PR.

### PostgreSQL vs. Neo4j: latest available captures

The table compares the P1 PostgreSQL implementation capture above (800 observations per case) with the latest recorded Neo4j original-PR capture (`4790a2c8`, 600 observations per case). Both used the same host and three-node/two-edge `base` fixture, four balanced rounds and 100 warm-ups. They were separate capture cohorts with different harness versions; this is a descriptive comparison of the latest available results, not a simultaneous comparison of the exact current commit. The P1 changes affect PostgreSQL, but Neo4j has not been recaptured at the current commit.

Timings are **p50 / p95**. Delta is **(PG / Neo4j − 1) × 100%**: negative values mean lower PostgreSQL latency. Raw cases use native SQL on PostgreSQL and native Cypher on Neo4j for the same scalar/count result.

| Query | Neo4j p50 / p95 | PostgreSQL p50 / p95 | PG vs. Neo4j p50 | PG vs. Neo4j p95 |
|---|---:|---:|---:|---:|
| Cypher scalar | 895.73µs / 1,726.93µs | 33.06µs / 41.41µs | -96.3% | -97.6% |
| Fluent node count | 927.79µs / 1,763.89µs | 29.53µs / 69.13µs | -96.8% | -96.1% |
| Cypher node count | 904.24µs / 1,584.74µs | 19.90µs / 57.51µs | -97.8% | -96.4% |
| Kind-filtered node projection | 960.91µs / 1,686.95µs | 33.55µs / 118.71µs | -96.5% | -93.0% |
| Raw scalar | 902.52µs / 1,606.39µs | 29.07µs / 38.10µs | -96.8% | -97.6% |
| Raw node count | 875.98µs / 1,510.16µs | 36.33µs / 96.00µs | -95.9% | -93.6% |
| Base all-shortest path | 1,484.71µs / 2,454.40µs | 5,846.04µs / 7,603.63µs | +293.7% | +209.8% |

PostgreSQL scalar/count/projection p50 was 95.9–97.8% lower in these captures; base all-shortest-path p50 was 293.7% higher (3.94× Neo4j). The path query and parameter types match, but the P1 harness additionally checks decoded path shape and kinds. Host variation, differing capture windows and that additional validation limit causal claims; these small fixtures do not establish production-scale backend performance.

### PostgreSQL vs. Neo4j: full shared-scenario spread

The broader committed-harness capture covers all **21 shared scenarios** on both backends at `upstream/main` (`2d3c62b7`) and the original PR (`4790a2c8`). Each scenario/backend/revision has 200 observations across four balanced rounds, with one worker, one warm-up and 50 timed iterations per round. Fixtures were reloaded per capture; all expected row counts matched. PostgreSQL 18.6 and Neo4j Community 4.4.44 ran sequentially on the same host with Go 1.27.3.

These captures predate the P1 kind-cache fix. They provide the complete recorded scenario spread; the newer P1 results above cover a smaller selection and should be used for that implementation. Warm-up drift affected the broad capture, and shortest-path endpoints were literals on main and parameters on the original PR. Each within-revision backend comparison uses the same query form, but changes between revisions also reflect that workload difference. These tables are descriptive fixture measurements.

Every timing cell is **p50 / p95 in µs**; every delta cell is **p50 / p95**, calculated as **(PG / Neo4j − 1) × 100%**. Negative deltas mean lower PostgreSQL latency. Main graph scoping differs from the PR, so main and PR timings are shown as separate backend comparisons rather than as a graph-query improvement claim.

**Base fixture — 3 nodes, 2 edges**

| Scenario | Main Neo4j | Main PG | Main PG vs. Neo4j | PR Neo4j | PR PG | PR PG vs. Neo4j |
|---|---:|---:|---:|---:|---:|---:|
| Match Nodes | 1,350.97 / 8,619.41 | 69.57 / 191.08 | -94.9% / -97.8% | 1,050.08 / 2,369.70 | 494.22 / 1,000.39 | -52.9% / -57.8% |
| Match Edges | 1,139.88 / 5,972.32 | 142.99 / 363.26 | -87.5% / -93.9% | 1,012.20 / 2,221.12 | 547.29 / 1,000.82 | -45.9% / -54.9% |
| Shortest Paths / n1 -> n3 | 1,267.06 / 6,561.97 | 15,202.86 / 18,661.49 | +1099.9% / +184.4% | 1,651.47 / 2,883.55 | 7,658.09 / 10,045.51 | +363.7% / +248.4% |
| Traversal / n1 | 1,118.67 / 5,166.10 | 638.30 / 1,197.38 | -42.9% / -76.8% | 1,046.22 / 2,129.26 | 649.62 / 1,178.07 | -37.9% / -44.7% |
| Match Return / n1 | 1,035.22 / 4,758.17 | 222.10 / 425.53 | -78.5% / -91.1% | 1,012.53 / 2,263.13 | 480.58 / 1,004.55 | -52.5% / -55.6% |
| Filter By Kind / NodeKind1 | 1,031.92 / 4,058.95 | 77.28 / 229.28 | -92.5% / -94.4% | 1,021.87 / 3,145.42 | 446.74 / 998.09 | -56.3% / -68.3% |
| Filter By Kind / NodeKind2 | 923.33 / 3,551.83 | 88.61 / 236.80 | -90.4% / -93.3% | 983.85 / 2,047.09 | 482.74 / 945.20 | -50.9% / -53.8% |

**Traversal-shapes fixture — 45 nodes, 41 edges**

| Scenario | Main Neo4j | Main PG | Main PG vs. Neo4j | PR Neo4j | PR PG | PR PG vs. Neo4j |
|---|---:|---:|---:|---:|---:|---:|
| Match Nodes | 829.45 / 2,802.78 | 82.86 / 231.01 | -90.0% / -91.8% | 951.21 / 1,870.11 | 562.72 / 944.38 | -40.8% / -49.5% |
| Match Edges | 749.63 / 3,020.37 | 100.59 / 304.03 | -86.6% / -89.9% | 955.20 / 2,003.29 | 617.80 / 1,003.56 | -35.3% / -49.9% |
| Traversal Depth / chain depth 1 | 863.84 / 2,846.83 | 176.19 / 526.08 | -79.6% / -81.5% | 1,019.88 / 2,560.41 | 515.41 / 1,024.74 | -49.5% / -60.0% |
| Traversal Depth / chain depth 3 | 907.21 / 2,681.76 | 839.88 / 1,698.64 | -7.4% / -36.7% | 1,149.14 / 2,897.40 | 536.00 / 1,155.42 | -53.4% / -60.1% |
| Traversal Depth / chain depth 10 | 898.55 / 2,488.29 | 1,098.04 / 1,827.72 | +22.2% / -26.5% | 1,152.47 / 2,606.00 | 652.72 / 1,183.76 | -43.4% / -54.6% |
| Traversal Depth / fanout depth 1 | 794.59 / 3,325.58 | 145.23 / 474.16 | -81.7% / -85.7% | 1,038.98 / 2,117.98 | 522.01 / 947.66 | -49.8% / -55.3% |
| Traversal Depth / fanout depth 2 | 910.39 / 2,725.82 | 887.27 / 1,657.87 | -2.5% / -39.2% | 924.72 / 2,498.18 | 601.39 / 1,190.02 | -35.0% / -52.4% |
| Traversal Depth / fanout depth 3 | 945.42 / 2,614.10 | 971.15 / 1,949.84 | +2.7% / -25.4% | 938.65 / 2,213.22 | 666.41 / 1,249.41 | -29.0% / -43.5% |
| Traversal Cycle / bounded cycle | 895.21 / 2,394.68 | 544.29 / 1,107.49 | -39.2% / -53.8% | 1,040.83 / 2,105.79 | 525.46 / 1,145.22 | -49.5% / -45.6% |
| Traversal Dead End / chain terminal | 919.36 / 2,108.44 | 202.63 / 443.62 | -78.0% / -79.0% | 876.12 / 1,791.25 | 510.09 / 1,122.23 | -41.8% / -37.3% |
| Edge Kind Traversal / Allowed | 960.32 / 2,467.22 | 550.34 / 1,126.23 | -42.7% / -54.4% | 878.81 / 1,663.19 | 518.91 / 1,269.93 | -41.0% / -23.6% |
| Edge Kind Traversal / all kinds | 955.07 / 1,750.02 | 623.54 / 1,437.21 | -34.7% / -17.9% | 932.53 / 1,837.16 | 616.20 / 1,120.94 | -33.9% / -39.0% |
| Shortest Paths / diamond many paths | 1,267.76 / 2,430.91 | 15,346.89 / 20,360.82 | +1110.5% / +737.6% | 1,530.77 / 2,724.60 | 8,328.70 / 11,355.85 | +444.1% / +316.8% |
| Shortest Paths / disconnected | 994.49 / 2,924.15 | 11,926.05 / 14,224.42 | +1099.2% / +386.4% | 1,584.53 / 2,703.09 | 5,131.06 / 6,805.74 | +223.8% / +151.8% |

The full-spread tables are derived from `.coverage/pr125/benchmark-summary.json`; newer P1 and matched-query results retain their own capture protocols and sample counts.

### Compiler microbenchmarks

These earlier measurements compare main with the original PR (`4790a2c8`); the P1 fix does not change the optimizer. The six shared optimizer/compiler-cache benchmarks used identical source and query inputs, five repetitions, and `-benchtime=200ms -benchmem`. The table reports the median observation per benchmark. Optimizer costs increased about 61–64%, uncached translation increased 13.3%, and cache-hit timings stayed close to baseline. These benchmark functions exercise the shared compiler cache.

| Unit benchmark | upstream/main ns/op | PR ns/op | Change | upstream/main → original PR B/op |
|---|---:|---:|---:|---:|
| OptimizeTrivialQuery | 6490.0 | 10448.0 | +61.0% | 5416 → 8607 |
| OptimizeADCSQuery | 111708.0 | 183509.0 | +64.3% | 91208 → 155502 |
| TranslationCacheUncached | 121296.0 | 137390.0 | +13.3% | 81049 → 91363 |
| TranslationCacheColdPopulation | 134625.0 | 141474.0 | +5.1% | 84878 → 95750 |
| TranslationCacheParameterizedHit | 737.4 | 686.9 | -6.8% | 496 → 496 |
| TranslationCacheParameterlessHit | 86.2 | 84.6 | -1.8% | 0 → 0 |

## Type of Change

- [x] Bug fix
- [x] New feature / enhancement
- [x] Refactor
- [x] Breaking API / result-shape changes (see compatibility notes)
- [x] Test coverage
- [x] Build / CI / tooling
- [x] Documentation

## Testing

Validation includes recorded unit and integration results plus a targeted PostgreSQL telemetry run against `fae5140f`. Full suites were not rerun for this description update.

- [x] Unit tests added / updated
- [x] Integration tests added / updated
- [x] Backend validation performed with `CONNECTION_STRING` set

| Check | Result |
|---|---|
| Unit tests (race detection and coverage) | Passed; 67.5% statement coverage versus the recorded 67.4% baseline |
| PostgreSQL telemetry tests (current commit, targeted rerun) | Both passed with race detection, integration tags and serial execution; no skips |
| PostgreSQL BDD (`make test_bdd_integration`) | Passed: 17 enforced scenarios, 0 failed |
| Final PostgreSQL driver and semantic suites | Passed with `-race -tags "manual_integration integration" -p=1 -parallel=1 ./drivers/pg/... ./integration -count=1` |
| Neo4j Community 4.4.44 (`make test_all`) | Passed, including BDD: 17 enforced scenarios, 0 failed |
| Live benchmark captures and expected-cardinality checks | Passed on both backends |
| Shared compiler microbenchmarks | Completed on both revisions |
| `make lint` and formatting check of changed Go files | Passed |
| `go mod verify` and `go mod tidy -diff` | Passed |

BDD pass counts refer to enforced scenarios; `@report-only` compatibility cases remain non-gating.

## Screenshots

N/A

## Driver Impact

- [x] PostgreSQL driver (`drivers/pg`)
- [x] Neo4j driver (`drivers/neo4j`)

## Checklist

- [x] Changed Go code passes formatting checks
- [x] Recorded gating checks pass, including the current PostgreSQL telemetry run
- [x] `go.mod` / `go.sum` are up to date
