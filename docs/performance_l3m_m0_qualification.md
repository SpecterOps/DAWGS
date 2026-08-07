# L3M shortest-path materializer qualification

Date: 2026-08-06

Status: `SP-S3-U-E+MAT-M0` is the production-selected one-path architecture for
the narrow `sp-static-v2` eligibility envelope. `SP-S3-U-D` is selected for the
corresponding distance-only envelope. All other shortest-path forms retain
`SP-S0`.

## Selected architecture

The repository-native emitter carries `(next_id, depth, edge_ids)` in recursive
state. It hydrates the ordered edges once, derives terminal nodes from the
direction-specific edge endpoint, and constructs `pathcomposite` directly.
It does not invoke the incumbent shortest-path harness or
`ordered_edge_ids_to_path`.

The whole-stack tournament compared edge-only `SP-S3-U-E+MAT-M0` with
node-and-edge `SP-S3-U-NE+MAT-M1` at the same complete-path boundary. M0 was
retained because M1 did not establish a stable advantage and regressed the
large D32/D64 tiers. Hydration-only and whole-stack results are reported
separately.

## Exactness envelope

PostgreSQL forced execution and Neo4j public-observation oracles passed for 12
cases covering:

- depth 0, 1, 2, 4, 8, 16, 32, and 64;
- fanout through 1,000;
- outbound and inbound direction;
- disconnected endpoints, cycles, parallel edges, and self-loops; and
- exact node/relationship order, kind, duplicate, property, and graph scope.

Focused translator coverage additionally proves that a complete forced-M0 path
survives `WITH` aliasing and that distance-only observations reject this
executor.

## Statistical gates

All reports use 97.5% intervals. The production/reference closure contains ten
matched rounds, 20 untimed warmups, and 50 measured samples per round. All 12
cases passed the 1.10 closure threshold; the worst median-ratio upper bound was
0.943616 for `GSP-D32-F512_path`.

The incumbent/candidate confirmation also contains ten matched rounds with 20
warmups and 50 samples per round. Its executable diagnostic gate includes 12
PostgreSQL performance records and 12 Neo4j oracle records. Every record passed.
The worst PostgreSQL median-ratio upper bound was 0.029390, the worst p95-ratio
upper bound was 0.036035, and the smallest median-saving lower bound was
4.170920 ms.

## Resource and lifecycle gates

The live PostgreSQL plan test verifies:

- edge-only recursive state and exactly one ordered hydration scan;
- no incumbent harness or helper materializer;
- positive recursive and hydration work for a reachable D16 path;
- zero edge-search loops for a missing endpoint;
- zero local buffers, temporary buffers/files/bytes, and read-only WAL; and
- exact concurrent execution at offered worker counts 1, 2, and 4 with a
  two-connection pool.

The live cancellation test cancels the D64/F1000 forced M0 query with a 1 ms
statement timeout, observes PostgreSQL cancellation code `57014`, rolls the
transaction back, reuses the same backend PID, and then executes the exact path
query successfully.

## Artifact index

Artifacts remain raw JSON/JSONL captures; checksums below bind this report to
the exact files produced by the qualification run.

| Artifact | SHA-256 |
|---|---|
| `postgres-l3m-m0-m1-pair-v1.jsonl` | `899b0ab3177fa96834014acd8f4ed4082baf5f19086bef11d23a176fa95fd350` |
| `postgres-l3m-m0-m1-pair-report-v1.json` | `762f1fe5addd1a5af300ebbf56624eb14296847b11c26f804e72627c0d3fb408` |
| `postgres-l3m-m0-m1-hydration-pair-v1.jsonl` | `01b08ebc9361a99ebd64fa4efc1dfff68babbeac3af88b0b9844d2f42444aa4f` |
| `postgres-l3m-m0-m1-hydration-pair-report-v1.json` | `e27215bb71f965b2dbad4cd01a09402813198d0e020777934aeca691d553c94f` |
| `postgres-sp-s3-m0-reference-closure-v1.jsonl` | `407107e811c94f1086ac4e73f8cc00d6fb92f28fd2c258f16f513c594181af83` |
| `postgres-sp-s3-m0-reference-gate-v1.json` | `ab33ac44be7d6019016d38b66ae3c75a5d841d11a5ac2f1b5784c2f037e3d9b3` |
| `postgres-sp-s3-m0-confirm-incumbent-with-oracle-v1.jsonl` | `50b2510a67a8a6e2151ba78282a2e0c8d9560285bff262318b6d48e6884eca41` |
| `postgres-sp-s3-m0-confirm-candidate-with-oracle-v1.jsonl` | `2ae4d7e2ebea76221d00c69cac016234bfbdb4dfbaf0440d74c0fe1260a2c1f3` |
| `postgres-sp-s3-m0-envelope-gate-v3.json` | `bff14a59e67655c1e598f4cd7e280703e22514d5268d12426adfd3fd9cb2461f` |

## Validation

- `make test`: passed.
- PostgreSQL `make test_all`: passed.
- Neo4j `make test_all`: passed.
- `go test -race ./drivers/pg ./cypher/models/pgsql/translate ./cmd/graphbench`: passed.
- Forced M0 plan/resource/concurrency and cancellation manual integration
  tests: passed.
- `git diff --check`: passed.
- Changed Go files were formatted with `gofmt`. `make format` could not run to
  completion because `goimports` is unavailable in the execution environment.

## Promotion result

The later L6/L7 release matrix authorized narrow automatic selection. Ten
matched predecessor/candidate rounds at the public driver boundary retained 500
warm samples per arm for each promoted representative. The candidate p95 ratio
upper bounds were 0.142399 (D2 distance), 0.171868 (D2 path), 0.031834 (D16
distance), 0.044805 (D16 path), and 0.020742 (D32 path). The complete 25-case
generated shortest corpus passed on both live backends, and D64 distance/path
each passed a 10,000-sample prepared-reuse soak with gated p99.
