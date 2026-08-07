# Production-lifting plan completion

Date: 2026-08-07

Status: the `perf_cont_4.md` continuation is complete by implementation,
qualification, or explicit gate disposition. Narrow shortest-path production
selection is active through `sp-static-v2`; ADCS remains on its exact incumbent
because no safe automatic selector passed.

## Phase disposition

| Phase | Disposition |
|---|---|
| L0/L1 | Measurement contracts, generated scale fixtures, exact observations, decision diagnostics, PostgreSQL plans, reference identities, and horizontal implementation increments are present and tested. |
| L2F | Closed with a quantified residual. Production forward lowering passes A0 reference closure on zero-result and high-reverse controls but misses sparse endpoint/path closure. |
| L2S | `SP-S3-U-D` is exact, reference-closed, and automatically selected for the qualified static distance envelope. |
| L3M | `SP-S3-U-E+MAT-M0` is exact, reference-closed, and automatically selected for the qualified static one-path envelope. M1 is closed. |
| L3A | Native `ADCS-A3` is exact and reference-closed. A2/A4 are closed. A3 automatic dispatch is closed by its high-reverse-fan-in regression. |
| L4 | No ADCS static selector can bound the observed crossover, and no runtime selector passed the prescribed same-snapshot fallback gates. Exact incumbent fallback remains selected. |
| L5 | Not triggered: the residuals require a new selector/release program, not an isolated cache, planner-mode, or workspace tweak justified by current evidence. |
| L6/L7 | Shortest automatic activation passed live semantics, immediate-predecessor confirmation, complete cumulative corpus, planner modes, resources, concurrency, cancellation, session reuse, race, and 10k-operation soak. ADCS activation remains closed by its control regression. |

## L2F residual

The production/A0 report contains ten independently reloaded rounds with 20
untimed warmups and 50 measurements per side in each round. Exact public
observations passed before timing was retained.

| Case | Median ratio upper bound | Median gap interval | Gate |
|---|---:|---:|---|
| Sparse endpoint | 1.502054 | +12.550128 to +17.731298 ms | fail |
| Sparse path | 1.652224 | +20.108542 to +25.131649 ms | fail |
| High reverse fan-in | 0.202711 | -4.894024 to -4.175446 ms | pass |
| Zero reachable | 0.964743 | -1.750907 to -0.487799 ms | pass |

This closes L2F's allowed “record the exact remaining planner/emitter gap” exit
path. The direct handwritten A0 comparator does not become production code.

| Artifact | SHA-256 |
|---|---|
| `postgres-adcs-a0-reference-closure-v1.jsonl` | `e061e6419b49717ef8984ba396df0b04ff7c98dd2b4bb395b596559d2c044bdb` |
| `postgres-adcs-a0-reference-gate-v1.json` | `79214f2a7d44aa2856565af91e5e10bcfa2fb17b9b82add84c7320a530e1d418` |

## Activation boundary

The public translator selects `SP-S3-U-D` only for qualified distance
observations and `SP-S3-U-E+MAT-M0` only for qualified one-path observations.
The static envelope requires one non-optional directed traversal, supported
bounded depth 0/1 through 64, no relationship variable or predicate, one static
ID equality per endpoint, no path predicate, one uncorrelated endpoint pair,
one statement-wide shortest call, and a read-only statement. Every failed fact
retains `SP-S0` and its specific fallback code. Tool forcing remains a
qualification seam, not runtime configuration.

ADCS continues to select `ADCS-INCUMBENT-STEPWISE`. Native A3 remains tool-only:
its sparse win is not safely inferable from query structure, and its
high-reverse-fan-in regression closes unconditional selection. The remaining
ADCS objective is therefore a separately scoped bounded runtime probe with
same-snapshot overflow fallback, not unfinished activation work from this plan.

## Final release evidence

- Ten alternating predecessor/candidate rounds retained 500 warm samples per
  arm for D2 distance/path, D16 distance/path, and D32 path. All exact
  observations matched. Candidate p95 ratio upper bounds ranged from 0.020742
  to 0.171868.
- All 25 generated shortest cases passed on their declared PostgreSQL/Neo4j
  modes (49 records), including zero depth, cycles, parallel-edge ties,
  self-loops, inbound traversal, disconnected endpoints, and D64/F1000.
- The cumulative corpus produced 935/935 `ok` records over five independent
  rounds: all 94 declarations and 187 supported backend declarations per
  round, with 150 warm samples per backend/case. Cold diagnostics were excluded.
- `force_custom_plan` and `force_generic_plan` both passed D16 distance/path.
- Half/full/twice-pool concurrency ran 25 operations per worker; cancellation
  returned in 1.1-1.2 ms under the enforced 250 ms bound and reused the same
  backend PID.
- D64 distance and path each passed 10,000 warm operations with gated p99,
  20,044 aggregate parse-cache hits, two misses, and no evictions or pending
  entries.
- Unit, PostgreSQL integration, Neo4j integration, focused race, plan/resource,
  rollback, and session-reuse tests passed.

The local reconstructible bundles and raw artifacts are checksum-bound in
`artifacts/perf/production-lift-final/REPORT.md`.
