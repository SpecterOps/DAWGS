# A1 all-shortest diagnostic prerequisite v1

Status: implemented and PostgreSQL-integration-validated. A clean source
commit and fresh P4 capture are still required before this can produce any
performance evidence. It does not change A1 selection, resource caps, or
public results.

## Observed gap

The P4 V1 first-round replay reached `ASP-A1-DAG` exactly, but PostgreSQL's
outer `Function Scan` hid all invocation-local work. The P3 B1/B2 diagnostic
workspace cannot be relabeled as A1 evidence: A1 is a separate single-ended
predecessor-DAG executor with its own `spd_*` workspace and scheduler.

## Implemented boundary

The implementation adds a session-local A1 diagnostic reader used only by
GraphBench's untimed Repeatable Read replay. Its begin step resets `spd_seen`,
`spd_candidate`, and `spd_predecessor`, records an invocation ID in a dedicated
temporary telemetry table, then executes the translated A1 statement once. Its
reader verifies that the same session produced exactly one A1 call and reports
only values observed in that replay:

- per-depth candidate, admitted-node, and predecessor counts from the three
  `spd_*` relations, with cumulative seen and predecessor peaks;
- single-ended scheduler, target depth/no-path branch, and no-fallback A1
  runtime identity;
- path count and edge cells derived from the exact replayed public path set;
- serialized output bytes from the exact GraphBench path observation;
- outer hydration loops/rows/time from the untimed timing-on plan; and
- session and pool workspace high water from `pg_total_relation_size` over
  `spd_*` only, excluding telemetry tables.

Depth-one and depth-two returns leave no `spd_*` rows by design. The reader
must label those exact branches explicitly and derive their path count/depth
from the replayed path set; it must never reuse rows from an earlier call.
No-path results likewise require a cleared workspace and an explicit no-path
receipt.

The A1 receipt has its own `a1_single_ended` schema and validation. It reuses
GraphBench's invocation-local replay transaction, all-shortest counter types,
hydration accounting, and workspace counter types, but does not call or
reinterpret the bidirectional B1/B2 diagnostic API.

The stored A1 function reads the local
`dawgs.asd_diagnostic_invocation_id` setting once per call. With that setting
absent, it performs no telemetry-table writes or workspace-count queries. With
the setting present, it records the invocation-local receipt. This is a small
production SQL instrumentation change, so the recapture must start from a
clean commit and must not be compared with the stopped V1 artifact.

## Validation and clean-recapture gate

The implementation includes SQL-shape tests for opt-in, session-local A1
telemetry and symmetric teardown; GraphBench unit tests for complete and
stale/contradictory receipts; and PostgreSQL integration coverage for one-hop,
two-hop, recursive inbound, reconvergent, and no-path records. The integration
contract checks the forced `ASP-A1-DAG` identity, exact public observation,
complete A1/hydration/workspace counters, Repeatable Read replay, two-session
isolation for a shared invocation key, cancellation rollback, and reuse of the
same backend PID.

The targeted A1 integration tests and the full PostgreSQL `make
test_integration` suite pass. Missing invocation identity, multiple calls,
hidden/stale counters, a mismatched public path count, or a workspace
measurement that includes telemetry relations fails closed.

Only after this implementation is committed from a clean source may the P4
open baseline V1 be recaptured from round one. The existing stop artifact, I1
archives, B1/B2 functions, all holdouts, diagnostic/stress cases, manifests,
and selectors remain outside that authorization.
