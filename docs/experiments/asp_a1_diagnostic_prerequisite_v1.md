# A1 all-shortest diagnostic prerequisite v1

Status: proposed prerequisite for resuming P4. It does not change the A1
executor, its production SQL, automatic selection, resource caps, or public
results.

## Observed gap

The P4 V1 first-round replay reached `ASP-A1-DAG` exactly, but PostgreSQL's
outer `Function Scan` hid all invocation-local work. The P3 B1/B2 diagnostic
workspace cannot be relabeled as A1 evidence: A1 is a separate single-ended
predecessor-DAG executor with its own `spd_*` workspace and scheduler.

## Proposed boundary

Add a session-local A1 diagnostic reader used only by GraphBench's untimed
Repeatable Read replay. Its begin step resets `spd_seen`, `spd_candidate`, and
`spd_predecessor`, records an invocation ID in a dedicated telemetry table,
then executes the unchanged translated A1 statement once. Its reader verifies
that the same session produced exactly one A1 call and reports only values
observed in that replay:

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

The new document must have its own `a1_single_ended` schema and validation.
It may reuse GraphBench's invocation-local replay transaction, all-shortest
counter types, hydration accounting, and workspace counter types, but must
not call or reinterpret the bidirectional B1/B2 diagnostic API.

## Validation and stop rules

The implementation must add SQL-shape and GraphBench unit tests for stale
workspace rejection, one/two-hop, recursive, reconvergent, inbound, and
no-path records; integration tests must verify exact path multisets, session
isolation, cancellation cleanup, and Repeatable Read replay. A clean
telemetry-only A1 run must preserve the same observed result and runtime
identity as the unarmed A1 statement. Missing invocation identity, multiple
calls, hidden/stale counters, a mismatched public path count, or a workspace
measurement that includes telemetry relations fails closed.

Only after those tests and a clean source commit may the P4 open baseline V1
be recaptured from round one. The existing stop artifact, I1 archives, B1/B2
functions, all holdouts, diagnostic/stress cases, manifests, and selectors
remain outside that authorization.
