# GraphBench

`graphbench` runs the scale benchmark corpus under `benchmark/testdata/scale`.
It is meant for runtime gap accounting: query duration, returned row counts,
PostgreSQL plan details, Neo4j plan operators, fallback reasons, and comparison
summaries.

The implemented execution modes are:

- `postgres_sql`: runs DAWGS' PostgreSQL SQL translation against a PostgreSQL database.
- `neo4j`: runs the same corpus against Neo4j through the DAWGS Neo4j backend.

`local_traversal` is accepted only to record explicit `not_implemented` diagnostic placeholders. It is excluded from
performance gates and must not be presented as an executor result.

Apache AGE is not an execution mode in this harness yet. AGE behavior can be
captured in corpus `reference_design` notes so DAWGS can use it as design input
without treating it as a direct benchmark comparison.

## Inputs

The command loads cases from `benchmark/testdata/scale` by default and imports
the fixture datasets from `integration/testdata`.

Corpus parameters support fixture IDs through `node_params` and
`node_list_params`. Tagged datetime values are decoded to `time.Time`, avoiding
lexical string comparisons in temporal cases. Mutating cases require an
explicit `write_scenario`; the runner checks matched and affected counts plus
post-state queries and rolls back warm-up, timed iterations, and PostgreSQL
plan capture.

Read cases that return node IDs can declare `expected.id_rows` using fixture
node names. GraphBench reverse-maps backend-assigned IDs through the complete
dataset ID map and compares the rows as a multiset, preserving duplicates.

Connection strings can be supplied as flags or environment variables:

- PostgreSQL: `-pg-connection`, `PG_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.
- Neo4j: `-neo4j-connection`, `NEO4J_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.

Every output record includes the DAWGS source version plus source commit,
dirty-worktree hash (including untracked files), binary hash, sanitized invocation,
Go/OS/CPU/kernel/cgroup data, run UUID, arm/block/order timestamps, pool settings,
and declared memory ceilings. Use
`-dawgs-version` to override the auto-detected DAWGS version.
Portable bundle creation reconstructs that dirty-worktree hash from the exact
bundled binary patch plus sorted untracked path/content bytes and refuses a
run-environment mismatch. Verification repeats the reconstruction and rejects
malformed, duplicated, mismatched, or unchecksummed untracked entries/copies.

GraphBench clears and reloads fixtures. A non-blocking local lock at
`.coverage/graphbench.lock` prevents overlapping processes; override it with
`-destructive-lock`. Runners on different hosts must use distinct disposable
databases because a filesystem lock cannot coordinate across machines.
Fixture-loading runs also require `DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1` and
an exact credential-free target in `DAWGS_INTEGRATION_DISPOSABLE_TARGETS`, for
example `postgresql://localhost:65432/dawgs`. Non-mutating `-existing-graph`
runs do not require this acknowledgement; their PostgreSQL sessions remain
read-write so temporary workspace behavior matches production.

## Examples

Run only PostgreSQL SQL translation:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench-postgres.jsonl \
  -summary .coverage/graphbench-postgres.md \
  -summary-json .coverage/graphbench-postgres.json
```

Capture PostgreSQL and Neo4j in one report:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md \
  -summary-json .coverage/graphbench.json
```

Compare a run against a previous JSONL capture:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -baseline .coverage/graphbench-baseline.jsonl \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md
```

Capture independent rounds with 30-50 warm observations each. The PostgreSQL
runner resets its one-connection pool before every case, records the first
query execution as `cold`, and keeps connection establishment outside that
sample. Use a distinct `-round` value for every independently reloaded run:
Even-numbered rounds reverse the requested backend order to alternate which
backend runs first.

```bash
go run ./cmd/graphbench \
  -round 1 \
  -iterations 30 \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench-round-1.jsonl
```

Concatenate the JSONL rounds for each version, then run the executable
confidence gate:

```bash
make perf_gate \
  PERF_BASELINE=.coverage/graphbench-baseline.jsonl \
  PERF_CANDIDATE=.coverage/graphbench-candidate.jsonl \
  PERF_GATE_AA=.coverage/perf-aa-resolution.json
```

The versioned gate report includes artifact and A/A-report SHA-256 checksums,
seeded 97.5%
bootstrap intervals over matched round medians, stratified p95 intervals once
each side has at least 150 samples, and candidate-minus-baseline p95 duration
intervals. Normal and envelope timing uses the greater of the matching host
A/A resolution and the 5%/100us minimum floors. Stress timing is descriptive;
stress correctness and the independent resource gate still apply. Matched
timing artifacts must carry complementary, round-balanced arm order, block,
run UUID, and warmup evidence. The version-controlled corpus `candidate_modes` declarations are the
required-key/status manifest: a missing or non-`ok` PostgreSQL record fails
instead of disappearing through intersection-only comparison. Neo4j records
must be present and `ok`, but Neo4j latency is informational and never fails a
CySQL performance gate. Missing/malformed host A/A, tier, pairing, selection,
round, or p95 evidence fails production promotion. Diagnostic comparisons may
omit promotion evidence but cannot emit a passing promotion result.
Prioritized traversal candidates additionally require nonempty, independently
passing training and frozen-holdout cases for every concrete runtime candidate
family; a holdout from ASP, another scheduler, or another observation boundary
cannot qualify an SP candidate.

Predeclare cases expected to improve with `PERF_TARGETS` (or
`-gate-targets`). A target passes materiality when its median-ratio upper bound
is at most `0.95` or its median-saving lower bound is at least `100us`; both
defaults are configurable. Calculate host-specific A/A resolution from a
baseline artifact with:

```bash
make perf_aa PERF_AA_ARTIFACT=.coverage/graphbench-aa.jsonl
```

The report accepts exactly two explicitly executed A/A arms sharing one run
UUID and SQL/workload identity. It requires complementary balanced order across
at least five independent rounds, ten samples per arm and round, and fingerprints
the host, reports p50/p95 ratio and absolute resolution, and keeps p99
diagnostic until each arm has at least 10,000 samples. When append-safe capture
keeps the two arm labels in separate files, repeat `-aa-artifact` instead of
concatenating them outside GraphBench:

```bash
graphbench \
  -aa-artifact .coverage/aa-a.jsonl \
  -aa-artifact .coverage/aa-b.jsonl \
  -aa-output .coverage/aa.json
```

### Targeted matched diagnostics

`-cases` accepts exact, unambiguous case names. `-datasets`, `-categories`, and
`-tags` add exact selectors; values within one selector are alternatives and
different selector dimensions are intersected. Unknown, duplicate, ambiguous,
or empty selections fail before a fixture is changed. Filtered captures are
marked `diagnostic_only`, record both the requested and resolved selection and
the omitted declaration count, and are refused by the ordinary complete gate.
Selection-manifest schema v2 also records the count and digest of any protocol-protected
declarations removed from its runnable universe; those omissions do not by
themselves make an otherwise unfiltered run diagnostic-only.
Use `-diagnostic-gate` only to compare two artifacts with the same resolved
subset checksum.

Configured `-warmup-iterations` run outside the recorded samples. The cold
diagnostic, exact preflight/postflight observations, and fixture reload/analyze
contract remain separate. A matched arm records `-arm`, `-arm-order`, `-block`,
`-round`, and a shared `-run-uuid`:

```bash
go build -trimpath -o .coverage/confirm/bin/graphbench ./cmd/graphbench
.coverage/confirm/bin/graphbench \
  -modes postgres_sql \
  -cases 'LOOKUP-05_repeated_case_insensitive_prefix,GSP-D02-F016_distance' \
  -warmup-iterations 20 -iterations 50 -pool-size 1 \
  -arm candidate -arm-order 1 -block 1 -round 1 -run-uuid "$RUN_UUID" \
  -pg-connection "$PG_CONNECTION_STRING" \
  -bundle-dir .coverage/confirm/candidate-round-1 \
  -jsonl-output .coverage/confirm/candidate-round-1.jsonl
```

`-bundle-dir` retains the tracked patch, checksummed copies of untracked files,
`go.mod`/`go.sum`, the running executable, the complete sorted corpus
declaration and its independently recomputed identity, raw
JSONL, a sanitized manifest, and bundle checksums. It never records connection
strings or arbitrary environment variables. Add repeatable, stable-named
auxiliary evidence with `-bundle-evidence name=path`, for example
`-bundle-evidence host-aa=.coverage/host-aa.json` and
`-bundle-evidence plan-delta=.coverage/plan-delta.json`. Evidence names use only
lowercase letters, digits, `-`, and `_`; the bundle records each source digest
without retaining its host path. The destination must be new or empty so stale
payloads cannot enter its checksum inventory.

Verify a portable bundle independently of database access. Verification rejects
missing, additional, symlinked, malformed, or checksum-mismatched payloads and
writes its report outside the bundle being checked:

```bash
go run ./cmd/graphbench \
  -bundle-verify .coverage/confirm/candidate-round-1 \
  -bundle-verify-output .coverage/candidate-round-1-verification.json \
  -bundle-require-clean

make perf_bundle_verify \
  PERF_BUNDLE_VERIFY_DIR=.coverage/confirm/candidate-round-1 \
  PERF_BUNDLE_REQUIRE_CLEAN=1
```

Omit `-bundle-require-clean` for a diagnostic capture that deliberately carries
a source patch. Structural or checksum failures always produce a nonzero exit;
the optional clean-source policy additionally rejects any dirty capture.

Compare matched arms with the matching checksummed host A/A report. Only a
same-executable block/reload A/A comparison may omit this input:

```bash
make perf_confirm \
  PERF_LEFT=.coverage/confirm/predecessor.jsonl \
  PERF_RIGHT=.coverage/confirm/candidate.jsonl \
  PERF_CONFIRM_AA=.coverage/confirm/block-aa.json \
  PERF_CASES='LOOKUP-05_repeated_case_insensitive_prefix,GSP-D02-F016_distance'
```

The report emits paired relative and absolute p50/p95 intervals. It classifies
fresh p95 evidence as confirmed, cleared/non-inferior, inconclusive, or a
fingerprint mismatch using a minimum 5%/0.10 ms noise floor. Comparing two
captures of the same executable produces a `block_reload_aa` report; alternate
arm order across independently reloaded rounds.

## Concurrency and PostgreSQL references

Serial behavior remains the default (`-pool-size 1`). An opt-in concurrency
smoke retains a physical pool and records pool wait, transaction setup,
execute/decode/drain time, backend PID, cold/warm session classification, wall
time, and QPS:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -pool-size 8 \
  -concurrency 1,8,16 \
  -iterations 30 \
  -session-memory-ceiling-bytes 67108864 \
  -pool-memory-ceiling-bytes 536870912 \
  -jsonl-output .coverage/graphbench-concurrency.jsonl
```

`-postgres-references` additionally captures an identical-SQL raw-pgx boundary
(pool wait, transaction, bind/prepare, first row, remaining decode, drain, and
allocations), a raw prepared round-trip, the C1 prepared round-trip,
endpoint validation, minimum graph-access ID floor, raw ordered-ID search,
path hydration from precomputed ordered edge IDs, and complete hand-written
PostgreSQL references for the active shortest-path and fixed-suffix expansion
targets. The main
case record remains the translated-CySQL boundary rather than a fixed ordinal
among the additive references. Component floors need not match the full query's
row count; complete references do. It also records
compile-stage timings and allocations. JSON/Markdown summaries include a
versioned exclusive-boundary cost table and its unexplained residual. The waterfall marks its translation
interval as overlapping optimization, so those fields must not be summed as an
additive attribution.

Use `-postgres-reference-arms` to run only named tournament arms; it implies
`-postgres-references` and rejects unknown or duplicate names. Generated
fixed-suffix expansion cases expose `search_ordered_ids`,
`stepwise_forward_aa_ordered_ids`, `root_reuse_*`, `late_hydration_*`,
`factored_suffix_forward_*`, `suffix_seeded_reverse_*`, and
`backward_viability_forward_*` boundaries. Complete arms are exact-multiset
checked against the public CySQL observation. Ordered-ID arms retain
relationship IDs for trail uniqueness. Exactly three selected arms use a
six-round doubled Williams design that places every arm in every position twice
and balances every directed carryover pair twice. Exactly five arms use the
fixed ten-round Williams/carryover-balanced slot schedule; other arm counts
retain the historical alternating order:

```text
0 1 4 2 3
1 2 0 3 4
2 3 1 4 0
3 4 2 0 1
4 0 3 1 2
3 2 4 1 0
4 3 0 2 1
0 4 1 3 2
1 0 2 4 3
2 1 3 0 4
```

The slots are the caller-selected arms, and rounds wrap after the tenth row.

`-postgres-force-shortest-executor SP-S0` is the exact-incumbent control at the
same public distance or path boundary. It records selected/applied `SP-S0` and
executes the existing workspace harness, making containment regret and
candidate/reference comparisons explicit.

`-postgres-force-shortest-executor SP-S0-DIRECT` is the tool-only direct-edge
preflight arm for structurally eligible bound-endpoint searches whose minimum
depth is one. A materialized indexed one-edge probe returns a valid singleton
witness immediately; a dependency-gated lateral branch invokes exact `SP-S0`
only when the probe is empty. Both branches share one SQL statement and
snapshot. Production `sp-static-v3` selection remains unchanged until the arm
passes exactness, zero-loop fallback, regret, resource, and concurrency gates.

`-postgres-force-shortest-executor SP-S3-U-D` is a qualification-only seam for
eligible bounded singleton distance cases. It executes the repository-native
recursive AST directly, using compact `(next_id, depth)` state when both
endpoints are ID-only and retaining `(root_id, next_id, depth)` otherwise. It
reports the exact forced/applied target and rejects path-observed or otherwise
ineligible cases. It does not enable the executor in the public query API.

`-postgres-force-shortest-executor SP-S3-U-E+MAT-M0` is the corresponding
qualification-only seam for eligible one-path observations. It emits
repository-native `(next_id, depth, edge_ids)` recursive state and hydrates the
ordered path directly from direction-specific edge endpoints. Distance-only,
directionless, correlated, optional, mutation, and other ineligible forms keep
the incumbent unless explicitly rejected by the tool request. Tool forcing
never broadens the structural correctness envelope.

`-postgres-force-expansion-search EXPANSION-SUFFIX-SEEDED-REVERSE` is the
qualification-only seam for an eligible directed, bounded variable expansion
followed by exactly three fixed directed relationships. It emits the
repository-native suffix-seeded reverse recursive AST, preserves
relationship-trail uniqueness and exact suffix multiplicity, and supports
endpoint-ID and complete-path observations. The request fails closed when the
target is structurally ineligible or translation does not record the requested
strategy as applied. It is mutually exclusive with forced shortest execution.
Automatic suffix-seeded reverse dispatch remains disabled because query shape
does not bound suffix density or reverse fan-in.

`-postgres-force-expansion-search EXPANSION-ENDPOINT-SEEDED-REVERSE` targets the production-qualified
fixed-prefix/terminal-expansion family. Its SQL has materialized 33-row endpoint and 4097-row reverse-state probes,
then mutually exclusive reverse and incumbent branches. Generated
`generated_endpoint_seeded_expansion_v1_d<d>_e<e>_q<q>_w<w>_o<o>_x<x>_m1_c<c>_p<p>` fixtures independently vary
matching/other endpoints, productive/unproductive lanes, cycles, and payload. Edge multiplicity is fixed at one because
DAWGS storage uniquely keys edges by start, end, kind, and graph. Structured plan metrics
report probe rows, guard overflow, and whether the incumbent branch executed.

Traversal telemetry is disabled by default. Opt in with
`-postgres-traversal-telemetry summary` or
`-postgres-traversal-telemetry diagnostic`; both modes require
`-pool-size 1` so the recorded backend identity cannot drift. Attachment runs
after the timed case, reference, raw-PGX, and concurrency blocks. Summary mode
uses only lightweight post-timing evidence and never performs the detailed
invocation-local replay. For a function-backed B arm whose outer plan cannot
prove its branch, it serializes `runtime_outcome_available=false` and leaves
runtime/applied/fallback facts unset. Diagnostic mode additionally retains the existing
`EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON)` plan replay. For SP-B1/B2 it also
replays the exact SQL in a separate Repeatable Read transaction on that same
physical connection, guarded by a unique invocation ID and the
`begin/read/clear_bidirectional_shortest_path_diagnostic_v1` session-local API;
ASP-B1/B2 uses the corresponding
`begin/read/clear_bidirectional_all_shortest_path_diagnostic_v1` API.
Cancellation and SQL errors roll the replay transaction back; replay duration
is never added to latency samples.

An outer PostgreSQL `Function Scan` is not treated as internal traversal work.
SP/ASP B counters are retained only when the invocation ID, connection,
scheduler, caps, exactly-one singleton search call, level rows, and runtime
outcome all validate. A B candidate that
executes exact S4 fallback retains its measured candidate/fallback evidence but
is marked incomplete because nested S4 work is still opaque. Witness SP and all
ASP executions separately require complete hydration counters. Workspace-backed
B arms also require measured per-session and pool high-water bytes; declared
memory flags alone never qualify. These counters are not yet exposed, so those
records fail closed while retaining their validated search evidence. Other
function-backed SP/ASP arms are recorded as
`hidden_counters_unavailable`, never as zero work. The resource gate requires
`counter_status=complete` for candidate architectures even when no numeric cap
was declared.

Traversal telemetry schema v2 gives guarded inline-predecessor evidence two non-interchangeable serialized
families. `ASP-I1-U-DAG+MAT-M0` emits `asp-i1-guarded-v1` and writes bounded
relation, output, and branch evidence under `diagnostic.counters.inline_asp`.
`SP-I1-C-WE+MAT-M0` emits the distinct
`sp-i1-canonical-guarded-v1` policy and writes the same-shaped evidence under
`diagnostic.counters.inline_shortest_path`; evidence from either namespace
cannot satisfy the other family. PostgreSQL's named candidate and fallback
marker CTEs must attribute exactly one arm, and the unselected output branch
must report zero rows. Parent-linked plan nodes also bind each branch body to
its direct inner executor; the selected executor must run and the unselected
executor must report zero loops. Canonical I1 reports `inline_canonical_witness` or
`inline_canonical_no_path` when its candidate marker executes, and
`exact_s4_fallback` with `SP-S4-C-WE+MAT-M0` when the fallback marker executes.
If any required named relation, marker, branch, or executor-loop counter is absent from the
plan replay, the diagnostic is `hidden_counters_unavailable`; absence is never
converted into a qualifying zero.
This adds fail-closed evidence for the default-off exact-query canary; it does
not change the automatic `sp-static-v5-contained` production selector.

An emitted `orientation-probe-v1` policy requires orientation probes, selected
ordinary expansion, and hydration families. Its exact executed-candidate and
executed-incumbent marker rows must select one arm, the other must be zero, and
each named probe may execute at most once. Attribution uses only PostgreSQL's
single `Subplan Name: CTE ...` materialization body, never repeated consumer
CTE scans; the unselected traversal branch must also report zero loops.
Plan-derived partial evidence cannot qualify.
Telemetry attaches to every reference whose declared architecture is itself a
traversal or hydration boundary. Protocol, endpoint/root validation, and other
component probes remain intentionally unannotated; their missing attachment is
not missing traversal evidence.

`-postgres-expansion-orientation-shadow` enables the tool-only
`orientation-probe-v1` shadow statement. It always executes the exact forward
incumbent and records the mutually exclusive SQL marker result separately as
`would_select_identity`; it never relabels that hypothetical choice as the
runtime or applied arm. A marker-first runtime receipt is emitted even when the
incumbent returns zero rows, and any cap+1 probe row is reflected in the shadow
overflow summary. The shadow flag is mutually exclusive with forced shortest-
path and forced expansion selectors.

Build the matched selector-regret and probe-overhead report from separate
true-shadow, exact incumbent, and forced suffix-reverse artifacts plus the
host A/A calibration:

```bash
go run ./cmd/graphbench \
  -orientation-shadow-artifact .coverage/orientation-shadow.jsonl \
  -orientation-incumbent-artifact .coverage/orientation-incumbent.jsonl \
  -orientation-reverse-artifact .coverage/orientation-reverse.jsonl \
  -orientation-aa .coverage/perf-aa-resolution.json \
  -orientation-output .coverage/orientation-selector.json \
  -orientation-protocol confirmation \
  -confidence-level 0.975 -seed 1
```

The report requires exact matching observations, stable workload/SQL/binary
identities, one SQL-derived `would_select_identity`, and position-balanced
three-arm rounds. Selector regret must be within a `1.10` median-ratio upper
bound or the host A/A absolute floor. Shadow probe overhead must be within
`10%` or `100us`. Training records may inform the frozen selector; holdout
records are evaluation-only; diagnostic and legacy records are serialized but
excluded from qualification. Discovery uses 5-20 rounds, five warmups, and ten
samples per arm. Confirmation uses 10-20 rounds, 20 warmups, and 50 samples per
arm.

`orientation-probe-v2` is a separate, immutable, tool-only experiment. It does
not reinterpret the v1 report or change the v1 exact-query production seam.
The v2 selector computes
`F2 = root_rows + maximum_depth * forward_degree_rows` and
`R2 = suffix_rows + boundary_rows + reverse_degree_rows`; it selects the exact
suffix-seeded reverse arm only when every cap+1 probe is complete and
`4 * R2 < 3 * F2`. Any probe or reverse-state overflow fails closed to the exact
forward arm. The checksum-bound v3 cohort has exactly eight training cases and
four holdouts. It independently varies maximum depth, fanout, reachable and
disconnected branches, reverse fan-in, suffix multiplicity, matching-root
multiplicity, zero depth, productive-boundary cycles and self-loops, payload,
and endpoint-ID versus complete-path observation. Holdouts use previously
unused depths 7, 11, 13, and 15 and must not be opened for threshold tuning.

Capture the four artifacts with these exact arm labels. Every invocation also
requires `-postgres-repeatable-read`, `-postgres-traversal-telemetry summary` or
`diagnostic`, and `-pool-size 1`.

| Artifact | Exact `-arm` label | Mode-specific flags |
| --- | --- | --- |
| Shadow | `shadow` | `-postgres-expansion-orientation-shadow -postgres-expansion-orientation-policy orientation-probe-v2` |
| Exact forward | `incumbent` | no orientation or forced-expansion flag |
| Exact reverse | `reverse` | `-postgres-force-expansion-search EXPANSION-SUFFIX-SEEDED-REVERSE` |
| Guarded selector | `guarded` | `-postgres-expansion-orientation-tournament -postgres-expansion-orientation-policy orientation-probe-v2` |

Build GraphBench once from the clean source tree and invoke that exact binary
for every A/A, arm, and report command. Repeated `go run` builds do not prove a
single binary identity:

```bash
CAPTURE=.coverage/orientation-v2-discovery
mkdir -p "$CAPTURE/bin"
go build -trimpath -o "$CAPTURE/bin/graphbench" ./cmd/graphbench
RUN_UUID="orientation-v2-discovery-$(git rev-parse HEAD)"
```

For example, the first shadow discovery round is captured with:

```bash
"$CAPTURE/bin/graphbench" \
  -modes postgres_sql \
  -tags orientation-v2-training \
  -warmup-iterations 5 -iterations 10 -pool-size 1 \
  -round 1 -block 1 -run-uuid "$RUN_UUID" \
  -arm shadow -arm-order 1 \
  -postgres-repeatable-read \
  -postgres-traversal-telemetry diagnostic \
  -postgres-expansion-orientation-shadow \
  -postgres-expansion-orientation-policy orientation-probe-v2 \
  -jsonl-output "$CAPTURE/shadow.jsonl" -append-jsonl
```

Repeat the invocation for the other table rows and rotate `-arm-order` in each
subsequent round. `-run-uuid` is one series identity: reuse the same value across
all four arms and every appended round. Change `-round` and `-block`, but not the
UUID; append validation rejects a per-round UUID. Discovery selects only
`orientation-v2-training` and keeps the holdout timings closed. Its four
artifacts must contain exactly the canonical eight training cases, with no
holdout or diagnostic timing. After the formula is frozen, confirmation selects
`-tags orientation-v2-training,orientation-v2-holdout`, writes separate
confirmation artifacts containing exactly the canonical eight training plus
four holdout cases, and uses 20 warmups and 50 measured samples per arm and
round.

Each matched round must give the four labels distinct `-arm-order` values from
1 through 4 and share the same nonzero `-block`, `-round`, and `-run-uuid`.
Rotate the positions across rounds so every arm occupies every position evenly;
the canonical four-round rotation is
`shadow/incumbent/reverse/guarded`,
`incumbent/reverse/guarded/shadow`,
`reverse/guarded/shadow/incumbent`, then
`guarded/shadow/incumbent/reverse`. The reporter rejects a position imbalance
greater than one, missing or extra cases, mismatched round sets, observation or
SQL drift, non-Repeatable-Read records, missing timed receipts on shadow or
guarded samples, and mixed source, dirty-diff, binary, corpus, host, or
PostgreSQL identities.
The shadow receipt branch is exactly `shadow_incumbent`. Guarded reverse
execution must report `suffix_seeded_reverse`; guarded forward selection and
overflow fallback both report `exact_forward_incumbent`, with
`fallback_executed=true` required only for overflow fallback.

Capture the two A/A arms as separate append-safe exact-forward artifacts using
the same built binary, exact cohort tag, Repeatable Read, diagnostic traversal
telemetry, size-one pool, warmups, samples, and fixture reload protocol as the
incumbent arm. Use one A/A series UUID and alternate the two positions across
rounds. No orientation or forced-expansion flag is permitted. Then let
GraphBench validate the logical pair directly:

```bash
"$CAPTURE/bin/graphbench" \
  -aa-artifact "$CAPTURE/aa-a.jsonl" \
  -aa-artifact "$CAPTURE/aa-b.jsonl" \
  -aa-output "$CAPTURE/aa.json" \
  -confidence-level 0.975 -seed 1
```

Discovery is the only workflow that creates a freeze. Run it from a clean source
tree after capturing the exact canonical eight-case training artifacts and their
matching host A/A evidence. Both output flags are mandatory: the command writes
the training-only discovery report and a freeze manifest that binds its SHA-256
together with the policy, formula, caps, source commit, clean dirty-diff,
binary, and canonical cohort declaration:

```bash
"$CAPTURE/bin/graphbench" \
  -orientation-v2-shadow-artifact "$CAPTURE/shadow.jsonl" \
  -orientation-v2-incumbent-artifact "$CAPTURE/incumbent.jsonl" \
  -orientation-v2-reverse-artifact "$CAPTURE/reverse.jsonl" \
  -orientation-v2-guarded-artifact "$CAPTURE/guarded.jsonl" \
  -orientation-v2-aa "$CAPTURE/aa.json" \
  -orientation-v2-output "$CAPTURE/report.json" \
  -orientation-v2-freeze-output "$CAPTURE/freeze.json" \
  -orientation-v2-protocol discovery \
  -confidence-level 0.975 -seed 1
```

Confirmation fails closed unless it receives that exact freeze manifest and
the discovery report whose digest the manifest binds. Its four timing artifacts
and matching host A/A report must cover exactly the canonical eight training and
four holdout cases:

```bash
CONFIRMATION=.coverage/orientation-v2-confirmation
"$CAPTURE/bin/graphbench" \
  -orientation-v2-shadow-artifact "$CONFIRMATION/shadow.jsonl" \
  -orientation-v2-incumbent-artifact "$CONFIRMATION/incumbent.jsonl" \
  -orientation-v2-reverse-artifact "$CONFIRMATION/reverse.jsonl" \
  -orientation-v2-guarded-artifact "$CONFIRMATION/guarded.jsonl" \
  -orientation-v2-aa "$CONFIRMATION/aa.json" \
  -orientation-v2-freeze "$CAPTURE/freeze.json" \
  -orientation-v2-discovery-report "$CAPTURE/report.json" \
  -orientation-v2-output "$CONFIRMATION/report.json" \
  -orientation-v2-protocol confirmation \
  -confidence-level 0.975 -seed 1
```

Every v2 A/A case carries separate checksums for its workload, the exact
PostgreSQL timing environment (including transaction isolation and normalized
ANALYZE state), and the exact validated fixture. Discovery and confirmation
reject missing or mismatched environment or fixture evidence.

The forward-selected shadow/forward and guarded/selected overhead gates use a
`1.10` median-ratio upper bound or a `100us` absolute-gap ceiling. The
guarded/fastest regret gate uses the same ratio limit or the matching host A/A
absolute floor. Shadow overhead remains visible but is not
qualification-applicable when v2 selects reverse. Confirmation requires all
eight training and all four holdout cases to pass independently. No v2
discovery or confirmation result has qualified yet; the flags and schema only
stage the experiment and do not authorize production rollout.

The bounded same-statement fallback and keyset-continuation experiments are
retired. They are not exposed by GraphBench or production translation. Their
negative results remain under `docs/experiments`; the active `GFSE-BOUNDARY-*`
cases are optimization-neutral cardinality holdouts.

Independent benchmark rounds can be accumulated with `-append-jsonl`. The
append path must be supplied with `-jsonl-output`; GraphBench rejects mismatched
run UUIDs, arms, binary/diff identities, and duplicate case rounds before
writing. This is the intended input shape for paired confirmation and the
round-stratified performance gate.

Use `-reference-closure-artifact` with a capture containing the translated
raw-pgx boundary and one exact PostgreSQL full-comparator arm to generate a
seeded production/reference closure report. The report requires 10-20 matched
rounds, at least 20 untimed warmups and 50 measured samples per side in every
round, and exact public observations. It passes when the production/reference
median-ratio upper bound is at most 1.10 or the absolute median-gap interval is
within the greater of the case's within-session A/A resolution and
`-materiality-absolute` (100 microseconds by default). The report derives and
records A/A resolution independently for the production and reference raw
boundaries by splitting alternating samples within each round. Single selected
reference captures run production first in odd rounds and the reference first
in even rounds; the order is recorded on both boundaries and enforced by the
reporter:

```bash
go run ./cmd/graphbench \
  -reference-closure-artifact .coverage/shortest-reference.jsonl \
  -reference-closure-arm s3_unidirectional_trail_cte \
  -reference-closure-output .coverage/shortest-reference-gate.json \
  -confidence-level 0.975 \
  -seed 1
```

Fixed-suffix expansion JSON plans are retained in both text and structured
forms. Structured metrics include per-node planned/actual rows, loops, width,
timing, buffers,
relation/index identity, recursive rows, access-direction probe counts, and
hydration lookup loops. Derived fields state their provenance and do not present
fixture-derived per-depth counts as PostgreSQL measurements. Resource gate
version 1 applies the portable resource checks to the first upstream artifact
schema.

The keyset-continuation v1 design and its GraphBench arm are retired. In the
10-round confirmation run, S513 had a 1.791
median ratio (97.5% CI 1.752–1.875) and S600 had a 5.898 ratio (5.649–6.462)
against `complete_reference`. S511/S512 selected the existing bounded reverse
branch, so their improvements are not evidence for keyset continuation. The
resource gate passed without spill, local workspace, or WAL. See
`docs/experiments/guarded_suffix_keyset_continuation_v1.md` and its compact JSON
evidence. Generic `GFSE-BOUNDARY-*` holdouts preserve exact-limit, overflow,
path, multiplicity, and cyclic-trail coverage without retaining an executable
copy of the rejected arm.

Supported generated singleton-shortest cases also run two additive comparators:
`s3_unidirectional_trail_cte` (legacy name
`complete_reference_s1_array_cte`) and `s3_bidirectional_trail_cte` (legacy name
`candidate_s2_bidirectional_cte`). New reference records declare a schema
version, architecture, implementation/state/observation shape, and semantic
validation level, raw-pgx timing boundary, normalized SQL fingerprint, and any
explicit A/A alias. A requested arm that is unavailable for a case fails the
run, and distinct architecture IDs with identical normalized SQL fail unless
the alias is declared. Full comparators are checked against untimed exact public
observations rather than row count alone. Distance S3-U uses node/depth frontier
state with no path or predecessor arrays. Historical readers preserve the old
labels in `legacy_name` while mapping them to `SP-S3-U-NE`/`SP-S3-B`. These remain
benchmark-only; S3-B is not evidence for the compact S2 architecture.

Distance-only generated cases also expose `s1_array_bfs_distance`, a genuine
typed PL/pgSQL SP-S1 prototype. It keeps frontier and visited node IDs in
bounded arrays, records a fixed 100,000-node state ceiling, and restarts the
exact S3-U distance reference in the same statement on overflow. It is a
benchmark arm only and is never selected by production translation.

Capture S3-U-D and SP-S1 together with 20 warmups and 50 observations, then
produce their seeded, order-balanced matched comparison with:

```bash
go run ./cmd/graphbench \
  -reference-pair-artifact .coverage/shortest-alternatives.jsonl \
  -reference-pair-baseline s3_unidirectional_trail_cte \
  -reference-pair-candidate s1_array_bfs_distance \
  -reference-pair-output .coverage/shortest-alternatives.json \
  -confidence-level 0.975 \
  -seed 1
```

The default confirmation pair reporter requires 10-20 independent rounds, 20
warmups, 50 samples per arm per round, and distinct recorded measurement order.
`-reference-pair-protocol discovery` produces an explicitly labeled exploratory
report from 5-20 rounds, five warmups, and ten samples per arm; it cannot be
mistaken for confirmation evidence because the protocol and requirements are
written into the report. The reporter accepts two exact public-observation
comparators, two exact ordered-ID comparators, or two hydration-only arms
independently validated from the same precomputed exact path inputs; mixed
boundaries are rejected. Fixed-suffix expansion ordered-ID candidates are
checked against the canonical stepwise-forward node/edge-ID arrays before their
timing is retained. Reports show
candidate/baseline median and p95 ratios, absolute median change, and
within-session A/A resolution without turning architecture selection into a
post-hoc pass threshold.

### Three- and five-arm reference tournaments

Use the generic tournament reporter when a candidate family has three or five
exact PostgreSQL reference arms. The first declared arm is the incumbent:

```bash
make perf_tournament \
  PERF_TOURNAMENT_ARTIFACT=.coverage/tournament.jsonl \
  PERF_TOURNAMENT_ARMS=expand_into_pair_join,expand_into_lower_degree_scan,expand_into_pair_cache \
  PERF_TOURNAMENT_PROTOCOL=confirmation
```

The reporter verifies exact public observations, immutable SQL/implementation
identity, the predeclared doubled-Williams measurement order, and per-round
sample floors. A confirmation is promotion-eligible only when one stable
candidate wins both training and frozen holdout, its median improvement clears
the configured 5% or 100us materiality floor, and its p95 ratio upper bound is
at most 1.05. Discovery reports are always non-promotional.

Function-backed SP/ASP candidates and guarded orientation runs use a
session-local receipt around every timed invocation when `-pool-size 1` is in
effect. Arming and reading occur outside the measured interval. The receipt
binds the requested identity to the exact executed branch, fallback outcome,
and a singular record count. Multi-connection runs remain available for the
operational matrix, but their timing samples are intentionally not eligible as
per-invocation promotion evidence.

### Promotion manifest

Promotion is authorized only by a version-2 manifest that binds the candidate,
selector, source/binary/corpus SHA-256 digests, immutable caps, exact query
cohorts, training and frozen-holdout buckets, and checksummed A/A,
confirmation, performance, resource, reference-closure, and operational
reports. Version 1 is decoded only to reject it for new authorization.

Every evidence report must repeat the manifest's complete authorization
identity. Generate the role-specific report first, then attach the identity
from a provisional manifest whose evidence map may still be empty:

```bash
go run ./cmd/graphbench \
  -promotion-bind-manifest .coverage/promotion-provisional.json \
  -promotion-bind-role performance \
  -promotion-bind-input .coverage/performance-unbound.json \
  -promotion-bind-output .coverage/performance.json
```

Repeat this for `aa`, `confirmation`, `performance`, `resource`,
`reference_closure`, and `operational`, checksum the bound reports, and place
those digests in the final manifest. Then verify the complete closure without
opening a database connection:

```bash
go run ./cmd/graphbench \
  -promotion-manifest .coverage/promotion.json \
  -promotion-manifest-output .coverage/promotion-verification.json
```

Verification fails closed for missing roles, mutated reports, path traversal,
non-passing evidence, invalid digests, absent caps, identity fields that differ
from the manifest, or buckets that do not bind both qualification splits. This
mode is mutually exclusive with benchmark, report, bind, and bundle operations.

### Fixed-one-hop ExpandInto study

Build the standalone three-arm fixed-one-hop report from records captured with
the `expand_into_one_hop` category and its exact PostgreSQL references:

```bash
go run ./cmd/graphbench \
  -expand-into-artifact .coverage/expand-into.jsonl \
  -expand-into-output .coverage/expand-into-study.json \
  -expand-into-protocol discovery \
  -confidence-level 0.975 -seed 1

make perf_expand_into \
  PERF_EXPAND_INTO_ARTIFACT=.coverage/expand-into.jsonl \
  PERF_EXPAND_INTO_PROTOCOL=confirmation
```

`discovery` requires 5-20 independently reloaded rounds, five warmups, and ten
samples per arm per round. `confirmation` requires 10-20 rounds, 20 warmups,
and 50 samples per arm per round. Both protocols require the frozen doubled
Williams order for `expand_into_pair_join`, `expand_into_lower_degree_scan`, and
`expand_into_pair_cache`, exact public observations, stable implementation/SQL
identities, and persisted plan-cache/operator evidence. Confirmation reports
also require one stable non-direct winner across training and frozen holdout,
the configured 5% or 100us materiality floor, and p95 containment at 1.05.
Even a passing report does not activate a production strategy; discovery
remains evidence-only.

Path-observed singleton cases additionally capture benchmark-only M0 and M1
materializer arms. Whole-query comparison uses each architecture's minimal
state: `SP-S3-U-E+MAT-M0` carries edge IDs only and derives node order from the
directed edge endpoints, while `SP-S3-U-NE+MAT-M1` carries node and edge IDs and
hydrates both streams independently by ordinality. Outbound and inbound M0 use
distinct implementation identities. Separate
hydration-only arms use precomputed IDs so search cost stays outside the timed
materializer boundary. These arms are exact-result checked but do not change
production path rendering. Odd benchmark rounds execute references in declared
order and even rounds reverse that order, balancing which M0/M1 arm runs first
across the required independently reloaded rounds.

Every PostgreSQL dataset reload truncates the active relationship and node
partitions together. Other backends delete relationships before nodes. PostgreSQL then checks
the physical row counts in the active `node_<graph>` and `edge_<graph>`
partitions against the fixture declaration before vacuuming or measuring. A
stale/orphan row therefore fails the run instead of silently contaminating scan
and count cases. Fixture records also retain active child-partition sizes rather
than the zero-sized partitioned-parent relations.

```bash
go run ./cmd/graphbench \
  -modes postgres_sql -postgres-references \
  -cases 'GSP-D01-F001_path,GSP-D02-F016_path,GSP-D04-F128_path,GSP-D08-F001_path_inbound,GSP-D16-F016_path,GSP-D32-F512_path,GSP-D64-F1000_path' \
  -warmup-iterations 20 -iterations 50 -pool-size 1 \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output .coverage/materializer-round-1.jsonl
```

The optimizer also emits a typed `ShortestPathExecutorDecision` for every
shortest traversal. It records a machine-readable structural-eligibility result,
SP family and planned candidate identities, observation mode, minimum/maximum
depth, selected/fallback executor, selector version/mode, limits, and stable
fallback code. These fields are also copied into each exact target outcome.
Call count and read-only status are statement-wide, including shortest calls or
mutations separated by `WITH`. Selector `sp-static-v5-contained` chooses
`SP-S3-U-D` for qualified distance observations, bounded
`SP-S3-U-E+MAT-M0` for directed single-kind one-path observations, and
canonical `SP-S4-C-WE+MAT-M0` for deep inbound, multi-kind, or untyped witness
work. Qualification requires one directed three-element shortest-path
traversal, a supported bounded depth, one static ID equality per endpoint, no
relationship variable or predicate, no path predicate, one uncorrelated
endpoint pair, one statement-wide shortest call, and a read-only statement.
The selector also records graph direction, physical expansion
column, relationship-kind count, wildcard state, and a static topology class.
Deep `end_id` distance expansion selects canonical `SP-S4-C-D`. S4 uses compact
ID state, a bounded ceiling, and exact same-statement overflow fallback.
`asp-static-v1` selects `ASP-A1-DAG` for the narrow singleton all-shortest
envelope and retains all minimum-depth predecessor edges before enumeration.
`ASP-I1-U-DAG+MAT-M0` is a distinct inline predecessor-DAG comparator and a
default-off exact-query production canary. Its guarded statement records the
executed candidate/no-path/A1-fallback branch, uses immutable manifest caps,
and requires Repeatable Read or Serializable isolation. Forced executors
remain qualification seams.
`SP-I1-C-WE+MAT-M0` is the corresponding guarded canonical-predecessor witness
canary, with four cap+1 gates, inline M0 hydration, exact S4 fallback, and an
ordered runtime fallback event chain. Its target outcome names the exact
candidate/fallback pair and emitted `sp-i1-canonical-guarded-v1` policy, while
diagnostic resource evidence remains isolated from the ASP I1 counter family.
It remains default-off; `sp-static-v5-contained` continues to select the
automatic S3/S4 production paths.

### Frozen canonical-I1 qualification

The `sp-i1-inbound-v1` study is a dedicated two-arm comparison between exact
forced `SP-S4-C-WE+MAT-M0` and guarded forced
`SP-I1-C-WE+MAT-M0`. Its fresh cohort contains four training cases at depths 4
and 16 and three unopened holdouts at depths 8 and 32. Every case uses the
same typed inbound one-path query with `min=1`, `max=64`, one `Traverse` kind,
exact path observations, and forbidden fallback. GraphBench excludes these
protocol-only holdouts from ordinary default, category, dataset, and generic-tag
selection. Only the exact holdout protocol tag (or an exact holdout case name)
enters the protected authorization path. Exact-name selection still fails
closed because the only executable confirmation selection is the complete
four-training/three-holdout cohort with a passing training freeze. The frozen
performance study executes PostgreSQL only; Neo4j remains part of the declared
cross-backend semantic contract, not an authorized holdout timing arm.

Build GraphBench once from a clean committed tree. Keep the binary and all
outputs under ignored `.coverage`; repeated `go run` invocations have different
binary identities and cannot satisfy the freeze:

```bash
CAPTURE=.coverage/sp-i1-inbound-v1
mkdir -p "$CAPTURE/bin"
go build -trimpath -o "$CAPTURE/bin/graphbench" ./cmd/graphbench
BIN="$CAPTURE/bin/graphbench"
DISCOVERY_UUID="sp-i1-discovery-$(git rev-parse HEAD)"
```

Discovery opens only the four training declarations. Capture 5-20 paired
rounds with at least 5 warmups and 10 samples per arm per round. Use the same
UUID for both artifacts and all rounds. Odd rounds put S4 first; even rounds
put canonical I1 first. For round 1, the two commands are:

```bash
"$BIN" \
  -modes postgres_sql \
  -tags sp-i1-inbound-v1-training \
  -round 1 -block 1 -run-uuid "$DISCOVERY_UUID" \
  -arm sp-i1-s4 -arm-order 1 \
  -warmup-iterations 5 -iterations 10 -pool-size 1 \
  -postgres-force-shortest-executor SP-S4-C-WE+MAT-M0 \
  -postgres-repeatable-read \
  -postgres-traversal-telemetry diagnostic \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output "$CAPTURE/discovery-s4.jsonl" -append-jsonl

"$BIN" \
  -modes postgres_sql \
  -tags sp-i1-inbound-v1-training \
  -round 1 -block 1 -run-uuid "$DISCOVERY_UUID" \
  -arm sp-i1-candidate -arm-order 2 \
  -warmup-iterations 5 -iterations 10 -pool-size 1 \
  -postgres-force-shortest-executor SP-I1-C-WE+MAT-M0 \
  -postgres-repeatable-read \
  -postgres-traversal-telemetry diagnostic \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output "$CAPTURE/discovery-i1.jsonl" -append-jsonl
```

After all training rounds, bind resource-gate v5 to the exact candidate JSONL,
then write the discovery report and freeze:

```bash
"$BIN" \
  -resource-artifact "$CAPTURE/discovery-i1.jsonl" \
  -resource-output "$CAPTURE/discovery-i1-resource.json"

"$BIN" \
  -sp-i1-baseline-artifact "$CAPTURE/discovery-s4.jsonl" \
  -sp-i1-candidate-artifact "$CAPTURE/discovery-i1.jsonl" \
  -sp-i1-resource-report "$CAPTURE/discovery-i1-resource.json" \
  -sp-i1-protocol discovery \
  -sp-i1-output "$CAPTURE/discovery-report.json" \
  -sp-i1-freeze-output "$CAPTURE/discovery-freeze.json"
```

For structurally valid evidence, the reporter preserves the discovery result
and freeze even when a statistical or resource disposition fails. Identity,
path, and source-validation failures do not write an artifact. A failed freeze
cannot authorize holdout capture. A passing freeze binds the clean source archive, commit,
binary, query, training/full declarations and resolved selections, training
artifacts, resource report, and the promotion-form cap names
`state_limit`, `predecessor_limit`, `enumeration_limit`, and
`output_bytes_limit`. Resource evidence uses the corresponding telemetry names
`state_rows`, `predecessor_rows`, `output_rows`, and `output_bytes`. The CLI
fixes the bootstrap seed at `1` and confidence at `0.975`, uses 10,000
resamples, and freezes all three settings. Schedule validation checks the
recorded invocation timestamps as well as the declared alternating order.
Resource-gate v5 binds every decision to the exact candidate arm, round,
block, run UUID, runtime receipt, and diagnostic counters.
Every warm sample also carries a unique session-local runtime invocation ID,
repeated on its receipt events; duplicate reuse anywhere in the paired study
is rejected. Fixture
and PostgreSQL comparison is deliberately strict, including byte-identical
node and edge relation sizes across paired arms and rounds.

Only after discovery passes may confirmation open the full four-training and
three-holdout cohort. Every capture command must provide the freeze and its
checksummed discovery report before database setup. Confirmation requires
10-20 paired rounds, at least 20 warmups, 50 samples per arm per round, pool
size 1, diagnostic telemetry, Repeatable Read, an explicit shared UUID, block
equal to round, and the exact alternating labels/order. For confirmation round
1, create a fresh series UUID, add these authorization and cohort flags to the
two discovery commands, increase the sample settings, and write separate
artifacts. Reuse that confirmation UUID across both arms and every confirmation
round:

```text
CONFIRMATION_UUID="sp-i1-confirmation-$(git rev-parse HEAD)"
-run-uuid "$CONFIRMATION_UUID"
-tags sp-i1-inbound-v1-training,sp-i1-inbound-v1-holdout
-sp-i1-freeze .coverage/sp-i1-inbound-v1/discovery-freeze.json
-sp-i1-discovery-report .coverage/sp-i1-inbound-v1/discovery-report.json
-sp-i1-training-baseline-artifact .coverage/sp-i1-inbound-v1/discovery-s4.jsonl
-sp-i1-training-candidate-artifact .coverage/sp-i1-inbound-v1/discovery-i1.jsonl
-sp-i1-training-resource-report .coverage/sp-i1-inbound-v1/discovery-i1-resource.json
-warmup-iterations 20 -iterations 50
```

Use `sp-i1-s4` at order 1 and `sp-i1-candidate` at order 2 on odd rounds;
reverse those orders on even rounds. Rounds after the first must use
`-append-jsonl`. GraphBench rejects partial or extra cohorts, a changed tag or
case declaration, source/binary drift, insufficient capture settings, path
aliasing with freeze inputs, supplemental arms, and any attempt to enter an
unrelated report mode with holdout authorization flags.
Before every protected capture, GraphBench reloads those three training inputs,
checks their frozen digests, and recomputes the discovery statistics and
resource decisions before opening the database.

Create resource-gate v5 from the complete confirmation I1 artifact, then issue
the final report with the frozen discovery inputs:

```bash
"$BIN" \
  -resource-artifact "$CAPTURE/confirmation-i1.jsonl" \
  -resource-output "$CAPTURE/confirmation-i1-resource.json"

"$BIN" \
  -sp-i1-baseline-artifact "$CAPTURE/confirmation-s4.jsonl" \
  -sp-i1-candidate-artifact "$CAPTURE/confirmation-i1.jsonl" \
  -sp-i1-resource-report "$CAPTURE/confirmation-i1-resource.json" \
  -sp-i1-freeze "$CAPTURE/discovery-freeze.json" \
  -sp-i1-discovery-report "$CAPTURE/discovery-report.json" \
  -sp-i1-training-baseline-artifact "$CAPTURE/discovery-s4.jsonl" \
  -sp-i1-training-candidate-artifact "$CAPTURE/discovery-i1.jsonl" \
  -sp-i1-training-resource-report "$CAPTURE/discovery-i1-resource.json" \
  -sp-i1-protocol confirmation \
  -sp-i1-output "$CAPTURE/confirmation-report.json"
```

Each case passes only when the candidate has complete per-sample timed runtime
receipts with no fallback or overflow, exact observations match S4, resource
evidence passes all four limits, the median-ratio upper bound is at most `0.95`
or the median-saving lower bound is at least `100us`, and the p95-ratio upper
bound is at most `1.05`. The study does not change the automatic production
selector; a passing report is input to later canary, rollback, and promotion
closure.

Use `-postgres-production-manifest` to measure the exact guarded production
statement from a provisional version-2 manifest before the evidence map can be
closed. The runner validates the candidate/fallback pair, selector,
family-specific immutable caps, unique exact query digests, and bucket match.
Guarded SP/ASP candidates require their four positive shortest-path caps.
`orientation-probe-v1` instead requires the optimizer's exact
`root_row_limit=512`, `reverse_seed_row_limit=512`,
`directional_degree_row_limit=16384`, and `state_limit=4096` contract, the
`EXPANSION-STEPWISE-FORWARD` fallback, and the `guarded_dual_arm` boundary; its
production options enable expansion orientation without selecting a
shortest-path executor. The runner executes each statement under Repeatable
Read and retains per-sample runtime
receipts. This flag is mutually exclusive with tool-forced and shadow modes;
evidence may be empty only because the capture is producing that evidence.
Final rollout still requires the ordinary complete manifest verifier.
Use `-postgres-repeatable-read` on the incumbent arm so a matched comparison
measures both sides under the stable-snapshot admission contract. A production
manifest implies this option and cannot be combined with it explicitly.

## Existing graph non-mutating mode

`-existing-graph` runs a selected PostgreSQL corpus without asserting schema,
clearing/loading fixtures, vacuuming, or creating persistent helpers. It
requires a versioned logical-key anchor manifest and refuses `write_scenario`
or mutation keywords before runner construction. It deliberately uses
read-write PostgreSQL sessions so session-local workspace setup, reset, and
statistics match production. Example:

```json
{
  "version": 1,
  "graph": "integration_test",
  "content_identity": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "anchors": {
    "outbound_source": {"logical_key": "sanitized-source", "kind": "Source"},
    "outbound_target": {"logical_key": "sanitized-target", "kind": "Target"}
  }
}
```

```bash
go run ./cmd/graphbench \
  -existing-graph \
  -anchor-manifest anchors.json \
  -cases LIVE-outbound-distance \
  -checkpoint .coverage/live/checkpoint.json \
  -progress .coverage/live/progress.jsonl \
  -jsonl-output .coverage/live/results.jsonl
```

Anchor values are used only at runtime. Durable records replace them with
one-way hashes, omit rendered parameters and Cypher, and redact observed-row
and error payloads in both primary and nested reference outcomes. The runner captures
before/after graph cardinalities, relation sizes, PostgreSQL settings, and
schema/index fingerprints. Artifact schema v2 records a digest of the complete
workload, fixture identity, corpus, and run configuration. Each completed record
is checkpointed by stable backend/dataset/case/workload identity using an atomic
rename; `-resume` accepts only a matching manifest, corpus, and run identity and
preserves the original run UUID.

Legacy graphs without `logical_key` properties may instead use a runtime-only
physical anchor with a content proof:

```json
{"physical_id": 42, "content_sha256": "sha256:<64 lowercase hex characters>", "kind": "Entity"}
```

The digest is SHA-256 over PostgreSQL's canonical `kind_ids::text`, a newline,
and `properties::text` for that node. The runner accepts the ID only after the
digest and optional kind match, then removes the ID and manifest values from
durable records. Each anchor must use exactly one of `logical_key` or
`physical_id`; a physical anchor always requires its content digest.
For exact path observations on a legacy graph, include content-proved anchors
for intermediate nodes as well as parameter endpoints so stable path identity
can be reconstructed without persisting physical IDs.

Existing-graph runs require the target database to have the DAWGS schema and
workspace functions from the current checkout already deployed. The runner
does not assert or upgrade schema in this mode because doing so would violate
its non-mutating existing-graph contract.

Adaptive discovery is explicit:

```bash
go run ./cmd/graphbench \
  -existing-graph -anchor-manifest anchors.json \
  -discovery -timeout-classes 100ms,1s,10s \
  -discovery-sample-floor 1 \
  -checkpoint .coverage/live/checkpoint.json
```

Every timeout and sample reduction stays in the case record. Adaptive artifacts
are refused by the complete performance gate. Confirmation omits `-discovery`
and uses fixed timeouts, arm order, warmups, and samples.

The independent state/resource report is produced with
`-resource-artifact results.jsonl -resource-output resources.json`. Schema v5
records the SHA-256 digest of the exact input JSONL so
promotion evidence can verify that resource decisions remain bound to their
capture. For non-stress portable PostgreSQL candidates it rejects temp spill,
local workspace, and WAL for non-mutating reads. S4 and ASP explicitly permit their
session-local compact workspace but still reject executor temp-file spill and
WAL; exact incumbent fallback retains its documented temporary-workspace
contract. `SP-S0-DIRECT` records are
attributed from the measured fallback function loops, so workspace use is
accepted only when the incumbent branch actually ran. Exact full-comparator
reference arms receive independent resource cases rather than inheriting the
outer production result.

Shortest tournament references are independently selectable with
`-postgres-reference-arms s4_canonical_source_distance`,
`s4_canonical_source_witness_m0`, `sp_b1_strict_alternating_distance`,
`sp_b1_strict_alternating_witness_m0`,
`sp_b2_smaller_frontier_distance`,
`sp_b2_smaller_frontier_witness_m0`, `asp_a1_stored_helper_m0`,
`asp_i1_inline_predecessor_dag_m0`,
`asp_b1_bidirectional_dag_strict_m0`, and
`asp_b2_bidirectional_dag_smaller_frontier_m0`. They are exact full-query comparators at the same
public observation boundary, not production selectors. S4 canonicalizes inbound
search to physical `start_id -> end_id`; B1 alternates one accepted node per
side, while B2 expands the smaller complete current level with a deterministic
forward tie-break. Both candidates retain ID-only state, reconstruct one stable
witness late, and fall back to exact S4 before output if a seen, frontier, or
predecessor cap overflows. Their multi-statement functions reject Read Committed;
GraphBench runs any selected B1/B2 production or reference arm at Repeatable
Read so candidate search and fallback share one transaction snapshot. The ASP
arms retain every relationship-distinct shortest-depth predecessor, select one
canonical completed meeting cut, and separately cap discovery state, frontier,
predecessors, saturating path count, enumerated rows, and output bytes before
exact A1 fallback. SP and ASP identities are forceable with
`-postgres-force-shortest-executor`; automatic selection remains on S3/S4 for
SP and A1 for ASP. Activation evidence still requires the saved
plan/resource, holdout, concurrency, cancellation, and reference-closure gates.

`-backend-delta-artifact combined.jsonl -backend-delta-output deltas.json`
produces matched PostgreSQL/Neo4j median and p95 ratios only when both records
exist, and reports logical-observation agreement. The report is explicitly
descriptive and never participates in PostgreSQL pass/fail selection.
Every other shape retains `SP-S0` and its specific fallback code.

Ordinary variable expansions with fixed continuations similarly emit a typed
`ExpansionSearchStrategyDecision`. It records suffix bounds, logical direction,
observation mode, depth bounds, structural facts, selection mode, and stable
fallback codes. It also reports the fixed-suffix expansion family, planned
candidate set, selector version, and distinct correlated-suffix/cross-region
fallback reasons.
Factored-suffix and backward-viability SQL remains reference-only.
`EXPANSION-SUFFIX-SEEDED-REVERSE` has a repository-native emitter for
qualification, but it is not selected by the public query API. Structurally
eligible forms select `EXPANSION-STEPWISE-FORWARD` with
`tournament_unqualified`.

## Outputs

JSONL output contains one `CaseResult` record per case and execution mode.
Markdown and JSON summaries aggregate mode status counts, per-case timings, row
counts, fallback reasons, and baseline regressions or improvements when a
baseline capture is supplied.

PostgreSQL case records also include aggregate query-text-free parse-cache
counters. Optimization diagnostics retain target-specific selected, applied,
and skipped identities; compile-time records do not claim a runtime branch.

Each timing record retains the unsorted cold and warm latency samples with round,
iteration, case, dataset, backend, and connection/session fields so confidence
interval and regression tooling does not have to reconstruct observations from
summary percentiles. Read cases run untimed preflight and postflight queries
and compare their complete row multisets, including duplicate rows, around the
timed block. For declared `id_rows`, `path_set`, and scalar results, recorded
`observed_rows` use stable fixture identities, retain relationship order,
kinds, and properties, and reject relationship reuse within a path. GraphBench
compares those stable result kinds across backends. Other result kinds still
receive per-backend preflight/postflight checks, but are not compared across
backends because they may contain backend-generated relationship IDs.
PostgreSQL fixture loads are followed by `VACUUM (ANALYZE)` through
the pool; a maintenance failure aborts the benchmark.
The PostgreSQL runner defaults to a one-connection pool and records
`pg_backend_pid()` as the serial sample connection identifier. Concurrency
blocks record the physical PID of every direct pool acquisition so per-session
cold state and pool queuing remain visible.

Write records additionally report matched and affected counts and each
post-state observation. The recorded duration covers the mutation query; setup,
verification, and rollback are outside that duration.

PostgreSQL records include translated SQL and its fingerprint, server settings,
fixture checksum/cardinalities, and `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)`
shared/local/temp metrics plus `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS,
FORMAT JSON)` for reads. Neo4j records include plan operator names
when an `EXPLAIN` plan can be captured.

## PostgreSQL scale-plan correctness gate

The PostgreSQL-only `TestPostgreSQLScalePlanInvariants` test loads the same
scale corpus and fixture as the command. It executes all required Cypher scale
representatives, requires their declared cardinalities and mutation post-state,
and verifies that the captured plan came from `EXPLAIN ANALYZE`. Stable
assertions cover relationship/node mutation targets, branch-local logical
structure, temporal filtering, and anchored edge-index orientation. The test
uses rollback isolation for writes and runs automatically under
`make test_all` when `CONNECTION_STRING` selects PostgreSQL.

Run only the scale-plan gate with:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
  CONNECTION_STRING="$PG_CONNECTION_STRING" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

The non-integration cardinality test also guarantees that every required stable
query-form ID remains represented in the scale corpus and declares an expected
read or write cardinality.
