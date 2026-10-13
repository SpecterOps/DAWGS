# PostgreSQL MERGE implementation evidence

This delivery implements the initial pipeline in `merge_gaps_plan.md` (phases 0–5). Ordered execution is the separate
extension in section 5 of that plan. The single-statement snapshot, ordered-input rejection messages, and concurrent
storage-conflict contract remain in force.

## Implementation and semantic gates

- Fixed match values are individually projected/validated once. Dynamic maps are validated once per input; string,
  array, SQL NULL, and other non-object map parameters receive SQLSTATE 22023. Nested JSON nulls remain valid.
- Matching references evaluated input columns. String lookups retain their stored-value type guard and expression
  index shape. Fixed property objects are assembled on creation.
- A full aggregate demands every actual input value. A materialized assertion combines that input validation with
  relational conflict facts. Empty upstream inputs succeed. The first entity MERGE with an INSERT action consumes a
  private sentinel whose DO NOTHING predicate demands the assertion. With only bound entities, a separate conditional
  INSERT MERGE anchors validation; it never inserts or consumes a sequence. Its INSERT statement triggers run, requiring
  INSERT permission on node. A DO NOTHING-only anchor and an UPDATE-only statically null sentinel were pruned in the
  PostgreSQL 18.6 execution spikes, including with LIMIT 0.
- Effective actions use UNION ALL and group graph/type/target ID; properties are excluded. Absent input identity is
  counted separately from result/action fanout. Fixed preserved keys group the complete JSONB tuple. Changed keys join
  original to final tuples in both directed possibilities. Empty fixed maps explicitly conflict for multiple absent
  inputs. Dynamic maps compare runtime fields exactly and retain a potentially quadratic fallback. A proven standalone
  single-node input cannot repeat target writes or overlap another creation, so its candidate conflict scans are omitted.
  That proof excludes upstream row sources, UNWIND, complete patterns, and carried action targets.
- A materialized patch evaluates each effective RHS against the incoming clause frame. Duplicate-field precedence
  comes from IndexedSlice's last-value replacement. One helper applies top-level null removals and non-null assignments
  using one subtraction and concatenation per binding/clause. Composite action stages are also materialized, preventing
  a later clause's many property reads from repeating an earlier bag reconstruction. Final fixed keys follow projected
  patches independently of payload properties. Whole composites are conservatively assembled at clause boundaries for
  downstream entity/property/path consumers; general whole-bag demand analysis is not introduced.
- Bound entities without actions emit no entity write. Runtime sources filter to effective actions without comparing
  changed values. Explicit same-value SET remains a write. RETURNING is joined by result identity; unchanged candidates
  supply fallback values. Private key/patch columns are pruned after use. MERGE wildcard projections expand visible
  user aliases in name order, excluding bookkeeping and anonymous entities.
- New helpers have exact-signature teardown and repeated/fresh/populated-schema lifecycle tests. The legacy JSON
  candidate helper remains installed for older running processes. Compiler policy is `compiler-v5:optimized`; existing
  schema assertion and cache generation invalidation remain in place.

The regression inventory covers no RETURN, LIMIT 0, scalar/count output, late invalid rows, several pattern maps,
empty upstream input, read-only bound repetitions, bound endpoints, SQLSTATE/atomicity, cached map shape changes,
composite/empty keys, changed-away duplicates, removal, dynamic subset maps, array order/duplicates/subsets, clause
swaps/dependencies, repeated assignments, nested nulls, labels, complete patterns, graph isolation, paths, and indexed
lookups. The input counter test observes four fixed validations for two keys on two rows and two dynamic validations
for two matched inputs under LIMIT 0. Core cases/templates have identical expectations on PostgreSQL and Neo4j. A MERGE containing only one already-bound
node is PostgreSQL-scoped because Neo4j rejects that syntax; shared read-only repetition cases use matched relationships
between bound endpoints.

## Reproducible evidence

Baseline source: `69bb2e8`, with the expanded benchmark/plan harness copied into an isolated checkout. Both versions
use the same local PostgreSQL 18.6 server on an Intel Core i9-12900HK. Server settings: work_mem 4 MB,
synchronous_commit on, fsync on, full_page_writes on. The benchmark graph has a name B-tree index. Parameters and
seeds are prepared outside timing; translation is warm. Matrix runs include result consumption and transaction
rollback; the small legacy scenarios include commit. Rolled-back sequence values are not reset. Creation fixtures
remain creations across iterations. Dynamic matching uses one map against a batch-sized fixture. Go allocation
figures measure client allocations, not PostgreSQL memory.

Generated SQL and plan evidence is under `.coverage/merge-plans/{before,after}`. Benchmark logs, including unsuccessful
samples, are under `.coverage/merge-evidence`. Reproduce captures with the commands in `integration/BENCHMARKS.md`.
Run captures, benchmarks, and database suites serially; shared integration setup can reset database tables.

The captured plans confirm these structural changes:

| Workload | Baseline | New pipeline |
| --- | --- | --- |
| Fixed preserved-key creation | JSON candidate aggregation; overlap inside helper | Key grouping with distinct input IDs; no candidate JSON envelope |
| Unrelated payload 0 versus 4096 bytes | Payload serialized into candidate validation | Identical target-group width 48 and absent-key group width 36 in both captured plans |
| Changed fixed keys | Pair enumeration inside JSON helper | Equality join on projected original/final JSONB keys, excluding self input |
| Relationship between unchanged bound nodes | Three entity MERGE statements | One edge MERGE, including the guard sentinel; endpoint versions unchanged |
| Compatible multi-field SET | Nested bag replacement per field | One patch helper per binding/clause, with materialized RHS and composite stages |
| Indexed string lookup | Type guard plus text comparison | Same stored index expression, projected text RHS; property-index test passes |

The optimizer may remove constant graph/type group dimensions, or choose nested loops for small equality joins.
These are relational key checks; neither algorithm nor planner choice justifies a universal linear-time claim.
Estimated widths are planner figures, not total or peak memory. The broader result/mutation candidate relation still
carries entity composites and therefore still incurs payload, materialization, TOAST, and WAL costs.

PostgreSQL 18.6 emitted malformed execution-plan JSON for partitioned MERGE: tuple counters were placed inside the
Target Tables array. A minimal standalone MERGE reproduced this. The harness retains the raw server output and obtains
valid planning JSON and execution text in separate rollback transactions. It does not silently repair instrumentation.
Rejected workloads use non-executing EXPLAIN, followed by an independently measured/asserted runtime rejection.

## Timing observations and limits

Exploratory five-sample, 200 ms matrix medians are shown below. These are observations from this environment, not
portable performance guarantees; generated plans establish which work was removed.

| Workload | Baseline median | Changed median |
| --- | ---: | ---: |
| Fixed creation, 8 inputs, no payload | 1.225 ms | 0.996 ms |
| Fixed creation, 64 inputs, no payload | 9.206 ms | 3.738 ms |
| Fixed creation, 512 inputs, no payload | 401.212 ms | 18.901 ms |
| Fixed creation, 64 inputs, 4096-byte payload | 13.652 ms | 9.307 ms |
| Matched update, 64 inputs, 4096-byte payload, 8 fields | 8.229 ms | 6.606 ms |
| Matched update, 64 inputs, 4096-byte payload, 32 fields | 17.093 ms | 8.146 ms |
| Dynamic unchanged match, 64-node fixture, 4096-byte payload | 3.609 ms | 3.958 ms |

The exploratory 4096-input baseline took roughly 22 seconds per operation, compared with under 0.2 seconds in changed
samples. That baseline run overlapped unrelated validation work and is preserved as exploratory evidence rather than
used to set a numerical acceptance threshold.

Small-query measurements did not establish a regression-free result. In later five-sample, one-second runs the indexed
unchanged median was 0.264 ms for baseline versus 0.767 ms for the changed pipeline; the alternating-update medians were
0.661 ms versus 1.322 ms. Other runs gave substantially different absolute timings. Host load rose above 10 during
sampling; fixture churn, plan preparation, and shared-server activity also limit cross-run comparisons. The initial
separate guard MERGE was removed from creation-capable queries after small-query regressions were observed; the
remaining singleton input scan/assertion/sentinel and extra clause materialization are real costs. Dynamic fallback
also remains expensive. These unsuccessful observations must not be interpreted as a passed wall-clock regression
budget. A stable, isolated baseline with interleaved samples is needed before setting and adjudicating numerical
regression/improvement thresholds. No general speedup or total-memory claim is made here.

## Validation

Use `make format` (or goimports on the touched files), `make test_update`, and `make test_all` separately with each
user-supplied backend connection. The generator writes updated fixtures before checking existing expectations; a first
regeneration with changed expectations may fail before Makefile's copy step. Install/review the generated artifacts
and rerun the target. Preserve source cases and remove unrelated serialization/whitespace diffs.

Both PostgreSQL and Neo4j `make test_all` runs passed. Comparable unit coverage without a database connection rose
from 61.1% to 61.4%; the PostgreSQL-selected unit run reported 62.7%. `make test_update` passed and generated artifacts
were reviewed. A separate temporary-sequence spike confirmed that the fallback anchor does not invoke column defaults
or insert rows under LIMIT 0.

The Go launcher in this environment cannot execute build artifacts in /tmp. Validation uses a project-local GOTMPDIR;
when builds are active, format only touched sources so the repository-wide find does not traverse transient Go files.
A missing transitive randomstring checksum prevented the original coverage report command; its two go.sum hashes were
added without changing dependency versions.
