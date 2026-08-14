# retriever

`retriever` exports Dawgs graphs to local `ret-collection-v1` collections,
loads JSONL collections, verifies artifacts or database contents, and manages
encrypted collection archives. Collection and archive paths must be on a local
filesystem; object stores and remote blob stores are not supported.

## Dump

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default
```

JSONL is enabled by default with zstd at the JSONL package's default level
(`-jsonl-level 0`). Parquet is independent and disabled by default:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph first \
  -graph second \
  -jsonl=true \
  -jsonl-compression zstd \
  -jsonl-level 0 \
  -parquet
```

For a Parquet-only analytical export, disable JSONL explicitly:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./parquet-dump \
  -graph default \
  -jsonl=false \
  -parquet=true
```

JSONL and Parquet are distinct first-class shard outputs. JSONL compression
flags do not configure Parquet; Parquet uses its own unshredded VARIANT
representation.

Repeated `-graph` values are preserved in command order. PostgreSQL
`-all-graphs` discovers graphs from Dawgs metadata; Neo4j selects its one
effective graph target. The database must remain quiescent while dumping.
Retriever snapshots node and relationship counts, rechecks them before
completion, and rejects a resume when either total has changed. This
count-stability guarantee cannot detect a same-count content replacement or
mutation because Dawgs does not expose a cross-transaction snapshot token.

Interrupted dumps can resume with the same configuration:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -resume
```

`-force` is available only for a fresh dump, is mutually exclusive with
`-resume`, and currently requires Linux or Darwin. Normal fresh and resumed
dumps remain portable. Force rejects destination or intermediate symlinks,
pins the physical parent and destination, and atomically quarantines the exact
approved directory with a handle-relative no-replace rename. It does not
enumerate or mutate anything inside the prior collection. The complete prior
collection remains alongside the new dump as a `.ret-force-*.preserved`
tombstone, and the command reports its exact path. Force therefore does not
reclaim the prior collection's disk space. Removing a preserved collection is
a separate manual action that requires a quiescent filesystem and accepts a
weaker concurrent-substitution contract than this command. Filesystem roots,
home or repository-wide targets, and their physical ancestors are rejected
before mutation. If another object appears at the original destination before
handoff, force stops before profiling, database access, or dumping; the prior
collection is restored when possible, otherwise both objects are preserved and
the error reports their absolute paths.

Force replacement is CLI policy. The `/ret` library facade has no force option
and never replaces a dump destination.

Full scrubbing uses the existing policy:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./scrubbed-dump \
  -graph default \
  -scrub full \
  -salt "$RETRIEVER_SCRUB_SALT"
```

`RETRIEVR_SCRUB_SALT` remains a fallback spelling. `-config` reads the direct
scrub TOML policy shape illustrated by
[`ret/scrub/example.toml`](../../ret/scrub/example.toml): scalar policy fields
are top-level, with `[graph_rules]` and `[classifier]` sections. Salt is
runtime-only and must come from `-salt` or the environment; TOML cannot set it.
Command-line mode controls whether the policy runs.

## Load

```bash
retriever load \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir
```

Load accepts a local collection directory only. It requires complete JSONL
output, validates the JSONL representation before database writes, and does
not open or validate Parquet artifacts. Parquet-only collections are valid
exports and pass `verify-collection`, but `load` intentionally rejects them.
Targets must be empty; load never clears or replaces a graph. Load is not
resumable, and a failed write can leave a partial graph. Clear that graph
before retrying.

Optional database verification is a distinct operation after a successful
load:

```bash
retriever load \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir \
  -verify-database
```

## Verification

Collection verification needs no database and validates every declared JSONL
and Parquet artifact:

```bash
retriever verify-collection -in ./dumpdir
```

Database verification opens the selected backend and compares current graph
metrics with `manifest.json`:

```bash
retriever verify-database \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir
```

## Encrypted archives

Archive creation is independent from dumping. Generate keys with:

```bash
retriever keygen \
  -private-key ./retriever-private.key \
  -public-key ./retriever-public.key
```

Pack a fully verified collection:

```bash
retriever pack \
  -in ./dumpdir \
  -archive ./dump.tar.enc \
  -recipient ./retriever-public.key
```

Unpack, authenticate, fully verify, and atomically publish a collection:

```bash
retriever unpack \
  -archive ./dump.tar.enc \
  -out ./restored-dump \
  -identity ./retriever-private.key
```

Archive publication currently requires Linux or Darwin. Pack and unpack never
replace their destinations.

An encrypted archive is not a load input. Unpack it to a verified local
collection first, then load the unpacked directory:

```bash
retriever unpack \
  -archive ./dump.tar.enc \
  -out ./restored-dump \
  -identity ./retriever-private.key

retriever load \
  -connection "$CONNECTION_STRING" \
  -in ./restored-dump
```

## Runtime profiling and progress

`dump`, `load`, `verify-database`, and `bench` accept
`-pprof-listen` with a loopback-only address such as `127.0.0.1:6060`.
Profiling is disabled when omitted. The dedicated server omits the command-line
endpoint because arguments may contain connection strings, salts, or key
paths.

Root operations emit typed events. The CLI translates them to structured
`slog` records and owns progress sampling, elapsed-rate calculation, Go runtime
memory statistics, and RSS sampling.

## Bench

`bench` measures database reads and each selected concrete artifact format:

```bash
retriever bench \
  -connection "$CONNECTION_STRING" \
  -graph default \
  -workers 1 \
  -batch-size 10000 \
  -sample-size 1000000 \
  -jsonl=true \
  -jsonl-compression zstd \
  -jsonl-level 0 \
  -parquet=true
```

JSONL and Parquet can be selected independently, but at least one must be
enabled. When both are selected, the report contains separate `jsonl` and
`parquet` results for each worker count. JSONL compression flags apply only to
JSONL; Parquet always uses its own unshredded VARIANT writer configuration.
`-json` emits the same report shape as JSON with a `format` field on each
result. Worker counts control benchmark-only concrete write processing; each
format phase owns its own ordered database source read. Benchmark-only worker
concurrency does not affect normal dump or load operations.

## Go library

New library consumers should use the small operation-oriented facade at
`github.com/specterops/dawgs/ret`, with concrete component packages such as
`ret/jsonl`, `ret/parquet`, `ret/scrub`, and `ret/archive` when their owned
types are needed. The legacy `github.com/specterops/dawgs/retriever` package
remains temporarily alongside `/ret` for review, but the CLI no longer imports
or delegates to it.

## Testing

Run command unit tests with:

```bash
go test ./cmd/retriever
```

Repository-wide integration validation uses `make test_all` and requires
`CONNECTION_STRING` for the selected backend.
