# Hash-Filtered Ingest: One Run, End to End

Hash-filtered ingest makes a large, repeatable graph load practical without
making a hash the source of truth. The proposed design keeps a run bounded,
recognizes complete records that are already current, and reconciles partial
updates in the client before it writes.

This guide follows one small run as a stand-in for a much larger one. The
PostgreSQL proof-of-concept's API, exact data contract, and operating guidance
live in [PostgreSQL hash-filtered ingest](postgresql_ingest.md). Hash values
below are illustrative.

## The run begins with a partial update

The database already contains these complete graph objects:

~~~text
| id    | kinds  | properties           | identity_hash | content_hash |
|-------|--------|----------------------|---------------|--------------|
| Alice | [User] | {group: "finance"} | 0x12342384    | c:7d91       |
| Eve   | [User] | {group: "sales"}   | 0x23456789    | c:aa20       |
| Bob   | [User] | {group: "legal"}   | 0x51234567    | c:1111       |
| Cara  | [User] | {group: "sales"}   | 0xa1234567    | c:2222       |
| Dan   | [User] | {group: "it"}      | 0xe1234567    | c:3333       |
~~~

The new input is a stream, not a complete in-memory collection:

~~~text
{id: "Alice", properties: {group: "hr"}}
{id: "Bob",   kinds: ["User"], properties: {group: "legal"}}
{id: "Cara",  properties: {group: "marketing"}}
{id: "Dan",   kinds: ["User"], properties: {group: "it"}}
~~~

Alice and Cara changed. Bob and Dan are complete replays. Alice and Cara also
demonstrate why comparing input payloads alone is insufficient: they omit
stored fields such as their kind. The answer we need is not “does this payload
look different?” but “does this payload produce a different complete object
after the additive merge?”

An ordinary upsert path must find and evaluate every one of these objects. A
naive client-side hash shortcut has the same problem: a partial payload cannot
produce the hash of the final complete state without first seeing and merging
that state.

## First, divide the run into bounded work

Each object has an exact source ID and a deliberately non-selective 32-bit
identity hash derived from it. The hash is not an identity lookup key; it
partitions both the input and stored graph into matching ranges. Exact IDs are
always compared before two objects are considered the same.

For this run, choose BucketCount = 4. That divides the 32-bit hash space into
four contiguous ranges:

~~~text
bucket 0: 0x00000000 through 0x3fffffff
bucket 1: 0x40000000 through 0x7fffffff
bucket 2: 0x80000000 through 0xbfffffff
bucket 3: 0xc0000000 through 0xffffffff
~~~

The client reads the input once, normalizes each object, computes its identity
hash, and appends it to the corresponding private spool or bounded queue:

~~~text
incoming object                              identity hash     bucket
---------------                              -------------     ------
Alice                                        0x12342384        0
Bob                                          0x51234567        1
Cara                                         0xa1234567        2
Dan                                          0xe1234567        3
~~~

At a realistic scale, every bucket contains many records. The important part
is that the client does not need the whole input and graph in memory together.
It has durable or bounded input work for each range, and can now work through
one range at a time.

BucketCount is a runtime tuning choice, not graph data. More buckets make each
work unit smaller but add spool, query, and transaction overhead. Fewer buckets
do the opposite. Changing it changes only the order and size of work units; it
does not change identities or graph meaning.

## Process bucket 0: reconcile Alice

The client opens the spool for bucket 0, which contains Alice. It asks
PostgreSQL for all stored objects whose identity hash falls within bucket 0's
range. That result contains Alice, Eve, and potentially many unrelated
objects:

~~~text
bucket 0 input                    stored rows in bucket 0 range
--------------                    -----------------------------
{id: "Alice",                     | id: Alice | kinds: [User] |
 properties: {group: "hr"}}       | properties: {group: "finance"} |
                                  | content_hash: c:7d91 |
                                  | id: Eve | ... |
~~~

The client compares exact IDs. Alice matches Alice; Eve is simply an unrelated
row from the same work range. A 32-bit hash collision is handled the same way:
it may add a candidate row, but it cannot make two different exact IDs match.

The client next needs to decide whether Alice changes. A full input record
could be canonicalized and content-hashed before the lookup; if that hash
matched the stored hash for exact ID Alice, it would skip the write immediately.
Alice's partial input cannot do that. It is a content-hash miss and moves to
client-side reconciliation:

~~~text
stored complete state              incoming partial payload
---------------------              ------------------------
id: Alice                           id: Alice
kinds: [User]                       properties: {group: "hr"}
properties: {group: "finance"}

client's additive merge
-----------------------
id: Alice
kinds: [User]
properties: {group: "hr"}
content_hash: c:40aa
~~~

The canonical merge rule is simple: kinds are unioned and incoming property
values win. The client owns that rule and computes the refreshed content hash
from the resulting complete state. PostgreSQL returns and persists objects and
metadata, but does not decide what a partial update means or calculate its
hash.

Because c:40aa differs from stored c:7d91, the client writes Alice's
reconciled complete object and its updated hash. It commits bucket 0 and
releases that bucket's input and stored candidates from memory.

~~~text
| id    | kinds  | properties      | identity_hash | content_hash |
|-------|--------|-----------------|---------------|--------------|
| Alice | [User] | {group: "hr"} | 0x12342384    | c:40aa       |
~~~

The client then performs the same sequence for buckets 1, 2, and 3. Bob and
Dan's complete records match their stored content hashes and bypass the write
path. Cara's partial update is reconciled just as Alice's was.

## Replay is the recovery path

Each bucket has its own transaction. If the run fails after bucket 0 commits
and before bucket 2 completes, the operator replays the original input rather
than constructing a special resume payload. Alice's complete state now matches
its stored content hash and skips a write. Buckets that never committed are
processed again.

This only works when the ingest path is the coordinated writer for its managed
graph. A separate writer that changes Alice without refreshing content_hash can
make a later hash comparison incorrectly skip a change.

## Edges follow the same flow

An edge has a stable source identity before either endpoint has a database ID:
the directed tuple (start ID, kind, end ID). For example:

~~~text
{start: "Alice", kind: "MemberOf", end: "Finance"}
~~~

The client hashes that exact tuple to assign the edge to a work range, fetches
the stored candidates for the range, compares exact tuples, and reconciles the
complete edge state on a content-hash miss. Nodes are processed before edges so
an edge can refer to a node supplied by the same run; PostgreSQL resolves its
internal endpoint references when the client persists the reconciled edge.

## What must remain true

- Ingest is additive: absent input does not imply deletion.
- The 32-bit identity hash partitions work; exact source IDs establish a match.
- A content hash represents a complete canonical state, never a partial
  payload.
- There is one canonical reconciliation rule. This proposal runs it in the
  client; its implementation location may change only if its meaning and hash
  contract do not.
- Hash and merge rules are versioned. Changing either requires a compatibility
  and rebuild strategy.

The storage engine, spool implementation, bucket count, and hash algorithms
can evolve. The flow remains the same: partition the input, load one matching
stored range, reconcile exact-ID matches in the client, write changed complete
state, commit, and continue.
