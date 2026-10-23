# Fixed-suffix first-use routing protocol v1

This protocol is the manifest-v5 successor to the cache-hit-only v4 selector.
It authorizes the fixed-suffix reverse candidate on the first matching query in
a repeatable-read or serializable transaction, but only after the current
topology synopsis passes the frozen sparse-topology estimate.

The protocol is deliberately separate from v4:

- candidate: `topology-fixed-suffix-first-use-v1`;
- execution boundary: `first_use_transaction_retry`;
- route-cache protocol: `topology-selected-first-use-routing-v1`;
- estimator: `topology-fixed-suffix-counts-v1` with
  `maximum_edge_to_node_ratio_per_mille=1000`;
- fallback: `EXPANSION-STEPWISE-FORWARD` in the same stable transaction.

Admission requires a v5 manifest with the exact frozen suffix, state,
output-row, and output-byte caps and a structural fixed-suffix bucket. The
driver rejects any changed protocol identity, cap, estimator, synopsis schema,
or threshold. Read-committed transactions, unavailable or stale synopsis data,
unverifiable parameter values, and dense topology remain incumbent.

The first-use candidate is still correctness-safe because the reverse arm
retains the exact forward fallback. The synopsis influences cost selection only;
it does not alter graph semantics. Its dedicated rollback switch is
`disable_topology_fixed_suffix_first_use`.

Promotion remains default-off until the v5 manifest has independently recorded
AA, confirmation, performance, resource, reference-closure, and operational
evidence. The v4 capture procedure remains the required evidence format; v5
must repeat it with first-use transaction samples rather than reusing a v4
authorization.
