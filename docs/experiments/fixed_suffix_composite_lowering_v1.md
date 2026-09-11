# Composite fixed-suffix lowering v1

The suffix-reverse retry lowering now applies to every independently eligible,
full-path fixed-suffix region in one optimized query. Previously it rejected a
plan containing more than one such region solely because the tool boundary
expected one target.

All selected regions receive the same immutable reverse-state, suffix-row,
output-row, and output-byte limits. Candidate results are buffered as one query
result; any guard overflow or candidate failure retries the original query as a
whole in the same stable transaction. This preserves query-level semantics and
avoids mixing partial candidate and incumbent results.

Automatic production routing remains conservative: a future composite selector
must bind the joint shape and its exact fallback behavior in a new manifest
version. This lowering is the execution substrate for that selector, not an
authorization to reinterpret single-target v4 or v5 manifests.
