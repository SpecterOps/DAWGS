# Mudroom

`mudroom` is a shape-preserving OpenGraph scrubber. It removes or
pseudonymizes identity-bearing graph properties while preserving node IDs, node
kinds, relationship endpoints, and relationship kinds.

The first implemented scrub mode is `shape_only`. Rules and classifier defaults
are embedded from `defaults.toml`; pass `-config` or set `MUDROOM_CONFIG` to use
a TOML override. Scrubbing requires a stable private salt. The salt is never
written to plans or manifests.

Common file workflows:

```bash
go run ./cmd/mudroom plan \
  -input graph.json \
  -output scrub-plan.json

go run ./cmd/mudroom dry-run \
  -input graph.json \
  -plan scrub-plan.json \
  -manifest scrub.manifest.json \
  -salt "$SCRUB_SALT"

go run ./cmd/mudroom scrub \
  -input graph.json \
  -output scrubbed.json \
  -manifest scrubbed.manifest.json \
  -salt "$SCRUB_SALT"

go run ./cmd/mudroom validate \
  -original graph.json \
  -scrubbed scrubbed.json
```

Database exploration and mutation use Dawgs connection strings:

```bash
go run ./cmd/mudroom explore \
  -connection "$CONNECTION_STRING" \
  -graph default \
  -json

go run ./cmd/mudroom scrub-db \
  -connection "$CONNECTION_STRING" \
  -graph default \
  -salt "$SCRUB_SALT" \
  -manifest mudroom-db.manifest.json \
  -confirm-source-mutation
```

`scrub-db` mutates graph properties in place. It does not change node counts,
relationship counts, node kinds, relationship kinds, or endpoints. The portable
Dawgs graph API does not expose graph enumeration, so `scrub-db` targets the
configured/default graph rather than every graph in a backend.
