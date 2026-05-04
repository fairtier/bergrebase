# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**v1.0 candidate.** The engine, REST catalog client, and end-to-end
testcontainers harness are implemented and pass the merge-gate
scenarios. Outstanding work for v1.0.0 is primarily real-world tenant
validation. The remaining deferred feature is V3 deletion-vector
rewrite, which needs a Go Puffin reader/writer; V2 position-delete and
equality-delete file rewrite both ship in v1.0
(`internal/rewrite/positiondelete.go` for the position-delete Parquet
column rewrite ; equality-deletes need no body rewrite because the file
contains only the projected equality column values).

Treat new work as either (a) closing roadmap follow-ups or (b) the V3
DV work. The public surfaces (`Catalog`, `Storage`, `rewrite.Engine`)
are stable; redesigning them needs a real reason.

## Common commands

```bash
go build -o bergrb ./cmd/bergrb        # build the CLI
go test ./...                          # all unit tests (rewrite, storage, catalog/rest)
go test ./test/...                     # testcontainers harness (Lakekeeper + Polaris + MinIO)
go test ./internal/rewrite -run TestPrefixMapping_Apply  # single test
docker build -t bergrebase:dev .       # static distroless image, ENTRYPOINT=/bin/bergrb
```

Go 1.26. The Dockerfile builds with `CGO_ENABLED=0`, `GOAMD64=v3`, distroless
nonroot final stage.

## Architecture

`bergrebase` rewrites the absolute object-storage URIs inside Apache Iceberg
metadata after a bulk byte copy from one S3-compatible bucket to another, then
re-points the catalog at the rewritten metadata. Three small packages, one
binary:

- `cmd/bergrb` — flag parsing, env-var credential pickup
  (`SOURCE_AWS_*` / `TARGET_AWS_*`), wiring of catalog + storage + engine, and
  the per-table loop (`migrateOne`: `LoadTable` → `RewriteTable` →
  `SwapMetadataLocation`).
- `internal/catalog` — three-method swap surface (`ListTables`, `LoadTable`,
  `SwapMetadataLocation`). Sub-package `rest/` is the only planned v1
  implementation; it covers Lakekeeper, Polaris, Tabular, Snowflake Open
  Catalog, and Glue's REST shim.
- `internal/storage` — minimal S3 surface (`GetObject`, `PutObject`,
  `HeadObject`) over `aws-sdk-go-v2` with endpoint override (works against R2,
  MinIO, B2, etc.). `s3://bucket/key` URI parsing lives here.
- `internal/rewrite` — the rewrite engine. `Engine.RewriteTable` walks the
  metadata graph **bottom-up** (data file paths → manifests → manifest lists
  → `metadata.json`), mutating known path-bearing fields and writing each file
  back to the **same key** in the target bucket.

### Rules to preserve when extending the rewrite engine

These are spelled out in the comments in `internal/rewrite/prefix.go`
and `metadata.go`. They are easy to violate by accident; check before
changing rewrite logic.

1. **Strict prefix substitution, never byte replacement.** Mutate
   strings only when they begin with `Source` (use
   `PrefixMapping.Apply` / `Matches`). A `strings.ReplaceAll` over
   Avro bytes will silently corrupt `lower_bounds`/`upper_bounds` if
   any user column happens to hold the bucket name.
2. **Mutate only the documented path-bearing fields**, enumerated by
   name in `internal/rewrite/metadata.go`
   (`PathFieldsInMetadataJSON`, `PathFieldsInManifestList`,
   `PathFieldsInManifest`, `MetadataPathProperties`). The
   `properties["write.*.path"]` keys are future-write hints — forget
   them and new writes after migration land in the old bucket.
3. **Bottom-up order, recompute `manifest_length`.** A manifest list
   entry's `manifest_length` (Avro field id 501) must match the actual
   byte length of the rewritten manifest, or readers reject the file.
   Always write the inner file before the outer.
4. **Same-key idempotent overwrite.** Write rewritten files at the
   substituted URI; a second run with the same prefix mapping must be
   a no-op for already-rewritten paths. `RewriteTable` checks
   `Mapping.Matches(oldMetadataLocation)` before doing anything.
5. **Refuse, don't silently skip, on unsupported features.** V3
   deletion vectors (position-delete entries pointing at `.puffin`
   files) are explicitly out of scope for v1 — return
   `ErrUnsupportedFeature` (or wrap it). The per-entry routing in
   `RewriteManifest` and `WalkManifest` distinguishes V2
   position-delete `.parquet` files (supported) from V3 `.puffin`
   (refused). `--keep-going` may demote per-table failures to logs at
   the CLI layer, but the engine itself fails closed.
6. **Catalog swap is the last step and must be atomic.** The engine
   returns the new metadata location; only then does `cmd/bergrb`
   call `SwapMetadataLocation`. The REST implementation prefers
   `register?overwrite=true` and falls back to `DropTable(purge=false)`
   + `RegisterTable`; on register failure it must roll back to the
   old pointer so reads keep working.

### Iceberg version scope (v1)

V1, V2 (append-only, position-deletes, and equality-deletes), and V3
without deletion vectors. The engine refuses V3 deletion vectors
(position-delete entries pointing at `.puffin` files) until a Go Puffin
reader/writer lands.

Position-delete `.parquet` files are rewritten by
`internal/rewrite/positiondelete.go` (`RewritePositionDeleteFile`),
which mutates the embedded `file_path` column in-place. Equality-delete
files need no body rewrite — they contain only the projected equality
column values (no embedded URIs).

## Where the rationale lives

When a question is bigger than "where is the function" — read the
package doc-comments, not just the code:

- `internal/rewrite/prefix.go` — strict prefix substitution semantics.
- `internal/rewrite/metadata.go` — exhaustive list of path-bearing
  fields the rewriter mutates.
- `internal/rewrite/walker.go` — top-level rewrite driver and the
  bottom-up walk order.
- `internal/catalog/rest/rest.go` — REST swap strategy
  (`register?overwrite=true` with drop+register fallback).
- `README.md` — public-facing usage, coverage matrix, and the
  algorithm overview.
- `docs/migration-recipes.md` — user-facing runbooks for the four
  migration scenarios (same-catalog bucket move, catalog swap with
  data in place, catalog swap + bucket move, DR/staging clone).
- `k8s/README.md` — running `bergrb` as a `batch/v1` Job.
