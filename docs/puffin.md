# Puffin support

[Puffin](https://github.com/apache/iceberg/blob/main/format/puffin-spec.md)
is the binary container format Iceberg uses for two things `bergrebase`
cares about:

1. **Statistics files** (V2+) — referenced from `metadata.json` under
   `statistics[].statistics-path`. Their internal blobs (theta sketches,
   NDV, ...) carry no paths, so we only need to rewrite the
   `statistics-path` string in `metadata.json`. This works today **without**
   a Puffin reader.
2. **Deletion vectors** (V3) — each blob's
   `properties["referenced-data-file"]` must equal the data file URI it
   deletes from. The spec requires that this match the manifest's
   `referenced_data_file`, so when we rewrite the data file URI in the
   manifest we must also rewrite it inside the Puffin's footer.

For (2) we need to read and write Puffin files, and there is **no Go
Puffin library** today. This document explains how we'll deal with it.

## Why we don't ship a brand-new project

Creating yet another `iceberg-go-puffin` repo is the wrong move:

- The natural home for Puffin in Go is `apache/iceberg-go` itself, just
  as `iceberg-rust` is tracking it in
  [`apache/iceberg-rust#744`](https://github.com/apache/iceberg-rust/issues/744).
- A standalone helper package competes with the canonical implementation
  and creates the "two truths" problem.
- Maintenance burden — ~200 LOC of binary format parsing — is large
  enough that we want community review, small enough that the upstream
  PR is realistic.

## Plan

### Phase 1 — vendor inside `bergrebase`

We implement Puffin read/write in `internal/puffin/` of this repository,
exactly as much as we need:

- Read the trailer (4 bytes magic, 4 bytes flags, 4 bytes footer payload
  size, footer JSON, leading magic).
- Parse `FooterPayload` JSON: blob metadata, file properties.
- Mutate blob `properties["referenced-data-file"]` strings via the
  rewriter's `PrefixMapping`.
- Re-emit the footer at the same byte offset structure, recomputing the
  trailing footer-payload-size field.
- Leave blob bytes themselves untouched — the manifest's `content_offset`
  / `content_size_in_bytes` fields then remain valid.

Coverage scope:

- Read **uncompressed** footer payloads first. LZ4 footer compression
  (per spec, optional) is added once we hit a real fixture that uses it.
- Read all blob types we encounter, but only modify the
  `deletion-vector-v1` blob's properties. Other blob types
  (`apache-datasketches-theta-v1`, `ndv`, ...) pass through untouched.
- Round-trip equivalence: for any input Puffin file with no source-prefix
  matches in any property, our writer must produce **byte-identical**
  output.

Tests live alongside the package and run as part of the standard
`go test ./...`. The acceptance bar is the same as the rest of the
rewriter: a Puffin file written by Spark + DuckDB on a V3 table with
deletion vectors must round-trip cleanly.

### Phase 2 — upstream PR to `apache/iceberg-go`

Once Phase 1 is green against real fixtures, the package is portable —
no `bergrebase` imports — and we open a PR to `apache/iceberg-go` adding
`pkg/puffin`. The PR scope:

- Reader: `Open(io.ReaderAt, size int64) (*File, error)` returning
  footer metadata and a `Blob(i int) ([]byte, error)` accessor.
- Writer: `NewWriter(io.Writer)`, `WriteBlob(meta BlobMetadata, body []byte)`,
  `Close()` — sufficient for `iceberg-go`'s own future deletion-vector
  write path.
- Tests vendored from this repo, plus integration with iceberg-go's
  manifest reader for the `referenced_data_file` cross-check.

If the PR is accepted we delete `internal/puffin/` and import upstream.
If the maintainers prefer a different shape, we adapt, vendor what we
need, and keep our own copy.

### Phase 3 — fall back if upstream stalls

Apache reviews can be slow (months). We don't block `bergrebase` v1.0.0
on it: ship Phase 1 inside our own tree, refuse V3 deletion vectors with
a clear error in v1.0.0, and quietly enable them in v1.1.0 once the
internal package is battle-tested. Upstreaming becomes a parallel,
non-blocking workstream.

## What this means for users today

- V2 + V3-without-DVs: works in v1.0.0 — `statistics-path` is a string in
  `metadata.json`, no Puffin reader required.
- V3 + DVs: refused in v1.0.0 with a message linking to this file.
  Supported in v1.1.0.

## References

- [Puffin spec](https://github.com/apache/iceberg/blob/main/format/puffin-spec.md)
- [`apache/iceberg-rust#744`](https://github.com/apache/iceberg-rust/issues/744) — Puffin in iceberg-rust
- [Iceberg V3 deletion vectors blog (AWS)](https://aws.amazon.com/blogs/big-data/accelerate-data-lake-operations-with-apache-iceberg-v3-deletion-vectors-and-row-lineage/)
