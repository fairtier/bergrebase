# Roadmap, coverage, and known blockers

## Iceberg version coverage

V1.0 covers V1, V2 (append-only, position-deletes, equality-deletes),
and V3 without deletion vectors. V3 tables that use deletion vectors
(position-delete entries pointing at `.puffin` files) are detected and
refused with a clear error that links to this document; rewriting them
needs a Go Puffin reader/writer (see [`puffin.md`](./puffin.md)).

## Catalog coverage

| Catalog                                 | Status                       | Notes                                                                                                                                                                                            |
|-----------------------------------------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Lakekeeper (REST)                       | supported                    | Primary target. v0.12.0 supports atomic `register?overwrite=true` (auto-detected); the drop+register fallback is locked in by `TestMigrateOne_E2E_Lakekeeper_DropRegisterFallback`.              |
| Apache Polaris (REST)                   | supported                    | Tested via `TestMigrateOne_E2E_Polaris` (testcontainer); same `rest.Client` as Lakekeeper. Atomic swap via `register?overwrite=true` (auto-detected on first swap; falls back to drop+register). |
| Tabular / Snowflake Open Catalog (REST) | supported                    | Same REST contract.                                                                                                                                                                              |
| AWS Glue REST shim                      | supported                    | Same REST contract.                                                                                                                                                                              |
| Nessie (REST)                           | best-effort                  | Branch/tag model differs; needs testing.                                                                                                                                                         |
| Hive Metastore (Thrift)                 | not yet supported (planned)  | Native client.                                                                                                                                                                                   |
| AWS Glue (native)                       | not yet supported (planned)  | AWS SDK `UpdateTable` of `metadata_location`.                                                                                                                                                    |
| JDBC catalog (native)                   | not yet supported (planned)  | A single SQL row update.                                                                                                                                                                         |

The `Catalog` Go interface (`internal/catalog/catalog.go`) isolates
catalog operations behind three methods: `ListTables`, `LoadTable`,
`SwapMetadataLocation`. Adding a new catalog is implementing those three;
the rewrite engine is unaffected.

## Storage coverage

Anything S3-compatible via `aws-sdk-go-v2` with endpoint override:

- AWS S3 (all regions, virtual-host style)
- Cloudflare R2 (path-style or `<account>.r2.cloudflarestorage.com`)
- MinIO (path-style)
- Backblaze B2 (S3 API)
- Wasabi, DigitalOcean Spaces, Linode Object Storage, Scaleway, OVHcloud

Configured per-side via `--source-endpoint`, `--target-endpoint`,
`--source-region`, `--target-region`, `--source-path-style`,
`--target-path-style`, plus credentials in env vars
(`SOURCE_AWS_ACCESS_KEY_ID`, `SOURCE_AWS_SECRET_ACCESS_KEY`, ditto for
`TARGET_*`).

GCS and Azure Blob are not yet supported. The storage interface is
small enough that adding either is a one-file addition.

## What's next

- **v1.1**: V3 deletion-vector Puffin rewrite (see
  [`puffin.md`](./puffin.md)); Iceberg view migration.
- **v1.2**: native Hive / Glue / JDBC catalogs; Nessie compatibility
  test.

Releases are cut as git tags `vX.Y.Z`; the GitHub Actions release
workflow publishes the corresponding container image.

## Test coverage status

Per [`testing.md`](./testing.md) merge-gate scenarios (1-9, 11, 13, 15,
16, 17). Engine-level path-rewrite is covered by hand-built fixtures
that exercise the same code paths a real writer would produce. The
DuckDB oracle (count + checksum equivalence after rebase) is **wired
for scenarios 1, 2, 3, 5, 9, and 11** — every read-side scenario in
the merge-gate set. Each oracle test seeds real Parquet, runs an
`iceberg_scan` pre-rebase, runs the engine, and asserts an identical
`(count, sum(id))` post-rebase. Wiring scenario 2 (multi-snapshot)
surfaced and fixed a latent engine bug where carry-forward manifests
lost their sequence numbers (`preserveSequenceNumbers` in
`internal/rewrite/manifest_write.go`).

| #  | Scenario             | Test                                                                                                                                                                    | Notes                                                                                                                                                                                                                          |
|----|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | V2 basic             | `TestMigrateOne_E2E_Lakekeeper`, `TestMigrateOne_E2E_Polaris`, `TestMigrateOne_E2E_Lakekeeper_DropRegisterFallback`, `TestEngine_E2E_MinIO`, `TestDuckDBOracle_V2Basic` | Full migrateOne against both Lakekeeper and Polaris (atomic path), plus a Lakekeeper run with the atomic probe forced to 409 to exercise drop+register against a real catalog. DuckDB oracle confirms post-rebase readability. |
| 2  | V2 multi-snapshot    | `TestEngine_E2E_MultiSnapshot`, `TestDuckDBOracle_MultiSnapshot`                                                                                                        | DuckDB oracle covers carry-forward manifests.                                                                                                                                                                                  |
| 3  | V2 partitioned       | `TestEngine_E2E_Partitioned`, `TestDuckDBOracle_Partitioned`                                                                                                            | Partition values + multi-column field IDs round-trip.                                                                                                                                                                          |
| 4  | V2 wide (1000 files) | `TestMigrateOne_E2E_V2Wide`                                                                                                                                             | Perf bound (60 s) on a 1000-entry manifest. Nightly CI gated by `BERGREBASE_NIGHTLY=1`, not merge gate.                                                                                                                        |
| 5  | V2 schema evolution  | `TestDuckDBOracle_SchemaEvolution`                                                                                                                                      | ADD COLUMN across 2 snapshots; per-snapshot schema-id round-trips.                                                                                                                                                             |
| 6  | V2 stats (Puffin)    | `TestEngine_E2E_MinIO/v2_basic`                                                                                                                                         | `statistics-path` rewrite asserted.                                                                                                                                                                                            |
| 7  | V2 partition stats   | `TestEngine_E2E_MinIO/v2_basic`                                                                                                                                         | `partition-statistics-path` rewrite asserted.                                                                                                                                                                                  |
| 8  | V2 write properties  | `TestEngine_E2E_MinIO/v2_basic`                                                                                                                                         | `write.object-storage.path` rewrite asserted.                                                                                                                                                                                  |
| 9  | V2 copy-on-write     | `TestDuckDBOracle_CoW`                                                                                                                                                  | "Overwrite"-style CoW: snap 2 list drops m1, contains only the new manifest.                                                                                                                                                   |
| 10 | V2 pos-deletes       | `TestDuckDBOracle_V2PositionDeletes`, `TestDuckDBOracle_V2EqualityDeletes`                                                                                              | Position-delete `file_path` column rewritten via `RewritePositionDeleteFile`; equality-delete files byte-copied (no embedded URIs). DuckDB confirms 50/2500 post-rebase.                                                       |
| 11 | V3 row lineage       | `TestEngine_E2E_V3_RowLineage`, `TestDuckDBOracle_V3_RowLineage`                                                                                                        | OCF first-row-id and per-file FirstRowId round-trip; oracle skips on DuckDB versions without V3 support.                                                                                                                       |
| 12 | V3 DV refuse         | `TestEngine_RefusesV3DeletionVectors_E2E` (`.puffin`-extension entries); e2e write-path arrives with V3 DV work.                                                        | Engine refuses position-delete entries pointing at `.puffin` files.                                                                                                                                                            |
| 13 | idempotency          | `TestEngine_E2E_MinIO/idempotent`                                                                                                                                       | Second run is byte-stable.                                                                                                                                                                                                     |
| 14 | resume after kill    | `TestMigrateOne_E2E_ResumeAfterSIGKILL`                                                                                                                                 | Spawns the bergrb binary, polls for the first metadata write, sends SIGKILL before the catalog swap, then re-spawns with identical args and asserts convergence. Nightly CI gated by `BERGREBASE_NIGHTLY=1`, not merge gate.   |
| 15 | rollback             | `TestSwapMetadataLocation_RollbackOnRegisterFailure`                                                                                                                    | Real Lakekeeper rejects malformed metadata.                                                                                                                                                                                    |
| 16 | bad prefix           | `TestEngine_E2E_MinIO/bad_prefix`                                                                                                                                       | Refuses, no writes.                                                                                                                                                                                                            |
| 17 | race detection       | `TestSwapMetadataLocation_DetectsDriftAgainstLakekeeper`                                                                                                                | Pre-swap reload catches drift, returns ErrCatalogDrift.                                                                                                                                                                        |

## Known blockers (production-ready, not MVP)

These are not existential — the architecture is sound — but each is
real work tracked separately from the v1.0 milestone.

1. **No Go Puffin library**. The deletion-vector Puffin reader/writer
   lives in `internal/puffin/`. The plan to upstream it to
   `apache/iceberg-go` is in [`puffin.md`](./puffin.md).
2. **`manifest_length` strictness**. Get the new byte length wrong by
   one and Lakekeeper refuses the read. Mitigation: round-trip every
   written manifest and compare lengths before catalog swap; assertion
   covered by acceptance criterion #1 in [`testing.md`](./testing.md).
3. **Best-effort validation**. `LoadTable` + HEAD + `count(*)` is not a
   correctness proof — true correctness would compare every data-file
   checksum. Acceptable for v1; documented limit in
   [`testing.md`](./testing.md).
4. **Bound-bytes footgun**. If a user column literally stores bucket
   strings, naïve byte replace would corrupt data. Our design avoids it
   (see [`design.md`](./design.md) D4) but anyone forking the code can
   reintroduce the bug. Document loudly.
5. **`iceberg-go` API stability** at v0.x. We pin a specific tag and
   re-evaluate per release. Worst case: vendor the manifest reader (~600
   LOC).
6. **Lakekeeper register-with-overwrite semantics** vary across
   versions. The REST client auto-detects on the first swap (probe
   `?overwrite=true`; cache `atomic` on 2xx, cache `drop+register` on
   409 / `AlreadyExistsException`) and uses the cached path for the
   rest of the run. Polaris and recent Lakekeeper hit the atomic path;
   older Lakekeeper falls back. Rollback behaviour for the fallback
   path is covered by scenario 15.
7. **`register-view` REST endpoint: spec landed, awaiting vendor
   implementations.** Apache Iceberg standardized
   `POST /v1/{prefix}/namespaces/{namespace}/register-view` in
   [apache/iceberg#14869](https://github.com/apache/iceberg/pull/14869)
   (merged 2026-01-17, originating from the dev-list
   [thread](http://www.mail-archive.com/dev@iceberg.apache.org/msg11822.html)).
   Neither Lakekeeper
   ([nightly OpenAPI](https://docs.lakekeeper.io/docs/nightly/api/rest-catalog-open-api.yaml))
   nor Polaris
   ([vendored spec](https://github.com/apache/polaris/blob/main/spec/iceberg-rest-catalog-open-api.yaml))
   has bumped their vendored snapshot to include it — both still ship
   only `create/load/replace/drop/rename`. View migration in bergrebase
   is gated on those vendors picking up the spec change and shipping
   `register-view`; we can implement against the wire format
   preemptively but can't test end-to-end until then.

## Follow-ups not yet scheduled

- Encryption (KMS) key migration for cross-account moves.
- GCS / Azure Blob support.
- A "verify-only" mode that walks an existing rebased table and asserts
  every URI references the target prefix (drift detection).
- Parallelisation across tables: today the engine processes tables
  sequentially; for large catalogs an `--jobs N` flag would help.
- DuckDB oracle through the Lakekeeper REST catalog instead of
  `iceberg_scan(metadata.json)`. Today's oracle proves the rewritten
  metadata graph is queryable; a catalog-attach variant would also
  exercise the swap end-to-end through DuckDB.
- A DELETED-status CoW oracle. Today's `TestDuckDBOracle_CoW` uses
  the "overwrite" pattern (snap N's list drops the previous
  manifest); a variant that carries the old manifest forward and
  exercises status=DELETED on a data-file entry would cover
  RewriteManifest's pass-through behaviour for delete-marking
  entries (distinct from V2 position-/equality-delete content
  manifests, which are now exercised by
  `TestDuckDBOracle_V2PositionDeletes` and
  `TestDuckDBOracle_V2EqualityDeletes`).
- `--validate-with-duckdb` flag in `cmd/bergrb` per `testing.md`.
