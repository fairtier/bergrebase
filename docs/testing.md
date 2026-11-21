# Testing strategy

## Acceptance criteria for v1.0.0

A release is acceptance-ready when **all** of the following hold:

1. **Round-trip equivalence on metadata** — for every supported Iceberg
   format version (V1, V2 append-only, V3 without DVs), the rewriter can
   read a metadata file, write it back unchanged with `Source == Target`,
   and produce **byte-identical output** for `metadata.json`, manifest
   lists, and manifests.
2. **End-to-end correctness on a real catalog** — DuckDB
   `SELECT count(*) + sum(<checksum_col>)` against the table is identical
   before and after the rebase, for every scenario in
   [§ Scenarios](#scenarios).
3. **Idempotency** — running the tool a second time with the same
   `--source-prefix` / `--target-prefix` is a no-op (no PUT requests
   issued, catalog pointer unchanged, exit 0).
4. **Resume after kill** — SIGKILL during the rebase, restart with the
   same arguments, and the table converges to the same final state as
   running once without interruption.
5. **Catalog rollback on register failure** — if the `RegisterTable` call
   fails, the catalog still resolves the table at its **original**
   `metadata-location`, and reads continue to work against the source
   bucket.
6. **Refusal on unsupported features** — V3 deletion vectors are
   detected and refused with a clear error that names the feature
   (Puffin / V3 deletion vector). The tool does not silently produce
   broken output.
7. **Refusal on prefix mismatch** — if the catalog's current
   `metadata-location` does not start with `--source-prefix`, the tool
   exits non-zero with an actionable error message and never writes.
8. **Documented best-effort validation** — the post-migration validation
   step (`LoadTable` + source-prefix check + HEAD and size of every live
   file + position-delete rows) runs
   by default but is best-effort.
   Its limitations are documented in this file.

## Test target

The test harness runs against a Lakekeeper REST catalog (the version
FairTier workspaces run) backed by PostgreSQL, with a MinIO-compatible
server providing two S3-compatible buckets (source and target; "MinIO" in
the harness means Silo today, see `test/harness/minio.go`). All are spun up
via [testcontainers-go]; tests run with `go test ./test/...` against a
Docker daemon.

There are two kinds of test, and the difference matters:

- **Real writers** (`test/real_writers_e2e_test.go`, `TestRealWriters_*`).
  DuckDB (1.5.4, 1.5.6, 2.0.0) and pyiceberg (0.12.0) commit through
  Lakekeeper. The real `bergrb` binary moves the tables, the original objects
  are deleted, and every row, snapshot ID, sequence number and time-travel
  count is compared through DuckDB and pyiceberg. This is what real tables
  look like: gzipped metadata, manifest lists with no OCF keys, manifests
  with no `schema-id`, 63-bit snapshot IDs, DELETED entries.
- **Hand-built fixtures** (`TestDuckDBOracle_*`, `TestEngine_*`,
  `TestMigrateOne_*`). `test/harness/seed.go` writes the metadata with
  iceberg-go and DuckDB **only reads** it. These pin specific structures but
  say nothing about what a writer produces: all of them passed while the
  tool could not rebase any non-empty table a real writer had written.

```bash
eval "$(test/tools/install-test-tools.sh ~/.cache/bergrebase-test-tools)"
go test ./test/...
```

The script installs the writers (DuckDB CLIs pinned and checksum-verified;
pyiceberg pinned by version, its transitive dependencies not pinned) and prints `BERGREBASE_TEST_DUCKDB`
(the reader), `BERGREBASE_TEST_DUCKDB_WRITERS` and `BERGREBASE_TEST_PYTHON`.
Without them, a plain `go test ./test/...` **skips** the tests it cannot run.
Set `BERGREBASE_TEST_REQUIRE_ALL=1` to make that a failure, as CI does.
`BERGREBASE_TEST_SCALE=N` multiplies `TestRealWriters_Scale`.

The diagram below is the hand-built-fixture flow; for real-writer tests the
writer and the reader sit on either side of the same catalog.

```
┌─────────────────┐         ┌──────────────────┐
│ DuckDB (writer) │ ──────> │ Lakekeeper REST  │
└─────────────────┘         └────────┬─────────┘
                                     │
                                     │ metadata-location
                                     v
                            ┌────────────────┐
                            │ MinIO source   │  ◄── seed data
                            └────────┬───────┘
                                     │
                              rclone copy
                                     │
                                     v
                            ┌────────────────┐
                            │ MinIO target   │
                            └────────┬───────┘
                                     │
                                     │ bergrebase walks + rewrites
                                     │ metadata + swaps catalog
                                     v
                            ┌────────────────┐
                            │ DuckDB (reader)│ ──── assert oracle match
                            └────────────────┘
```

The seed data for hand-built fixtures comes from `iceberg-go`.

## Scenarios

These are the hand-built-fixture scenarios. Every scenario has the same
shape: seed → copy → run the engine or `bergrb` → oracle check. Differences
are in the seed data and the assertion sets. The "Seed" column describes the
table the fixture imitates; no SQL engine runs it. Real-writer coverage is
listed after the table.

| #  | Tier      | Seed                                                                                     | Asserts                                                             |
|----|-----------|------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| 1  | V2 basic  | empty CREATE + single INSERT (10 rows)                                                   | oracle match; 1 manifest, 1 manifest list                           |
| 2  | V2 multi  | 5 successive INSERTs (10 rows each)                                                      | oracle match; manifest-log has 5 entries; time-travel queries work  |
| 3  | V2 part   | `PARTITION BY date_col`, 30 partitions × 10 rows                                         | oracle match; partition bounds preserved verbatim                   |
| 4  | V2 wide   | partitioned, ~1000 data files in one snapshot                                            | oracle match; all manifest entries rewritten; perf < 60 s           |
| 5  | V2 evolve | schema evolution across 3 snapshots (ADD COLUMN, RENAME COLUMN, DROP COLUMN)             | oracle match at each historical snapshot ID                         |
| 6  | V2 stats  | INSERT + ANALYZE (writes a Puffin statistics file)                                       | oracle match; `statistics-path` rewritten                           |
| 7  | V2 pstat  | partitioned + partition statistics                                                       | oracle match; `partition-statistics-path` rewritten                 |
| 8  | V2 prop   | INSERT + `ALTER TABLE … SET TBLPROPERTIES('write.object-storage.path' = 's3://old/...')` | property rewritten; future writes land in target bucket             |
| 9  | V2 cow    | INSERT + UPDATE (Copy-on-Write rewrite of data files, no position deletes)               | oracle match; no `referenced_data_file` fields produced             |
| 10 | V2 posdel | INSERT + DELETE with `write.delete.mode=merge-on-read` (position-delete files)           | oracle match; position-delete `file_path` column rewritten in-place |
| 11 | V3 basic  | V3 table with row lineage enabled                                                        | oracle match; `next-row-id` and `first_row_id` preserved            |
| 12 | V3 dv     | V3 + deletion vectors (Puffin)                                                           | tool refuses with a clear "deletion vectors not yet supported"      |
| 13 | idem      | run scenario 1, then run `bergrb` again                                                  | second run is a no-op; zero PUTs to target; catalog unchanged       |
| 14 | resume    | SIGKILL mid-rebase of scenario 4, restart                                                | converges to the same state as scenario 4 with no interruption      |
| 15 | rollback  | scenario 1, but inject `RegisterTable` 500                                               | catalog still resolves to original metadata; oracle still matches   |
| 16 | bad-pref  | scenario 1, but pass `--source-prefix s3://wrong-bucket/`                                | exit non-zero, no writes, message points at the actual source       |
| 17 | racewrt   | scenario 1, then commit a write between LoadTable and SwapMetadataLocation               | tool detects snapshot-ID drift and refuses to register              |

The key invariant for scenarios 1–11 (excluding 12) is **DuckDB count
+ checksum match**. For scenario 12 the assertion is the **error
message** (refusal must name the unsupported feature: "Puffin" and
"V3 deletion vector").

### Real-writer scenarios

| Test                                     | Writers                  | What it pins                                                                                              |
|------------------------------------------|--------------------------|-----------------------------------------------------------------------------------------------------------|
| `TestRealWriters_DuckDB`                 | each DuckDB in the matrix | gzip metadata, list headers from `metadata.json`, snapshot IDs, position deletes, partitions, post-rebase writes |
| `TestRealWriters_PyIceberg`              | pyiceberg                | no `schema-id`; DELETED entries stay DELETED: exact rows after a delete, replace and upsert, including history |
| `TestRealWriters_CompactedByPyIceberg`   | DuckDB, then pyiceberg   | mixed writers; time travel across a compaction                                                            |
| `TestRealWriters_DuckDBContentHeaderBug` | DuckDB ≤ 1.5.4           | delete manifests labelled `content=data` in their header                                                  |
| `TestRealWriters_V3RowLineage`           | DuckDB (V3)              | every manifest's `first_row_id` survives; deletion vectors are refused and the table is untouched         |
| `TestRealWriters_BucketMove`             | DuckDB                   | `docs/migration-recipes.md` §1: refused without the warehouse row edit, correct with it                   |
| `TestRealWriters_UpgradedTable`          | pyiceberg (v1 upgraded to v2) | `manifest_length` equals the object size in every list; a second migration works                     |
| `TestRealWriters_IncompleteCopy`         | DuckDB                   | a re-run keeps failing while files are missing; swap-back and finishing the copy both recover            |
| `TestRealWriters_ValidationSeesWholeCopy` | DuckDB                   | any one live data or delete file missing fails validation; so does a source re-copied over the rebase, whole or only its data directory |
| `TestRealWriters_Scale`                  | DuckDB, pyiceberg        | many snapshots, manifests and data files (nightly-gated; on in CI)                                        |

## Validation step in the binary itself

The `bergrb` binary runs a built-in post-rebase validation:

1. `LoadTable(id)` — assert `current-snapshot-id` matches the value
   captured before the swap.
2. Assert no path-bearing field of the new `metadata.json` (location,
   manifest lists, metadata-log, statistics, `write.*.path`) and no manifest
   or file URI of the current snapshot still starts with the source prefix.
   This catches a source re-copied over the rewritten metadata, whose URIs
   target credentials might still resolve.
3. `HEAD` every live (not DELETED) data and delete file of the current
   snapshot on the target, with bounded concurrency, and compare its size
   with the manifest's `file_size_in_bytes`; fail naming the bad ones.
4. Read every live position-delete file and fail if a `file_path` row still
   starts with the source prefix. A re-copy of just the data directory puts
   the original delete files back at the rewritten keys; readers match
   deletes by path, so the deleted rows would silently return.

A re-run that finds the table already migrated runs the same checks, so a failed
validation does not turn into exit 0 the second time.

This is **best-effort**, not a correctness proof, and it runs **after** the
catalog swap: a failure here means the catalog already points at the new
location, and `bergrb` does not roll it back. It cannot see wrong row counts,
as deleted rows coming back did not fail it, nor a data file replaced by
another of the same size, nor a missing file that only an older snapshot
references. A truly complete check would
re-read every data file's footer and compare per-column min/max bounds and row
counts to what the manifest claims — expensive and not worth the wall-clock
cost for v1. Operators should compare counts and checksums themselves before
deleting the source.

The flag `--no-validate` skips both. Useful in CI when the test harness runs
its own oracle check immediately after.

## Documented limit: process death on the drop+register fallback

Scenario 14 covers SIGKILL *before* the catalog swap — `bergrb` resumes
because the catalog still points at the source metadata.json and the
mapping is idempotent. There is one window scenario 14 does **not**
exercise: a kill *between* the `DropTable` and the subsequent
`RegisterTable` on the fallback swap path (older Lakekeeper, any catalog
that doesn't support `register?overwrite=true`). In that window the
catalog has no entry for the table; the data files and rewritten
metadata.json are intact in the target bucket, but `bergrb` cannot
auto-recover because there is no `LoadTable` to read the previous
pointer from. Operators must re-register the table manually using the
new metadata location. Catalogs on the atomic register-with-overwrite
path (Lakekeeper v0.12.4) are not affected — see design.md D2. Polaris 1.4.0
takes the fallback.

## CI

The whole suite runs on every PR in GitHub Actions
(`.github/workflows/ci.yml`), with the pinned writers installed and
`BERGREBASE_NIGHTLY=1`, so scenarios 4 (perf) and 14 (resume) and
`TestRealWriters_Scale` run too; there is no separate nightly workflow.

**A skip fails CI.** A skipped oracle reports the same green as a passed one,
so: under `CI` (or `BERGREBASE_TEST_REQUIRE_ALL`) the harness turns a missing
tool into a failure (`test/harness/required.go`), and a workflow step fails the
job on any skipped test in the `go test -json` stream, and if fewer oracle or
real-writer tests passed than expected. Scenario 12 asserts the refusal message
for V3 deletion vectors; it unblocks once the Puffin rewriter lands.

[testcontainers-go]: https://golang.testcontainers.org/
