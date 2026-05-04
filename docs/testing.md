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
   step (`LoadTable` + HEAD + count) runs by default but is best-effort.
   Its limitations are documented in this file.

## Test target

The test harness runs against a Lakekeeper REST catalog backed by
PostgreSQL, with MinIO providing two S3-compatible buckets (source and
target). All four are spun up via [testcontainers-go]; tests are pure Go,
run with `go test ./test/...` against a Docker daemon.

DuckDB serves as the **oracle**: every scenario writes a known dataset
through DuckDB → Lakekeeper before the rebase, then re-reads the table
through DuckDB after the rebase and asserts row count + per-row checksum
equality.

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

The seed data writer for V2 uses `iceberg-go`. For V1 fixtures and
anything outside the writer's coverage, we fall back to PyIceberg invoked
via `os/exec` from the test (Python is available in the test image only).

## Scenarios

Every scenario has the same shape: seed → rclone copy → run `bergrb` →
oracle check. Differences are in the seed data and the assertion sets.

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

## Validation step in the binary itself

The `bergrb` binary runs a built-in post-rebase validation:

1. `LoadTable(id)` — assert `current-snapshot-id` matches the value
   captured before the swap.
2. From the latest snapshot's manifest, pick one data file URI; `HEAD` it
   on the target; assert HTTP 200.
3. (Optional, behind `--validate-with-duckdb`) shell out to `duckdb` and
   run `SELECT count(*) FROM <table>` against the new pointer; assert the
   pre-swap count matches.

This is **best-effort**, not a correctness proof. A truly complete check
would re-read every data file's footer and compare per-column min/max
bounds and row counts to what the manifest claims — expensive and not
worth the wall-clock cost for v1. We document the limit and stop there.

The flag `--no-validate` skips all three. Useful in CI when the test
harness runs its own oracle check immediately after.

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
path are not affected — see design.md D2 — and we recommend deploying
against those for production migrations.

## CI

The scenario suite runs on every PR via `go test -count=1 ./test/...`
in GitHub Actions (`.github/workflows/ci.yml`). Scenarios 1–7, 9, 11,
13, 15, 16, 17 **must pass** for a PR to merge. Scenarios 4 (perf) and
14 (resume) are gated by `BERGREBASE_NIGHTLY=1` and run only in
nightly runs; failures there do not block merges. Scenario 12 asserts
the refusal message for V3 deletion vectors; it unblocks once the
Puffin rewriter lands.

[testcontainers-go]: https://golang.testcontainers.org/
