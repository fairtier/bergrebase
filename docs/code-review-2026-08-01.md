# Code review — correctness, security, bugs

Date: 2026-08-01. Scope: all non-test Go code (`cmd/bergrb`, `internal/catalog`,
`internal/catalog/rest`, `internal/storage`, `internal/rewrite`), read in full,
plus `docs/design.md` cross-checks and iceberg-go v0.5.0 / arrow-go v18.6.0
source inspection where behavior depends on the dependency. Baseline: `go build
./...` and `go test ./internal/... ./cmd/...` pass; `go vet` clean.

Overall: the engine is careful and well-documented — strict prefix substitution
is honored everywhere, `putAndVerify` closes the manifest-length class of
failures, the catalog swap has a drift check and a rollback path, and the V3
deletion-vector refusal fails closed. The serious findings cluster around one
theme: **the position-delete body rewrite invalidates manifest-entry statistics
that the rest of the design assumes are immutable**, and the DuckDB-based e2e
oracle cannot see it.

## Summary

| #  | Severity | Area                  | Finding                                                                    |
|----|----------|-----------------------|----------------------------------------------------------------------------|
| 1  | High     | rewrite/pos-deletes   | Stale `file_path` column bounds → deletes silently not applied (ghost rows) |
| 2  | High     | rewrite/pos-deletes   | Stale `file_size_in_bytes` / `split_offsets` after body re-encode           |
| 3  | High     | CLI validation        | No guard against overlapping or slash-less prefixes → non-idempotent runs   |
| 4  | Medium   | rewrite/manifest list | Partition summaries dropped from rewritten manifest-list entries            |
| 5  | Medium   | catalog/rest          | SIGINT during drop+register leaves table unregistered (rollback uses canceled ctx) |
| 6  | Medium   | storage               | `ParseS3URI` percent-decodes keys → wrong key for escaped partition paths   |
| 7  | Medium   | storage               | 64 MiB `GetObject` cap also applies to position-delete bodies; not tunable via CLI |
| 8  | Medium   | CLI                   | Re-run after successful swap errors instead of skipping as already migrated |
| 9  | Medium   | rewrite/walker        | Out-of-prefix parent files are overwritten in place, pre-swap               |
| 10 | Low      | CLI/logging           | `StatsFilesRewritten` is never incremented                                  |
| 11 | Low      | rewrite               | Dry-run does not open position-delete bodies (weaker than live path)        |
| 12 | Low      | storage               | `HeadObject` returns `(0, nil)` when `ContentLength` is nil                 |
| 13 | Low      | CLI                   | `listAllNamespaces` BFS has no cycle guard                                  |
| 14 | Low      | catalog/rest          | Swap probe never falls back if server 400s on unknown `overwrite` param     |
| 15 | Low      | docs                  | `docs/design.md` stale/unimplemented claims                                 |

---

## High severity

### 1. Position-delete rewrite leaves `lower_bounds`/`upper_bounds` pointing at the old bucket — deleted rows can resurrect

`RewriteManifest` (`internal/rewrite/manifest_write.go:38`) mutates only
`file_path` and `referenced_data_file` on each entry. For **data** files that is
correct — the design-doc rationale "bounds describe data, not paths"
(`docs/design.md:52`) holds. It does **not** hold for V2 position-delete
entries: their `lower_bounds`/`upper_bounds` for the reserved `file_path`
column (field id 2147483546) contain **absolute data-file URIs**, and after the
rebase they still name the old bucket while the data files and the delete
file's own body name the new one.

Java-based engines (Spark, Trino, Flink via Iceberg Java's `DeleteFileIndex`)
use exactly these bounds to decide which position-delete files apply to which
data files: when `referenced_data_file` is absent (the common case for
Spark-written V2 deletes), `ContentFileUtil.referencedDataFile()` falls back to
"lower bound == upper bound" of the `file_path` column, and range matching uses
the bounds. A delete file whose bounds say `[s3://old/…, s3://old/…]` will
never be matched against a data file at `s3://new/…`, so the delete is silently
skipped and **deleted rows reappear in query results**. That is data corruption
from the reader's point of view, with no error anywhere.

The DuckDB e2e oracle does not catch this because DuckDB's iceberg reader
matches deletes by reading the delete file's parquet body (which *is*
rewritten), not by manifest bounds.

Fix direction: when an entry is `EntryContentPosDeletes`, additionally rewrite
the bounds map values for field id 2147483546 iff they begin with
`Mapping.Source` (this stays within the strict-prefix rule — these particular
bounds are paths by spec, not user data). Add an e2e assertion that reads the
rewritten manifest's bounds, and ideally a Spark or Trino based oracle test.

### 2. Position-delete body re-encode invalidates `file_size_in_bytes` (and `split_offsets`, `column_sizes`)

`RewritePositionDeleteFile` (`internal/rewrite/positiondelete.go:43`) re-encodes
the parquet body uncompressed, single row group, no dictionary
(`encodePositionDeleteParquet`, line 191). A real-world delete file written by
Spark is zstd/snappy-compressed, so the rewritten object's size differs from
the original — usually it is larger. The manifest entry's
`file_size_in_bytes` (field 104) is never updated (`docs/design.md:53`
explicitly declares it immutable, with the rationale "the data file … we copied
byte-perfect", which is untrue for the one file class whose body we rewrite).

Iceberg Java opens delete files with the declared length (`fileSizeInBytes` is
passed as the input-file length), so it looks for the parquet footer magic at
the declared end of file — which now lands mid-file → "not a Parquet file" /
footer-parse errors at read time, after the swap. `split_offsets` (field 132)
and `column_sizes` also describe the old byte layout.

The test harness masks this: the seed files are written with the **same**
writer properties the rewriter uses (uncompressed, no dictionary —
`test/harness/parquet.go`), and the test prefixes are equal length, so old and
new sizes coincide in e2e runs.

Fix direction: `RewritePositionDeleteFile` should return the new byte length
and `RewriteManifest` should set `file_size_in_bytes` (and clear or recompute
`split_offsets`) on the mutated entry. Alternatively preserve the original
compression codec so sizes are *less* likely to drift — but the size must be
corrected regardless.

### 3. No validation of prefix shape: overlapping prefixes break idempotency, slash-less prefixes rebase sibling paths

`config.validate()` (`cmd/bergrb/main.go:87`) only rejects
`sourcePrefix == targetPrefix`. Two dangerous shapes pass:

- **Target starts with source**, e.g. `--source-prefix s3://b/x/
  --target-prefix s3://b/x/deep/`. Every rewritten path still matches the
  source prefix, so a second run rewrites again (`s3://b/x/deep/deep/…`).
  The engine's core idempotency claim (`prefix.go:17`, design D1) silently
  fails.
- **No trailing slash**, e.g. `--source-prefix s3://old/warehouse` also
  matches `s3://old/warehouse2/table1/…`. Those paths are rewritten onto the
  target even though the bulk copy likely never copied them; post-swap
  validation HEADs only one data file (`validate.go`), so a broken table can
  reach production.

Fix direction: reject `strings.HasPrefix(target, source)` and
`strings.HasPrefix(source, target)`; require (or at minimum warn loudly on)
prefixes that don't end in `/`.

## Medium severity

### 4. Rewritten manifest-list entries lose their partition summaries

`preserveSequenceNumbers` (`internal/rewrite/manifest_write.go:157`) rebuilds
the `ManifestFile` but never calls `.Partitions(...)`, even though
`iceberg.WriteManifest` recomputes valid summaries into `newMF`
(iceberg-go v0.5.0 `manifest.go`: `constructPartitionSummaries`). The
`partitions` field (id 507: `contains_null`, `contains_nan`, per-field
lower/upper bounds) is therefore absent from every rewritten manifest list.
The field is optional, so reads stay correct, but engines lose manifest-level
partition pruning on every migrated table — a silent, permanent planning
regression. One-line fix: `.Partitions(newMF.Partitions())`.

### 5. Ctrl-C during the drop+register window strands the table unregistered

`swapByDropRegister` (`internal/catalog/rest/rest.go:390`) runs drop, register,
and the rollback register all on the caller's context, which is the
`signal.NotifyContext` from `main`. If SIGINT/SIGTERM arrives after `dropTable`
succeeds, `registerTable(new)` fails with `context canceled` — and the
best-effort rollback `registerTable(old)` fails instantly for the same reason.
Result: the table is deregistered from the catalog until manual repair. The
swap critical section (and the rollback especially) should run on
`context.WithoutCancel(ctx)` with its own short timeout.

### 6. `ParseS3URI` decodes percent-escapes in object keys

`ParseS3URI` (`internal/storage/s3.go:178`) goes through `url.Parse`, whose
`u.Path` is percent-decoded. Iceberg data paths routinely contain Hive-style
escaped partition values (e.g. `ts_hour=2024-01-01-00%3A00/…`); the decoded key
(`…00:00/…`) is a different S3 object, so GET/HEAD miss. Keys containing a bare
`%` don't parse at all ("invalid URL escape"). Split the URI manually: strip
`s3://`, split at the first `/`, treat the remainder as the literal key.

### 7. The 64 MiB `GetObject` cap silently gates position-delete bodies and large metadata.json files

The `rewrite.Storage` doc-comment (`walker.go:17`) claims Storage "is only ever
pointed at Iceberg metadata files", but `RewritePositionDeleteFile` fetches
delete-file parquet bodies through the same client, and long-lived tables can
have metadata.json documents well past 64 MiB (thousands of retained
snapshots). Either case fails the whole table with a message asserting a
"configuration error". `Config.MaxObjectSize` exists but is not settable from
the CLI (`cmd/bergrb/main.go:173` never populates it). Wire a flag, raise the
default for the delete-file read path, and soften the error text.

### 8. A completed migration cannot be re-run cleanly

After a successful swap the pointer starts with the target prefix, so a re-run
fails in `RewriteTable` with "does not start with source prefix"
(`walker.go:84`) — exit 1, and under `--all-tables` a partially-migrated
namespace can't converge without `--keep-going` (which still exits non-zero).
`migrateOne` should treat a pointer that already starts with
`Mapping.Target` as "already migrated: skip", which also makes the CLI
idempotent end-to-end, matching the engine's documented contract.

### 9. Out-of-prefix parents are overwritten in place, before the swap

`RewriteManifest` / `RewriteManifestList` compute `newURI :=
mapping.Apply(parentURI)`. If a manifest (or manifest list) lives **outside**
the source prefix while entries under it are in-prefix (mixed-location tables:
prior partial migrations, custom `write.metadata.path` history), `Apply` is a
no-op and the rewritten file is PUT at the **original URI** — mutating the live
source table's metadata before any catalog swap, using target credentials. If
the swap later fails or rolls back, the old table now references new-bucket
paths. Per the project's own rule 5 ("refuse, don't silently skip"), the engine
should refuse to rewrite a child of an out-of-prefix parent instead of
overwriting the original.

## Low severity

- **10.** `RewriteResult.StatsFilesRewritten` (`walker.go:67`) is never
  incremented; the per-table log line always reports `stats_files=0` even when
  statistics paths were rewritten.
- **11.** Dry-run (`WalkManifest`) never opens position-delete bodies, so a
  clean dry-run can be followed by a live-run failure (e.g. the foreign-prefix
  error from `rewriteFilePathColumn`). Worth reading (not writing) the bodies
  under `--dry-run`, or documenting the gap.
- **12.** `HeadObject` returns `(0, nil)` when the SDK's `ContentLength` is nil
  (`s3.go:169`). `putAndVerify` then reports a bogus "storage truncated?"
  error, and `loadV1ManifestDescriptor` builds a size-0 descriptor that fails
  later with a confusing OCF error. Treat nil `ContentLength` as an error.
- **13.** `listAllNamespaces` (`main.go:303`) has no visited-set; a buggy
  catalog that lists a namespace under itself loops forever and grows the
  queue unboundedly.
- **14.** The swap probe treats only 409/`AlreadyExistsException` as "server
  ignored `overwrite=true`" (`rest.go:222`). A server that rejects the unknown
  query parameter with 400 never triggers the drop+register fallback even
  though it would work.
- **15.** Doc drift in `docs/design.md`: edge-case item 8 still says
  position-delete files are "refused with a clear error in v1" (they are
  rewritten since v1.0); the "snapshot.summary — rewrite values that begin
  with the source prefix" guidance is not implemented
  (`ApplyMetadataJSONMapping` never touches summaries); and the field-table
  rationales at lines 52–53 are contradicted by findings 1–2 above.

## Security review

No command injection, path traversal, or unsafe deserialization surfaces; the
binary talks only to the operator-specified catalog and S3 endpoints.
Dependencies are current (`go.mod`: aws-sdk-go-v2 1.41.x, arrow-go 18.6,
iceberg-go 0.5.0); `go vet` is clean. Specific observations:

- **Bearer token handling is good**: read from an env var, sent only in the
  `Authorization` header, never logged. Go's HTTP client strips the header on
  cross-host redirects.
- **No HTTPS enforcement** on `--catalog-uri`: an `http://` URI sends the
  bearer token in cleartext. Fine for dev stacks; consider a warning when the
  scheme is `http` and a token is present.
- **Ambient credential fallback is a footgun**: if `SOURCE_AWS_*` /
  `TARGET_AWS_*` are unset (or the operator typos the names), the SDK default
  chain (env `AWS_*`, shared config, IMDS) is silently used **for both sides**
  (`internal/storage/s3.go:72` — static provider only when `AccessKeyID` is
  non-empty). In a two-account migration tool this can sign writes with the
  wrong identity without any indication. Also, a `SecretAccessKey` without an
  `AccessKeyID` is silently ignored. Recommend logging which credential source
  each side resolved to, and erroring on a half-set pair.
- **Catalog error bodies (≤ 8 KiB) are propagated into logs** (`rest.go:189`):
  acceptable, but they land in the JSON error log verbatim — worth remembering
  if logs are shipped somewhere with weaker access control than the catalog.
- **Swap TOCTOU is acknowledged**: the drift check (`rest.go:352`) is
  load-then-act, not CAS; a writer committing between the check and the
  register loses its snapshot. The documented mitigation (writers paused) is
  reasonable for v1; a commit-based swap via the standard REST
  `POST /tables/{table}` update with a requirement on the current metadata
  location would close it properly.
- **DoS-hardening is decent**: the 64 MiB object cap bounds memory per read;
  there is no recursion in the metadata walk. The only unbounded loop is
  finding 13.

## Test-coverage note

The e2e suite is broad (Lakekeeper + Polaris, rollback, resume, race, refusal,
DuckDB oracles for V1/V2/V3/partitioned/schema-evolution), but every
data-correctness oracle is DuckDB. DuckDB neither uses manifest-entry bounds
for delete-file assignment nor the declared `file_size_in_bytes` for reads,
which is exactly why findings 1 and 2 pass the gate. A single Spark or Trino
container test reading a migrated MoR table (position deletes written
compressed by a real writer, prefixes of different lengths) would cover both.
