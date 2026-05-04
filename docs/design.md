# Design

`bergrebase` rebases an Apache Iceberg table onto a new object-storage
backing: data is copied byte-for-byte to the new bucket, every metadata
file is rewritten so its absolute URIs reference the new location, and
the catalog pointer is atomically swapped. The table identity, history,
schema, and snapshot IDs are preserved.

This document covers the architecture: what gets rewritten, the design
decisions behind the rewrite + swap, the algorithm, operational ordering,
and edge cases.

For acceptance criteria and scenarios see
[`testing.md`](./testing.md). For coverage matrices and the roll-out plan
see [`roadmap.md`](./roadmap.md). For the Puffin sub-project plan see
[`puffin.md`](./puffin.md).

## What gets rewritten

Drawn from the Iceberg [table spec](https://iceberg.apache.org/spec/) and
[manifest spec](https://github.com/apache/iceberg/blob/main/format/spec.md).

### `metadata.json` (table metadata, JSON)

| Field                                                | Path?                                                                                                                    |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `location`                                           | YES — table base URI                                                                                                     |
| `metadata-log[].metadata-file`                       | YES — past `vN.metadata.json` URIs                                                                                       |
| `snapshots[].manifest-list`                          | YES — manifest-list `.avro` URI                                                                                          |
| `snapshots[].manifests` (V1 only)                    | YES — flat list of manifest URIs                                                                                         |
| `statistics[].statistics-path`                       | YES — Puffin stats file URI                                                                                              |
| `partition-statistics[].statistics-path`             | YES                                                                                                                      |
| `properties["write.object-storage.path"]`            | YES (and `write.metadata.path`, `write.data.path`, `write.folder-storage.path`) — **future-write hints, easy to forget** |
| `refs.<name>.snapshot-id`                            | NO                                                                                                                       |
| `encryption-keys[].kms-key-id`                       | NO (KMS, not S3)                                                                                                         |
| `next-row-id` (V3)                                   | NO                                                                                                                       |

### Manifest list — Avro

| Field                      | Path?                                                                                                   |
|----------------------------|---------------------------------------------------------------------------------------------------------|
| `manifest_path` (id 500)   | YES                                                                                                     |
| `manifest_length` (id 501) | NO, but **must be re-computed** because the manifest's byte length changes when its path strings change |
| everything else            | NO                                                                                                      |

### Manifest — Avro

| Field                                                                                  | Path?                                                                                      |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `data_file.file_path` (id 100)                                                         | YES                                                                                        |
| `data_file.referenced_data_file` (id 143, V2+ for delete files, V3 mandatory for DVs)  | YES                                                                                        |
| `data_file.lower_bounds` / `upper_bounds` (ids 125/128)                                | NO, even if a data column literally holds bucket strings — bounds describe data, not paths |
| `data_file.file_size_in_bytes` (id 104)                                                | NO — describes the data file, which we copied byte-perfect                                 |
| `data_file.content_offset`, `content_size_in_bytes` (V3 DV)                            | NO — offsets inside the Puffin, unaffected by metadata-only rewrites                       |

### Position delete files (V2)

Parquet files with a `file_path` column whose **values are absolute URIs of
data files being deleted from**. Must rewrite the column. Only present in
V2 tables that use position deletes; V3 supersedes with deletion vectors.

### Deletion vector files (V3, Puffin)

Each blob's `properties["referenced-data-file"]` must equal the data file
URI. Spec requires it to match the manifest's `referenced_data_file`, so
when we change one we must change the other. See
[`puffin.md`](./puffin.md).

### Statistics & partition-statistics files (Puffin / Avro)

Their **path** is in `metadata.json`. Their **content** has no internal
paths unless they include a deletion-vector blob (rare for stats files).

### Things that look like they need rewriting but don't

- Parquet / ORC data file footers — Iceberg writers don't stamp a self-URI.
- Avro OCF header schema JSON — describes columns, not locations.
- `snapshot.summary` map — counters and stats, no paths in spec-defined
  keys. Custom keys are possible — be conservative: rewrite values that
  begin with the source prefix.

## Design decisions

### D1. Approach A: in-place overwrite at the same key

Every metadata file is rewritten at its **same key in the new bucket**, in
place. The alternative (write fresh files at new key prefixes and re-point
the metadata chain) was rejected because:

- The byte copy preserved keys, so the target objects already exist (we
  overwrite).
- Inter-file references stay consistent: a manifest entry's `manifest_path`
  equals the manifest's actual location after we rewrite both.
- Re-runs are idempotent: a second run with the same prefix mapping is a
  no-op for already-rewritten files.

The trade-off: we lose the audit trail of "old metadata.json untouched, new
one written next to it". Mitigation: the source bucket is preserved during
the migration window, so the originals stay intact in the source until the
operator finalizes.

### D2. Atomic catalog swap: register-with-overwrite, falling back to Drop+Register

The preferred swap is a single atomic call:

1. `POST /v1/{prefix}/namespaces/{ns}/register?overwrite=true` with the
   new `metadata-location`.

When the catalog doesn't support that variant, fall back to two-step:

1. `DELETE /v1/{prefix}/namespaces/{ns}/tables/{table}?purgeRequested=false`
   — frees the identifier, **leaves files alone**.
2. `POST /v1/{prefix}/namespaces/{ns}/register` with the new
   `metadata-location`.

The Iceberg REST OpenAPI doesn't encode query-string capabilities in
`/v1/config`'s `endpoints` list, so the rest client (`internal/catalog/rest`)
auto-detects on the first swap: try `register?overwrite=true`; on 2xx
cache `swapMode=atomic`; on 409 / `AlreadyExistsException` cache
`swapMode=drop+register` (a server that ignores the query parameter
treats the call as a duplicate registration); on any other error
propagate without poisoning the cache (transient errors retry detection
on the next swap). The pre-swap `LoadTable` drift check runs before
either path.

Polaris and recent Lakekeeper hit the atomic path; older Lakekeeper
versions fall back. The brief unavailability between drop and register
in the fallback path is bounded to one HTTP round-trip, and writers
should be paused for the entire migration window anyway (see
[§ Example operational pattern](#example-operational-pattern)). The
atomic path eliminates that window entirely.

**Process-death window on the fallback path.** If `bergrb` is killed
(SIGKILL, OOM, Pod evict) *between* the `DropTable` succeeding and the
`RegisterTable` issuing, the catalog is left with no entry for the
table. The data files are intact (`purgeRequested=false`) and the new
metadata.json is intact in object storage, but the catalog entry must
be re-registered manually — `bergrb` cannot resume because it no
longer has a `LoadTable` source of truth to compare against. The
atomic register-with-overwrite path **does not** have this window.
Operators on stacks that exercise the fallback (older Lakekeeper) should
be aware that resume after process death may need manual intervention;
prefer catalogs that support `register?overwrite=true`.

### D3. Process bottom-up

Always rewrite a file **before** the file that references its byte length:
manifest → manifest list → metadata.json. Statistics and partition-stats
files have no length-references and can be rewritten in any order.

### D4. Strict prefix substitution, not byte replacement

`PrefixMapping.Apply(p)` only acts on strings that **start with** Source.
A naïve `strings.ReplaceAll` would corrupt user data — for example, a
column named `source_url` whose values literally contain the source bucket
string would be modified inside the Avro `lower_bounds`/`upper_bounds` byte
arrays. We restrict mutation to known path-bearing fields by name and apply
strict prefix substitution to those fields only.

### D5. Idempotent re-runs

Running `bergrebase` twice with the same `--source-prefix` and
`--target-prefix` is safe: the second pass finds paths that already begin
with `target-prefix` and leaves them alone. This is the recovery story for
"process died mid-table".

### D6. No caller-specific coupling

`bergrebase` has no caller-specific imports. It is a stateless
one-shot binary intended to be invoked from any orchestrator — a
Kubernetes `batch/v1` Job, a CI workflow, an ad-hoc shell session —
that has finished a bulk byte copy and now wants the catalog
re-pointed.

### D7. Language: Go

- `github.com/apache/iceberg-go/manifest` — Avro manifest read/write,
  V1+V2, partial V3.
- `github.com/apache/iceberg-go/catalog/rest` — REST catalog client with
  `LoadTable`, `DropTable(purge=false)`, `RegisterTable`.
- `github.com/aws/aws-sdk-go-v2/service/s3` — object storage R/W with
  endpoint override (R2, MinIO, B2 all work).
- A hand-rolled (~200 LOC) Puffin reader/writer for V3 deletion-vector
  files; see [`puffin.md`](./puffin.md).

Python (PyIceberg) was the only viable alternative; rejected for binary
size and ops fit. Java/Spark rejected for JVM weight.

## Algorithm

```
For each table in scope:
  1. catalog.LoadTable -> oldMetadataLocation, *table.Metadata
  2. Walk the metadata graph:
     a. For each snapshot:
          For each manifest_list entry:
            Read manifest. Detect unsupported features (V2 pos-deletes /
            V3 DVs); error or skip per --keep-going.
            Mutate data_file.file_path and referenced_data_file.
            Write manifest at substitutePrefix(originalURI, src, dst).
            Capture newManifestLength.
            Update the manifest_list entry's manifest_path and manifest_length.
          Write the manifest_list at substitutePrefix(originalURI, src, dst).
     b. For statistics[] and partition-statistics[]: rewrite path in
        metadata.json. (File contents have no paths to rewrite for v1.)
  3. Mutate metadata.json:
       - location
       - snapshots[].manifest-list (already rewritten by step 2a)
       - metadata-log[].metadata-file
       - statistics[].statistics-path, partition-statistics[].statistics-path
       - properties[write.object-storage.path / write.data.path /
         write.metadata.path / write.folder-storage.path]
  4. Write the new metadata.json bytes at substitutePrefix(oldMetaLoc).
  5. Catalog swap:
       a. catalog.DropTable(purgeRequested=false)
       b. catalog.RegisterTable(newMetadataLocation)
       c. On register error: best-effort RegisterTable(oldMetadataLocation).
  6. Validate (unless --no-validate):
       a. LoadTable; assert current-snapshot-id matches expected.
       b. HEAD one data file URI from the latest snapshot's manifest.
       c. (Optional, future) DuckDB SELECT count(*).
```

## Example operational pattern

`bergrebase` is a stateless one-shot — it doesn't take catalog locks,
schedule the byte copy, or pause writers. The orchestrator wrapping it
owns those concerns. A typical sequence:

1. **Pause writers** to the table. Iceberg uses optimistic concurrency
   on `metadata.json` swaps, so a writer that commits during the rebase
   creates a snapshot pointing at the source bucket and is missed by
   the rewriter. How writers are paused is caller-specific: scale a
   data-loader Deployment to zero, flip a feature flag, take an
   application-level lock — whichever fits.
2. **Bulk byte copy** from source to target bucket. Any tool works
   (`rclone sync`, `aws s3 sync`, `mc mirror`, `s5cmd`); bergrebase
   only requires that the data and metadata bytes exist at the same
   keys under the new prefix.
3. **Run `bergrb`** with `--source-prefix` / `--target-prefix` and
   credentials for both sides. The engine walks the metadata graph
   bottom-up, rewrites paths, and atomically swaps the catalog
   pointer.
4. **Validate** (`bergrb` does a best-effort `LoadTable` + HEAD by
   default; the orchestrator can layer its own `SELECT count(*)` /
   checksum check on top).
5. **Resume writers** against the new bucket.

This pattern works equally well from a Kubernetes Job, a CI pipeline,
or a one-off shell session.

## Edge cases & gotchas

1. **Multiple snapshots / time travel**. Default: rewrite all snapshots in
   `metadata.json` and the last K entries of `metadata-log[]` (K =
   `write.metadata.previous-versions-max`, default 100). Flag
   `--current-snapshot-only` opts into "lose time travel" for speed.
2. **`metadata-log` chain pointing at GC'd files**. We don't fail if a
   referenced previous metadata.json no longer exists; we rewrite the URI
   string and trust that engines tolerate missing historical files
   (Spark/Trino do).
3. **Concurrent writers**. Iceberg uses optimistic concurrency on
   `metadata.json` swaps. A writer committing during the rebase would
   create a snapshot pointing at the OLD bucket, missed by us.
   **Mitigation**: writers paused; the orchestrator is the single source
   of writes during a migration; finalize is a positive confirmation.
4. **Avro byte length must update**. Rewriting strings inside an OCF block
   changes block size; the manifest list's `manifest_length` must reflect
   the new on-disk size or implementations will reject the read.
5. **Bound bytes**. Per D4 — never rewrite bytes inside
   `lower_bounds`/`upper_bounds`.
6. **Future-write hints in `properties`**. If we forget to rewrite
   `write.object-storage.path` etc., new writes after migration go to the
   old bucket. Easy to miss; explicit list above.
7. **Partial failure**. Deterministic-key idempotency makes re-running
   safe: a second run with the same prefix mapping is a no-op for
   already-rewritten files and a redo for the rest.
8. **Position-delete files (V2)** and **deletion vectors (V3)** — refused
   with a clear error in v1; tracked in [`roadmap.md`](./roadmap.md).
