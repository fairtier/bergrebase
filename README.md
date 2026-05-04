# bergrebase

Rebase an Apache Iceberg table onto a new object-storage backing.

`bergrebase` (binary: `bergrb`) is a small standalone tool that moves an
Iceberg table's data and metadata from one S3-compatible bucket to another
and re-points the catalog at the new location. The table identity, history,
and schema are unchanged — like `git rebase`, the table keeps its commits but
sits on a new base.

## Why

Iceberg metadata stores **absolute URIs** for every data file, manifest,
manifest list, statistics file, and historical metadata pointer. If you copy
the bytes from `s3://old/...` to `s3://new/...` and just swap the catalog,
every metadata read still points at the old bucket — reads, writes, and time
travel all break.

The closest existing options are:

- Apache Iceberg's Spark `RewriteTablePath` action — requires Spark.
- Internal Python scripts at various vendors — not packaged.
- [`apache/iceberg-python#2014`](https://github.com/apache/iceberg-python/issues/2014),
  [`trinodb/trino#25499`](https://github.com/trinodb/trino/issues/25499) —
  open requests.

`bergrebase` fills that gap. Single static Go binary, Apache-2.0.

## Coverage

### Iceberg versions

| Version                         | Read | Rewrite | Status                                                                 |
|---------------------------------|------|---------|------------------------------------------------------------------------|
| V1                              | ✅    | ✅       | supported                                                              |
| V2 — append-only                | ✅    | ✅       | supported                                                              |
| V2 — with position-delete files | ✅    | ✅       | supported (file_path column rewritten in-place)                        |
| V2 — with equality-delete files | ✅    | ✅       | supported (file body has no embedded URIs; byte-copy is sufficient)    |
| V3 — without deletion vectors   | ✅    | ✅       | supported                                                              |
| V3 — with deletion vectors      | ✅    | ⛔       | refused with `ErrUnsupportedFeature`; deferred (Puffin footer rewrite) |

V3 deletion vectors live in Puffin files; rewriting them needs a Go
Puffin reader/writer (work item; vendored locally for an upcoming
release). bergrebase detects them up front and aborts the table with a
clear error rather than producing a half-rebased table.

### Catalogs

| Catalog                                 | Status                      |
|-----------------------------------------|-----------------------------|
| Lakekeeper (REST)                       | ✅ supported                 |
| Apache Polaris (REST)                   | ✅ supported                 |
| Tabular / Snowflake Open Catalog (REST) | ✅ supported                 |
| AWS Glue REST shim                      | ✅ supported                 |
| Nessie (REST)                           | best-effort, untested in CI |
| Hive Metastore (Thrift)                 | 🚧 v1.2 — native client     |
| AWS Glue (native)                       | 🚧 v1.2                     |
| JDBC catalog                            | 🚧 v1.2                     |

The REST client auto-detects whether the server supports atomic
`register?overwrite=true`; on older servers it falls back to
`DropTable(purge=false)` + `RegisterTable`.

### Storage

Anything S3-compatible via `aws-sdk-go-v2` with endpoint override: AWS S3,
Cloudflare R2, MinIO, Backblaze B2, Wasabi, DigitalOcean Spaces, Linode
Object Storage, Scaleway, OVHcloud. GCS and Azure Blob are out of scope for
v1.0.

## Usage

```
bergrb \
    --catalog-uri https://lakekeeper.example.com/catalog \
    --catalog-warehouse my-warehouse \
    --catalog-token-from-env LAKEKEEPER_TOKEN \
    --source-prefix s3://old-bucket/iceberg/ \
    --target-prefix s3://new-bucket/iceberg/ \
    --target-endpoint https://<account>.r2.cloudflarestorage.com \
    --target-region auto \
    --namespace analytics \
    --all-tables \
    --dry-run
```

Other flags worth knowing: `--table` (single-table instead of
`--all-tables`), `--keep-going` (continue past per-table failures rather
than aborting the run), `--current-snapshot-only` (skip historical
snapshots — faster, but breaks time travel), `--no-validate` (skip the
post-migration check), `--source-path-style` / `--target-path-style` (for
MinIO and other servers that need path-style addressing), and matching
`--source-endpoint` / `--source-region` for source-side overrides.

Credentials for object storage are read from environment variables, with
`SOURCE_*` and `TARGET_*` prefixes:

```
SOURCE_AWS_ACCESS_KEY_ID, SOURCE_AWS_SECRET_ACCESS_KEY
TARGET_AWS_ACCESS_KEY_ID, TARGET_AWS_SECRET_ACCESS_KEY
```

For full operational recipes — same-catalog bucket move, catalog swap
with data in place (where you don't need `bergrebase`), catalog swap
with bucket move, and disaster-recovery clone — see
[`docs/migration-recipes.md`](./docs/migration-recipes.md).

## How it works

### The problem in one paragraph

Iceberg metadata embeds absolute URIs at every layer: data file paths
inside manifests, manifest paths inside manifest lists, snapshot paths and
metadata-log entries and statistics-file paths inside `metadata.json`, and
future-write hints inside `properties["write.*.path"]`. A bulk byte copy
preserves none of those references. To move a table to a new bucket you
have to rewrite all of them, in the right order, without touching anything
that *isn't* a path.

### Algorithm

1. **Load the table** from the catalog and resolve the current
   `metadata.json` URI. If it already begins with the target prefix, the
   table is already rebased — exit cleanly (idempotent).
2. **Walk the metadata graph bottom-up**: for each snapshot, read its
   manifest list, then each manifest it points to, then the data file
   entries inside each manifest. Path-bearing fields are mutated in
   memory.
3. **Rewrite paths via strict prefix substitution.** Only strings that
   begin with `--source-prefix` are rewritten, and only on the documented
   path-bearing fields. Nothing else is touched.
4. **Write rewritten manifests first**, then update each manifest list
   entry's `manifest_length` (Avro field id 501) to match the actual
   on-disk byte length of the new manifest. Mismatches are rejected by
   readers, so this has to be exact.
5. **Write rewritten manifest lists**, then rewrite `metadata.json` with
   updated `location`, `snapshots[].manifest-list`, `metadata-log[]`,
   `statistics`, `partition-statistics`, and the `write.*.path` property
   keys.
6. **Every file is written at the same key it had in the source bucket**
   (just under the new prefix). Re-running the same command is a no-op for
   files already present, which means partial runs and SIGKILLs resume
   cleanly.
7. **Atomic catalog swap.** Prefer `register?overwrite=true`; fall back to
   `DropTable(purge=false)` + `RegisterTable` on older catalogs. On
   register failure, roll the catalog pointer back to the old metadata
   location so reads keep working.
8. **Validate.** `LoadTable` post-swap, confirm the snapshot ID matches,
   and `HEAD` one data file URI from the latest manifest to confirm the
   target bucket is reachable. `--no-validate` skips this.

### Footguns we avoid

- **No global string replace.** A `strings.ReplaceAll` over the source
  bucket name would corrupt user data — Avro `lower_bounds` and
  `upper_bounds` are raw bytes, and a column whose values happen to start
  with the bucket name would be silently mangled. Only documented
  path-bearing fields are touched.
- **Future-write hints are rewritten too.** The properties
  `write.object-storage.path`, `write.data.path`, `write.metadata.path`,
  and `write.folder-storage.path` tell engines where to put new files.
  Skip them and the next write after migration lands in the old bucket.
- **Bottom-up order is required.** `manifest_length` references the
  rewritten manifest's actual byte size, so the inner file has to exist
  before the outer file can record its length.

### Validation is best-effort

The post-rebase check confirms the catalog now serves the new
`metadata.json` (matching snapshot ID) and that one data file URI in the
latest manifest is reachable in the target bucket. It does **not** re-read
every data file footer or compare per-column statistics. Full
correctness verification belongs to your data pipeline (e.g.
`SELECT count(*)` and checksum comparisons against the pre-rebase table)
and to the test harness in this repo, which exercises that against
DuckDB.

### Concurrent writers must be paused

Writers committing during a rebase create new snapshots that point at the
old bucket and are missed by the rewriter. Pausing writers is the
caller's responsibility — bergrebase doesn't take catalog locks.

## Implemented in v1.0

- Rewrite engine for V1, V2 (append-only, position-deletes,
  equality-deletes), and V3 without deletion vectors.
- V2 position-delete Parquet bodies rewritten in-place
  (`file_path` column substituted via `arrow-go`); equality-delete
  bodies pass through unchanged because they hold no embedded URIs.
- REST catalog client with atomic `register?overwrite=true` and a
  `DropTable + RegisterTable` fallback. Auto-detects which the server
  supports; rolls back on register failure.
- S3-compatible storage via `aws-sdk-go-v2` with endpoint, region, and
  path-style overrides (works against AWS S3, R2, MinIO, etc.).
- CLI with `--dry-run`, `--all-tables` / `--table`, `--keep-going`,
  `--current-snapshot-only`, `--no-validate`, and matching
  `--source-*` / `--target-*` storage flags.
- Explicit refusal (`ErrUnsupportedFeature`) for V3 deletion vectors
  — never produces a half-rebased table.
- Testcontainers harness covering merge-gate scenarios across
  Lakekeeper + Polaris + MinIO + DuckDB.

## Deferred to v1.1

- **V3 deletion-vector rewrite** — Puffin footer rewrite. Blocked on
  there being no Go Puffin library; vendored locally, upstreaming in
  progress.
- **Iceberg view migration** — separate metadata shape, not handled by
  the table-rewrite path.

## Planned for v1.2

- Native Hive Metastore (Thrift) catalog client.
- Native AWS Glue catalog client.
- JDBC catalog client.
- Nessie compatibility test (REST should work today; not yet covered in
  CI).

## Known limits

- `iceberg-go` is at v0.x; minor-version bumps may require coordinated
  updates here.
- Lakekeeper register-with-overwrite auto-detection is heuristic
  (capability advertisement varies across server versions).
- A SIGKILL between `DropTable` and `RegisterTable` on the fallback path
  leaves the catalog with no entry for the table. The data files and
  rewritten `metadata.json` survive intact in the target bucket; manual
  re-registration is required. The atomic-swap path is unaffected.

## Building

```bash
go build -o bergrb ./cmd/bergrb
```

A multi-stage Dockerfile produces a static distroless image:

```bash
docker build -t bergrebase:dev .
```

Go 1.26. The Docker build uses `CGO_ENABLED=0`, `GOAMD64=v3`, and a
distroless nonroot final stage.

## Running on Kubernetes

See [`k8s/README.md`](./k8s/README.md) for running `bergrb` as a
`batch/v1` Job.

## License

Apache 2.0. See [`LICENSE`](./LICENSE).
