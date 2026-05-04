# Migration recipes

Operational recipes for the four scenarios `bergrebase` users hit in
practice. Pick by what you're trying to do:

| You want to...                                               | Recipe                                                  | bergrebase needed? |
|--------------------------------------------------------------|---------------------------------------------------------|--------------------|
| Move a table's bytes to a new bucket, same catalog instance  | [§1](#1-same-catalog-new-bucket)                        | yes                |
| Switch catalogs (Lakekeeper ↔ Polaris) with data staying put | [§2](#2-catalog-swap-same-bucket--no-bergrebase-needed) | **no**             |
| Switch catalogs *and* move bytes                             | [§3](#3-catalog-swap--new-bucket)                       | yes                |
| Make a separate copy of a catalog (DR, staging, rehearsal)   | [§4](#4-disaster-recovery--staging-clone)               | yes                |

These are runbooks. For *why* the steps work see
[`design.md`](./design.md); for the engine's known limits see
[`roadmap.md`](./roadmap.md).

## Common preconditions

All four recipes assume:

- **Writers are paused** for the migration window. `bergrebase` doesn't
  take catalog locks; a writer committing during a rebase creates a
  snapshot pointing at the old bucket which the rewriter never sees.
- **Both buckets are reachable** from the host running `bergrb`, with
  S3 credentials exported as `SOURCE_AWS_ACCESS_KEY_ID` /
  `SOURCE_AWS_SECRET_ACCESS_KEY` and `TARGET_AWS_*`. `bergrebase` talks
  to S3 directly — it does not use catalog-vended credentials.
- **Tables are V1, V2 (any delete shape), or V3 without deletion
  vectors.** V3 DV tables are refused with `ErrUnsupportedFeature`
  until the Puffin rewrite lands ([`puffin.md`](./puffin.md)). Inventory
  tables before you start; `--keep-going` only demotes per-table
  failures to logs.
- **Target catalog supports atomic `register?overwrite=true`** (recent
  Lakekeeper, current Polaris). Older Lakekeeper falls back to
  `DropTable(purge=false)` + `RegisterTable`, which has a documented
  process-death window — see [`design.md` D2](./design.md).

## 1. Same catalog, new bucket

The canonical use case: the catalog instance stays put, only the
storage location changes. This is what `bergrebase` was designed for.

### Procedure

1. Pause writers.
2. `rclone copy s3-source:bucket s3-target:bucket` (or equivalent).
   Keys must be preserved 1:1 — `bergrebase` rewrites under a strict
   prefix substitution and does not handle key remapping.
3. **Update the catalog's storage configuration** to point at the new
   bucket (variant-specific, see below). Order vs. step 4 doesn't
   matter — `bergrebase` authenticates to S3 directly — but both must
   be done before clients come back online.
4. Run `bergrb` with the prefix mapping.
5. Validate (default; skip with `--no-validate`).
6. Resume writers; decommission the source bucket once you're satisfied.

```bash
bergrb \
    --catalog-uri https://catalog.example.com/catalog \
    --catalog-warehouse my-warehouse \
    --catalog-token-from-env CATALOG_TOKEN \
    --source-prefix s3://old-bucket/iceberg/ \
    --target-prefix s3://new-bucket/iceberg/ \
    --target-endpoint https://<account>.r2.cloudflarestorage.com \
    --target-region auto \
    --all-tables
```

Re-running with the same flags is a no-op for tables already rebased
(idempotent on the prefix check), so SIGKILL recovery is "run it
again."

### Lakekeeper variant

Lakekeeper stores a **storage profile** per warehouse: bucket,
endpoint, region, path-style flag, plus a reference to a
**storage-credential** resource. After the byte copy, update the
profile to point at the new bucket and the credential to one that
grants access on the new bucket. Use Lakekeeper's management API
(`/management/v1/warehouse/...` — exact path depends on your
Lakekeeper version, consult its OpenAPI spec) or update the
`warehouse` row in Postgres directly if you control the database.

The `storage-credential` referenced by the warehouse is a separate
resource. Either rotate the credential value to grant the new bucket
(simplest if you control IAM), or create a new credential and rebind
the warehouse to it.

### Polaris variant

Polaris stores a **storage configuration** on each catalog
(`storage-config-info`): type (`S3` / `GCS` / `AZURE`), role ARN,
external ID, `allowed-locations`, region. For S3, credentials are
*vended* via STS `AssumeRole` against the configured role.

Two changes before `bergrb` runs:

1. **`allowed-locations`** must include the new bucket prefix.
   During cutover, include both old and new locations; trim the old
   entry after validation. Update via Polaris's management API
   (`/api/management/v1/catalogs/{catalogName}` — consult your
   Polaris version's spec for exact shape).
2. **IAM role policy** must permit the role to `GetObject` /
   `PutObject` / `ListBucket` on the new bucket. The trust policy
   (which principals can assume the role) does not change.

For non-AWS S3-compatibles (R2, MinIO, B2, etc.) Polaris uses static
credentials in the storage config rather than `AssumeRole`; replace
those with credentials valid for the new bucket.

The `bergrb` invocation is identical to the Lakekeeper case.

## 2. Catalog swap, same bucket — no bergrebase needed

**Stop.** If you're switching from Lakekeeper to Polaris (or
vice-versa) and your data files aren't moving, `bergrebase` is the
wrong tool. There are no URIs to rewrite — every `metadata.json` in
the existing bucket already references the existing bucket
correctly. Iceberg's REST `register` endpoint is exactly the
operation you need.

### Why bergrebase isn't the answer here

`bergrebase`'s job is rewriting absolute URIs inside metadata files.
When the bucket doesn't change, every URI is already correct.
Running `bergrebase` would attempt to rewrite paths from
`s3://bucket/...` to the same `s3://bucket/...` — the prefix-match
guard short-circuits this as a no-op, but you've still spent the
operational complexity for nothing. Use the catalog's REST API
directly.

### Procedure

1. **Pause writes** on the source catalog.
2. **Stand up the target catalog**, configured with a storage
   profile (Lakekeeper) / storage-config (Polaris) pointing at the
   *existing* bucket and using the same credentials/IAM role the
   source catalog uses.
3. **Recreate scaffolding manually**: namespaces, principals, roles,
   grants. **Lakekeeper's auth model and Polaris's
   catalog-roles + principal-roles + grants do not translate
   automatically.** Plan for this as a real engineering task, not a
   script. There is no tool that does it.
4. **For each table, fetch its current `metadata-location`** from
   the source catalog (`GET /v1/{prefix}/namespaces/{ns}/tables/{name}`)
   and register it in the target:

   ```http
   POST /v1/{prefix}/namespaces/{ns}/register
   Content-Type: application/json

   {
     "name": "{table-name}",
     "metadata-location": "s3://existing-bucket/.../v123.metadata.json"
   }
   ```
5. **Cut clients over** to the target catalog endpoint.
6. **Decommission the source catalog** once clients are stable.

### What's preserved vs. lost

Preserved (lives in `metadata.json`, untouched by this procedure):
Iceberg `table-uuid`, schema/partition-spec/sort-order history, all
snapshots, snapshot summaries, statistics paths, time-travel history.

Lost or re-mapped: catalog-internal table UUIDs (each catalog
assigns its own), audit trail, ACL/grant mappings, and any
catalog-implementation-specific state (Lakekeeper soft-delete,
Polaris realm metadata, Nessie branches).

### A scripted version

A short loop with `iceberg-go`'s catalog client, PyIceberg, or
`curl` is enough: list namespaces, list tables per namespace, fetch
each `metadata-location`, POST to register. There's no
`bergrebase`-shaped tool here because the operation is small enough
that codifying it isn't worth a binary.

## 3. Catalog swap + new bucket

The combined case: switching catalogs *and* moving data. Two recipes
layered. The non-obvious step is the ordering — register tables in
the target catalog at their **old-bucket** pointers first, then let
`bergrebase` rewrite them.

### Procedure

1. Pause writes.
2. `rclone` source bucket → target bucket.
3. **Stand up target catalog**, storage configuration pointing at
   the **new** bucket. `allowed-locations` (Polaris) or storage
   profile (Lakekeeper) covers both buckets during cutover; trim
   afterwards.
4. **Recreate namespaces** in the target catalog. If the migration
   is operator-led (e.g. a SaaS provider doing a hand-off), grants /
   principals / roles in the target catalog are typically the
   *customer's* responsibility, scoped before or after the migration
   window — the operator running `bergrebase` does not need to
   populate the customer's auth model. If a single party owns both
   catalogs, recreate grants here too; same caveat as §2 applies
   (Lakekeeper ↔ Polaris ACLs don't translate automatically).
5. **For each source table, register it in the target catalog at
   its old-bucket `metadata-location`**. Yes, intentionally — the
   metadata.json contents under the new bucket still reference the
   old bucket too, and `bergrebase` will fix both in step 6.
6. Run `bergrb` against the **target** catalog with
   `--source-prefix s3://old-bucket/...`
   `--target-prefix s3://new-bucket/...`. It loads each table from
   the target catalog (sees the old-bucket pointer), reads metadata
   from the source bucket, rewrites it into the target bucket, and
   swaps the catalog pointer.
7. Validate. Trim cutover-only entries from `allowed-locations`.
   Cut clients over.

### Why register old pointers first

`bergrebase` discovers work via the catalog (`ListTables` →
`LoadTable`). Registering at the new-bucket pointer would require
the new-bucket `metadata.json` to already be self-consistent, which
it isn't after a plain `rclone` — its internal URIs still reference
the old bucket. Registering at the old-bucket pointer lets
`bergrebase` do its normal job: read old, rewrite, swap.

### Permissions required on the target catalog

`bergrebase` does **not** need server-level admin on the target
catalog. The operator running steps 5–6 needs only **table-scoped
CRUD on the namespaces being migrated**:

| Step                                            | Catalog operation                                                        | Permission                           |
|-------------------------------------------------|--------------------------------------------------------------------------|--------------------------------------|
| §3 step 5 (pre-`bergrb` register)               | `POST .../namespaces/{ns}/register` at old-bucket pointer                | create-table on namespace            |
| `bergrb` `ListTables`                           | `GET .../namespaces/{ns}/tables`                                         | list-tables on namespace             |
| `bergrb` `LoadTable`                            | `GET .../tables/{name}`                                                  | read-table                           |
| `bergrb` `SwapMetadataLocation` (atomic path)   | `POST .../register?overwrite=true`                                       | create-table-with-overwrite          |
| `bergrb` `SwapMetadataLocation` (fallback path) | `DELETE .../tables/{name}?purgeRequested=false` then `POST .../register` | drop-table (no purge) + create-table |

Four permission verbs total: `list-tables`, `read-table`,
`create-table`, `drop-table-without-purge`. Bounded by namespace and
bounded by time.

What's *not* needed:

- Server-level or catalog-level admin
- Manage-roles / manage-principals / manage-grants on the target
- Access to namespaces or catalogs outside the migration scope
- Anything outside the migration window — credentials should be
  short-lived

#### Lakekeeper

A role with table-CRUD permissions bound to the target namespaces,
granted to a service principal whose credentials the operator
holds. Customer creates the role, scopes it, hands over credentials,
revokes after the migration completes. No `server_admin`
involvement.

#### Polaris

A `CATALOG_ROLE` with `TABLE_CREATE`, `TABLE_READ_PROPERTIES`,
`TABLE_LIST`, `TABLE_DROP` predicates, scoped to the catalog (or to
specific namespaces — Polaris supports namespace granularity).
Granted to a `PRINCIPAL_ROLE` assigned to a `PRINCIPAL` whose client
credentials the operator holds. No `service_admin` or
catalog-manage-access grants.

In both cases the grant is **bounded by namespace and bounded by
time**: a precisely-scoped migration role created for the window,
revoked after. This matters for SaaS hand-offs where the operator
and the customer are different organizations — the customer should
not be asked for, and the operator should not request, broader
access than these four verbs.

### Cleanup

Decommission the source bucket and source catalog after validation.
Order doesn't matter as long as clients have already moved.

## 4. Disaster-recovery / staging clone

Variant of §1 where the goal is a *separate* catalog instance — for
DR readiness, staging refreshes, or migration rehearsal — not a
single one-way move.

### Procedure (Lakekeeper)

1. Pause writers (or accept staleness equal to the time between
   step 1 and step 5).
2. `pg_dump` the source Lakekeeper Postgres → restore into the
   target Postgres.
3. `rclone` source bucket → target bucket.
4. **Update the warehouse storage profile** in the target Lakekeeper
   to point at the new bucket, new endpoint, and new credentials.
   The dump carried the storage profile *as it was on the source* —
   it points at the source bucket and source credentials and must be
   rewritten before the target instance is usable.
5. Run `bergrb` against the target Lakekeeper with the prefix
   mapping.
6. Validate.

### Procedure (Polaris)

Same shape with the variants from §1 Polaris: update the catalog's
`storage-config-info` (role ARN, `allowed-locations`) on the target
side, then run `bergrebase`.

If the source Polaris uses in-memory persistence there is no
metastore to dump — recreate scaffolding by hand and use the §3
recipe instead.

### Honest limits

- **This is a snapshot, not continuous replication.** Drift begins
  the moment step 5 finishes. For ongoing DR, repeat the procedure
  on a schedule and accept the RPO of your interval. There is no
  incremental mode.
- **Each run rewrites every metadata file referenced by current
  refs.** Idempotent for unchanged files (re-uploads bytes equal to
  what's already there), but not free for large tables with many
  snapshots. `--current-snapshot-only` reduces work at the cost of
  losing time-travel on the clone.
- **Schema-version skew.** A `pg_dump` from `vX.Y` restored into
  `vX.Z` may need a schema migration on first start. Match versions
  or rehearse the upgrade.
- **Auth backend cloning is separate.** OpenFGA tuples (Lakekeeper)
  or external IdP role mappings live outside Postgres — replicate
  separately if relevant to your access model.
- **Secret references aren't credentials.** A `storage-credential`
  row pointing at a Vault path or K8s Secret name is carried by the
  dump as a *reference*, not a value. The referenced secret must
  exist on the target side or the catalog can't vend credentials.

### When *not* to use this

If you only need a read-only mirror with the same data lifecycle as
production, a simpler answer is: a second catalog instance pointed
at the *same* S3 bucket. No `rclone`, no `bergrebase`, no drift —
but no isolation either, since a write on the source is immediately
visible on the mirror because they share storage. Use this only when
"isolation" isn't a requirement.
