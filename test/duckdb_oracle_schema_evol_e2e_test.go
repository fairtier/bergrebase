package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestDuckDBOracle_SchemaEvolution is the independent reader for the
// V2 schema-evolution scenario. Two snapshots:
//
//   - schema 0: (id int64 fid=1).
//
//   - schema 1: (id int64 fid=1, name string fid=2). ADD COLUMN name.
//
//   - snap 1 (schema-id=0): m1 contains file1 (10 rows, ids 1..10) — only
//     the id column.
//
//   - snap 2 (schema-id=1): m2 contains file2 (10 rows, ids 11..20, name
//     "name-<id>"). m1 carry-forwarded into snap 2's list.
//
// DuckDB reads the table through the *current* schema (1):
//   - file1's rows project as (id, NULL for name) — name field-id=2 is
//     not present in file1's Parquet, so the reader fills NULL.
//   - file2's rows project as (id, name).
//   - count=20, sum(id)=210.
//
// What this test catches that the existing engine tests don't:
//   - Whether per-manifest schema (encoded in OCF metadata) round-trips
//     through RewriteManifest. If the engine substitutes the wrong
//     schema-id during re-encode, file1's rows would lose their id
//     mapping and the oracle would either fail to read or return wrong
//     sums.
//   - Whether last-column-id, schemas[], and per-snapshot schema-id
//     references in metadata.json are preserved by RewriteMetadataJSON
//     (these are not path-bearing, so they should pass through
//     untouched — but a buggy JSON pass-through would silently drop
//     them).
func TestDuckDBOracle_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_oracle_evol"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	// Schema 0: just id.
	schema0 := iceberg.NewSchemaWithIdentifiers(0, nil, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	// Schema 1: id, name (ADD COLUMN name).
	schema1 := iceberg.NewSchemaWithIdentifiers(1, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := *iceberg.UnpartitionedSpec

	// Snap 1 (schema-id=0): m1 = [file1 ADDED]. file1 has only id.
	const snap1ID, snap1Seq int64 = 100, 1
	file1Bytes, file1Sum := harness.WriteDeterministicIDRangeParquet(t, 1, 10)
	file1URI := srcURI("data/file-1.parquet")
	if err := store.PutObject(ctx, file1URI, file1Bytes); err != nil {
		t.Fatalf("put file1: %v", err)
	}
	dfb1, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, file1URI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 10, int64(len(file1Bytes)))
	if err != nil {
		t.Fatalf("DataFileBuilder file1: %v", err)
	}
	sid1 := snap1ID
	add1Entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid1, dfb1.Build()).
		SequenceNum(snap1Seq).FileSequenceNum(snap1Seq).Build()

	manURI1 := srcURI("metadata/m1.avro")
	manBuf1 := &bytes.Buffer{}
	mf1, err := iceberg.WriteManifest(manURI1, manBuf1, 2, spec, schema0, snap1ID, []iceberg.ManifestEntry{add1Entry})
	if err != nil {
		t.Fatalf("WriteManifest m1 (schema 0): %v", err)
	}
	if err := store.PutObject(ctx, manURI1, manBuf1.Bytes()); err != nil {
		t.Fatalf("put m1: %v", err)
	}

	listURI1 := srcURI("metadata/snap-100.avro")
	listBuf1 := &bytes.Buffer{}
	seq1 := snap1Seq
	if err := iceberg.WriteManifestList(2, listBuf1, snap1ID, nil, &seq1, 0, []iceberg.ManifestFile{mf1}); err != nil {
		t.Fatalf("WriteManifestList snap1: %v", err)
	}
	if err := store.PutObject(ctx, listURI1, listBuf1.Bytes()); err != nil {
		t.Fatalf("put snap1 list: %v", err)
	}

	// Snap 2 (schema-id=1, after ADD COLUMN): m2 = [file2 ADDED].
	// file2 has both id and name columns.
	const snap2ID, snap2Seq int64 = 200, 2
	file2Bytes, file2Sum := harness.WriteDeterministicEvolvedParquet(t, 11, 20)
	file2URI := srcURI("data/file-2.parquet")
	if err := store.PutObject(ctx, file2URI, file2Bytes); err != nil {
		t.Fatalf("put file2: %v", err)
	}
	dfb2, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, file2URI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 10, int64(len(file2Bytes)))
	if err != nil {
		t.Fatalf("DataFileBuilder file2: %v", err)
	}
	sid2 := snap2ID
	add2Entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid2, dfb2.Build()).
		SequenceNum(snap2Seq).FileSequenceNum(snap2Seq).Build()

	manURI2 := srcURI("metadata/m2.avro")
	manBuf2 := &bytes.Buffer{}
	mf2, err := iceberg.WriteManifest(manURI2, manBuf2, 2, spec, schema1, snap2ID, []iceberg.ManifestEntry{add2Entry})
	if err != nil {
		t.Fatalf("WriteManifest m2 (schema 1): %v", err)
	}
	if err := store.PutObject(ctx, manURI2, manBuf2.Bytes()); err != nil {
		t.Fatalf("put m2: %v", err)
	}

	// Snap 2 list = [m1 carry-forward, m2 fresh].
	listURI2 := srcURI("metadata/snap-200.avro")
	listBuf2 := &bytes.Buffer{}
	seq2 := snap2Seq
	if err := iceberg.WriteManifestList(2, listBuf2, snap2ID, nil, &seq2, 0, []iceberg.ManifestFile{
		harness.CarryForwardManifest(mf1, snap1Seq),
		mf2,
	}); err != nil {
		t.Fatalf("WriteManifestList snap2: %v", err)
	}
	if err := store.PutObject(ctx, listURI2, listBuf2.Bytes()); err != nil {
		t.Fatalf("put snap2 list: %v", err)
	}

	// metadata.json carries both schemas, current-schema-id = 1.
	// last-column-id = 2 (highest assigned field id after ADD COLUMN).
	metaURI := srcURI("metadata/v2.metadata.json")
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000000000,
  "last-column-id": 2,
  "schemas": [
    {"schema-id": 0, "type": "struct", "fields": [
      {"id": 1, "name": "id", "required": true, "type": "long"}
    ]},
    {"schema-id": 1, "type": "struct", "fields": [
      {"id": 1, "name": "id", "required": true, "type": "long"},
      {"id": 2, "name": "name", "required": false, "type": "string"}
    ]}
  ],
  "current-schema-id": 1,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "default-spec-id": 0,
  "last-partition-id": 999,
  "properties": {},
  "current-snapshot-id": %d,
  "snapshots": [
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000000000, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0},
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000001000, "parent-snapshot-id": %d, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 1}
  ],
  "snapshot-log": [
    {"snapshot-id": %d, "timestamp-ms": 1700000000000},
    {"snapshot-id": %d, "timestamp-ms": 1700000001000}
  ],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`,
		"s3://"+mi.SourceBucket+"/"+tablePrefix,
		snap2Seq,
		snap2ID,
		snap1ID, snap1Seq, listURI1,
		snap2ID, snap2Seq, snap1ID, listURI2,
		snap1ID, snap2ID,
	)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

	wantCount := int64(20)
	wantSum := file1Sum + file2Sum
	pre := harness.QueryIcebergScan(t, mi, metaURI)
	if pre.Count != wantCount || pre.SumID != wantSum {
		t.Fatalf("pre-rebase oracle: got %+v, want count=%d sum=%d (schema evolution wiring likely off — file1 may not be projecting id under schema 1)",
			pre, wantCount, wantSum)
	}

	keysToCopy := []string{
		strings.TrimPrefix(file1URI, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(file2URI, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(manURI1, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(manURI2, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(listURI1, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(listURI2, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(metaURI, "s3://"+mi.SourceBucket+"/"),
	}
	harness.CopyBucketToBucket(ctx, t, store, keysToCopy, mi.SourceBucket, mi.TargetBucket)

	eng := &rewrite.Engine{
		Source: store, Target: store,
		Opts: rewrite.Options{
			Mapping: rewrite.PrefixMapping{
				Source: "s3://" + mi.SourceBucket + "/",
				Target: "s3://" + mi.TargetBucket + "/",
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	res, err := eng.RewriteTable(ctx, metaURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	post := harness.QueryIcebergScan(t, mi, res.NewMetadataLocation)
	if post != pre {
		t.Fatalf("schema-evolution oracle drift: pre=%+v post=%+v (schemas[] or per-snapshot schema-id references likely lost)", pre, post)
	}
}
