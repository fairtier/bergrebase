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

// TestDuckDBOracle_CoW is the independent reader for the V2
// Copy-on-Write scenario. The fixture simulates an UPDATE
// that rewrites all rows: snap 1 ADDS file1 (ids 1..10), snap 2
// "rewrites" by ADDING file2 (ids 11..20) in m2, and snap 2's
// manifest list contains ONLY m2 — m1 is dropped. This is the standard
// CoW "rewrite" shape used by Spark and other Iceberg writers: the
// previous snapshot's manifests are not carried forward when the
// operation replaces them.
//
// The oracle on snap 2 must return rows from file2 only (count=10,
// sum=155 for ids 11..20). Any post-rebase result of (20, 210) would
// mean the engine accidentally re-introduced the dropped m1 into
// snap 2's list, or the metadata.json's current-snapshot-id pointer
// got corrupted.
//
// `summary.operation: overwrite` distinguishes this from append-only
// scenario 2 where each snapshot's list grows. m2's exclusion of m1
// is the load-bearing structural difference.
//
// V2 position-delete files and V3 deletion vectors — the other
// "deletes" mechanisms — are explicitly refused by RewriteManifest;
// status-based CoW (this test) is the supported path in v1.0.
func TestDuckDBOracle_CoW(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_oracle_cow"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	// Snapshot 1: m1 contains [file1 ADDED].
	const snap1ID, snap1Seq int64 = 100, 1
	file1Bytes, _ := harness.WriteDeterministicIDRangeParquet(t, 1, 10)
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
	addEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid1, dfb1.Build()).
		SequenceNum(snap1Seq).FileSequenceNum(snap1Seq).Build()

	manURI1 := srcURI("metadata/m1.avro")
	manBuf1 := &bytes.Buffer{}
	mf1, err := iceberg.WriteManifest(manURI1, manBuf1, 2, spec, schema, snap1ID, []iceberg.ManifestEntry{addEntry})
	if err != nil {
		t.Fatalf("WriteManifest m1: %v", err)
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

	// Snapshot 2 (CoW rewrite): m2 contains [file2 ADDED]. m1 is NOT
	// carried forward into snap 2's list — that's the structural marker
	// of CoW "overwrite" semantics. file1 remains physically present in
	// storage (byte-copy preserves it) but is not referenced by any
	// live manifest at snap 2.
	const snap2ID, snap2Seq int64 = 200, 2
	file2Bytes, file2Sum := harness.WriteDeterministicIDRangeParquet(t, 11, 20)
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
	mf2, err := iceberg.WriteManifest(manURI2, manBuf2, 2, spec, schema, snap2ID, []iceberg.ManifestEntry{add2Entry})
	if err != nil {
		t.Fatalf("WriteManifest m2: %v", err)
	}
	if err := store.PutObject(ctx, manURI2, manBuf2.Bytes()); err != nil {
		t.Fatalf("put m2: %v", err)
	}

	// Snap 2's list = [m2 only]. The CoW rewrite drops m1 from snap 2's
	// view of the table.
	listURI2 := srcURI("metadata/snap-200.avro")
	listBuf2 := &bytes.Buffer{}
	seq2 := snap2Seq
	if err := iceberg.WriteManifestList(2, listBuf2, snap2ID, nil, &seq2, 0, []iceberg.ManifestFile{mf2}); err != nil {
		t.Fatalf("WriteManifestList snap2: %v", err)
	}
	if err := store.PutObject(ctx, listURI2, listBuf2.Bytes()); err != nil {
		t.Fatalf("put snap2 list: %v", err)
	}

	metaURI := srcURI("metadata/v2.metadata.json")
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000000000,
  "last-column-id": 1,
  "schemas": [{"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
  "current-schema-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "default-spec-id": 0,
  "last-partition-id": 999,
  "properties": {},
  "current-snapshot-id": %d,
  "snapshots": [
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000000000, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0},
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000001000, "parent-snapshot-id": %d, "manifest-list": %q, "summary": {"operation": "overwrite"}, "schema-id": 0}
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

	// Pre-rebase oracle: snap 2 sees only m2 → file2. Expected: ids 11..20.
	wantCount := int64(10)
	wantSum := file2Sum
	pre := harness.QueryIcebergScan(t, mi, metaURI)
	if pre.Count != wantCount || pre.SumID != wantSum {
		t.Fatalf("pre-rebase oracle on CoW: got %+v, want count=%d sum=%d (snap 2's manifest list might still reference file1's manifest)",
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
		t.Fatalf("CoW oracle drift: pre=%+v post=%+v (snap 2's manifest list rewrite might have re-introduced m1 or pointed at the source bucket)", pre, post)
	}
}
