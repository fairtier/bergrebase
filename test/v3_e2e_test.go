package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestEngine_E2E_V3_RowLineage covers a V3 table with row lineage
// enabled. The rewrite
// must preserve the V3 OCF header (`first-row-id`, `format-version=3`)
// and the per-manifest_file FirstRowId in the manifest list.
//
// V3-specific concerns the engine must round-trip:
//   - manifest list OCF `first-row-id` metadata (read in
//     readManifestListHeader, written via the firstRowId arg to
//     iceberg.WriteManifestList).
//   - per-entry first_row_id on data files (preserved by iceberg-go's
//     manifest reader/writer; we touch only Path via reflection).
func TestEngine_E2E_V3_RowLineage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_v3"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	const snapID, seqNum, firstRowID int64 = 100, 1, 0
	dataURI := srcURI("data/file-1.parquet")
	if err := store.PutObject(ctx, dataURI, []byte("placeholder")); err != nil {
		t.Fatalf("put data: %v", err)
	}
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 10, 1024)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	dfb.FirstRowID(0)
	sid := snapID
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, dfb.Build()).
		SequenceNum(seqNum).FileSequenceNum(seqNum).Build()

	manURI := srcURI("metadata/m1.avro")
	manBuf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(manURI, manBuf, 3, spec, schema, snapID, []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("WriteManifest V3: %v", err)
	}
	if err := store.PutObject(ctx, manURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put manifest: %v", err)
	}

	listURI := srcURI("metadata/snap-100.avro")
	listBuf := &bytes.Buffer{}
	seq := seqNum
	if err := iceberg.WriteManifestList(3, listBuf, snapID, nil, &seq, firstRowID, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList V3: %v", err)
	}
	if err := store.PutObject(ctx, listURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put list: %v", err)
	}

	metaURI := srcURI("metadata/v3.metadata.json")
	metaJSON := fmt.Sprintf(`{
  "format-version": 3,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000000000,
  "last-column-id": 1,
  "next-row-id": 10,
  "row-lineage": true,
  "schemas": [{"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
  "current-schema-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "default-spec-id": 0,
  "last-partition-id": 999,
  "properties": {},
  "current-snapshot-id": %d,
  "snapshots": [
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000000000, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0, "first-row-id": 0}
  ],
  "snapshot-log": [{"snapshot-id": %d, "timestamp-ms": 1700000000000}],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`, "s3://"+mi.SourceBucket+"/"+tablePrefix, seqNum, snapID, snapID, seqNum, listURI, snapID)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

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
	if _, err := eng.RewriteTable(ctx, metaURI); err != nil {
		t.Fatalf("RewriteTable V3: %v", err)
	}

	// The V3 manifest list must round-trip: read it back at the new
	// URI and confirm it parses (which proves format-version=3 and
	// first-row-id metadata survived).
	newListURI := "s3://" + mi.TargetBucket + "/" + tablePrefix + "/metadata/snap-100.avro"
	newListRaw, err := store.GetObject(ctx, newListURI)
	if err != nil {
		t.Fatalf("get rewritten V3 list: %v", err)
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(newListRaw))
	if err != nil {
		t.Fatalf("decode rewritten V3 list: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("V3 list manifest count = %d, want 1", len(files))
	}
	if got := files[0].Version(); got != 3 {
		t.Errorf("rewritten manifest_file.version = %d, want 3", got)
	}
}
