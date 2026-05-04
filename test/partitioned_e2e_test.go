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

// TestEngine_E2E_Partitioned covers a partitioned V2 table.
//
// The risk this test guards against is specific to our reflection-
// based DataFile.Path mutation: setDataFilePath touches one field on
// an unexported struct, but a typo in the field name (or an iceberg-go
// rename that adds new path-bearing fields) could clobber sibling
// state. Partition values live next to the file path on the same
// struct, so verifying they survive the round-trip is the cheapest
// way to assert "we only mutate what we mean to mutate".
//
// Fixture: identity-partitioned schema on (id int64, day string), two
// data files with distinct partition values "2024-01-01" / "2024-01-02".
// Run the engine, read the rewritten manifest back, and assert each
// entry's partition value matches the input.
func TestEngine_E2E_Partitioned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_part"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	// Schema: id int64, day string. Partition by identity(day).
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "day", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID:  2,
		FieldID:   1000,
		Name:      "day",
		Transform: iceberg.IdentityTransform{},
	})

	type dataFile struct {
		uri string
		day string
	}
	files := []dataFile{
		{uri: srcURI("data/day=2024-01-01/file-1.parquet"), day: "2024-01-01"},
		{uri: srcURI("data/day=2024-01-02/file-2.parquet"), day: "2024-01-02"},
	}

	// Build the manifest entries.
	var entries []iceberg.ManifestEntry
	const snapID int64 = 100
	for _, f := range files {
		if err := store.PutObject(ctx, f.uri, []byte("placeholder")); err != nil {
			t.Fatalf("put data %s: %v", f.uri, err)
		}
		// partitionData keys are partition field IDs (1000 here).
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, f.uri, iceberg.ParquetFile,
			map[int]any{1000: f.day}, nil, nil, 10, 1024)
		if err != nil {
			t.Fatalf("DataFileBuilder: %v", err)
		}
		sid := snapID
		entries = append(entries, iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, dfb.Build()).
			SequenceNum(1).FileSequenceNum(1).Build())
	}

	manURI := srcURI("metadata/m1.avro")
	manBuf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(manURI, manBuf, 2, spec, schema, snapID, entries)
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := store.PutObject(ctx, manURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put manifest: %v", err)
	}

	listURI := srcURI("metadata/snap-100.avro")
	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	if err := store.PutObject(ctx, listURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put list: %v", err)
	}

	metaURI := srcURI("metadata/v2.metadata.json")
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": 1,
  "last-updated-ms": 1700000000000,
  "last-column-id": 2,
  "schemas": [{"schema-id": 0, "type": "struct", "fields": [
    {"id": 1, "name": "id", "required": true, "type": "long"},
    {"id": 2, "name": "day", "required": true, "type": "string"}
  ]}],
  "current-schema-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 2, "field-id": 1000, "name": "day", "transform": "identity"}]}],
  "default-spec-id": 0,
  "last-partition-id": 1000,
  "properties": {},
  "current-snapshot-id": %d,
  "snapshots": [
    {"snapshot-id": %d, "sequence-number": 1, "timestamp-ms": 1700000000000, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0}
  ],
  "snapshot-log": [{"snapshot-id": %d, "timestamp-ms": 1700000000000}],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`, "s3://"+mi.SourceBucket+"/"+tablePrefix, snapID, snapID, listURI, snapID)
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
		t.Fatalf("RewriteTable: %v", err)
	}

	// Read the rewritten manifest back. Each entry must reference the
	// new prefix on file_path AND keep its original partition value
	// for "day".
	newManURI := strings.Replace(manURI, mi.SourceBucket, mi.TargetBucket, 1)
	newManRaw, err := store.GetObject(ctx, newManURI)
	if err != nil {
		t.Fatalf("get rewritten manifest: %v", err)
	}
	mfList := iceberg.NewManifestFile(2, newManURI, int64(len(newManRaw)), 0, snapID).Build()
	gotEntries, err := iceberg.ReadManifest(mfList, bytes.NewReader(newManRaw), false)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	if len(gotEntries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(gotEntries))
	}

	wantDays := map[string]string{
		"s3://" + mi.TargetBucket + "/" + tablePrefix + "/data/day=2024-01-01/file-1.parquet": "2024-01-01",
		"s3://" + mi.TargetBucket + "/" + tablePrefix + "/data/day=2024-01-02/file-2.parquet": "2024-01-02",
	}
	for _, e := range gotEntries {
		df := e.DataFile()
		wantDay, ok := wantDays[df.FilePath()]
		if !ok {
			t.Errorf("unexpected file path: %q", df.FilePath())
			continue
		}
		// Partition data round-trip: it's stored as a map keyed by field ID.
		got := df.Partition()[1000]
		if got != wantDay {
			t.Errorf("partition[%q].day = %v, want %q", df.FilePath(), got, wantDay)
		}
	}
}
