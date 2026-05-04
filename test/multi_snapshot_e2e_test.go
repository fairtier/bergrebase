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

// TestEngine_E2E_MultiSnapshot covers a V2 table with multiple
// successive snapshots, each carrying its own manifest list, manifest,
// and data file. The engine must walk
// every snapshot in metadata.Snapshots, rewrite every manifest list,
// and the post-rebase metadata.json must reference the new prefix
// from every entry.
//
// Two snapshots is the smallest fixture that exercises the iteration;
// the same code path covers 5 (the number testing.md scenario 2 uses).
func TestEngine_E2E_MultiSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_multi"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	// Two self-consistent snapshots, each at its own metadata sub-tree.
	type snap struct {
		id      int64
		seq     int64
		dataURI string
		manURI  string
		listURI string
		manRaw  []byte
		listRaw []byte
	}
	snaps := []snap{
		{id: 100, seq: 1, dataURI: srcURI("data/file-1.parquet"), manURI: srcURI("metadata/m1.avro"), listURI: srcURI("metadata/snap-100.avro")},
		{id: 200, seq: 2, dataURI: srcURI("data/file-2.parquet"), manURI: srcURI("metadata/m2.avro"), listURI: srcURI("metadata/snap-200.avro")},
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	for i := range snaps {
		s := &snaps[i]
		if err := store.PutObject(ctx, s.dataURI, fmt.Appendf(nil, "placeholder-%d", s.id)); err != nil {
			t.Fatalf("put data: %v", err)
		}
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, s.dataURI, iceberg.ParquetFile,
			map[int]any{}, nil, nil, 10, 1024)
		if err != nil {
			t.Fatalf("DataFileBuilder: %v", err)
		}
		entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &s.id, dfb.Build()).
			SequenceNum(s.seq).FileSequenceNum(s.seq).Build()

		manBuf := &bytes.Buffer{}
		mf, err := iceberg.WriteManifest(s.manURI, manBuf, 2, spec, schema, s.id, []iceberg.ManifestEntry{entry})
		if err != nil {
			t.Fatalf("WriteManifest: %v", err)
		}
		s.manRaw = manBuf.Bytes()
		if err := store.PutObject(ctx, s.manURI, s.manRaw); err != nil {
			t.Fatalf("put manifest: %v", err)
		}

		listBuf := &bytes.Buffer{}
		seq := s.seq
		if err := iceberg.WriteManifestList(2, listBuf, s.id, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
			t.Fatalf("WriteManifestList: %v", err)
		}
		s.listRaw = listBuf.Bytes()
		if err := store.PutObject(ctx, s.listURI, s.listRaw); err != nil {
			t.Fatalf("put list: %v", err)
		}
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
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000001000, "parent-snapshot-id": %d, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0}
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
		snaps[1].seq,
		snaps[1].id,
		snaps[0].id, snaps[0].seq, snaps[0].listURI,
		snaps[1].id, snaps[1].seq, snaps[0].id, snaps[1].listURI,
		snaps[0].id, snaps[1].id,
	)
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
	res, err := eng.RewriteTable(ctx, metaURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	if res.ManifestListsRewritten != 2 {
		t.Errorf("ManifestListsRewritten = %d, want 2", res.ManifestListsRewritten)
	}
	if res.ManifestsRewritten != 2 {
		t.Errorf("ManifestsRewritten = %d, want 2", res.ManifestsRewritten)
	}

	// Both rewritten manifest lists must exist under the target bucket.
	for _, s := range snaps {
		newListURI := strings.Replace(s.listURI, mi.SourceBucket, mi.TargetBucket, 1)
		if _, err := store.HeadObject(ctx, newListURI); err != nil {
			t.Errorf("missing rewritten list %q: %v", newListURI, err)
		}
		newManURI := strings.Replace(s.manURI, mi.SourceBucket, mi.TargetBucket, 1)
		if _, err := store.HeadObject(ctx, newManURI); err != nil {
			t.Errorf("missing rewritten manifest %q: %v", newManURI, err)
		}
	}
}
