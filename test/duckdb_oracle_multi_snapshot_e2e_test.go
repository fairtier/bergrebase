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

// TestDuckDBOracle_MultiSnapshot is the independent reader for the
// multi-snapshot V2 scenario. The current snapshot of the
// table is the union of all data files across the snapshot's
// manifest-list. DuckDB's iceberg_scan against the metadata.json must
// return the same (count, sum(id)) pre and post rebase.
//
// Two snapshots is the smallest fixture that exercises:
//   - manifest_list iteration in walker.go
//   - per-snapshot manifest rewrite (each list points at a different
//     manifest, each manifest at a different data file)
//   - manifest_length recomputation across multiple manifests
//
// If the post-rebase scan returns a row count that's smaller than pre,
// one of the manifest_lists wasn't rewritten — easy bug to introduce
// when the walker skips a list because nothing inside changed.
func TestDuckDBOracle_MultiSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_oracle_multi"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	type snap struct {
		id      int64
		seq     int64
		idMin   int64
		idMax   int64
		dataURI string
		manURI  string
		listURI string
	}
	snaps := []snap{
		{
			id: 100, seq: 1, idMin: 1, idMax: 10,
			dataURI: srcURI("data/file-1.parquet"),
			manURI:  srcURI("metadata/m1.avro"),
			listURI: srcURI("metadata/snap-100.avro"),
		},
		{
			id: 200, seq: 2, idMin: 11, idMax: 20,
			dataURI: srcURI("data/file-2.parquet"),
			manURI:  srcURI("metadata/m2.avro"),
			listURI: srcURI("metadata/snap-200.avro"),
		},
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	// Build per-snapshot manifests, then build manifest-lists that carry
	// forward prior snapshots' manifests. Real Iceberg appends produce
	// this shape: snapshot N's list references manifests 1..N. Without
	// it, an iceberg_scan against the current snapshot would see only
	// the latest delta, not the table-as-of-snapshot.
	//
	// `priorMFs` accumulates carry-forward copies of prior manifests
	// (sequence numbers explicitly set; iceberg-go's WriteManifestList
	// rejects unassigned sequence numbers when the manifest's snapshot
	// differs from the list's commit snapshot).
	var (
		totalCount, totalSum int64
		priorMFs             []iceberg.ManifestFile
		keysToCopy           []string
	)
	for i := range snaps {
		s := &snaps[i]
		parquetBytes, sumID := harness.WriteDeterministicIDRangeParquet(t, s.idMin, s.idMax)
		totalCount += s.idMax - s.idMin + 1
		totalSum += sumID

		if err := store.PutObject(ctx, s.dataURI, parquetBytes); err != nil {
			t.Fatalf("put data: %v", err)
		}
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, s.dataURI, iceberg.ParquetFile,
			map[int]any{}, nil, nil, s.idMax-s.idMin+1, int64(len(parquetBytes)))
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
		if err := store.PutObject(ctx, s.manURI, manBuf.Bytes()); err != nil {
			t.Fatalf("put manifest: %v", err)
		}

		// Snapshot N's list = [carry-forward manifests 1..N-1, fresh manifest N].
		listEntries := append([]iceberg.ManifestFile(nil), priorMFs...)
		listEntries = append(listEntries, mf)
		listBuf := &bytes.Buffer{}
		seq := s.seq
		if err := iceberg.WriteManifestList(2, listBuf, s.id, nil, &seq, 0, listEntries); err != nil {
			t.Fatalf("WriteManifestList: %v", err)
		}
		if err := store.PutObject(ctx, s.listURI, listBuf.Bytes()); err != nil {
			t.Fatalf("put list: %v", err)
		}

		// Carry mf forward as a prior manifest with explicit sequence.
		priorMFs = append(priorMFs, harness.CarryForwardManifest(mf, s.seq))

		for _, u := range []string{s.dataURI, s.manURI, s.listURI} {
			keysToCopy = append(keysToCopy, strings.TrimPrefix(u, "s3://"+mi.SourceBucket+"/"))
		}
	}

	metaURI := srcURI("metadata/v2.metadata.json")
	keysToCopy = append(keysToCopy, strings.TrimPrefix(metaURI, "s3://"+mi.SourceBucket+"/"))
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

	pre := harness.QueryIcebergScan(t, mi, metaURI)
	if pre.Count != totalCount || pre.SumID != totalSum {
		t.Fatalf("pre-rebase oracle: got %+v, want count=%d sum=%d", pre, totalCount, totalSum)
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
		t.Fatalf("oracle drift: pre=%+v post=%+v (one of the manifest_lists likely missed a rewrite)", pre, post)
	}
}
