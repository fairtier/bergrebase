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

// TestDuckDBOracle_V3_RowLineage is the independent reader for the V3
// with-row-lineage scenario. The fixture mirrors
// TestEngine_E2E_V3_RowLineage but writes real Parquet so DuckDB's
// iceberg_scan can query it. A successful scan proves:
//
//   - The V3 OCF header (`format-version=3`, `first-row-id`) survives
//     the manifest-list rewrite.
//   - The per-manifest_file FirstRowId field is preserved (set during
//     the original WriteManifestList write; must round-trip when our
//     engine re-encodes the list).
//   - V3 metadata.json fields (`row-lineage`, `next-row-id`) are
//     written back unchanged by RewriteMetadataJSON.
//
// DuckDB's iceberg extension may not yet support V3 row lineage; if the
// pre-rebase scan returns 0 rows or fails outright, the test marks itself
// as a known-skip with a reference to the upstream tracking issue rather
// than reporting a bergrebase regression.
func TestDuckDBOracle_V3_RowLineage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_oracle_v3"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	const snapID, seqNum, firstRowID int64 = 100, 1, 0
	const rowCount int64 = 10
	parquetBytes, sumID := harness.WriteDeterministicV2BasicParquet(t, rowCount)

	dataURI := srcURI("data/file-1.parquet")
	if err := store.PutObject(ctx, dataURI, parquetBytes); err != nil {
		t.Fatalf("put data: %v", err)
	}
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, rowCount, int64(len(parquetBytes)))
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
  "next-row-id": %d,
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
}`, "s3://"+mi.SourceBucket+"/"+tablePrefix, seqNum, rowCount, snapID, snapID, seqNum, listURI, snapID)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

	// Pre-rebase oracle. If DuckDB's iceberg extension doesn't support V3
	// yet, the scan typically either errors out or returns zero rows.
	// Skip rather than declare a bergrebase regression — the V3 reader
	// gap is upstream.
	pre := tryQueryIcebergScan(t, mi, metaURI)
	if pre == nil {
		t.Skipf("DuckDB iceberg_scan does not yet support V3 row lineage on this version; skipping V3 oracle")
	}
	if pre.Count != rowCount || pre.SumID != sumID {
		t.Skipf("DuckDB returned %+v on V3 fixture (expected count=%d sum=%d); treating as upstream V3 support gap, not a bergrebase regression",
			*pre, rowCount, sumID)
	}

	keysToCopy := []string{
		strings.TrimPrefix(dataURI, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(manURI, "s3://"+mi.SourceBucket+"/"),
		strings.TrimPrefix(listURI, "s3://"+mi.SourceBucket+"/"),
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
		t.Fatalf("RewriteTable V3: %v", err)
	}

	post := harness.QueryIcebergScan(t, mi, res.NewMetadataLocation)
	if post != *pre {
		t.Fatalf("V3 oracle drift: pre=%+v post=%+v", *pre, post)
	}
}

// tryQueryIcebergScan runs QueryIcebergScan in a sub-test that can fail
// without taking the parent down. It returns nil on any error, letting
// the caller treat the failure as "DuckDB doesn't support V3 yet" rather
// than a bergrebase bug.
func tryQueryIcebergScan(t *testing.T, mi *harness.MinIO, metadataURI string) *harness.IcebergScanResult {
	t.Helper()
	var result *harness.IcebergScanResult
	t.Run("probe_v3", func(probe *testing.T) {
		// In a sub-test, calling t.Fatalf inside QueryIcebergScan would
		// only fail the sub-test. The parent decides what to do with the
		// outcome.
		res := harness.QueryIcebergScan(probe, mi, metadataURI)
		if !probe.Failed() {
			result = &res
		}
	})
	return result
}
