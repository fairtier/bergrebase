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

// TestDuckDBOracle_Partitioned is the independent reader for the V2
// identity-partitioned scenario. DuckDB's iceberg_scan
// reads the partition spec from metadata.json, projects partition values
// onto rows that lack them in the data file, and returns a unified row
// set. A successful post-rebase scan proves:
//
//   - Identity partition values (held in data_file.partition map, NOT in
//     the Parquet file) survive the reflection-based file_path mutation
//     in setDataFilePath.
//   - Per-column field-id metadata is preserved on multi-column schemas
//     ((id int64 fid=1, day string fid=2)).
//   - The partition-spec entries in metadata.json are unaffected by the
//     prefix rewrite (they describe transforms, not paths).
//
// Two partitions × 10 rows = 20 rows total, ids 1..20, sum = 210. The
// per-partition group_by query asserts each partition has the expected
// row count — a sanity check that partitions weren't accidentally merged
// or dropped during the rewrite.
func TestDuckDBOracle_Partitioned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	const tablePrefix = "iceberg/db/orders_oracle_part"
	srcURI := func(suffix string) string {
		return fmt.Sprintf("s3://%s/%s/%s", mi.SourceBucket, tablePrefix, suffix)
	}

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

	type partFile struct {
		uri    string
		day    string
		idMin  int64
		idMax  int64
		bytes  []byte
		nrows  int64
		nbytes int64
	}
	files := []partFile{
		{uri: srcURI("data/day=2024-01-01/file-1.parquet"), day: "2024-01-01", idMin: 1, idMax: 10},
		{uri: srcURI("data/day=2024-01-02/file-2.parquet"), day: "2024-01-02", idMin: 11, idMax: 20},
	}

	const snapID, seqNum int64 = 100, 1
	var totalCount, totalSum int64
	keysToCopy := []string{}
	var entries []iceberg.ManifestEntry
	for i := range files {
		f := &files[i]
		parquetBytes, sumID := harness.WriteDeterministicPartitionedDayParquet(t, f.idMin, f.idMax, f.day)
		f.bytes = parquetBytes
		f.nrows = f.idMax - f.idMin + 1
		f.nbytes = int64(len(parquetBytes))
		totalCount += f.nrows
		totalSum += sumID

		if err := store.PutObject(ctx, f.uri, parquetBytes); err != nil {
			t.Fatalf("put data %s: %v", f.uri, err)
		}
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, f.uri, iceberg.ParquetFile,
			map[int]any{1000: f.day}, nil, nil, f.nrows, f.nbytes)
		if err != nil {
			t.Fatalf("DataFileBuilder: %v", err)
		}
		sid := snapID
		entries = append(entries, iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, dfb.Build()).
			SequenceNum(seqNum).FileSequenceNum(seqNum).Build())
		keysToCopy = append(keysToCopy, strings.TrimPrefix(f.uri, "s3://"+mi.SourceBucket+"/"))
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
	keysToCopy = append(keysToCopy, strings.TrimPrefix(manURI, "s3://"+mi.SourceBucket+"/"))

	listURI := srcURI("metadata/snap-100.avro")
	listBuf := &bytes.Buffer{}
	seq := seqNum
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	if err := store.PutObject(ctx, listURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put list: %v", err)
	}
	keysToCopy = append(keysToCopy, strings.TrimPrefix(listURI, "s3://"+mi.SourceBucket+"/"))

	metaURI := srcURI("metadata/v2.metadata.json")
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": %d,
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
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000000000, "manifest-list": %q, "summary": {"operation": "append"}, "schema-id": 0}
  ],
  "snapshot-log": [{"snapshot-id": %d, "timestamp-ms": 1700000000000}],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`,
		"s3://"+mi.SourceBucket+"/"+tablePrefix,
		seqNum, snapID, snapID, seqNum, listURI, snapID,
	)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}
	keysToCopy = append(keysToCopy, strings.TrimPrefix(metaURI, "s3://"+mi.SourceBucket+"/"))

	pre := harness.QueryIcebergScan(t, mi, metaURI)
	if pre.Count != totalCount || pre.SumID != totalSum {
		t.Fatalf("pre-rebase oracle: got %+v, want count=%d sum=%d (PARQUET:field_id likely missing on multi-column schema)",
			pre, totalCount, totalSum)
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
		t.Fatalf("oracle drift: pre=%+v post=%+v (partition values likely lost during reflection mutation)", pre, post)
	}
}
