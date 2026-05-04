package harness

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"

	bergstorage "github.com/fairtier/bergrebase/internal/storage"
)

// V2Seed is the URI set produced by SeedV2Basic. Tests use it to assert
// what landed where, and (for the DuckDB oracle) what the data values are.
type V2Seed struct {
	MetadataURI     string
	ManifestListURI string
	ManifestURI     string
	DataFileURI     string
	StatsFileURI    string // statistics[].statistics-path
	PartStatsURI    string // partition-statistics[].statistics-path
	SnapshotID      int64

	// AllURIs lists every object the seed wrote, in the order written.
	// Tests that need to copy the seed bucket to a new prefix iterate
	// this list rather than re-listing the bucket. Populated by every
	// SeedV2* helper.
	AllURIs []string

	// RowCount and SumID describe the data file contents — values 1..RowCount
	// in a single int64 column "id". An oracle reader (DuckDB iceberg_scan)
	// must return (RowCount, SumID) for SELECT count(*), sum(id).
	RowCount int64
	SumID    int64
}

// SeedV2Basic uploads a self-consistent V2 fixture to bucket under
// table=<name>: one Parquet data file (10 rows, "id" int64 = 1..10), one
// manifest with a single entry, one manifest list, and one metadata.json.
// The fixture matches the basic V2 scenario, minus the Lakekeeper
// catalog wiring. The table name lets multiple subtests share one
// MinIO instance without colliding.
//
// The data file is real Parquet (bytes from WriteDeterministicV2BasicParquet)
// so an oracle reader like DuckDB iceberg_scan can query the table.
//
// Layout under bucket:
//
//	iceberg/db/<table>/data/file-1.parquet
//	iceberg/db/<table>/metadata/m1.avro
//	iceberg/db/<table>/metadata/snap-100-1-uuid.avro
//	iceberg/db/<table>/metadata/v2.metadata.json
func SeedV2Basic(ctx context.Context, t *testing.T, store *bergstorage.Client, bucket, table string) V2Seed {
	t.Helper()

	prefix := fmt.Sprintf("s3://%s/iceberg/db/%s", bucket, table)
	const rowCount int64 = 10
	parquetBytes, sumID := WriteDeterministicV2BasicParquet(t, rowCount)

	seed := V2Seed{
		DataFileURI:     prefix + "/data/file-1.parquet",
		ManifestURI:     prefix + "/metadata/m1.avro",
		ManifestListURI: prefix + "/metadata/snap-100-1-uuid.avro",
		MetadataURI:     prefix + "/metadata/v2.metadata.json",
		StatsFileURI:    prefix + "/metadata/snap-100.stats",
		PartStatsURI:    prefix + "/metadata/snap-100-part.stats",
		SnapshotID:      100,
		RowCount:        rowCount,
		SumID:           sumID,
	}

	if err := store.PutObject(ctx, seed.DataFileURI, parquetBytes); err != nil {
		t.Fatalf("put data file: %v", err)
	}

	// 2. Manifest with one EntryStatusADDED data file. file_size_in_bytes
	//    is set to the real Parquet length so any reader that cross-checks
	//    against the manifest is happy.
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec
	dfb, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		seed.DataFileURI,
		iceberg.ParquetFile,
		map[int]any{},
		nil, nil,
		rowCount,
		int64(len(parquetBytes)),
	)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	df := dfb.Build()
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &seed.SnapshotID, df).
		SequenceNum(1).
		FileSequenceNum(1).
		Build()

	manBuf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(seed.ManifestURI, manBuf, 2, spec, schema, seed.SnapshotID, []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := store.PutObject(ctx, seed.ManifestURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put manifest: %v", err)
	}

	// 3. Manifest list referencing the manifest.
	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, seed.SnapshotID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	if err := store.PutObject(ctx, seed.ManifestListURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put manifest list: %v", err)
	}

	// 4. metadata.json pointing at the manifest list. Includes every
	//    field a strict reader (e.g. Lakekeeper's RegisterTable) checks
	//    against the V2 spec — schemas, partition-specs, snapshot-log,
	//    sort-orders, the *-id fields. Don't trim further without
	//    re-validating the e2e suite.
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": %q,
  "last-sequence-number": 1,
  "last-updated-ms": 1700000000000,
  "last-column-id": 1,
  "schemas": [
    {"schema-id": 0, "type": "struct", "fields": [
      {"id": 1, "name": "id", "required": true, "type": "long"}
    ]}
  ],
  "current-schema-id": 0,
  "partition-specs": [
    {"spec-id": 0, "fields": []}
  ],
  "default-spec-id": 0,
  "last-partition-id": 999,
  "properties": {
    "write.object-storage.path": %q
  },
  "current-snapshot-id": %d,
  "snapshots": [
    {
      "snapshot-id": %d,
      "sequence-number": 1,
      "timestamp-ms": 1700000000000,
      "manifest-list": %q,
      "summary": {"operation": "append"},
      "schema-id": 0
    }
  ],
  "snapshot-log": [
    {"snapshot-id": %d, "timestamp-ms": 1700000000000}
  ],
  "metadata-log": [
    {"timestamp-ms": 1699999999000, "metadata-file": %q}
  ],
  "statistics": [
    {"snapshot-id": %d, "statistics-path": %q, "file-size-in-bytes": 100, "file-footer-size-in-bytes": 10, "blob-metadata": []}
  ],
  "partition-statistics": [
    {"snapshot-id": %d, "statistics-path": %q, "file-size-in-bytes": 50}
  ],
  "sort-orders": [
    {"order-id": 0, "fields": []}
  ],
  "default-sort-order-id": 0
}`,
		prefix,
		prefix+"/data",
		seed.SnapshotID,
		seed.SnapshotID, seed.ManifestListURI,
		seed.SnapshotID,
		prefix+"/metadata/v1.metadata.json",
		seed.SnapshotID, seed.StatsFileURI,
		seed.SnapshotID, seed.PartStatsURI,
	)
	if err := store.PutObject(ctx, seed.MetadataURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata.json: %v", err)
	}

	seed.AllURIs = []string{
		seed.DataFileURI,
		seed.ManifestURI,
		seed.ManifestListURI,
		seed.MetadataURI,
	}
	return seed
}

// SeedV2Wide uploads a self-consistent V2 fixture with numFiles data
// files in a single snapshot, identity-partitioned on a "day" string
// column. Used by the perf scenario (testing.md #4) and as the input
// to the resume scenario (#14).
//
// Schema:    (id int64 fid=1, day string fid=2)
// Partition: identity(day), fieldID 1000
// Files:     numFiles Parquet files, day=2020-01-01..+(numFiles-1) days,
//
//	one row per file, id=1..numFiles
//
// Manifest:  one Avro manifest with numFiles entries; one manifest list
// Aggregates: RowCount=numFiles, SumID=N*(N+1)/2
//
// Layout under bucket:
//
//	iceberg/db/<table>/data/day=YYYY-MM-DD/file-<i>.parquet
//	iceberg/db/<table>/metadata/m1.avro
//	iceberg/db/<table>/metadata/snap-100.avro
//	iceberg/db/<table>/metadata/v2.metadata.json
func SeedV2Wide(ctx context.Context, t *testing.T, store *bergstorage.Client, bucket, table string, numFiles int) V2Seed {
	t.Helper()
	if numFiles < 1 {
		t.Fatalf("SeedV2Wide: numFiles must be >= 1, got %d", numFiles)
	}

	prefix := fmt.Sprintf("s3://%s/iceberg/db/%s", bucket, table)
	const snapID, seqNum int64 = 100, 1

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

	// Anchor the deterministic day sequence at 2020-01-01 UTC. UTC midnight
	// avoids DST glitches and keeps the day strings reproducible across
	// platforms.
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	allURIs := make([]string, 0, numFiles+3)
	entries := make([]iceberg.ManifestEntry, 0, numFiles)
	var totalSum int64
	var firstDataURI string

	for i := range numFiles {
		day := base.AddDate(0, 0, i).Format("2006-01-02")
		id := int64(i + 1)
		dataURI := fmt.Sprintf("%s/data/day=%s/file-%d.parquet", prefix, day, id)
		if i == 0 {
			firstDataURI = dataURI
		}

		parquetBytes, sumID := WriteDeterministicPartitionedDayParquet(t, id, id, day)
		totalSum += sumID

		if err := store.PutObject(ctx, dataURI, parquetBytes); err != nil {
			t.Fatalf("put %s: %v", dataURI, err)
		}
		allURIs = append(allURIs, dataURI)

		dfb, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			dataURI,
			iceberg.ParquetFile,
			map[int]any{1000: day},
			nil, nil,
			1,
			int64(len(parquetBytes)),
		)
		if err != nil {
			t.Fatalf("DataFileBuilder %d: %v", i, err)
		}
		sid := snapID
		entries = append(entries,
			iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, dfb.Build()).
				SequenceNum(seqNum).FileSequenceNum(seqNum).Build(),
		)
	}

	manURI := prefix + "/metadata/m1.avro"
	manBuf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(manURI, manBuf, 2, spec, schema, snapID, entries)
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := store.PutObject(ctx, manURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	allURIs = append(allURIs, manURI)

	listURI := prefix + "/metadata/snap-100.avro"
	listBuf := &bytes.Buffer{}
	seq := seqNum
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	if err := store.PutObject(ctx, listURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put manifest list: %v", err)
	}
	allURIs = append(allURIs, listURI)

	metaURI := prefix + "/metadata/v2.metadata.json"
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c2",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000000000,
  "last-column-id": 2,
  "schemas": [{"schema-id": 0, "type": "struct", "fields": [
    {"id": 1, "name": "id", "required": true, "type": "long"},
    {"id": 2, "name": "day", "required": true, "type": "string"}
  ]}],
  "current-schema-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [
    {"source-id": 2, "field-id": 1000, "name": "day", "transform": "identity"}
  ]}],
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
		prefix, seqNum, snapID, snapID, seqNum, listURI, snapID,
	)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata.json: %v", err)
	}
	allURIs = append(allURIs, metaURI)

	return V2Seed{
		MetadataURI:     metaURI,
		ManifestListURI: listURI,
		ManifestURI:     manURI,
		DataFileURI:     firstDataURI, // representative; full set in AllURIs
		SnapshotID:      snapID,
		AllURIs:         allURIs,
		RowCount:        int64(numFiles),
		SumID:           totalSum,
	}
}

// V2DeletesSeed extends V2Seed with the URIs and counts for a V2 fixture
// that includes a delete file in a second snapshot. PostDeleteCount and
// PostDeleteSum are what an oracle reading the current snapshot must
// return after the delete file is applied.
type V2DeletesSeed struct {
	V2Seed

	SnapshotID2       int64
	DeleteFileURI     string
	DeleteManifestURI string
	ManifestListURI2  string

	PostDeleteCount int64
	PostDeleteSum   int64
}

// SeedV2WithPositionDeletes uploads a V2 fixture with two snapshots:
// snap 1 adds a 100-row data file (id=1..100), snap 2 adds a
// position-delete file marking every even position (positions
// 0, 2, ..., 98 → 50 rows survive, the odd ids 1, 3, ..., 99,
// sum=2500). The result reads as 50 rows post-rebase iff the
// engine correctly rewrites the file_path column inside the
// position-delete Parquet to point at the target bucket.
//
// Layout under bucket:
//
//	iceberg/db/<table>/data/file-1.parquet
//	iceberg/db/<table>/data/pos-del-1.parquet
//	iceberg/db/<table>/metadata/m1.avro              (data manifest)
//	iceberg/db/<table>/metadata/m-deletes-1.avro     (delete manifest)
//	iceberg/db/<table>/metadata/snap-100.avro        (snap 1 list: [m1])
//	iceberg/db/<table>/metadata/snap-200.avro        (snap 2 list: [m1 carried, m-deletes])
//	iceberg/db/<table>/metadata/v2.metadata.json
func SeedV2WithPositionDeletes(ctx context.Context, t *testing.T, store *bergstorage.Client, bucket, table string) V2DeletesSeed {
	t.Helper()

	prefix := fmt.Sprintf("s3://%s/iceberg/db/%s", bucket, table)
	const rowCount int64 = 100
	const snap1ID, snap2ID int64 = 100, 200
	const snap1Seq, snap2Seq int64 = 1, 2

	dataParquet, sumID := WriteDeterministicIDRangeParquet(t, 1, rowCount)

	dataURI := prefix + "/data/file-1.parquet"
	delURI := prefix + "/data/pos-del-1.parquet"
	manURI := prefix + "/metadata/m1.avro"
	delManURI := prefix + "/metadata/m-deletes-1.avro"
	listURI1 := prefix + "/metadata/snap-100.avro"
	listURI2 := prefix + "/metadata/snap-200.avro"
	metaURI := prefix + "/metadata/v2.metadata.json"

	// Delete every even position: 0, 2, ..., 98 (50 rows). Survivors are
	// the odd ids 1, 3, ..., 99: count=50, sum=2500.
	positions := make([]int64, 0, rowCount/2)
	var deletedSum int64
	for p := int64(0); p < rowCount; p += 2 {
		positions = append(positions, p)
		// id at position p is p+1 (zero-indexed positions, ids start at 1).
		deletedSum += p + 1
	}
	delParquet := WritePositionDeleteParquet(t, dataURI, positions)

	if err := store.PutObject(ctx, dataURI, dataParquet); err != nil {
		t.Fatalf("put data file: %v", err)
	}
	if err := store.PutObject(ctx, delURI, delParquet); err != nil {
		t.Fatalf("put position-delete file: %v", err)
	}

	// Data manifest (snap 1).
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec
	dataDFB, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, rowCount, int64(len(dataParquet)))
	if err != nil {
		t.Fatalf("DataFileBuilder data: %v", err)
	}
	sid1 := snap1ID
	dataEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid1, dataDFB.Build()).
		SequenceNum(snap1Seq).FileSequenceNum(snap1Seq).Build()

	manBuf := &bytes.Buffer{}
	mfData, err := iceberg.WriteManifest(manURI, manBuf, 2, spec, schema, snap1ID, []iceberg.ManifestEntry{dataEntry})
	if err != nil {
		t.Fatalf("WriteManifest data: %v", err)
	}
	if err := store.PutObject(ctx, manURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put data manifest: %v", err)
	}

	// Delete manifest (snap 2): one entry, EntryContentPosDeletes,
	// referencing the data file via ReferencedDataFile.
	delDFB, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, delURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, int64(len(positions)), int64(len(delParquet)))
	if err != nil {
		t.Fatalf("DataFileBuilder pos-deletes: %v", err)
	}
	delDFB = delDFB.ReferencedDataFile(dataURI)
	sid2 := snap2ID
	delEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid2, delDFB.Build()).
		SequenceNum(snap2Seq).FileSequenceNum(snap2Seq).Build()

	delManBuf := &bytes.Buffer{}
	mfDel, err := WriteDeleteManifest(delManURI, delManBuf, 2, spec, schema, snap2ID, snap2Seq, []iceberg.ManifestEntry{delEntry})
	if err != nil {
		t.Fatalf("WriteDeleteManifest: %v", err)
	}
	if err := store.PutObject(ctx, delManURI, delManBuf.Bytes()); err != nil {
		t.Fatalf("put delete manifest: %v", err)
	}

	// Snap 1 list: [data manifest only].
	listBuf1 := &bytes.Buffer{}
	seq1 := snap1Seq
	if err := iceberg.WriteManifestList(2, listBuf1, snap1ID, nil, &seq1, 0, []iceberg.ManifestFile{mfData}); err != nil {
		t.Fatalf("WriteManifestList snap1: %v", err)
	}
	if err := store.PutObject(ctx, listURI1, listBuf1.Bytes()); err != nil {
		t.Fatalf("put snap1 list: %v", err)
	}

	// Snap 2 list: [data manifest carried forward, delete manifest].
	// Data manifest must be carried forward with its original sequence
	// number — see CarryForwardManifest.
	listBuf2 := &bytes.Buffer{}
	seq2 := snap2Seq
	if err := iceberg.WriteManifestList(2, listBuf2, snap2ID, nil, &seq2, 0,
		[]iceberg.ManifestFile{CarryForwardManifest(mfData, snap1Seq), mfDel}); err != nil {
		t.Fatalf("WriteManifestList snap2: %v", err)
	}
	if err := store.PutObject(ctx, listURI2, listBuf2.Bytes()); err != nil {
		t.Fatalf("put snap2 list: %v", err)
	}

	parentSnap1 := snap1ID
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69d1",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000001000,
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
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000001000, "parent-snapshot-id": %d, "manifest-list": %q, "summary": {"operation": "delete"}, "schema-id": 0}
  ],
  "snapshot-log": [
    {"snapshot-id": %d, "timestamp-ms": 1700000000000},
    {"snapshot-id": %d, "timestamp-ms": 1700000001000}
  ],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`,
		prefix,
		snap2Seq,
		snap2ID,
		snap1ID, snap1Seq, listURI1,
		snap2ID, snap2Seq, parentSnap1, listURI2,
		snap1ID, snap2ID,
	)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

	survivors := rowCount - int64(len(positions))
	postSum := sumID - deletedSum

	return V2DeletesSeed{
		V2Seed: V2Seed{
			MetadataURI:     metaURI,
			ManifestListURI: listURI1,
			ManifestURI:     manURI,
			DataFileURI:     dataURI,
			SnapshotID:      snap2ID, // current
			RowCount:        rowCount,
			SumID:           sumID,
			AllURIs: []string{
				dataURI, delURI, manURI, delManURI, listURI1, listURI2, metaURI,
			},
		},
		SnapshotID2:       snap2ID,
		DeleteFileURI:     delURI,
		DeleteManifestURI: delManURI,
		ManifestListURI2:  listURI2,
		PostDeleteCount:   survivors,
		PostDeleteSum:     postSum,
	}
}

// SeedV2WithEqualityDeletes uploads a V2 fixture with two snapshots:
// snap 1 adds a 100-row data file (id=1..100), snap 2 adds an
// equality-delete file marking every even id (ids 2, 4, ..., 100 →
// 50 rows survive, the odd ids 1, 3, ..., 99, sum=2500).
//
// Equality-delete files contain only the projected equality column
// values (no embedded URIs). bergrebase byte-copies the file unchanged
// and only mutates the manifest entry's FilePath and the manifest list
// entry's manifest_path/manifest_length.
//
// Layout matches SeedV2WithPositionDeletes with `eq-del-1.parquet`
// and `m-eqdeletes-1.avro` substituted for the position-delete names.
func SeedV2WithEqualityDeletes(ctx context.Context, t *testing.T, store *bergstorage.Client, bucket, table string) V2DeletesSeed {
	t.Helper()

	prefix := fmt.Sprintf("s3://%s/iceberg/db/%s", bucket, table)
	const rowCount int64 = 100
	const snap1ID, snap2ID int64 = 100, 200
	const snap1Seq, snap2Seq int64 = 1, 2
	const idFieldID = 1

	dataParquet, sumID := WriteDeterministicIDRangeParquet(t, 1, rowCount)

	dataURI := prefix + "/data/file-1.parquet"
	delURI := prefix + "/data/eq-del-1.parquet"
	manURI := prefix + "/metadata/m1.avro"
	delManURI := prefix + "/metadata/m-eqdeletes-1.avro"
	listURI1 := prefix + "/metadata/snap-100.avro"
	listURI2 := prefix + "/metadata/snap-200.avro"
	metaURI := prefix + "/metadata/v2.metadata.json"

	// Delete every even id: 2, 4, ..., 100 (50 rows). Survivors are
	// the odd ids 1, 3, ..., 99: count=50, sum=2500.
	deletedIDs := make([]int64, 0, rowCount/2)
	var deletedSum int64
	for id := int64(2); id <= rowCount; id += 2 {
		deletedIDs = append(deletedIDs, id)
		deletedSum += id
	}
	delParquet := WriteEqualityDeleteParquet(t, idFieldID, deletedIDs)

	if err := store.PutObject(ctx, dataURI, dataParquet); err != nil {
		t.Fatalf("put data file: %v", err)
	}
	if err := store.PutObject(ctx, delURI, delParquet); err != nil {
		t.Fatalf("put equality-delete file: %v", err)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec
	dataDFB, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, rowCount, int64(len(dataParquet)))
	if err != nil {
		t.Fatalf("DataFileBuilder data: %v", err)
	}
	sid1 := snap1ID
	dataEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid1, dataDFB.Build()).
		SequenceNum(snap1Seq).FileSequenceNum(snap1Seq).Build()

	manBuf := &bytes.Buffer{}
	mfData, err := iceberg.WriteManifest(manURI, manBuf, 2, spec, schema, snap1ID, []iceberg.ManifestEntry{dataEntry})
	if err != nil {
		t.Fatalf("WriteManifest data: %v", err)
	}
	if err := store.PutObject(ctx, manURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put data manifest: %v", err)
	}

	// Equality-delete entry: required EqualityFieldIDs naming the equality
	// columns (here, just "id" = field id 1).
	delDFB, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentEqDeletes, delURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, int64(len(deletedIDs)), int64(len(delParquet)))
	if err != nil {
		t.Fatalf("DataFileBuilder eq-deletes: %v", err)
	}
	delDFB = delDFB.EqualityFieldIDs([]int{idFieldID})
	sid2 := snap2ID
	delEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid2, delDFB.Build()).
		SequenceNum(snap2Seq).FileSequenceNum(snap2Seq).Build()

	delManBuf := &bytes.Buffer{}
	mfDel, err := WriteDeleteManifest(delManURI, delManBuf, 2, spec, schema, snap2ID, snap2Seq, []iceberg.ManifestEntry{delEntry})
	if err != nil {
		t.Fatalf("WriteDeleteManifest: %v", err)
	}
	if err := store.PutObject(ctx, delManURI, delManBuf.Bytes()); err != nil {
		t.Fatalf("put delete manifest: %v", err)
	}

	listBuf1 := &bytes.Buffer{}
	seq1 := snap1Seq
	if err := iceberg.WriteManifestList(2, listBuf1, snap1ID, nil, &seq1, 0, []iceberg.ManifestFile{mfData}); err != nil {
		t.Fatalf("WriteManifestList snap1: %v", err)
	}
	if err := store.PutObject(ctx, listURI1, listBuf1.Bytes()); err != nil {
		t.Fatalf("put snap1 list: %v", err)
	}

	listBuf2 := &bytes.Buffer{}
	seq2 := snap2Seq
	if err := iceberg.WriteManifestList(2, listBuf2, snap2ID, nil, &seq2, 0,
		[]iceberg.ManifestFile{CarryForwardManifest(mfData, snap1Seq), mfDel}); err != nil {
		t.Fatalf("WriteManifestList snap2: %v", err)
	}
	if err := store.PutObject(ctx, listURI2, listBuf2.Bytes()); err != nil {
		t.Fatalf("put snap2 list: %v", err)
	}

	parentSnap1 := snap1ID
	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69d2",
  "location": %q,
  "last-sequence-number": %d,
  "last-updated-ms": 1700000001000,
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
    {"snapshot-id": %d, "sequence-number": %d, "timestamp-ms": 1700000001000, "parent-snapshot-id": %d, "manifest-list": %q, "summary": {"operation": "delete"}, "schema-id": 0}
  ],
  "snapshot-log": [
    {"snapshot-id": %d, "timestamp-ms": 1700000000000},
    {"snapshot-id": %d, "timestamp-ms": 1700000001000}
  ],
  "metadata-log": [],
  "sort-orders": [{"order-id": 0, "fields": []}],
  "default-sort-order-id": 0
}`,
		prefix,
		snap2Seq,
		snap2ID,
		snap1ID, snap1Seq, listURI1,
		snap2ID, snap2Seq, parentSnap1, listURI2,
		snap1ID, snap2ID,
	)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

	survivors := rowCount - int64(len(deletedIDs))
	postSum := sumID - deletedSum

	return V2DeletesSeed{
		V2Seed: V2Seed{
			MetadataURI:     metaURI,
			ManifestListURI: listURI1,
			ManifestURI:     manURI,
			DataFileURI:     dataURI,
			SnapshotID:      snap2ID,
			RowCount:        rowCount,
			SumID:           sumID,
			AllURIs: []string{
				dataURI, delURI, manURI, delManURI, listURI1, listURI2, metaURI,
			},
		},
		SnapshotID2:       snap2ID,
		DeleteFileURI:     delURI,
		DeleteManifestURI: delManURI,
		ManifestListURI2:  listURI2,
		PostDeleteCount:   survivors,
		PostDeleteSum:     postSum,
	}
}

// CopyBucketToBucket simulates the bulk byte copy step that precedes
// bergrebase: every object under sourceBucket is copied verbatim to
// targetBucket at the same key. The test seed is small (4 keys) so an
// explicit list is fine — production runs use rclone for this step.
func CopyBucketToBucket(ctx context.Context, t *testing.T, store *bergstorage.Client, keys []string, sourceBucket, targetBucket string) {
	t.Helper()
	for _, key := range keys {
		srcURI := fmt.Sprintf("s3://%s/%s", sourceBucket, key)
		dstURI := fmt.Sprintf("s3://%s/%s", targetBucket, key)
		body, err := store.GetObject(ctx, srcURI)
		if err != nil {
			t.Fatalf("get %s: %v", srcURI, err)
		}
		if err := store.PutObject(ctx, dstURI, body); err != nil {
			t.Fatalf("put %s: %v", dstURI, err)
		}
	}
}
