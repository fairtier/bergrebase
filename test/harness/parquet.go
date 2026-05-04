package harness

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// fieldIDMeta returns the PARQUET:field_id metadata Iceberg-aware readers
// (e.g. DuckDB's iceberg_scan) need to match a Parquet column to an Iceberg
// schema field by id, not by column name. Without it, those readers silently
// return zero rows — the #1 failure mode of hand-built Iceberg fixtures.
func fieldIDMeta(id int) arrow.Metadata {
	return arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(id)})
}

// writeParquetRecord serialises rec to deterministic Parquet bytes:
// uncompressed, single row group, default (plain) encoding, no dictionary.
// Bit-stable across runs so an oracle's checksum stays reproducible.
func writeParquetRecord(t *testing.T, schema *arrow.Schema, rec arrow.RecordBatch) []byte {
	t.Helper()
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Uncompressed),
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(rec.NumRows()+1),
	)
	arrProps := pqarrow.NewArrowWriterProperties()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, props, arrProps)
	if err != nil {
		t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatalf("FileWriter.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("FileWriter.Close: %v", err)
	}
	return buf.Bytes()
}

// WriteDeterministicV2BasicParquet returns Parquet bytes for an int64 "id"
// column with values 1..rows. Convenience wrapper over
// WriteDeterministicIDRangeParquet for SeedV2Basic.
func WriteDeterministicV2BasicParquet(t *testing.T, rows int64) (data []byte, sumID int64) {
	t.Helper()
	return WriteDeterministicIDRangeParquet(t, 1, rows)
}

// WriteDeterministicIDRangeParquet returns Parquet bytes for an int64 "id"
// column with values idMin..idMax (inclusive). Used by oracle tests that
// span multiple data files (multi-snapshot, partitioned) where each file
// owns a disjoint id range.
func WriteDeterministicIDRangeParquet(t *testing.T, idMin, idMax int64) (data []byte, sumID int64) {
	t.Helper()
	if idMax < idMin {
		t.Fatalf("idMax %d < idMin %d", idMax, idMin)
	}

	field := arrow.Field{
		Name:     "id",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: fieldIDMeta(1),
	}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)

	rows := idMax - idMin + 1
	values := make([]int64, rows)
	for i := range rows {
		values[i] = idMin + i
		sumID += values[i]
	}

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues(values, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return writeParquetRecord(t, schema, rec), sumID
}

// WriteDeterministicEvolvedParquet returns Parquet bytes for the
// post-ADD-COLUMN schema (id int64 fid=1, name string fid=2). Used by the
// schema-evolution oracle to seed the second snapshot's data file. The
// field IDs match Iceberg field IDs in the evolved schema; readers use
// them (not column names) to project data through the current schema.
//
// name values are "name-<id>" — deterministic, distinct per row.
func WriteDeterministicEvolvedParquet(t *testing.T, idMin, idMax int64) (data []byte, sumID int64) {
	t.Helper()
	if idMax < idMin {
		t.Fatalf("idMax %d < idMin %d", idMax, idMin)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta(1)},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldIDMeta(2)},
	}, nil)

	rows := idMax - idMin + 1
	ids := make([]int64, rows)
	names := make([]string, rows)
	for i := range rows {
		ids[i] = idMin + i
		names[i] = "name-" + strconv.FormatInt(ids[i], 10)
		sumID += ids[i]
	}

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return writeParquetRecord(t, schema, rec), sumID
}

// WritePositionDeleteParquet returns Parquet bytes for a V2 position-delete
// file: schema (file_path string, pos long) with the iceberg-mandated field
// IDs (file_path=2147483546, pos=2147483545). Every row delete-marks the
// same data file at the given positions.
//
// Iceberg's PositionalDeleteSchema reserves these high field IDs to avoid
// collisions with user schemas (see iceberg-go/manifest.go:2348-2349).
// Readers (DuckDB, Spark, Iceberg-Java) match columns by id, so the
// PARQUET:field_id metadata is load-bearing — without it the file looks
// empty.
func WritePositionDeleteParquet(t *testing.T, dataFileURI string, positions []int64) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldIDMeta(2147483546)},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta(2147483545)},
	}, nil)

	paths := make([]string, len(positions))
	for i := range positions {
		paths[i] = dataFileURI
	}

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.StringBuilder).AppendValues(paths, nil)
	bldr.Field(1).(*array.Int64Builder).AppendValues(positions, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return writeParquetRecord(t, schema, rec)
}

// WriteEqualityDeleteParquet returns Parquet bytes for a V2 equality-delete
// file: schema (id int64) with field id matching the seed table's id column
// (typically field id 1). Every row equality-deletes table rows whose id
// matches one of the given values.
//
// Equality-delete file content is the projection of the equality columns
// named by the manifest entry's EqualityFieldIDs. There are no embedded
// URIs — bergrebase byte-copies the file unchanged and only mutates the
// manifest entry's FilePath.
func WriteEqualityDeleteParquet(t *testing.T, fieldID int, ids []int64) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta(fieldID)},
	}, nil)

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return writeParquetRecord(t, schema, rec)
}

// WriteDeterministicPartitionedDayParquet returns Parquet bytes for a
// (id int64, day string) schema where every row carries the same day value.
// Used by the partitioned oracle: one Parquet file per partition; partition
// values live in the manifest entry's data_file.partition map, not in the
// Parquet itself, but the file still contains both columns because Iceberg
// readers project them through unchanged.
//
// Field ids: id=1, day=2. Both carry PARQUET:field_id metadata.
func WriteDeterministicPartitionedDayParquet(t *testing.T, idMin, idMax int64, day string) (data []byte, sumID int64) {
	t.Helper()
	if idMax < idMin {
		t.Fatalf("idMax %d < idMin %d", idMax, idMin)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta(1)},
		{Name: "day", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldIDMeta(2)},
	}, nil)

	rows := idMax - idMin + 1
	ids := make([]int64, rows)
	days := make([]string, rows)
	for i := range rows {
		ids[i] = idMin + i
		days[i] = day
		sumID += ids[i]
	}

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues(days, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return writeParquetRecord(t, schema, rec), sumID
}
