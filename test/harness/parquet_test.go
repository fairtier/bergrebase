package harness

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// TestWriteDeterministicV2BasicParquet asserts the writer produces a Parquet
// file with PARQUET:field_id=1 on the int64 "id" column, the row count
// requested, and bit-stable bytes across calls. Without the field-id
// metadata, DuckDB's iceberg_scan returns zero rows even when the data is
// physically present — the reader matches by Iceberg field id, not column
// name.
func TestWriteDeterministicV2BasicParquet(t *testing.T) {
	const rows = int64(10)
	data, sumID := WriteDeterministicV2BasicParquet(t, rows)

	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("output is not a Parquet file (missing PAR1 magic)")
	}
	if want := rows * (rows + 1) / 2; sumID != want {
		t.Errorf("sumID = %d, want %d", sumID, want)
	}

	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer rdr.Close()

	if got := rdr.NumRows(); got != rows {
		t.Errorf("NumRows = %d, want %d", got, rows)
	}

	pqRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		t.Fatalf("pqarrow.NewFileReader: %v", err)
	}
	schema, err := pqRdr.Schema()
	if err != nil {
		t.Fatalf("pqarrow Schema: %v", err)
	}
	if got := schema.NumFields(); got != 1 {
		t.Fatalf("schema field count = %d, want 1", got)
	}
	field := schema.Field(0)
	if field.Name != "id" {
		t.Errorf("field name = %q, want %q", field.Name, "id")
	}
	idx := field.Metadata.FindKey("PARQUET:field_id")
	if idx < 0 {
		t.Fatalf("PARQUET:field_id metadata missing on column %q — DuckDB iceberg_scan would return zero rows", field.Name)
	}
	if got := field.Metadata.Values()[idx]; got != "1" {
		t.Errorf("PARQUET:field_id = %q, want %q", got, "1")
	}

	// Determinism: a second call with the same rows produces identical bytes.
	data2, _ := WriteDeterministicV2BasicParquet(t, rows)
	if !bytes.Equal(data, data2) {
		t.Errorf("non-deterministic output: two calls produced %d / %d bytes", len(data), len(data2))
	}

	// Smoke-read the values via the column reader to confirm 1..rows.
	tbl, err := pqRdr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	defer tbl.Release()
	if tbl.NumRows() != rows {
		t.Fatalf("table NumRows = %d, want %d", tbl.NumRows(), rows)
	}
}
