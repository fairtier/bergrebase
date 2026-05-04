package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// RewritePositionDeleteFile reads the V2 position-delete Parquet at
// sourceURI from src, rewrites every row's file_path column value via
// mapping (strict prefix substitution), writes the result at the
// substituted URI to dst, and returns the new URI.
//
// Position-delete files have schema (file_path: string, pos: long) plus
// an optional `row` column (passed through unchanged). The file_path
// column embeds absolute URIs of the data files being delete-marked;
// after a bucket move those URIs go stale and readers see ghost deletes
// against missing files. This rewriter is the only place those URIs
// get fixed.
//
// Idempotency: if sourceURI doesn't begin with mapping.Source, the
// function returns sourceURI unchanged and writes nothing — same
// convention as RewriteTable. Re-runs against an already-rewritten
// file under the source path are no-ops because every row already
// matches the target prefix.
//
// Foreign-prefix safety: if a file_path column value is non-empty and
// matches neither the source nor the target prefix, the function
// returns an error naming the offending row. A position-delete file
// pointing at a bucket bergrebase wasn't told about is a real
// configuration error we surface rather than silently miss; the user
// confirmed full-successful migration is the requirement.
func RewritePositionDeleteFile(ctx context.Context, src, dst Storage, sourceURI string, mapping PrefixMapping) (string, error) {
	if !mapping.Matches(sourceURI) {
		return sourceURI, nil
	}

	raw, err := src.GetObject(ctx, sourceURI)
	if err != nil {
		return "", fmt.Errorf("read position-delete %s: %w", sourceURI, err)
	}

	pqf, err := file.NewParquetReader(bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("open position-delete parquet %s: %w", sourceURI, err)
	}
	defer func() { _ = pqf.Close() }()

	mem := memory.DefaultAllocator
	rdr, err := pqarrow.NewFileReader(pqf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return "", fmt.Errorf("pqarrow reader %s: %w", sourceURI, err)
	}
	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return "", fmt.Errorf("ReadTable %s: %w", sourceURI, err)
	}
	defer tbl.Release()

	schema := tbl.Schema()
	idxs := schema.FieldIndices("file_path")
	if len(idxs) == 0 {
		return "", fmt.Errorf("position-delete %s lacks file_path column (schema=%s)", sourceURI, schema)
	}
	if len(idxs) > 1 {
		return "", fmt.Errorf("position-delete %s has %d file_path columns, expected 1", sourceURI, len(idxs))
	}
	fpIdx := idxs[0]

	cols := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()

	mutated := false
	for i := 0; i < int(tbl.NumCols()); i++ {
		col := tbl.Column(i)
		concat, err := concatChunks(col.Data().Chunks(), mem)
		if err != nil {
			return "", fmt.Errorf("concat column %s: %w", schema.Field(i).Name, err)
		}
		if i == fpIdx {
			strCol, ok := concat.(*array.String)
			if !ok {
				concat.Release()
				return "", fmt.Errorf("position-delete %s: file_path column is %T, expected *array.String", sourceURI, concat)
			}
			rewritten, changed, err := rewriteFilePathColumn(strCol, mapping, mem, sourceURI)
			concat.Release()
			if err != nil {
				return "", err
			}
			cols[i] = rewritten
			mutated = mutated || changed
		} else {
			cols[i] = concat
		}
	}

	if !mutated {
		// Idempotent: every file_path already targets the new bucket.
		// Skip the write so a second run produces zero PUTs.
		return mapping.Apply(sourceURI), nil
	}

	rec := array.NewRecordBatch(schema, cols, tbl.NumRows())
	defer rec.Release()

	newURI := mapping.Apply(sourceURI)
	rewrittenBytes, err := encodePositionDeleteParquet(schema, rec)
	if err != nil {
		return "", fmt.Errorf("encode position-delete %s -> %s: %w", sourceURI, newURI, err)
	}
	if err := putAndVerify(ctx, dst, newURI, rewrittenBytes); err != nil {
		return "", fmt.Errorf("position-delete %s: %w", newURI, err)
	}
	return newURI, nil
}

// concatChunks returns a single Array spanning every chunk of a
// chunked column, retaining the caller-owned reference appropriately.
// Position-delete files written by Iceberg writers are typically a
// single row group with a single chunk per column; the multi-chunk
// case is handled defensively.
func concatChunks(chunks []arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if len(chunks) == 0 {
		return nil, errors.New("column has zero chunks")
	}
	if len(chunks) == 1 {
		chunks[0].Retain()
		return chunks[0], nil
	}
	return array.Concatenate(chunks, mem)
}

// rewriteFilePathColumn returns a new String array with each value
// passed through mapping.Apply. Empty values are passed through.
// Already-target-prefixed values are passed through (idempotency).
// Values matching neither prefix surface as an error.
//
// changed is true iff at least one value was rewritten. Used by the
// caller to decide whether to PUT a new file (skip the write when
// every value is already on target — preserves the
// "second run = zero PUTs" invariant).
func rewriteFilePathColumn(col *array.String, mapping PrefixMapping, mem memory.Allocator, sourceURI string) (out arrow.Array, changed bool, err error) {
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
			continue
		}
		v := col.Value(i)
		if v == "" {
			bldr.Append(v)
			continue
		}
		switch {
		case mapping.Matches(v):
			bldr.Append(mapping.Apply(v))
			changed = true
		case strings.HasPrefix(v, mapping.Target):
			bldr.Append(v)
		default:
			return nil, false, fmt.Errorf("position-delete %s row %d: file_path %q matches neither source prefix %q nor target prefix %q",
				sourceURI, i, v, mapping.Source, mapping.Target)
		}
	}
	return bldr.NewArray(), changed, nil
}

// encodePositionDeleteParquet serialises rec to deterministic Parquet
// bytes using the same writer properties the test harness uses
// (uncompressed, single row group, no dictionary). Determinism is
// load-bearing for idempotency: re-running the rewrite must produce
// byte-identical output so a same-key re-PUT is a no-op.
func encodePositionDeleteParquet(schema *arrow.Schema, rec arrow.RecordBatch) ([]byte, error) {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Uncompressed),
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(rec.NumRows()+1),
	)
	arrProps := pqarrow.NewArrowWriterProperties()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, props, arrProps)
	if err != nil {
		return nil, err
	}
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
