package rewrite

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
)

// encodePosDeleteBody builds a (file_path, pos) position-delete Parquet
// body with one row per path, using the same deterministic writer the
// engine uses.
func encodePosDeleteBody(t *testing.T, paths []string) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	fpB := array.NewStringBuilder(mem)
	defer fpB.Release()
	posB := array.NewInt64Builder(mem)
	defer posB.Release()
	for i, p := range paths {
		fpB.Append(p)
		posB.Append(int64(i))
	}
	fp := fpB.NewArray()
	defer fp.Release()
	pos := posB.NewArray()
	defer pos.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{fp, pos}, int64(len(paths)))
	defer rec.Release()
	raw, err := encodePositionDeleteParquet(schema, rec)
	if err != nil {
		t.Fatalf("encodePositionDeleteParquet: %v", err)
	}
	return raw
}

// buildPosDeleteManifest writes a V2 delete manifest with a single
// position-delete entry carrying realistic statistics: file_path-column
// bounds (reserved field id), a stale file size, split offsets, and
// column sizes, plus bounds for a user column that happen to hold the
// source prefix (which must never be rewritten).
func buildPosDeleteManifest(t *testing.T, manifestURI, deleteURI string, deleteSize int64, dataURI string) (iceberg.ManifestFile, []byte) {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, deleteURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 2, deleteSize)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	df := dfb.
		LowerBoundValues(map[int][]byte{
			reservedFieldIDFilePath: []byte(dataURI),
			1:                       []byte(dataURI), // user column — must stay untouched
		}).
		UpperBoundValues(map[int][]byte{
			reservedFieldIDFilePath: []byte(dataURI),
			1:                       []byte(dataURI),
		}).
		SplitOffsets([]int64{4}).
		ColumnSizes(map[int]int64{reservedFieldIDFilePath: 64}).
		Build()
	snapshotID := int64(1)
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df).
		SequenceNum(1).FileSequenceNum(1).Build()

	buf := &bytes.Buffer{}
	cnt := &countingWriter{w: buf}
	w, err := iceberg.NewManifestWriter(2, cnt, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	if err != nil {
		t.Fatalf("NewManifestWriter: %v", err)
	}
	if err := w.Add(entry); err != nil {
		t.Fatalf("ManifestWriter.Add: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ManifestWriter.Close: %v", err)
	}
	mf, err := w.ToManifestFile(manifestURI, cnt.n,
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	if err != nil {
		t.Fatalf("ToManifestFile: %v", err)
	}
	return mf, buf.Bytes()
}

// TestRewriteManifest_PositionDelete_FixesBoundsAndFileSize covers the
// two silent-corruption classes on position-delete entries: stale
// file_path-column bounds (engines skip the delete file → deleted rows
// resurrect) and a stale file_size_in_bytes after the body re-encode
// (readers look for the Parquet footer at the wrong offset). The
// target prefix is longer than the source so old and new sizes cannot
// coincide by accident.
func TestRewriteManifest_PositionDelete_FixesBoundsAndFileSize(t *testing.T) {
	const (
		oldDataURI     = "s3://old/wh/t/data/f1.parquet"
		oldDeleteURI   = "s3://old/wh/t/data/d1.parquet"
		oldManifestURI = "s3://old/wh/t/metadata/m1.avro"
		newDataURI     = "s3://brand-new/wh/t/data/f1.parquet"
		newDeleteURI   = "s3://brand-new/wh/t/data/d1.parquet"
	)
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://brand-new/"}

	body := encodePosDeleteBody(t, []string{oldDataURI, oldDataURI})
	// Deliberately stale declared size (as if the original was
	// compressed by a real writer).
	mf, rawManifest := buildPosDeleteManifest(t, oldManifestURI, oldDeleteURI, 999, oldDataURI)

	store := newMemStorage()
	store.objects[oldDeleteURI] = body
	store.objects[oldManifestURI] = rawManifest

	newMF, _, err := RewriteManifest(context.Background(), store, store, mf, mapping)
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}

	newBody, ok := store.puts[newDeleteURI]
	if !ok {
		t.Fatalf("expected rewritten delete body at %q, puts: %v", newDeleteURI, putKeys(store))
	}

	written := store.puts[newMF.FilePath()]
	entries, err := iceberg.ReadManifest(newMF, bytes.NewReader(written), false)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(entries))
	}
	df := entries[0].DataFile()

	if got := df.FilePath(); got != newDeleteURI {
		t.Errorf("file_path = %q, want %q", got, newDeleteURI)
	}
	if got := df.FileSizeBytes(); got != int64(len(newBody)) {
		t.Errorf("file_size_in_bytes = %d, want %d (rewritten body length)", got, len(newBody))
	}
	if lb := df.LowerBoundValues()[reservedFieldIDFilePath]; string(lb) != newDataURI {
		t.Errorf("file_path lower bound = %q, want %q", lb, newDataURI)
	}
	if ub := df.UpperBoundValues()[reservedFieldIDFilePath]; string(ub) != newDataURI {
		t.Errorf("file_path upper bound = %q, want %q", ub, newDataURI)
	}
	// User-column bounds are data, not paths — even when they happen to
	// hold the source prefix they must survive verbatim (rule D4).
	if lb := df.LowerBoundValues()[1]; string(lb) != oldDataURI {
		t.Errorf("user column lower bound = %q, want untouched %q", lb, oldDataURI)
	}
	if ub := df.UpperBoundValues()[1]; string(ub) != oldDataURI {
		t.Errorf("user column upper bound = %q, want untouched %q", ub, oldDataURI)
	}
	if so := df.SplitOffsets(); so != nil {
		t.Errorf("split_offsets = %v, want cleared (they describe the old byte layout)", so)
	}
	if cs := df.ColumnSizes(); len(cs) != 0 {
		t.Errorf("column_sizes = %v, want cleared", cs)
	}

	// Idempotency: a second pass over the rewritten manifest must issue
	// zero PUTs.
	store.puts = map[string][]byte{}
	again, _, err := RewriteManifest(context.Background(), store, store, newMF, mapping)
	if err != nil {
		t.Fatalf("second RewriteManifest: %v", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("second run issued %d PUTs, want 0: %v", len(store.puts), putKeys(store))
	}
	if again.FilePath() != newMF.FilePath() {
		t.Errorf("second run moved the manifest: %q", again.FilePath())
	}
}

// TestRewriteManifest_RefusesOutOfPrefixManifest covers the
// mixed-location refusal: a manifest living outside the source prefix
// whose entries are in-prefix must error instead of being overwritten
// at its original URI (which would mutate the live source table before
// any catalog swap).
func TestRewriteManifest_RefusesOutOfPrefixManifest(t *testing.T) {
	const (
		manifestURI = "s3://elsewhere/metadata/m1.avro"
		dataURI     = "s3://old/wh/t/data/f1.parquet"
	)
	mf, raw := buildV2Manifest(t, manifestURI, dataURI, 7)
	store := newMemStorage()
	store.objects[manifestURI] = raw

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	_, _, err := RewriteManifest(context.Background(), store, store, mf, mapping)
	if !errors.Is(err, ErrOutsidePrefix) {
		t.Fatalf("expected ErrOutsidePrefix, got %v", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("refusal must not write anything, got PUTs: %v", putKeys(store))
	}
}

// TestWalkPositionDeleteFile_ForeignPrefixFailsDryRun asserts the
// dry-run walker reads delete bodies and surfaces the same
// foreign-prefix error the live rewrite would.
func TestWalkPositionDeleteFile_ForeignPrefixFailsDryRun(t *testing.T) {
	const deleteURI = "s3://old/wh/t/data/d1.parquet"
	body := encodePosDeleteBody(t, []string{"s3://unknown-bucket/f1.parquet"})
	store := newMemStorage()
	store.objects[deleteURI] = body

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	err := WalkPositionDeleteFile(context.Background(), store, deleteURI, mapping)
	if err == nil || !strings.Contains(err.Error(), "matches neither source prefix") {
		t.Fatalf("expected foreign-prefix error, got %v", err)
	}

	// Happy path: all rows in scope → no error, no writes.
	const okURI = "s3://old/wh/t/data/d2.parquet"
	store.objects[okURI] = encodePosDeleteBody(t, []string{"s3://old/wh/t/data/f1.parquet"})
	if err := WalkPositionDeleteFile(context.Background(), store, okURI, mapping); err != nil {
		t.Fatalf("WalkPositionDeleteFile happy path: %v", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("dry-run walk must not write, got PUTs: %v", putKeys(store))
	}
}
