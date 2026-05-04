package rewrite

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// buildV2Manifest constructs a tiny in-memory V2 manifest with one
// added data-file entry. Used by the round-trip and mutation tests.
//
// schema: single Int64 field. spec: unpartitioned. content: data.
func buildV2Manifest(t *testing.T, manifestURI, dataFileURI string, snapshotID int64) (iceberg.ManifestFile, []byte) {
	t.Helper()

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	dfb, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		dataFileURI,
		iceberg.ParquetFile,
		map[int]any{},
		nil, nil,
		10,   // record count
		1024, // file size
	)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	df := dfb.Build()
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df).
		SequenceNum(1).
		FileSequenceNum(1).
		Build()

	buf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(manifestURI, buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	return mf, buf.Bytes()
}

func TestRewriteManifest_MutatesPathAndWritesAtNewURI(t *testing.T) {
	const oldDataURI = "s3://old/iceberg/db/orders/data/file-1.parquet"
	const oldManifestURI = "s3://old/iceberg/db/orders/metadata/m1.avro"

	mf, raw := buildV2Manifest(t, oldManifestURI, oldDataURI, 100)
	store := newMemStorage()
	store.objects[oldManifestURI] = raw

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	newMF, hits, err := RewriteManifest(context.Background(), store, store, mf, mapping)
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}

	wantNewURI := "s3://new/iceberg/db/orders/metadata/m1.avro"
	if newMF.FilePath() != wantNewURI {
		t.Errorf("new manifest URI = %q, want %q", newMF.FilePath(), wantNewURI)
	}
	if newMF.Length() <= 0 {
		t.Errorf("new manifest length = %d, expected > 0", newMF.Length())
	}
	if _, ok := store.puts[wantNewURI]; !ok {
		t.Errorf("expected put at %q, store has: %v", wantNewURI, putKeys(store))
	}

	// Hits include both data file paths visited.
	if len(hits) == 0 {
		t.Fatal("expected at least one PathHit")
	}
	for _, h := range hits {
		if !h.Rewritten() {
			t.Errorf("hit %q not rewritten: old=%q new=%q", h.Field, h.Old, h.New)
		}
	}

	// Read the written manifest back and assert path was substituted.
	written := store.puts[wantNewURI]
	entries, err := iceberg.ReadManifest(newMF, bytes.NewReader(written), false)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(entries))
	}
	got := entries[0].DataFile().FilePath()
	want := "s3://new/iceberg/db/orders/data/file-1.parquet"
	if got != want {
		t.Errorf("rewritten data_file.file_path = %q, want %q", got, want)
	}
}

// TestRewriteManifest_NoChange_SkipsWrite asserts the idempotency
// invariant: when no path is in scope (Source has no match), no PUT is
// issued. This is what makes acceptance criterion #1's
// "byte-identical-when-Source==Target" trivially hold.
func TestRewriteManifest_NoChange_SkipsWrite(t *testing.T) {
	const dataURI = "s3://other/data/file-1.parquet"
	const manifestURI = "s3://other/metadata/m1.avro"

	mf, raw := buildV2Manifest(t, manifestURI, dataURI, 200)
	store := newMemStorage()
	store.objects[manifestURI] = raw

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	newMF, hits, err := RewriteManifest(context.Background(), store, store, mf, mapping)
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("expected zero puts when nothing matches, got %d: %v", len(store.puts), putKeys(store))
	}
	if newMF.FilePath() != manifestURI {
		t.Errorf("manifest URI changed despite no path match: %q", newMF.FilePath())
	}
	for _, h := range hits {
		if h.Rewritten() {
			t.Errorf("hit %q reported as rewritten despite no source match: old=%q new=%q", h.Field, h.Old, h.New)
		}
	}
}

// TestRewriteManifest_RefusesPuffinDeletes asserts that a position-delete
// manifest entry pointing at a .puffin file (V3 deletion vector) is
// refused with ErrUnsupportedFeature. V2 position-delete .parquet
// entries are now supported; .puffin entries remain deferred until a
// Go Puffin reader/writer lands.
func TestRewriteManifest_RefusesPuffinDeletes(t *testing.T) {
	const manifestURI = "s3://old/m-puffin-deletes.avro"
	const puffinURI = "s3://old/data/dv-001.puffin"

	mf, raw := buildV2DeleteManifest(t, manifestURI, puffinURI, iceberg.EntryContentPosDeletes, 1)
	store := newMemStorage()
	store.objects[manifestURI] = raw

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	_, _, err := RewriteManifest(context.Background(), store, store, mf, mapping)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected ErrUnsupportedFeature, got %v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "Puffin") {
		t.Errorf("error message must mention Puffin to point operators at the deferred V3 DV work: %v", err)
	}
}

// buildV2DeleteManifest constructs a tiny in-memory V2 delete-manifest
// with one delete-file entry. content selects position vs equality
// deletes; deleteFileURI's extension drives the per-entry routing
// (.parquet → supported, .puffin → refused as V3 DV).
//
// Uses NewManifestWriter with WithManifestWriterContent so the OCF
// metadata's 'content' field matches the manifest list entry's
// content type. iceberg.WriteManifest only writes data-content, which
// would cause iceberg.ReadManifest to reject the file with a
// content-mismatch error.
func buildV2DeleteManifest(t *testing.T, manifestURI, deleteFileURI string, content iceberg.ManifestEntryContent, snapshotID int64) (iceberg.ManifestFile, []byte) {
	t.Helper()

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec

	dfb, err := iceberg.NewDataFileBuilder(spec, content, deleteFileURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 1, 100)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	if content == iceberg.EntryContentEqDeletes {
		dfb = dfb.EqualityFieldIDs([]int{1})
	}
	df := dfb.Build()
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
	mf = iceberg.NewManifestFile(mf.Version(), mf.FilePath(), mf.Length(), mf.PartitionSpecID(), mf.SnapshotID()).
		SequenceNum(1, 1).
		Content(iceberg.ManifestContentDeletes).
		AddedFiles(mf.AddedDataFiles()).
		AddedRows(mf.AddedRows()).
		ExistingFiles(mf.ExistingDataFiles()).
		ExistingRows(mf.ExistingRows()).
		DeletedFiles(mf.DeletedDataFiles()).
		DeletedRows(mf.DeletedRows()).
		KeyMetadata(mf.KeyMetadata()).
		Build()
	return mf, buf.Bytes()
}

// countingWriter mirrors iceberg-go's internal CountingWriter so we
// can pass an exact byte count to ToManifestFile without reaching into
// iceberg-go's internal package.
type countingWriter struct {
	n int64
	w interface {
		Write(p []byte) (int, error)
	}
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// TestRewriteManifestList_RoundTripV2 builds two V2 manifests, writes a
// manifest list referencing them, calls RewriteManifestList with a
// no-op mapping, and asserts the resulting list still references the
// same manifests with the same lengths.
func TestRewriteManifestList_RoundTripV2(t *testing.T) {
	mf1, _ := buildV2Manifest(t, "s3://x/m1.avro", "s3://x/data/f1.parquet", 1)
	mf2, _ := buildV2Manifest(t, "s3://x/m2.avro", "s3://x/data/f2.parquet", 1)

	store := newMemStorage()
	const listURI = "s3://x/snap-1.avro"

	seq := int64(1)
	hdr := ManifestListHeader{SnapshotID: 1, SequenceNumber: &seq}
	mapping := PrefixMapping{Source: "s3://nonmatching/", Target: "s3://other/"}
	newURI, err := RewriteManifestList(context.Background(), store, listURI, mapping, 2, hdr, []iceberg.ManifestFile{mf1, mf2})
	if err != nil {
		t.Fatalf("RewriteManifestList: %v", err)
	}
	if newURI != listURI {
		t.Errorf("no-op mapping should leave URI unchanged: %q", newURI)
	}

	written, ok := store.puts[listURI]
	if !ok {
		t.Fatalf("expected put at %q, got: %v", listURI, putKeys(store))
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(written))
	if err != nil {
		t.Fatalf("decode rewritten list: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("manifest count = %d, want 2", len(files))
	}
	if files[0].FilePath() != "s3://x/m1.avro" || files[1].FilePath() != "s3://x/m2.avro" {
		t.Errorf("manifest paths drifted: %v", []string{files[0].FilePath(), files[1].FilePath()})
	}
	for i, mf := range files {
		if mf.Length() <= 0 {
			t.Errorf("file[%d] length = %d, want > 0", i, mf.Length())
		}
	}
}

// TestSetDataFilePath_GuardsAPIBreakage ensures the reflection helper
// errors clearly if iceberg-go renames the Path field — the unit test
// catches drift before production does.
func TestSetDataFilePath_HappyPath(t *testing.T) {
	_, raw := buildV2Manifest(t, "s3://old/m.avro", "s3://old/data/f.parquet", 1)
	mfList := iceberg.NewManifestFile(2, "s3://old/m.avro", int64(len(raw)), 0, 1).Build()
	entries, err := iceberg.ReadManifest(mfList, bytes.NewReader(raw), false)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	df := entries[0].DataFile()
	if err := setDataFilePath(df, "s3://new/data/f.parquet"); err != nil {
		t.Fatalf("setDataFilePath: %v", err)
	}
	if df.FilePath() != "s3://new/data/f.parquet" {
		t.Errorf("file path after mutation = %q", df.FilePath())
	}
}

func TestSetDataFileReferencedDataFile_HappyPath(t *testing.T) {
	_, raw := buildV2Manifest(t, "s3://old/m.avro", "s3://old/data/f.parquet", 1)
	mfList := iceberg.NewManifestFile(2, "s3://old/m.avro", int64(len(raw)), 0, 1).Build()
	entries, err := iceberg.ReadManifest(mfList, bytes.NewReader(raw), false)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	df := entries[0].DataFile()
	if err := setDataFileReferencedDataFile(df, "s3://new/data/ref.parquet"); err != nil {
		t.Fatalf("setDataFileReferencedDataFile: %v", err)
	}
	got := df.ReferencedDataFile()
	if got == nil || *got != "s3://new/data/ref.parquet" {
		t.Errorf("referenced_data_file after mutation = %v", got)
	}
}

func putKeys(s *memStorage) []string {
	out := make([]string, 0, len(s.puts))
	for k := range s.puts {
		out = append(out, k)
	}
	return out
}
