package rewrite

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// Fast regression tests for what real writers produce and the hand-built
// fixtures did not. test/real_writers_e2e_test.go covers the same findings
// against tables really written by DuckDB and pyiceberg.

func gzipped(t *testing.T, s string) []byte {
	t.Helper()
	b, err := deflateMetadataJSON([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseMetadataJSON_Gzip(t *testing.T) {
	m, err := ParseMetadataJSON(gzipped(t, `{"format-version":2,"location":"s3://old/t"}`))
	if err != nil {
		t.Fatalf("ParseMetadataJSON: %v", err)
	}
	if m.Location != "s3://old/t" {
		t.Errorf("Location = %q", m.Location)
	}
}

func TestParseMetadataJSON_CorruptGzip(t *testing.T) {
	raw := gzipped(t, `{"format-version":2}`)
	if _, err := ParseMetadataJSON(raw[:len(raw)-4]); err == nil {
		t.Fatal("truncated gzip parsed without error")
	}
}

func TestRewriteMetadataJSON_GzipStaysGzip(t *testing.T) {
	const oldURI = "s3://old/t/metadata/00001-x.gz.metadata.json"
	st := newMemStorage()
	st.objects[oldURI] = gzipped(t, `{"format-version":2,"location":"s3://old/t"}`)
	newURI, _, err := RewriteMetadataJSON(context.Background(), st, st, oldURI, PrefixMapping{Source: "s3://old/", Target: "s3://new/"})
	if err != nil {
		t.Fatalf("RewriteMetadataJSON: %v", err)
	}
	if newURI != "s3://new/t/metadata/00001-x.gz.metadata.json" {
		t.Fatalf("new URI = %q", newURI)
	}
	out := st.puts[newURI]
	if len(out) < 2 || out[0] != 0x1f || out[1] != 0x8b {
		t.Fatalf("rewritten metadata is not gzip, but its name says it is: %q", out[:min(len(out), 16)])
	}
	m, err := ParseMetadataJSON(out)
	if err != nil {
		t.Fatalf("parse rewritten: %v", err)
	}
	if m.Location != "s3://new/t" {
		t.Errorf("Location = %q", m.Location)
	}
}

func TestRewriteMetadataJSON_PlainStaysPlain(t *testing.T) {
	const oldURI = "s3://old/t/metadata/v1.metadata.json"
	st := newMemStorage()
	st.objects[oldURI] = []byte(`{"format-version":2,"location":"s3://old/t"}`)
	newURI, _, err := RewriteMetadataJSON(context.Background(), st, st, oldURI, PrefixMapping{Source: "s3://old/", Target: "s3://new/"})
	if err != nil {
		t.Fatalf("RewriteMetadataJSON: %v", err)
	}
	if !json.Valid(st.puts[newURI]) {
		t.Fatalf("rewritten metadata is not plain JSON: %q", st.puts[newURI])
	}
}

// Snapshot IDs are random 63-bit values. Every number must survive verbatim.
func TestRewriteMetadataJSON_PreservesLargeIntegers(t *testing.T) {
	const oldURI = "s3://old/t/metadata/v1.metadata.json"
	const id, parent = "6755982795203693628", "9007199254740993" // Both above 2^53.
	st := newMemStorage()
	st.objects[oldURI] = []byte(`{"format-version":2,"location":"s3://old/t","current-snapshot-id":` + id +
		`,"snapshots":[{"snapshot-id":` + id + `,"parent-snapshot-id":` + parent + `,"timestamp-ms":1700000000000}],` +
		`"refs":{"main":{"snapshot-id":` + id + `,"type":"branch"}}}`)
	newURI, _, err := RewriteMetadataJSON(context.Background(), st, st, oldURI, PrefixMapping{Source: "s3://old/", Target: "s3://new/"})
	if err != nil {
		t.Fatalf("RewriteMetadataJSON: %v", err)
	}
	out := string(st.puts[newURI])
	if n := strings.Count(out, id); n != 3 {
		t.Errorf("snapshot id appears %d times, want 3:\n%s", n, out)
	}
	if !strings.Contains(out, parent) {
		t.Errorf("parent snapshot id lost:\n%s", out)
	}
}

func TestRewriteMetadataJSON_RejectsTrailingData(t *testing.T) {
	const oldURI = "s3://old/t/metadata/v1.metadata.json"
	st := newMemStorage()
	st.objects[oldURI] = []byte(`{"location":"s3://old/t"} {"location":"s3://other/"}`)
	if _, _, err := RewriteMetadataJSON(context.Background(), st, st, oldURI, PrefixMapping{Source: "s3://old/", Target: "s3://new/"}); err == nil {
		t.Fatal("trailing data accepted")
	}
}

func ptr(v int64) *int64 { return &v }

// DuckDB's manifest lists carry no format-version or any other OCF key.
func TestReadManifestListHeader_NoKeys(t *testing.T) {
	var buf bytes.Buffer
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, &buf, 7, nil, &seq, 0, nil); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	for _, key := range []string{"format-version", "snapshot-id", "parent-snapshot-id", "sequence-number"} {
		raw = editOCF(t, raw, key, nil)
	}
	hdr, version, err := readManifestListHeader(raw)
	if err != nil {
		t.Fatalf("readManifestListHeader: %v", err)
	}
	if version != 0 || hdr.SnapshotID != nil || hdr.SequenceNumber != nil || hdr.ParentSnapshotID != nil {
		t.Errorf("version %d, header %+v: want everything absent", version, hdr)
	}
}

func TestResolveManifestListHeader_FillsFromMetadata(t *testing.T) {
	// A DuckDB list: no header keys at all.
	snap := SnapshotJSON{SnapshotID: 7, ParentSnapshotID: ptr(6), SequenceNumber: ptr(3)}
	hdr, version, err := resolveManifestListHeader(ManifestListHeader{}, 0, snap, 2, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if version != 2 || *hdr.SnapshotID != 7 || *hdr.ParentSnapshotID != 6 || *hdr.SequenceNumber != 3 {
		t.Errorf("got version %d header %+v", version, hdr)
	}
}

func TestResolveManifestListHeader_VersionFromListSchema(t *testing.T) {
	// A v2-era list in a table since upgraded to v3 stays v2.
	v2 := iceberg.NewManifestFile(2, "s3://old/m.avro", 1, 0, 7).Build()
	_, version, err := resolveManifestListHeader(ManifestListHeader{}, 0, SnapshotJSON{SnapshotID: 7, SequenceNumber: ptr(1)}, 3, []iceberg.ManifestFile{v2})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2", version)
	}
}

func TestResolveManifestListHeader_RefusesContradiction(t *testing.T) {
	for name, hdr := range map[string]ManifestListHeader{
		"snapshot-id":        {SnapshotID: ptr(8)},
		"sequence-number":    {SequenceNumber: ptr(9)},
		"parent-snapshot-id": {ParentSnapshotID: ptr(9)},
		"first-row-id":       {FirstRowID: ptr(9)},
	} {
		snap := SnapshotJSON{SnapshotID: 7, ParentSnapshotID: ptr(6), SequenceNumber: ptr(3), FirstRowID: ptr(0)}
		_, _, err := resolveManifestListHeader(hdr, 2, snap, 2, nil)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestResolveManifestListHeader_V3RefusesLostLineage(t *testing.T) {
	snap := SnapshotJSON{SnapshotID: 7, SequenceNumber: ptr(1)}
	if _, _, err := resolveManifestListHeader(ManifestListHeader{}, 3, snap, 3, nil); !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("no first-row-id anywhere: err = %v", err)
	}
	snap.FirstRowID = ptr(0)
	noRowID := iceberg.NewManifestFile(3, "s3://old/m.avro", 1, 0, 7).Build()
	if _, _, err := resolveManifestListHeader(ManifestListHeader{}, 3, snap, 3, []iceberg.ManifestFile{noRowID}); !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("data manifest without first_row_id: err = %v", err)
	}
}

// statusManifest writes a manifest holding one entry of each status.
func statusManifest(t *testing.T, uri string, snapshotID int64) (iceberg.ManifestFile, []byte) {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := *iceberg.UnpartitionedSpec
	entry := func(status iceberg.ManifestEntryStatus, path string, entrySnapshot int64) iceberg.ManifestEntry {
		b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile, map[int]any{}, nil, nil, 10, 100)
		if err != nil {
			t.Fatal(err)
		}
		sid := entrySnapshot
		return iceberg.NewManifestEntryBuilder(status, &sid, b.Build()).SequenceNum(1).FileSequenceNum(1).Build()
	}
	buf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(uri, buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{
		entry(iceberg.EntryStatusADDED, "s3://old/t/data/added.parquet", snapshotID),
		entry(iceberg.EntryStatusEXISTING, "s3://old/t/data/existing.parquet", snapshotID-1),
		entry(iceberg.EntryStatusDELETED, "s3://old/t/data/deleted.parquet", snapshotID),
	})
	if err != nil {
		t.Fatal(err)
	}
	return iceberg.NewManifestFile(2, uri, mf.Length(), 0, snapshotID).SequenceNum(1, 1).
		AddedFiles(1).ExistingFiles(1).DeletedFiles(1).Build(), buf.Bytes()
}

// A DELETED entry rewritten as ADDED resurrects its data file: the deleted rows
// come back, and nothing reports an error.
func TestRewriteManifest_KeepsEntryStatus(t *testing.T) {
	const uri = "s3://old/t/metadata/m.avro"
	const snapshotID int64 = 1234567890123456789
	mf, raw := statusManifest(t, uri, snapshotID)
	st := newMemStorage()
	st.objects[uri] = raw
	newMF, _, err := RewriteManifest(context.Background(), st, st, mf, PrefixMapping{Source: "s3://old/", Target: "s3://new/"})
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}
	entries, err := iceberg.ReadManifest(newMF, bytes.NewReader(st.puts[newMF.FilePath()]), false)
	if err != nil {
		t.Fatalf("read rewritten: %v", err)
	}
	want := map[string]iceberg.ManifestEntryStatus{
		"s3://new/t/data/added.parquet":    iceberg.EntryStatusADDED,
		"s3://new/t/data/existing.parquet": iceberg.EntryStatusEXISTING,
		"s3://new/t/data/deleted.parquet":  iceberg.EntryStatusDELETED,
	}
	if len(entries) != len(want) {
		t.Fatalf("%d entries, want %d", len(entries), len(want))
	}
	for _, e := range entries {
		if got := e.Status(); got != want[e.DataFile().FilePath()] {
			t.Errorf("%s: status %v, want %v", e.DataFile().FilePath(), got, want[e.DataFile().FilePath()])
		}
		if e.DataFile().FilePath() == "s3://new/t/data/existing.parquet" && e.SnapshotID() != snapshotID-1 {
			t.Errorf("EXISTING entry snapshot = %d, want %d", e.SnapshotID(), snapshotID-1)
		}
	}
	if newMF.AddedDataFiles() != 1 || newMF.ExistingDataFiles() != 1 || newMF.DeletedDataFiles() != 1 {
		t.Errorf("list counts added=%d existing=%d deleted=%d", newMF.AddedDataFiles(), newMF.ExistingDataFiles(), newMF.DeletedDataFiles())
	}
}

// editOCF returns raw with one header key replaced, or removed if value is nil.
func editOCF(t *testing.T, raw []byte, key string, value []byte) []byte {
	t.Helper()
	pairs, end, ok := parseOCFMetadata(raw)
	if !ok {
		t.Fatal("parse OCF header")
	}
	out := append([]byte{}, raw[:len(ocfMagic)]...)
	kept := pairs[:0]
	for _, kv := range pairs {
		if string(kv[0]) == key {
			if value == nil {
				continue
			}
			kv[1] = value
		}
		kept = append(kept, kv)
	}
	out = binary.AppendVarint(out, int64(len(kept)))
	for _, kv := range kept {
		for _, s := range kv {
			out = binary.AppendVarint(out, int64(len(s)))
			out = append(out, s...)
		}
	}
	out = binary.AppendVarint(out, 0)
	return append(out, raw[end:]...)
}

// pyiceberg writes no schema-id key on its manifests.
func TestRewriteManifest_NoSchemaIDHeader(t *testing.T) {
	const uri = "s3://old/t/metadata/m.avro"
	mf, raw := statusManifest(t, uri, 5)
	st := newMemStorage()
	st.objects[uri] = editOCF(t, raw, "schema-id", nil)
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, PrefixMapping{Source: "s3://old/", Target: "s3://new/"}); err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}
}

// DuckDB <= 1.5.4 labels delete manifests content=data.
func TestRewriteManifest_MislabelledDeleteManifest(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	const delURI = "s3://old/t/data/eq.parquet"
	mf, raw := buildV2DeleteManifest(t, manURI, delURI, iceberg.EntryContentEqDeletes, 5)
	st := newMemStorage()
	st.objects[manURI] = editOCF(t, raw, "content", []byte("data"))
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}

	if _, err := WalkManifest(context.Background(), st, mf, mapping); err != nil {
		t.Fatalf("WalkManifest (dry run): %v", err)
	}
	newMF, _, err := RewriteManifest(context.Background(), st, st, mf, mapping)
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}
	if newMF.ManifestContent() != iceberg.ManifestContentDeletes {
		t.Errorf("list entry content = %v", newMF.ManifestContent())
	}
	pairs, _, ok := parseOCFMetadata(st.puts[newMF.FilePath()])
	if !ok {
		t.Fatal("parse rewritten header")
	}
	for _, kv := range pairs {
		if string(kv[0]) == "content" && string(kv[1]) != "deletes" {
			t.Errorf("rewritten header content = %q", kv[1])
		}
	}
}

// Only the known bug is excused: a data manifest labelled deletes stays an error.
func TestRewriteManifest_OtherContentMismatchStillRefused(t *testing.T) {
	const uri = "s3://old/t/metadata/m.avro"
	mf, raw := buildV2Manifest(t, uri, "s3://old/t/data/a.parquet", 5)
	st := newMemStorage()
	st.objects[uri] = editOCF(t, raw, "content", []byte("deletes"))
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, PrefixMapping{Source: "s3://old/", Target: "s3://new/"}); err == nil {
		t.Fatal("data manifest labelled deletes was accepted")
	}
}

// Every truncation of a header must come back as "not parseable", not a panic.
func TestParseOCFMetadata_Truncated(t *testing.T) {
	_, raw := buildV2Manifest(t, "s3://old/t/metadata/m.avro", "s3://old/t/data/a.parquet", 5)
	_, end, ok := parseOCFMetadata(raw)
	if !ok {
		t.Fatal("parse complete header")
	}
	for i := range end {
		if _, _, ok := parseOCFMetadata(raw[:i]); ok {
			t.Fatalf("truncated header of %d bytes parsed", i)
		}
	}
}

// A second data manifest must keep its first_row_id, not be given a fresh one.
func TestRewriteManifest_KeepsFirstRowID(t *testing.T) {
	const uri = "s3://old/t/metadata/m2.avro"
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := *iceberg.UnpartitionedSpec
	b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "s3://old/t/data/b.parquet", iceberg.ParquetFile, map[int]any{}, nil, nil, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	sid := int64(5)
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, b.Build()).SequenceNum(2).FileSequenceNum(2).Build()
	buf := &bytes.Buffer{}
	if _, err := iceberg.WriteManifest(uri, buf, 3, spec, schema, sid, []iceberg.ManifestEntry{entry}); err != nil {
		t.Fatal(err)
	}
	mf := iceberg.NewManifestFile(3, uri, int64(buf.Len()), 0, sid).SequenceNum(2, 2).AddedFiles(1).AddedRows(100).Build()
	if err := setManifestFirstRowID(mf, ptr(1000)); err != nil {
		t.Fatal(err)
	}

	st := newMemStorage()
	st.objects[uri] = buf.Bytes()
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	newMF, _, err := RewriteManifest(context.Background(), st, st, mf, mapping)
	if err != nil {
		t.Fatalf("RewriteManifest: %v", err)
	}
	if got := newMF.FirstRowID(); got == nil || *got != 1000 {
		t.Fatalf("rewritten manifest first_row_id = %v, want 1000", got)
	}

	const listURI = "s3://old/t/metadata/snap.avro"
	seq := int64(2)
	// The snapshot's own first-row-id is where it starts allocating, not the
	// value of this carried-forward manifest.
	hdr := ManifestListHeader{SnapshotID: &sid, SequenceNumber: &seq, FirstRowID: ptr(5000)}
	newList, err := RewriteManifestList(context.Background(), st, listURI, mapping, 3, hdr, []iceberg.ManifestFile{newMF})
	if err != nil {
		t.Fatalf("RewriteManifestList: %v", err)
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(st.puts[newList]))
	if err != nil {
		t.Fatalf("read list: %v", err)
	}
	if got := files[0].FirstRowID(); got == nil || *got != 1000 {
		t.Errorf("first_row_id in rewritten list = %v, want 1000", got)
	}
}
