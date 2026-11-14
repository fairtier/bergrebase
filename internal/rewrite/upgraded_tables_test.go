package rewrite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/twmb/avro/ocf"
)

var testMapping = PrefixMapping{Source: "s3://old/", Target: "s3://new/"}

type testEntry struct {
	status   iceberg.ManifestEntryStatus
	path     string
	snapshot int64
	seq      int64
	content  iceberg.ManifestEntryContent
	ref      string
	size     int64 // file_size_in_bytes; 0 means 100
}

// writeTestManifest encodes a manifest in the given format version and returns
// a list-level descriptor for it.
func writeTestManifest(t *testing.T, version int, uri string, snapshotID int64, content iceberg.ManifestContent, entries []testEntry) (iceberg.ManifestFile, []byte) {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := *iceberg.UnpartitionedSpec
	buf := &bytes.Buffer{}
	w, err := iceberg.NewManifestWriter(version, buf, spec, schema, snapshotID, iceberg.WithManifestWriterContent(content))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		size := e.size
		if size == 0 {
			size = 100
		}
		b, err := iceberg.NewDataFileBuilder(spec, e.content, e.path, iceberg.ParquetFile, map[int]any{}, nil, nil, 10, size)
		if err != nil {
			t.Fatal(err)
		}
		if e.ref != "" {
			b = b.ReferencedDataFile(e.ref)
		}
		sid := e.snapshot
		entry := iceberg.NewManifestEntryBuilder(e.status, &sid, b.Build()).SequenceNum(e.seq).FileSequenceNum(e.seq).Build()
		switch e.status {
		case iceberg.EntryStatusADDED:
			err = w.Add(entry)
		case iceberg.EntryStatusEXISTING:
			err = w.Existing(entry)
		default:
			err = w.Delete(entry)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	mf := iceberg.NewManifestFile(version, uri, int64(buf.Len()), 0, snapshotID).SequenceNum(1, 1).Content(content).AddedRows(10).Build()
	return mf, buf.Bytes()
}

// setEntrySnapshot re-encodes a manifest with every entry's snapshot_id changed,
// which ManifestWriter cannot produce for ADDED and DELETED entries.
func setEntrySnapshot(t *testing.T, raw []byte, snapshotID int64) []byte {
	t.Helper()
	return editEntries(t, raw, func(row map[string]any) {
		row["snapshot_id"] = snapshotID
	})
}

// editEntries re-encodes a manifest with edit applied to each raw entry.
func editEntries(t *testing.T, raw []byte, edit func(map[string]any)) []byte {
	t.Helper()
	rd, err := ocf.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rd.Close() }()
	extra := map[string][]byte{}
	for k, v := range rd.Metadata() {
		if !strings.HasPrefix(k, "avro.") {
			extra[k] = v
		}
	}
	var out bytes.Buffer
	enc, err := ocf.NewWriter(&out, rd.Schema(), ocf.WithMetadata(extra))
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row map[string]any
		if err := rd.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		edit(row)
		if err := enc.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// countingStorage counts PUTs per URI.
type countingStorage struct {
	*memStorage
	puts map[string]int
}

func (c *countingStorage) PutObject(ctx context.Context, uri string, body []byte) error {
	c.puts[uri]++
	return c.memStorage.PutObject(ctx, uri, body)
}

// upgradedTable builds a v2 table upgraded to v3: one v2 manifest, referenced by
// the v2 list of snapshot 10 and the v3 list of snapshot 20, which also has a
// v3 manifest of its own. It returns the metadata URI.
func upgradedTable(t *testing.T, st *memStorage) string {
	t.Helper()
	const m2, m3 = "s3://old/t/metadata/m2.avro", "s3://old/t/metadata/m3.avro"
	v2mf, v2raw := writeTestManifest(t, 2, m2, 10, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/a.parquet", snapshot: 10, seq: 1, content: iceberg.EntryContentData},
	})
	v3mf, v3raw := writeTestManifest(t, 3, m3, 20, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/b.parquet", snapshot: 20, seq: 2, content: iceberg.EntryContentData},
	})
	st.objects[m2], st.objects[m3] = v2raw, v3raw

	writeList := func(uri string, version int, id, seq, firstRow int64, files ...iceberg.ManifestFile) {
		buf := &bytes.Buffer{}
		if err := iceberg.WriteManifestList(version, buf, id, nil, &seq, firstRow, files); err != nil {
			t.Fatal(err)
		}
		st.objects[uri] = buf.Bytes()
	}
	writeList("s3://old/t/metadata/l1.avro", 2, 10, 1, 0, v2mf)
	writeList("s3://old/t/metadata/l2.avro", 3, 20, 2, 0, v2mf, v3mf)

	const meta = "s3://old/t/metadata/v.metadata.json"
	// Newest first, as Lakekeeper can return them: the shared manifest is reached
	// through the v3 list before the v2 one, the order that exposes a rewrite
	// taking its version from the list.
	st.objects[meta] = []byte(`{"format-version":3,"location":"s3://old/t","current-snapshot-id":20,"next-row-id":20,"snapshots":[
		{"snapshot-id":20,"parent-snapshot-id":10,"sequence-number":2,"first-row-id":0,"manifest-list":"s3://old/t/metadata/l2.avro"},
		{"snapshot-id":10,"sequence-number":1,"manifest-list":"s3://old/t/metadata/l1.avro"}]}`)
	return meta
}

// assertListsMatchObjects requires manifest_length to equal the object's size in
// every list, and no list to reference a manifest newer than itself.
func assertListsMatchObjects(t *testing.T, st *memStorage, lists map[string]int) {
	t.Helper()
	for uri, listVersion := range lists {
		files, err := iceberg.ReadManifestList(bytes.NewReader(st.objects[uri]))
		if err != nil {
			t.Fatalf("read list %s: %v", uri, err)
		}
		for _, mf := range files {
			body := st.objects[mf.FilePath()]
			if mf.Length() != int64(len(body)) {
				t.Errorf("%s: manifest_length of %s is %d, object is %d", uri, mf.FilePath(), mf.Length(), len(body))
			}
			header, err := manifestHeader(body)
			if err != nil {
				t.Fatalf("header of %s: %v", mf.FilePath(), err)
			}
			if header.version > listVersion {
				t.Errorf("v%d list %s references v%d manifest %s", listVersion, uri, header.version, mf.FilePath())
			}
		}
	}
}

func TestRewriteTable_UpgradedTable(t *testing.T) {
	st := newMemStorage()
	meta := upgradedTable(t, st)
	store := &countingStorage{memStorage: st, puts: map[string]int{}}
	eng := &Engine{Source: st, Target: store, Opts: Options{Mapping: testMapping}}
	res, err := eng.RewriteTable(context.Background(), meta)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	assertListsMatchObjects(t, st, map[string]int{"s3://new/t/metadata/l1.avro": 2, "s3://new/t/metadata/l2.avro": 3})
	if n := store.puts["s3://new/t/metadata/m2.avro"]; n != 1 {
		t.Errorf("shared manifest written %d times, want 1", n)
	}
	header, err := manifestHeader(st.objects["s3://new/t/metadata/m2.avro"])
	if err != nil {
		t.Fatal(err)
	}
	if header.version != 2 {
		t.Errorf("v2 manifest rewritten as v%d", header.version)
	}

	// The list-level first_row_id belongs to each list.
	readFiles := func(uri string) []iceberg.ManifestFile {
		files, err := iceberg.ReadManifestList(bytes.NewReader(st.objects[uri]))
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	if got := readFiles("s3://new/t/metadata/l1.avro")[0].FirstRowID(); got != nil {
		t.Errorf("v2 list entry has first_row_id %d", *got)
	}
	if got := readFiles("s3://new/t/metadata/l2.avro")[0].FirstRowID(); got == nil || *got != 0 {
		t.Errorf("v3 list entry first_row_id = %v, want 0", got)
	}

	// The real consequence: the result has to be movable again.
	second := &Engine{Source: st, Target: st, Opts: Options{Mapping: PrefixMapping{Source: "s3://new/", Target: "s3://newer/"}}}
	if _, err := second.RewriteTable(context.Background(), res.NewMetadataLocation); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	assertListsMatchObjects(t, st, map[string]int{"s3://newer/t/metadata/l1.avro": 2, "s3://newer/t/metadata/l2.avro": 3})
}

func TestWalkManifest_RefusesWhatLiveRefuses_EntrySnapshot(t *testing.T) {
	const uri = "s3://old/t/metadata/m.avro"
	for name, status := range map[string]iceberg.ManifestEntryStatus{"added": iceberg.EntryStatusADDED, "deleted": iceberg.EntryStatusDELETED} {
		t.Run(name, func(t *testing.T) {
			mf, raw := writeTestManifest(t, 2, uri, 5, iceberg.ManifestContentData, []testEntry{
				{status: status, path: "s3://old/t/data/a.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentData},
			})
			st := newMemStorage()
			st.objects[uri] = setEntrySnapshot(t, raw, 4)
			if _, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping); !errors.Is(err, ErrUnsupportedFeature) {
				t.Errorf("live: err = %v", err)
			}
			if _, err := WalkManifest(context.Background(), st, mf, testMapping); !errors.Is(err, ErrUnsupportedFeature) {
				t.Errorf("dry run: err = %v", err)
			}
			if len(st.puts) != 0 {
				t.Errorf("refused manifest wrote %v", putKeys(st))
			}
		})
	}
}

// The encoder has refusals of its own, here a DELETED entry with no sequence
// number. Dry run goes through the same encoder.
func TestWalkManifest_RefusesWhatLiveRefuses_EncoderError(t *testing.T) {
	const uri = "s3://old/t/metadata/m.avro"
	mf, raw := writeTestManifest(t, 2, uri, 5, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusDELETED, path: "s3://old/t/data/a.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentData},
	})
	st := newMemStorage()
	st.objects[uri] = editEntries(t, raw, func(row map[string]any) {
		row["sequence_number"] = nil
		row["file_sequence_number"] = nil
	})
	_, _, liveErr := RewriteManifest(context.Background(), st, st, mf, testMapping)
	if liveErr == nil {
		t.Fatal("the encoder no longer refuses this; the test needs another case")
	}
	if _, err := WalkManifest(context.Background(), st, mf, testMapping); err == nil {
		t.Errorf("dry run is clean, live run refuses: %v", liveErr)
	}
}

func TestWalkManifest_RefusesWhatLiveRefuses_OutsidePrefixManifest(t *testing.T) {
	const uri = "s3://elsewhere/t/metadata/m.avro"
	mf, raw := writeTestManifest(t, 2, uri, 5, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/a.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentData},
	})
	st := newMemStorage()
	st.objects[uri] = raw
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping); !errors.Is(err, ErrOutsidePrefix) {
		t.Errorf("live: err = %v", err)
	}
	if _, err := WalkManifest(context.Background(), st, mf, testMapping); !errors.Is(err, ErrOutsidePrefix) {
		t.Errorf("dry run: err = %v", err)
	}
}

func TestRewriteTable_DryRunRefusesOutsidePrefixList(t *testing.T) {
	const m = "s3://old/t/metadata/m.avro"
	mf, raw := writeTestManifest(t, 2, m, 5, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/a.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentData},
	})
	st := newMemStorage()
	st.objects[m] = raw
	seq := int64(1)
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(2, buf, 5, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatal(err)
	}
	st.objects["s3://elsewhere/l.avro"] = buf.Bytes()
	const meta = "s3://old/t/metadata/v.metadata.json"
	st.objects[meta] = []byte(`{"format-version":2,"location":"s3://old/t","current-snapshot-id":5,"snapshots":[{"snapshot-id":5,"sequence-number":1,"manifest-list":"s3://elsewhere/l.avro"}]}`)
	for _, dry := range []bool{true, false} {
		eng := &Engine{Source: st, Target: st, Opts: Options{Mapping: testMapping, DryRun: dry}}
		if _, err := eng.RewriteTable(context.Background(), meta); !errors.Is(err, ErrOutsidePrefix) {
			t.Errorf("dry=%v: err = %v", dry, err)
		}
	}
}

// A delete file outside the prefix can't be rewritten; if its rows name files
// that are moving, the delete would stop applying and the rows would come back.
func TestPositionDeleteOutsidePrefix_RowsInsidePrefix_Refused(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	const delURI = "s3://elsewhere/deletes/pos.parquet"
	mf, raw := writeTestManifest(t, 2, manURI, 5, iceberg.ManifestContentDeletes, []testEntry{
		{status: iceberg.EntryStatusADDED, path: delURI, snapshot: 5, seq: 1, content: iceberg.EntryContentPosDeletes},
	})
	st := newMemStorage()
	st.objects[manURI] = raw
	st.objects[delURI] = encodePosDeleteBody(t, []string{"s3://old/t/data/a.parquet"})
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping); !errors.Is(err, ErrOutsidePrefix) {
		t.Errorf("live: err = %v", err)
	}
	if _, err := WalkManifest(context.Background(), st, mf, testMapping); !errors.Is(err, ErrOutsidePrefix) {
		t.Errorf("dry run: err = %v", err)
	}
	if len(st.puts) != 0 {
		t.Errorf("refused manifest wrote %v", putKeys(st))
	}
}

// Rows that stay put are fine: nothing about that delete changes.
func TestPositionDeleteOutsidePrefix_RowsOutsidePrefix_Allowed(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	const delURI = "s3://elsewhere/deletes/pos.parquet"
	mf, raw := writeTestManifest(t, 2, manURI, 5, iceberg.ManifestContentDeletes, []testEntry{
		{status: iceberg.EntryStatusADDED, path: delURI, snapshot: 5, seq: 1, content: iceberg.EntryContentPosDeletes},
	})
	st := newMemStorage()
	st.objects[manURI] = raw
	body := encodePosDeleteBody(t, []string{"s3://elsewhere/data/a.parquet"})
	st.objects[delURI] = body
	if _, err := WalkManifest(context.Background(), st, mf, testMapping); err != nil {
		t.Errorf("dry run: %v", err)
	}
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping); err != nil {
		t.Errorf("live: %v", err)
	}
	if !bytes.Equal(st.objects[delURI], body) {
		t.Error("delete file outside the prefix was changed")
	}
}

// Expiry removes the file of a DELETED position-delete entry while the tombstone
// stays in a retained manifest. That must not block the table.
func TestPositionDeleteTombstone_MissingBody(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	mf, raw := writeTestManifest(t, 2, manURI, 5, iceberg.ManifestContentDeletes, []testEntry{
		{status: iceberg.EntryStatusDELETED, path: "s3://old/t/data/expired-pos.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentPosDeletes},
	})
	st := newMemStorage()
	st.objects[manURI] = raw
	if _, err := WalkManifest(context.Background(), st, mf, testMapping); err != nil {
		t.Errorf("dry run: %v", err)
	}
	newMF, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping)
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	entries, err := iceberg.ReadManifest(newMF, bytes.NewReader(st.objects[newMF.FilePath()]), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Status() != iceberg.EntryStatusDELETED || entries[0].DataFile().FilePath() != "s3://new/t/data/expired-pos.parquet" {
		t.Errorf("tombstone rewritten wrongly: %+v", entries)
	}
}

// A live position-delete entry whose body is gone is still an error.
func TestPositionDeleteLive_MissingBody_StillFails(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	mf, raw := writeTestManifest(t, 2, manURI, 5, iceberg.ManifestContentDeletes, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/missing-pos.parquet", snapshot: 5, seq: 1, content: iceberg.EntryContentPosDeletes},
	})
	st := newMemStorage()
	st.objects[manURI] = raw
	if _, _, err := RewriteManifest(context.Background(), st, st, mf, testMapping); err == nil {
		t.Error("live delete entry with a missing body was accepted")
	}
}

func validationFixture(t *testing.T, st *memStorage) string {
	t.Helper()
	const m = "s3://new/t/metadata/m.avro"
	mf, raw := writeTestManifest(t, 2, m, 900, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusDELETED, path: "s3://new/t/data/tombstone.parquet", snapshot: 900, seq: 1, content: iceberg.EntryContentData},
		{status: iceberg.EntryStatusADDED, path: "s3://new/t/data/live.parquet", snapshot: 900, seq: 1, content: iceberg.EntryContentData},
	})
	st.objects[m] = raw
	seq := int64(1)
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(2, buf, 900, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatal(err)
	}
	st.objects["s3://new/t/metadata/l.avro"] = buf.Bytes()
	const meta = "s3://new/t/metadata/v.metadata.json"
	st.objects[meta] = []byte(`{"format-version":2,"location":"s3://new/t","current-snapshot-id":900,"snapshots":[{"snapshot-id":900,"sequence-number":1,"manifest-list":"s3://new/t/metadata/l.avro"}]}`)
	return meta
}

func TestValidateAfterSwap_TombstoneFirst_NoFalseFailure(t *testing.T) {
	st := newMemStorage()
	meta := validationFixture(t, st)
	st.objects["s3://new/t/data/live.parquet"] = make([]byte, 100)
	if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); err != nil {
		t.Errorf("tombstone's file is legitimately gone, live file present: %v", err)
	}
}

func TestValidateAfterSwap_TombstoneFirst_NoFalsePass(t *testing.T) {
	st := newMemStorage()
	meta := validationFixture(t, st)
	st.objects["s3://new/t/data/tombstone.parquet"] = []byte("x")
	if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); !errors.Is(err, ErrValidationFailed) {
		t.Errorf("the only live file is missing: err = %v", err)
	}
}

// iceberg-go's v2 schema has no field 143, so V2 drops referenced_data_file; the
// hit has to say so. V3 keeps it, rewritten.
func TestReferencedDataFile_DroppedOnV2_KeptOnV3(t *testing.T) {
	const manURI = "s3://old/t/metadata/d.avro"
	const delURI = "s3://old/t/data/pos.parquet"
	const ref = "s3://old/t/data/a.parquet"
	for _, tc := range []struct {
		version int
		wantNew string
	}{{2, ""}, {3, "s3://new/t/data/a.parquet"}} {
		// iceberg-go will not write field 143 on V2, but DuckDB 2.0 does. A v3
		// encoding relabelled v2 has that shape: the field, in a v2 manifest.
		_, raw := writeTestManifest(t, 3, manURI, 5, iceberg.ManifestContentDeletes, []testEntry{
			{status: iceberg.EntryStatusADDED, path: delURI, snapshot: 5, seq: 1, content: iceberg.EntryContentPosDeletes, ref: ref},
		})
		raw = editOCF(t, raw, "format-version", []byte(strconv.Itoa(tc.version)))
		mf := iceberg.NewManifestFile(tc.version, manURI, int64(len(raw)), 0, 5).SequenceNum(1, 1).Content(iceberg.ManifestContentDeletes).Build()
		st := newMemStorage()
		st.objects[manURI] = raw
		st.objects[delURI] = encodePosDeleteBody(t, []string{ref})
		refHit := func(hits []PathHit) *PathHit {
			for i := range hits {
				if strings.HasSuffix(hits[i].Field, "referenced_data_file") {
					return &hits[i]
				}
			}
			return nil
		}
		walked, err := WalkManifest(context.Background(), st, mf, testMapping)
		if err != nil {
			t.Fatalf("v%d walk: %v", tc.version, err)
		}
		if h := refHit(walked); h == nil || h.New != tc.wantNew {
			t.Errorf("v%d dry run hit = %+v, want New %q", tc.version, h, tc.wantNew)
		}
		newMF, hits, err := RewriteManifest(context.Background(), st, st, mf, testMapping)
		if err != nil {
			t.Fatalf("v%d rewrite: %v", tc.version, err)
		}
		if h := refHit(hits); h == nil || h.New != tc.wantNew {
			t.Errorf("v%d live hit = %+v, want New %q", tc.version, h, tc.wantNew)
		}
		entries, err := iceberg.ReadManifest(newMF, bytes.NewReader(st.objects[newMF.FilePath()]), false)
		if err != nil {
			t.Fatal(err)
		}
		got := entries[0].DataFile().ReferencedDataFile()
		if tc.wantNew == "" && got != nil {
			t.Errorf("v2 manifest kept referenced_data_file %q: the dropped claim is out of date", *got)
		}
		if tc.wantNew != "" && (got == nil || *got != tc.wantNew) {
			t.Errorf("v3 referenced_data_file = %v, want %q", got, tc.wantNew)
		}
	}
}
