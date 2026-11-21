package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// validateMapping is the mapping the s3://b/ fixtures below were rebased
// with: none of their URIs is under its source prefix.
var validateMapping = PrefixMapping{Source: "s3://src/", Target: "s3://b/"}

// TestValidateAfterSwap_HappyPath uses a self-consistent V2 fixture
// (metadata.json -> manifest list -> manifest -> data file URI HEAD'd
// at the data file URI) and asserts no error.
func TestValidateAfterSwap_HappyPath(t *testing.T) {
	store := newMemStorage()
	const (
		dataURI     = "s3://b/iceberg/db/orders/data/file-1.parquet"
		manifestURI = "s3://b/iceberg/db/orders/metadata/m1.avro"
		listURI     = "s3://b/iceberg/db/orders/metadata/snap-1.avro"
		metaURI     = "s3://b/iceberg/db/orders/metadata/v1.metadata.json"
		snapID      = int64(42)
	)

	mf, manRaw := buildV2Manifest(t, manifestURI, dataURI, snapID)
	store.objects[manifestURI] = manRaw
	store.objects[dataURI] = make([]byte, 1024) // buildV2Manifest's declared size

	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	store.objects[listURI] = listBuf.Bytes()

	store.objects[metaURI] = fmt.Appendf(nil, `{
		"format-version": 2,
		"location": "s3://b/iceberg/db/orders",
		"current-snapshot-id": %d,
		"snapshots": [
			{"snapshot-id": %d, "manifest-list": %q}
		]
	}`, snapID, snapID, listURI)

	if err := ValidateAfterSwap(context.Background(), store, metaURI, validateMapping); err != nil {
		t.Fatalf("ValidateAfterSwap: %v", err)
	}
}

// TestValidateAfterSwap_DataFileMissing asserts the validation flags a
// missing data file as ErrValidationFailed (this is exactly the "bulk
// byte copy didn't run" failure mode the validation step exists to
// catch).
func TestValidateAfterSwap_DataFileMissing(t *testing.T) {
	store := newMemStorage()
	const (
		dataURI     = "s3://b/data/file-1.parquet"
		manifestURI = "s3://b/m1.avro"
		listURI     = "s3://b/snap-1.avro"
		metaURI     = "s3://b/v1.metadata.json"
		snapID      = int64(7)
	)

	mf, manRaw := buildV2Manifest(t, manifestURI, dataURI, snapID)
	store.objects[manifestURI] = manRaw
	// Note: dataURI deliberately NOT seeded.

	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	store.objects[listURI] = listBuf.Bytes()

	store.objects[metaURI] = fmt.Appendf(nil, `{
		"format-version": 2,
		"location": "s3://b",
		"current-snapshot-id": %d,
		"snapshots": [{"snapshot-id": %d, "manifest-list": %q}]
	}`, snapID, snapID, listURI)

	err := ValidateAfterSwap(context.Background(), store, metaURI, validateMapping)
	if !errors.Is(err, ErrValidationFailed) {
		t.Fatalf("expected ErrValidationFailed, got %v", err)
	}
}

// TestValidateAfterSwap_EmptyTable: a metadata.json with no snapshots
// has no data file to HEAD. Validation succeeds quietly.
func TestValidateAfterSwap_EmptyTable(t *testing.T) {
	store := newMemStorage()
	const metaURI = "s3://b/v1.metadata.json"
	store.objects[metaURI] = []byte(`{
		"format-version": 2,
		"location": "s3://b",
		"snapshots": []
	}`)

	if err := ValidateAfterSwap(context.Background(), store, metaURI, validateMapping); err != nil {
		t.Errorf("empty table should validate, got: %v", err)
	}
}

// TestValidateAfterSwap_MetadataMissing wraps the read error as
// ErrValidationFailed so the caller can branch on the sentinel.
func TestValidateAfterSwap_MetadataMissing(t *testing.T) {
	store := newMemStorage()
	err := ValidateAfterSwap(context.Background(), store, "s3://b/v1.metadata.json", validateMapping)
	if !errors.Is(err, ErrValidationFailed) {
		t.Fatalf("expected ErrValidationFailed, got %v", err)
	}
}

func TestPickValidationSnapshot_PrefersCurrent(t *testing.T) {
	id := int64(7)
	m := &MetadataJSON{
		CurrentSnapshotID: &id,
		Snapshots: []SnapshotJSON{
			{SnapshotID: 1, ManifestList: "s3://b/snap-1.avro"},
			{SnapshotID: 7, ManifestList: "s3://b/snap-7.avro"},
			{SnapshotID: 3, ManifestList: "s3://b/snap-3.avro"},
		},
	}
	got, ok := pickValidationSnapshot(m)
	if !ok || got.SnapshotID != 7 {
		t.Errorf("expected snapshot 7, got %+v ok=%v", got, ok)
	}
}

func TestPickValidationSnapshot_FallbackToLast(t *testing.T) {
	m := &MetadataJSON{
		Snapshots: []SnapshotJSON{
			{SnapshotID: 1},
			{SnapshotID: 2},
		},
	}
	got, ok := pickValidationSnapshot(m)
	if !ok || got.SnapshotID != 2 {
		t.Errorf("expected snapshot 2, got %+v ok=%v", got, ok)
	}
}

// fullTableFixture seeds, under testMapping's target prefix, a table whose
// current snapshot has one data manifest with three live files (plus a
// tombstone) and one delete manifest with a live position-delete file. It
// returns the metadata URI and every live file URI. The live files are not
// seeded; callers decide which exist.
func fullTableFixture(t *testing.T, st *memStorage, rewriteMeta func(string) string) (string, []string, map[string][]byte) {
	t.Helper()
	const (
		dataMan = "s3://new/t/metadata/data.avro"
		delMan  = "s3://new/t/metadata/del.avro"
		list    = "s3://new/t/metadata/snap-900.avro"
		meta    = "s3://new/t/metadata/v3.metadata.json"
	)
	live := []string{
		"s3://new/t/data/a.parquet",
		"s3://new/t/data/b.parquet",
		"s3://new/t/data/c.parquet",
		"s3://new/t/data/pos-1.parquet",
	}
	posBody := encodePosDeleteBody(t, []string{live[0], live[1]})
	bodies := map[string][]byte{
		live[0]: make([]byte, 100), live[1]: make([]byte, 100), live[2]: make([]byte, 100),
		live[3]: posBody,
	}
	dmf, draw := writeTestManifest(t, 2, dataMan, 900, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: live[0], snapshot: 900, seq: 1, content: iceberg.EntryContentData},
		{status: iceberg.EntryStatusDELETED, path: "s3://new/t/data/gone.parquet", snapshot: 900, seq: 1, content: iceberg.EntryContentData},
		{status: iceberg.EntryStatusEXISTING, path: live[1], snapshot: 900, seq: 1, content: iceberg.EntryContentData},
		{status: iceberg.EntryStatusADDED, path: live[2], snapshot: 900, seq: 1, content: iceberg.EntryContentData},
	})
	xmf, xraw := writeTestManifest(t, 2, delMan, 900, iceberg.ManifestContentDeletes, []testEntry{
		{status: iceberg.EntryStatusADDED, path: live[3], snapshot: 900, seq: 1, content: iceberg.EntryContentPosDeletes, size: int64(len(posBody))},
	})
	st.objects[dataMan] = draw
	st.objects[delMan] = xraw
	seq := int64(1)
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(2, buf, 900, nil, &seq, 0, []iceberg.ManifestFile{dmf, xmf}); err != nil {
		t.Fatal(err)
	}
	st.objects[list] = buf.Bytes()
	doc := `{"format-version":2,"location":"s3://new/t","current-snapshot-id":900,` +
		`"metadata-log":[{"timestamp-ms":1,"metadata-file":"s3://new/t/metadata/v2.metadata.json"}],` +
		`"snapshots":[{"snapshot-id":900,"sequence-number":1,"manifest-list":"` + list + `"}]}`
	if rewriteMeta != nil {
		doc = rewriteMeta(doc)
	}
	st.objects[meta] = []byte(doc)
	return meta, live, bodies
}

func TestValidateAfterSwap_AllLiveFilesPresent(t *testing.T) {
	st := newMemStorage()
	meta, live, bodies := fullTableFixture(t, st, nil)
	for _, u := range live {
		st.objects[u] = bodies[u]
	}
	if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); err != nil {
		t.Fatalf("every live file is present: %v", err)
	}
}

// A partial copy must fail no matter which live file it missed: the first
// one being present proves nothing about the rest.
func TestValidateAfterSwap_AnyLiveFileMissing(t *testing.T) {
	for skip := range 4 {
		st := newMemStorage()
		meta, live, bodies := fullTableFixture(t, st, nil)
		for i, u := range live {
			if i != skip {
				st.objects[u] = bodies[u]
			}
		}
		err := ValidateAfterSwap(context.Background(), st, meta, testMapping)
		if !errors.Is(err, ErrValidationFailed) {
			t.Errorf("%s missing: err = %v, want ErrValidationFailed", live[skip], err)
			continue
		}
		if !strings.Contains(err.Error(), live[skip]) {
			t.Errorf("error should name the missing file %s: %v", live[skip], err)
		}
	}
}

// A re-copy of the source after the rebase puts the original metadata back at
// the rewritten keys. Its URIs point at the source bucket, which target
// credentials may well be able to read, so presence alone does not catch it.
func TestValidateAfterSwap_SourceURIsLeft(t *testing.T) {
	for name, edit := range map[string]func(string) string{
		"location": func(d string) string {
			return strings.Replace(d, `"location":"s3://new/t"`, `"location":"s3://old/t"`, 1)
		},
		"metadata-log": func(d string) string {
			return strings.Replace(d, "s3://new/t/metadata/v2.metadata.json", "s3://old/t/metadata/v2.metadata.json", 1)
		},
	} {
		st := newMemStorage()
		meta, live, bodies := fullTableFixture(t, st, edit)
		for _, u := range live {
			st.objects[u] = bodies[u]
		}
		if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); !errors.Is(err, ErrValidationFailed) {
			t.Errorf("%s under source prefix: err = %v, want ErrValidationFailed", name, err)
		}
	}
}

func TestValidateAfterSwap_ManifestListPointsAtSource(t *testing.T) {
	st := newMemStorage()
	meta, live, bodies := fullTableFixture(t, st, nil)
	for _, u := range live {
		st.objects[u] = bodies[u]
	}
	// The original (source) manifest list copied over the rewritten one.
	mf, raw := writeTestManifest(t, 2, "s3://old/t/metadata/data.avro", 900, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/a.parquet", snapshot: 900, seq: 1, content: iceberg.EntryContentData},
	})
	st.objects["s3://old/t/metadata/data.avro"] = raw
	st.objects["s3://old/t/data/a.parquet"] = make([]byte, 100)
	seq := int64(1)
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(2, buf, 900, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatal(err)
	}
	st.objects["s3://new/t/metadata/snap-900.avro"] = buf.Bytes()
	if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); !errors.Is(err, ErrValidationFailed) {
		t.Errorf("manifest list references a source manifest: err = %v", err)
	}
}

func TestValidateAfterSwap_ManifestPointsAtSource(t *testing.T) {
	st := newMemStorage()
	meta, live, bodies := fullTableFixture(t, st, nil)
	for _, u := range live {
		st.objects[u] = bodies[u]
	}
	// The original manifest copied over the rewritten one, at the same key.
	_, raw := writeTestManifest(t, 2, "s3://new/t/metadata/data.avro", 900, iceberg.ManifestContentData, []testEntry{
		{status: iceberg.EntryStatusADDED, path: "s3://old/t/data/a.parquet", snapshot: 900, seq: 1, content: iceberg.EntryContentData},
	})
	st.objects["s3://new/t/metadata/data.avro"] = raw
	st.objects["s3://old/t/data/a.parquet"] = make([]byte, 100)
	if err := ValidateAfterSwap(context.Background(), st, meta, testMapping); !errors.Is(err, ErrValidationFailed) {
		t.Errorf("manifest references a source data file: err = %v", err)
	}
}

// A re-copy of only the source data/ prefix restores the original
// position-delete files over the rewritten ones. Same key, every manifest
// still clean, but their rows name the source, so readers stop applying them.
func TestValidateAfterSwap_PositionDeleteBodyPointsAtSource(t *testing.T) {
	st := newMemStorage()
	meta, live, bodies := fullTableFixture(t, st, nil)
	for _, u := range live {
		st.objects[u] = bodies[u]
	}
	original := encodePosDeleteBody(t, []string{"s3://old/t/data/a.parquet", "s3://old/t/data/b.parquet"})
	if len(original) != len(bodies[live[3]]) {
		t.Fatalf("fixture wants equal sizes so only the body check can catch it: %d vs %d", len(original), len(bodies[live[3]]))
	}
	st.objects[live[3]] = original
	err := ValidateAfterSwap(context.Background(), st, meta, testMapping)
	if !errors.Is(err, ErrValidationFailed) || !strings.Contains(err.Error(), live[3]) {
		t.Fatalf("restored original delete file: err = %v, want ErrValidationFailed naming %s", err, live[3])
	}
}

func TestValidateAfterSwap_SizeMismatch(t *testing.T) {
	st := newMemStorage()
	meta, live, bodies := fullTableFixture(t, st, nil)
	for _, u := range live {
		st.objects[u] = bodies[u]
	}
	st.objects[live[1]] = make([]byte, 60) // truncated copy
	err := ValidateAfterSwap(context.Background(), st, meta, testMapping)
	if !errors.Is(err, ErrValidationFailed) || !strings.Contains(err.Error(), "size 60") {
		t.Fatalf("truncated object: err = %v", err)
	}
}
