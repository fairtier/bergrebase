package rewrite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// memStorage is a tiny in-memory Storage stub used in unit tests.
type memStorage struct {
	objects map[string][]byte
	puts    map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{
		objects: map[string][]byte{},
		puts:    map[string][]byte{},
	}
}

func (m *memStorage) GetObject(_ context.Context, uri string) ([]byte, error) {
	b, ok := m.objects[uri]
	if !ok {
		return nil, errors.New("not found: " + uri)
	}
	return append([]byte(nil), b...), nil
}

func (m *memStorage) PutObject(_ context.Context, uri string, body []byte) error {
	m.puts[uri] = append([]byte(nil), body...)
	m.objects[uri] = append([]byte(nil), body...)
	return nil
}

func (m *memStorage) HeadObject(_ context.Context, uri string) (int64, error) {
	b, ok := m.objects[uri]
	if !ok {
		return 0, errors.New("not found: " + uri)
	}
	return int64(len(b)), nil
}

const dryRunMetadataJSON = `{
	"format-version": 2,
	"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
	"location": "s3://old/iceberg/db/orders",
	"snapshots": [],
	"metadata-log": [
		{"timestamp-ms": 1700000000000, "metadata-file": "s3://old/iceberg/db/orders/metadata/v1.metadata.json"}
	],
	"properties": {
		"write.object-storage.path": "s3://old/iceberg/db/orders/data"
	}
}`

func TestEngine_RewriteTable_DryRun_NoManifests(t *testing.T) {
	store := newMemStorage()
	const oldLoc = "s3://old/iceberg/db/orders/metadata/v2.metadata.json"
	store.objects[oldLoc] = []byte(dryRunMetadataJSON)

	eng := &Engine{
		Source: store,
		Target: store,
		Opts: Options{
			Mapping: PrefixMapping{Source: "s3://old/", Target: "s3://new/"},
			DryRun:  true,
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	res, err := eng.RewriteTable(context.Background(), oldLoc)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	if res.NewMetadataLocation != "s3://new/iceberg/db/orders/metadata/v2.metadata.json" {
		t.Errorf("new location wrong: %s", res.NewMetadataLocation)
	}
	if len(store.puts) != 0 {
		t.Errorf("dry-run wrote %d objects, want 0", len(store.puts))
	}

	// Expect three hits: location, metadata-log[0].metadata-file, write.object-storage.path.
	wantFields := []string{
		"location",
		"metadata-log[0].metadata-file",
		`properties["write.object-storage.path"]`,
	}
	gotFields := map[string]bool{}
	for _, h := range res.Hits {
		gotFields[h.Field] = true
		if !h.Rewritten() {
			t.Errorf("hit %q not rewritten", h.Field)
		}
	}
	for _, f := range wantFields {
		if !gotFields[f] {
			t.Errorf("missing hit %q in %v", f, gotFields)
		}
	}
}

func TestEngine_RewriteTable_RefusesWrongPrefix(t *testing.T) {
	store := newMemStorage()
	eng := &Engine{
		Source: store, Target: store,
		Opts: Options{
			Mapping: PrefixMapping{Source: "s3://expected/", Target: "s3://new/"},
			DryRun:  true,
		},
	}
	_, err := eng.RewriteTable(context.Background(), "s3://wrong/iceberg/v1.metadata.json")
	if err == nil {
		t.Fatal("expected prefix-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "does not start with source prefix") {
		t.Errorf("error message lacks expected text: %v", err)
	}
}

// TestEngine_RewriteTable_LiveWritesMetadataJSON exercises the live
// (non-DryRun) path on a metadata.json with no manifest list entries.
// It asserts the rewritten metadata.json lands at the substituted URI
// with rewritten path-bearing fields.
func TestEngine_RewriteTable_LiveWritesMetadataJSON(t *testing.T) {
	store := newMemStorage()
	const oldLoc = "s3://old/v1.metadata.json"
	store.objects[oldLoc] = []byte(`{
		"format-version": 2,
		"location": "s3://old/x",
		"snapshots": []
	}`)

	eng := &Engine{
		Source: store, Target: store,
		Opts: Options{
			Mapping: PrefixMapping{Source: "s3://old/", Target: "s3://new/"},
		},
	}
	res, err := eng.RewriteTable(context.Background(), oldLoc)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	if res.NewMetadataLocation != "s3://new/v1.metadata.json" {
		t.Errorf("new metadata location = %q", res.NewMetadataLocation)
	}
	if _, ok := store.puts["s3://new/v1.metadata.json"]; !ok {
		t.Errorf("expected put at substituted URI; got: %v", putKeys(store))
	}
}

// TestEngine_RewriteTable_LiveEndToEnd_V2 builds a V2 fixture with one
// snapshot, one manifest list, one manifest, and one data file. It runs
// the engine on the live path and asserts the bottom-up rewrite landed:
// metadata.json, manifest list, and manifest are all written at the
// substituted URIs and their internal paths reference the new prefix.
func TestEngine_RewriteTable_LiveEndToEnd_V2(t *testing.T) {
	const (
		oldDataURI     = "s3://old/iceberg/db/orders/data/file-1.parquet"
		oldManifestURI = "s3://old/iceberg/db/orders/metadata/m1.avro"
		oldListURI     = "s3://old/iceberg/db/orders/metadata/snap-100-1-uuid.avro"
		oldMetaURI     = "s3://old/iceberg/db/orders/metadata/v2.metadata.json"
	)

	mf, manifestRaw := buildV2Manifest(t, oldManifestURI, oldDataURI, 100)

	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, 100, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}

	metaJSON := `{
		"format-version": 2,
		"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
		"location": "s3://old/iceberg/db/orders",
		"current-snapshot-id": 100,
		"snapshots": [
			{"snapshot-id": 100, "manifest-list": "` + oldListURI + `"}
		]
	}`

	store := newMemStorage()
	store.objects[oldMetaURI] = []byte(metaJSON)
	store.objects[oldListURI] = listBuf.Bytes()
	store.objects[oldManifestURI] = manifestRaw

	eng := &Engine{
		Source: store, Target: store,
		Opts: Options{
			Mapping: PrefixMapping{Source: "s3://old/", Target: "s3://new/"},
		},
	}
	res, err := eng.RewriteTable(context.Background(), oldMetaURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	wantNewMeta := "s3://new/iceberg/db/orders/metadata/v2.metadata.json"
	if res.NewMetadataLocation != wantNewMeta {
		t.Errorf("new metadata location = %q", res.NewMetadataLocation)
	}

	for _, k := range []string{
		wantNewMeta,
		"s3://new/iceberg/db/orders/metadata/snap-100-1-uuid.avro",
		"s3://new/iceberg/db/orders/metadata/m1.avro",
	} {
		if _, ok := store.puts[k]; !ok {
			t.Errorf("missing put at %q; got: %v", k, putKeys(store))
		}
	}

	// Rewritten manifest must reference the new data file URI.
	rewrittenManifestRaw := store.puts["s3://new/iceberg/db/orders/metadata/m1.avro"]
	mfList := iceberg.NewManifestFile(2, "s3://new/iceberg/db/orders/metadata/m1.avro", int64(len(rewrittenManifestRaw)), 0, 100).Build()
	entries, err := iceberg.ReadManifest(mfList, bytes.NewReader(rewrittenManifestRaw), false)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if got := entries[0].DataFile().FilePath(); got != "s3://new/iceberg/db/orders/data/file-1.parquet" {
		t.Errorf("rewritten data_file.file_path = %q", got)
	}

	// Rewritten manifest list must reference the new manifest URI with
	// the new on-disk length.
	rewrittenListRaw := store.puts["s3://new/iceberg/db/orders/metadata/snap-100-1-uuid.avro"]
	files, err := iceberg.ReadManifestList(bytes.NewReader(rewrittenListRaw))
	if err != nil {
		t.Fatalf("read rewritten manifest list: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("manifest count in list = %d, want 1", len(files))
	}
	if got := files[0].FilePath(); got != "s3://new/iceberg/db/orders/metadata/m1.avro" {
		t.Errorf("rewritten manifest_path = %q", got)
	}
	if got := files[0].Length(); got != int64(len(rewrittenManifestRaw)) {
		t.Errorf("rewritten manifest_length = %d, want %d (actual on-disk)", got, len(rewrittenManifestRaw))
	}
}

// TestWalkManifest_RefusesPuffinDeletes asserts the dry-run walker
// refuses position-delete entries pointing at .puffin files (V3
// deletion vectors). V2 position-delete .parquet entries are now
// supported by the walker.
func TestWalkManifest_RefusesPuffinDeletes(t *testing.T) {
	const manifestURI = "s3://old/m-puffin-deletes.avro"
	const puffinURI = "s3://old/data/dv-001.puffin"

	mf, raw := buildV2DeleteManifest(t, manifestURI, puffinURI, iceberg.EntryContentPosDeletes, 1)
	store := newMemStorage()
	store.objects[manifestURI] = raw

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	_, err := WalkManifest(context.Background(), store, mf, mapping)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected ErrUnsupportedFeature, got %v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "Puffin") {
		t.Errorf("error message must mention Puffin: %v", err)
	}
}

// TestSelectSnapshots_CurrentOnlyFiltersHistorical pins B1 from the
// review: --current-snapshot-only must drop historical snapshots when
// metadata.json carries a current-snapshot-id.
func TestSelectSnapshots_CurrentOnlyFiltersHistorical(t *testing.T) {
	cur := int64(200)
	m := &MetadataJSON{
		CurrentSnapshotID: &cur,
		Snapshots: []SnapshotJSON{
			{SnapshotID: 100, ManifestList: "s3://old/m-100.avro"},
			{SnapshotID: 200, ManifestList: "s3://old/m-200.avro"},
			{SnapshotID: 300, ManifestList: "s3://old/m-300.avro"},
		},
	}

	got := selectSnapshots(m, true)
	if len(got) != 1 || got[0].SnapshotID != 200 {
		t.Fatalf("currentOnly=true: got %+v, want only snapshot 200", got)
	}

	all := selectSnapshots(m, false)
	if len(all) != 3 {
		t.Errorf("currentOnly=false: got %d snapshots, want 3", len(all))
	}

	// Missing pointer falls back to all snapshots so we never silently
	// drop work.
	m.CurrentSnapshotID = nil
	if got := selectSnapshots(m, true); len(got) != 3 {
		t.Errorf("currentOnly=true with no pointer: got %d snapshots, want 3 (fallback)", len(got))
	}

	// Pointer that doesn't resolve also falls back, not panics.
	stale := int64(999)
	m.CurrentSnapshotID = &stale
	if got := selectSnapshots(m, true); len(got) != 3 {
		t.Errorf("currentOnly=true with unresolved pointer: got %d, want 3 (fallback)", len(got))
	}
}

// TestEngine_RewriteTable_CurrentSnapshotOnly_SkipsHistorical exercises
// the full engine path: a metadata.json with two snapshots, only one of
// which is current, must result in a rewrite that touches only the
// current snapshot's manifest list.
func TestEngine_RewriteTable_CurrentSnapshotOnly_SkipsHistorical(t *testing.T) {
	const (
		oldDataURI       = "s3://old/iceberg/db/orders/data/file-current.parquet"
		oldManifestURI   = "s3://old/iceberg/db/orders/metadata/m-current.avro"
		oldListURIOld    = "s3://old/iceberg/db/orders/metadata/snap-100.avro"
		oldListURICurr   = "s3://old/iceberg/db/orders/metadata/snap-200.avro"
		oldMetaURI       = "s3://old/iceberg/db/orders/metadata/v2.metadata.json"
		newListURICurr   = "s3://new/iceberg/db/orders/metadata/snap-200.avro"
		newManifestURI   = "s3://new/iceberg/db/orders/metadata/m-current.avro"
		newListURIOld    = "s3://new/iceberg/db/orders/metadata/snap-100.avro"
		newMetaURI       = "s3://new/iceberg/db/orders/metadata/v2.metadata.json"
		histManifestURI  = "s3://old/iceberg/db/orders/metadata/m-historical.avro"
		histManifestPath = "s3://new/iceberg/db/orders/metadata/m-historical.avro"
	)

	mf, manifestRaw := buildV2Manifest(t, oldManifestURI, oldDataURI, 200)

	listBuf := &bytes.Buffer{}
	seq := int64(1)
	if err := iceberg.WriteManifestList(2, listBuf, 200, nil, &seq, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}

	metaJSON := `{
		"format-version": 2,
		"location": "s3://old/iceberg/db/orders",
		"current-snapshot-id": 200,
		"snapshots": [
			{"snapshot-id": 100, "manifest-list": "` + oldListURIOld + `"},
			{"snapshot-id": 200, "manifest-list": "` + oldListURICurr + `"}
		]
	}`

	store := newMemStorage()
	store.objects[oldMetaURI] = []byte(metaJSON)
	store.objects[oldListURICurr] = listBuf.Bytes()
	store.objects[oldManifestURI] = manifestRaw
	// Historical list/manifest are intentionally NOT seeded — if the
	// engine reaches for them, GetObject will return "not found" and
	// the test fails.

	eng := &Engine{
		Source: store, Target: store,
		Opts: Options{
			Mapping:             PrefixMapping{Source: "s3://old/", Target: "s3://new/"},
			CurrentSnapshotOnly: true,
		},
	}
	if _, err := eng.RewriteTable(context.Background(), oldMetaURI); err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	for _, k := range []string{newMetaURI, newListURICurr, newManifestURI} {
		if _, ok := store.puts[k]; !ok {
			t.Errorf("missing put at %q; got: %v", k, putKeys(store))
		}
	}
	for _, k := range []string{newListURIOld, histManifestPath} {
		if _, ok := store.puts[k]; ok {
			t.Errorf("unexpected put at %q (historical snapshot should be skipped)", k)
		}
	}
}

// Sanity check that bytes.NewReader is what we think — guards against
// accidental import drift if the file is reorganised.
var _ io.Reader = bytes.NewReader(nil)
