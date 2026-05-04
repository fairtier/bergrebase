package rewrite

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// buildV1Manifest builds a tiny V1 manifest with one EntryStatusADDED
// data file. V1 differs from V2 in the OCF format-version header and
// the absence of sequence-number on entries.
func buildV1Manifest(t *testing.T, manifestURI, dataFileURI string, snapshotID int64) (iceberg.ManifestFile, []byte) {
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
		10,
		1024,
	)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	df := dfb.Build()
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df).Build()

	buf := &bytes.Buffer{}
	mf, err := iceberg.WriteManifest(manifestURI, buf, 1, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("WriteManifest V1: %v", err)
	}
	return mf, buf.Bytes()
}

// TestEngine_RewriteTable_V1_FlatManifests covers the V1 metadata layout:
// snapshots[].manifests[] holds a flat list of manifest URIs (no
// manifest-list file). The engine must walk each, rewrite paths, and
// re-emit the metadata.json with substituted URIs.
func TestEngine_RewriteTable_V1_FlatManifests(t *testing.T) {
	const (
		oldDataURI     = "s3://old/iceberg/db/orders/data/file-1.parquet"
		oldManifestURI = "s3://old/iceberg/db/orders/metadata/m1.avro"
		oldMetaURI     = "s3://old/iceberg/db/orders/metadata/v1.metadata.json"
		snapID         = int64(99)
	)

	mf, manifestRaw := buildV1Manifest(t, oldManifestURI, oldDataURI, snapID)
	_ = mf

	metaJSON := `{
		"format-version": 1,
		"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
		"location": "s3://old/iceberg/db/orders",
		"current-snapshot-id": 99,
		"snapshots": [
			{"snapshot-id": 99, "manifests": ["` + oldManifestURI + `"]}
		]
	}`

	store := newMemStorage()
	store.objects[oldMetaURI] = []byte(metaJSON)
	store.objects[oldManifestURI] = manifestRaw

	eng := &Engine{
		Source: store, Target: store,
		Opts: Options{
			Mapping: PrefixMapping{Source: "s3://old/", Target: "s3://new/"},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	res, err := eng.RewriteTable(context.Background(), oldMetaURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	wantNewMeta := "s3://new/iceberg/db/orders/metadata/v1.metadata.json"
	if res.NewMetadataLocation != wantNewMeta {
		t.Errorf("NewMetadataLocation = %q, want %q", res.NewMetadataLocation, wantNewMeta)
	}

	wantNewManifest := "s3://new/iceberg/db/orders/metadata/m1.avro"
	rewrittenRaw, ok := store.puts[wantNewManifest]
	if !ok {
		t.Fatalf("expected put at %q, got: %v", wantNewManifest, putKeys(store))
	}

	// Read the rewritten V1 manifest back. Use NewManifestFile(1, ...)
	// to drive the V1 reader path.
	mfRead := iceberg.NewManifestFile(1, wantNewManifest, int64(len(rewrittenRaw)), 0, snapID).Build()
	entries, err := iceberg.ReadManifest(mfRead, bytes.NewReader(rewrittenRaw), false)
	if err != nil {
		t.Fatalf("read rewritten V1 manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(entries))
	}
	wantNewData := "s3://new/iceberg/db/orders/data/file-1.parquet"
	if got := entries[0].DataFile().FilePath(); got != wantNewData {
		t.Errorf("data_file.file_path = %q, want %q", got, wantNewData)
	}

	// metadata.json's snapshots[].manifests[] entry must reference the
	// new URI, since RewriteMetadataJSON applies the prefix mapping.
	rewrittenMeta, ok := store.puts[wantNewMeta]
	if !ok {
		t.Fatalf("expected put at %q", wantNewMeta)
	}
	if !bytes.Contains(rewrittenMeta, []byte(wantNewManifest)) {
		t.Errorf("rewritten metadata.json missing %q; got: %s", wantNewManifest, rewrittenMeta)
	}
	if bytes.Contains(rewrittenMeta, []byte(oldManifestURI)) {
		t.Errorf("rewritten metadata.json still references old manifest URI %q", oldManifestURI)
	}
}
