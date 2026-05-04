package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestEngine_RefusesV3DeletionVectors_E2E asserts the engine refuses a
// position-delete manifest entry pointing at a .puffin file (V3
// deletion vector). V2 position-delete .parquet files are now
// supported (see TestDuckDBOracle_V2PositionDeletes); .puffin entries
// remain deferred until a Go Puffin reader/writer lands.
//
// Unit-test coverage exists at the WalkManifest /
// RewriteManifest layer (TestWalkManifest_RefusesPuffinDeletes,
// TestRewriteManifest_RefusesPuffinDeletes); this exercises the same
// guard through the full engine path against MinIO.
func TestEngine_RefusesV3DeletionVectors_E2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	ctx := context.Background()
	m := harness.StartMinIO(ctx, t)
	store := m.StorageClient()

	const (
		listURI   = "s3://source/iceberg/db/refuse/metadata/snap-1.avro"
		mDelURI   = "s3://source/iceberg/db/refuse/metadata/m-puffin-deletes.avro"
		puffinURI = "s3://source/iceberg/db/refuse/data/dv-001.puffin"
		metaURI   = "s3://source/iceberg/db/refuse/metadata/v2.metadata.json"
		snapID    = int64(7)
	)
	seqNum := int64(1)

	// Build a real delete-manifest with one position-delete entry whose
	// FilePath ends in .puffin. The engine must refuse based on that
	// file extension before attempting any rewrite.
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := *iceberg.UnpartitionedSpec
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, puffinURI, iceberg.ParquetFile,
		map[int]any{}, nil, nil, 1, 100)
	if err != nil {
		t.Fatalf("DataFileBuilder: %v", err)
	}
	sid := snapID
	delEntry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &sid, dfb.Build()).
		SequenceNum(seqNum).FileSequenceNum(seqNum).Build()

	manBuf := &bytes.Buffer{}
	mfDel, err := harness.WriteDeleteManifest(mDelURI, manBuf, 2, spec, schema, snapID, seqNum, []iceberg.ManifestEntry{delEntry})
	if err != nil {
		t.Fatalf("WriteDeleteManifest: %v", err)
	}
	if err := store.PutObject(ctx, mDelURI, manBuf.Bytes()); err != nil {
		t.Fatalf("put delete manifest: %v", err)
	}

	listBuf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(2, listBuf, snapID, nil, &seqNum, 0, []iceberg.ManifestFile{mfDel}); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	if err := store.PutObject(ctx, listURI, listBuf.Bytes()); err != nil {
		t.Fatalf("put manifest list: %v", err)
	}

	metaJSON := fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://source/iceberg/db/refuse",
  "current-snapshot-id": %d,
  "snapshots": [
    {"snapshot-id": %d, "manifest-list": %q}
  ]
}`, snapID, snapID, listURI)
	if err := store.PutObject(ctx, metaURI, []byte(metaJSON)); err != nil {
		t.Fatalf("put metadata: %v", err)
	}

	eng := &rewrite.Engine{
		Source: store, Target: store,
		Opts: rewrite.Options{
			Mapping: rewrite.PrefixMapping{
				Source: "s3://source/",
				Target: "s3://target/",
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	_, err = eng.RewriteTable(ctx, metaURI)
	if !errors.Is(err, rewrite.ErrUnsupportedFeature) {
		t.Fatalf("expected ErrUnsupportedFeature, got %v", err)
	}
	if !strings.Contains(err.Error(), "Puffin") {
		t.Errorf("error message must mention Puffin to point operators at the deferred V3 DV work: %v", err)
	}
	if !strings.Contains(err.Error(), "V3 deletion vector") {
		t.Errorf("error message must name the unsupported feature: %v", err)
	}
}
