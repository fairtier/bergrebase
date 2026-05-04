package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

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
	store.objects[dataURI] = []byte("placeholder")

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

	if err := ValidateAfterSwap(context.Background(), store, metaURI); err != nil {
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

	err := ValidateAfterSwap(context.Background(), store, metaURI)
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

	if err := ValidateAfterSwap(context.Background(), store, metaURI); err != nil {
		t.Errorf("empty table should validate, got: %v", err)
	}
}

// TestValidateAfterSwap_MetadataMissing wraps the read error as
// ErrValidationFailed so the caller can branch on the sentinel.
func TestValidateAfterSwap_MetadataMissing(t *testing.T) {
	store := newMemStorage()
	err := ValidateAfterSwap(context.Background(), store, "s3://b/v1.metadata.json")
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
