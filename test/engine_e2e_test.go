package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestEngine_E2E_MinIO drives the rewrite engine end-to-end against a
// real MinIO container with two buckets ("source" and "target"). One
// container is shared across subtests; each subtest uses a distinct
// table name so seed data does not collide.
//
// Scenarios covered:
//
//	v2_basic            — bottom-up rewrite + validate after byte copy.
//	idempotent          — second run is a no-op.
//	bad_prefix          — prefix mismatch refused, no writes.
//	validate_no_copy    — validation flags missing data file.
//
// Catalog-side scenarios (rollback, race) live in a separate suite
// once the Lakekeeper container is wired up.
func TestEngine_E2E_MinIO(t *testing.T) {
	ctx := context.Background()
	m := harness.StartMinIO(ctx, t)
	store := m.StorageClient()

	t.Run("v2_basic", func(t *testing.T) {
		seed := harness.SeedV2Basic(ctx, t, store, m.SourceBucket, "orders")

		// Bulk byte copy: every object key copies verbatim from source
		// bucket to target bucket. This is what rclone does in
		// production, before bergrebase runs.
		harness.CopyBucketToBucket(ctx, t, store, []string{
			"iceberg/db/orders/data/file-1.parquet",
			"iceberg/db/orders/metadata/m1.avro",
			"iceberg/db/orders/metadata/snap-100-1-uuid.avro",
			"iceberg/db/orders/metadata/v2.metadata.json",
		}, m.SourceBucket, m.TargetBucket)

		eng := &rewrite.Engine{
			Source: store,
			Target: store,
			Opts: rewrite.Options{
				Mapping: rewrite.PrefixMapping{
					Source: "s3://" + m.SourceBucket + "/",
					Target: "s3://" + m.TargetBucket + "/",
				},
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		res, err := eng.RewriteTable(ctx, seed.MetadataURI)
		if err != nil {
			t.Fatalf("RewriteTable: %v", err)
		}

		newPrefix := "s3://" + m.TargetBucket + "/iceberg/db/orders"
		wantNewMeta := newPrefix + "/metadata/v2.metadata.json"
		if res.NewMetadataLocation != wantNewMeta {
			t.Errorf("NewMetadataLocation = %q, want %q", res.NewMetadataLocation, wantNewMeta)
		}

		// All three rewritten files exist at the substituted URIs.
		for _, uri := range []string{
			wantNewMeta,
			newPrefix + "/metadata/snap-100-1-uuid.avro",
			newPrefix + "/metadata/m1.avro",
		} {
			if _, err := store.HeadObject(ctx, uri); err != nil {
				t.Errorf("expected object at %q: %v", uri, err)
			}
		}

		// metadata.json on target references the new prefix end-to-end.
		raw, err := store.GetObject(ctx, wantNewMeta)
		if err != nil {
			t.Fatalf("get rewritten metadata: %v", err)
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("parse rewritten metadata: %v", err)
		}
		if got := doc["location"].(string); got != newPrefix {
			t.Errorf("location = %q, want %q", got, newPrefix)
		}
		if got := doc["snapshots"].([]any)[0].(map[string]any)["manifest-list"].(string); !strings.HasPrefix(got, "s3://"+m.TargetBucket+"/") {
			t.Errorf("manifest-list still points at source: %q", got)
		}

		// statistics-path, partition-statistics-path, and
		// properties.write.object-storage.path are all path-bearing
		// fields that must be rewritten under the new prefix.
		// Forgetting any of them is a known bug class (write.*
		// properties especially — future writes would land in the old
		// bucket).
		if got := doc["statistics"].([]any)[0].(map[string]any)["statistics-path"].(string); !strings.HasPrefix(got, "s3://"+m.TargetBucket+"/") {
			t.Errorf("statistics-path still points at source: %q", got)
		}
		if got := doc["partition-statistics"].([]any)[0].(map[string]any)["statistics-path"].(string); !strings.HasPrefix(got, "s3://"+m.TargetBucket+"/") {
			t.Errorf("partition-statistics-path still points at source: %q", got)
		}
		if got := doc["properties"].(map[string]any)["write.object-storage.path"].(string); !strings.HasPrefix(got, "s3://"+m.TargetBucket+"/") {
			t.Errorf("properties.write.object-storage.path still points at source: %q", got)
		}

		// The rewritten manifest references the rewritten data file URI,
		// and the rewritten manifest list references the manifest with
		// its actual on-disk byte length (D3 invariant).
		newManifestURI := newPrefix + "/metadata/m1.avro"
		newManifestRaw, err := store.GetObject(ctx, newManifestURI)
		if err != nil {
			t.Fatalf("get rewritten manifest: %v", err)
		}
		mfList := iceberg.NewManifestFile(2, newManifestURI, int64(len(newManifestRaw)), 0, seed.SnapshotID).Build()
		entries, err := iceberg.ReadManifest(mfList, bytes.NewReader(newManifestRaw), false)
		if err != nil {
			t.Fatalf("read rewritten manifest: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("entry count = %d, want 1", len(entries))
		}
		wantNewData := newPrefix + "/data/file-1.parquet"
		if got := entries[0].DataFile().FilePath(); got != wantNewData {
			t.Errorf("data_file.file_path = %q, want %q", got, wantNewData)
		}

		newListURI := newPrefix + "/metadata/snap-100-1-uuid.avro"
		newListRaw, err := store.GetObject(ctx, newListURI)
		if err != nil {
			t.Fatalf("get rewritten manifest list: %v", err)
		}
		files, err := iceberg.ReadManifestList(bytes.NewReader(newListRaw))
		if err != nil {
			t.Fatalf("decode rewritten list: %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("manifest count in list = %d, want 1", len(files))
		}
		if got := files[0].FilePath(); got != newManifestURI {
			t.Errorf("rewritten manifest_path = %q, want %q", got, newManifestURI)
		}
		if got, want := files[0].Length(), int64(len(newManifestRaw)); got != want {
			t.Errorf("rewritten manifest_length = %d, want %d (actual on-disk)", got, want)
		}

		// Acceptance criterion #8: post-swap storage-side validation.
		// With the data file present in target (byte-copied above), the
		// rewritten metadata graph reaches a HEAD-able URI.
		if err := rewrite.ValidateAfterSwap(ctx, store, res.NewMetadataLocation); err != nil {
			t.Errorf("ValidateAfterSwap: %v", err)
		}
	})

	// Acceptance #8 negative path: validation must surface a missing
	// data file. We seed source, run the rewrite, but skip the byte
	// copy, so the rewritten metadata in target references a data file
	// URI that does not exist on target storage.
	t.Run("validate_no_copy", func(t *testing.T) {
		seed := harness.SeedV2Basic(ctx, t, store, m.SourceBucket, "orders_nocopy")

		eng := &rewrite.Engine{
			Source: store,
			Target: store,
			Opts: rewrite.Options{
				Mapping: rewrite.PrefixMapping{
					Source: "s3://" + m.SourceBucket + "/",
					Target: "s3://" + m.TargetBucket + "/",
				},
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		res, err := eng.RewriteTable(ctx, seed.MetadataURI)
		if err != nil {
			t.Fatalf("RewriteTable: %v", err)
		}

		err = rewrite.ValidateAfterSwap(ctx, store, res.NewMetadataLocation)
		if !errors.Is(err, rewrite.ErrValidationFailed) {
			t.Fatalf("expected ErrValidationFailed, got %v", err)
		}
	})

	// Scenario 13: idempotency. Running the engine a second time with
	// inputs that already point at the target prefix must skip writes.
	// We seed straight into the target bucket (paths self-consistent
	// with target prefix already), use the no-op mapping s3://target/ ->
	// s3://target/, and assert zero PUT-induced changes.
	t.Run("idempotent", func(t *testing.T) {
		seed := harness.SeedV2Basic(ctx, t, store, m.TargetBucket, "orders_idem")

		eng := &rewrite.Engine{
			Source: store,
			Target: store,
			Opts: rewrite.Options{
				Mapping: rewrite.PrefixMapping{
					Source: "s3://" + m.TargetBucket + "/",
					Target: "s3://" + m.TargetBucket + "/",
				},
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		// Capture pre-state: byte hashes of the metadata files.
		before := map[string][]byte{}
		for _, uri := range []string{seed.MetadataURI, seed.ManifestListURI, seed.ManifestURI} {
			b, err := store.GetObject(ctx, uri)
			if err != nil {
				t.Fatalf("get %s: %v", uri, err)
			}
			before[uri] = b
		}

		res, err := eng.RewriteTable(ctx, seed.MetadataURI)
		if err != nil {
			t.Fatalf("RewriteTable: %v", err)
		}
		if res.NewMetadataLocation != seed.MetadataURI {
			t.Errorf("idempotent run changed metadata URI: %q -> %q",
				seed.MetadataURI, res.NewMetadataLocation)
		}

		// Files must be byte-identical: no writes happened. (S3 PutObject
		// would write the same bytes to the same key on a no-op anyway,
		// so we check bytes directly rather than counting PUTs.)
		for uri, want := range before {
			got, err := store.GetObject(ctx, uri)
			if err != nil {
				t.Fatalf("get %s after rewrite: %v", uri, err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("idempotent run mutated %s (%d -> %d bytes)",
					uri, len(want), len(got))
			}
		}
	})

	// Scenario 16: prefix mismatch on the metadata-location must refuse
	// before any write occurs.
	t.Run("bad_prefix", func(t *testing.T) {
		seed := harness.SeedV2Basic(ctx, t, store, m.SourceBucket, "orders_bad")

		eng := &rewrite.Engine{
			Source: store,
			Target: store,
			Opts: rewrite.Options{
				Mapping: rewrite.PrefixMapping{
					Source: "s3://wrong-bucket/",
					Target: "s3://" + m.TargetBucket + "/",
				},
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		_, err := eng.RewriteTable(ctx, seed.MetadataURI)
		if err == nil {
			t.Fatal("expected prefix-mismatch error, got nil")
		}
		if !strings.Contains(err.Error(), "does not start with source prefix") {
			t.Errorf("error message lacks expected text: %v", err)
		}

		// Nothing landed in the target bucket for this table.
		if _, err := store.HeadObject(ctx, "s3://"+m.TargetBucket+"/iceberg/db/orders_bad/metadata/v2.metadata.json"); err == nil {
			t.Error("target metadata.json exists despite refusal")
		}
	})
}
