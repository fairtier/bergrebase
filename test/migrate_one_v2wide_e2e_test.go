package test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestMigrateOne_E2E_V2Wide: a partitioned V2 table with ~1000 data
// files in one snapshot. Asserts
// (a) the rewrite engine completes within 60 seconds (perf bound),
// (b) the catalog swap succeeds against a real Lakekeeper, and
// (c) every data_file.file_path entry in the rewritten manifest
// points at the new prefix.
//
// Nightly-only (BERGREBASE_NIGHTLY=1) per testing.md:134 — this is a
// regression guard against accidentally turning the rewrite walk into
// O(files²), not a merge-gate test. The 1000-entry manifest is the
// stressed artefact; the data files themselves are deliberately tiny
// (one row each) so the seed step doesn't dominate wall time.
func TestMigrateOne_E2E_V2Wide(t *testing.T) {
	harness.MaybeSkipNotNightly(t)
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	const tableName = "orders_v2wide"
	const numFiles = 1000
	const oldTable = "orders_wide_old"
	const newTable = "orders_wide_new"

	seed := harness.SeedV2Wide(ctx, t, store, s.MinIO.TargetBucket, oldTable, numFiles)

	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	cat := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable pre-rebase: %v", err)
	}
	if tbl.MetadataLocation != seed.MetadataURI {
		t.Fatalf("pre-rebase pointer: have %q, want %q", tbl.MetadataLocation, seed.MetadataURI)
	}

	// Bulk byte copy old prefix → new prefix. Iterate seed.AllURIs
	// instead of bucket-listing — the seed knows exactly what it wrote
	// and the test stays dependency-free of S3 list semantics.
	oldKeyPrefix := "iceberg/db/" + oldTable + "/"
	newKeyPrefix := "iceberg/db/" + newTable + "/"
	for _, srcURI := range seed.AllURIs {
		dstURI := strings.Replace(srcURI, oldKeyPrefix, newKeyPrefix, 1)
		body, err := store.GetObject(ctx, srcURI)
		if err != nil {
			t.Fatalf("get %s: %v", srcURI, err)
		}
		if err := store.PutObject(ctx, dstURI, body); err != nil {
			t.Fatalf("put %s: %v", dstURI, err)
		}
	}

	eng := &rewrite.Engine{
		Source: store,
		Target: store,
		Opts: rewrite.Options{
			Mapping: rewrite.PrefixMapping{
				Source: "s3://" + s.MinIO.TargetBucket + "/" + oldKeyPrefix,
				Target: "s3://" + s.MinIO.TargetBucket + "/" + newKeyPrefix,
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	start := time.Now()
	res, err := eng.RewriteTable(ctx, tbl.MetadataLocation)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	t.Logf("RewriteTable on %d-file manifest: %s", numFiles, elapsed)
	// Perf bound from testing.md row #4. Nightly CI surfaces drift early;
	// failing here usually means an accidental O(files²) regression.
	const perfBound = 60 * time.Second
	if elapsed > perfBound {
		t.Errorf("RewriteTable took %s (> %s); perf regression on %d-file manifest", elapsed, perfBound, numFiles)
	}

	wantNewLoc := strings.Replace(seed.MetadataURI, oldKeyPrefix, newKeyPrefix, 1)
	if res.NewMetadataLocation != wantNewLoc {
		t.Fatalf("NewMetadataLocation = %q, want %q", res.NewMetadataLocation, wantNewLoc)
	}
	if res.ManifestsRewritten != 1 {
		t.Errorf("ManifestsRewritten = %d, want 1 (single wide manifest)", res.ManifestsRewritten)
	}
	if res.ManifestListsRewritten != 1 {
		t.Errorf("ManifestListsRewritten = %d, want 1", res.ManifestListsRewritten)
	}

	// "All manifest entries rewritten" (testing.md row #4): re-read the
	// rewritten manifest and assert every data_file.file_path now lives
	// under the new prefix. A regression that walked but failed to mutate
	// some entries would silently slip past the engine's own counters.
	wantManifestURI := strings.Replace(seed.ManifestURI, oldKeyPrefix, newKeyPrefix, 1)
	manifestRaw, err := store.GetObject(ctx, wantManifestURI)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	mfDesc := iceberg.NewManifestFile(2, wantManifestURI, int64(len(manifestRaw)), 0, seed.SnapshotID).Build()
	entries, err := iceberg.ReadManifest(mfDesc, bytes.NewReader(manifestRaw), false)
	if err != nil {
		t.Fatalf("decode rewritten manifest: %v", err)
	}
	if len(entries) != numFiles {
		t.Fatalf("rewritten manifest entries = %d, want %d", len(entries), numFiles)
	}
	wantDataPrefix := "s3://" + s.MinIO.TargetBucket + "/" + newKeyPrefix + "data/"
	for i, e := range entries {
		if got := e.DataFile().FilePath(); !strings.HasPrefix(got, wantDataPrefix) {
			t.Fatalf("entry %d: file_path = %q, want prefix %q", i, got, wantDataPrefix)
		}
	}

	if err := cat.SwapMetadataLocation(ctx, id, tbl.MetadataLocation, res.NewMetadataLocation); err != nil {
		t.Fatalf("SwapMetadataLocation: %v", err)
	}

	reloaded, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable post-rebase: %v", err)
	}
	if reloaded.MetadataLocation != wantNewLoc {
		t.Errorf("post-rebase pointer: have %q, want %q", reloaded.MetadataLocation, wantNewLoc)
	}
	if err := rewrite.ValidateAfterSwap(ctx, store, wantNewLoc); err != nil {
		t.Errorf("ValidateAfterSwap: %v", err)
	}
}
