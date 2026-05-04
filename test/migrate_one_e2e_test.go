package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestMigrateOne_E2E_Lakekeeper exercises the full bergrebase flow
// against a real Lakekeeper catalog backed by MinIO:
//
//  1. Seed a V2 metadata graph in the target bucket under one prefix.
//  2. Register that table in Lakekeeper.
//  3. Copy the bytes to a new prefix (in-bucket simulation of rclone).
//  4. Run engine.RewriteTable to rewrite metadata at the new prefix.
//  5. Call SwapMetadataLocation to atomically re-point the catalog.
//  6. Validate post-swap (acceptance criterion #8).
//
// Closes the catalog-side half of the basic V2 scenario and proves the
// REST client's drop+register sequence works against real Lakekeeper.
func TestMigrateOne_E2E_Lakekeeper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	// Seed at the "old" prefix in the target bucket. The seed places
	// data under iceberg/db/<table>/, so the source prefix becomes
	// s3://target/iceberg/db/orders_old/.
	const tableName = "orders_v1"
	seed := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders_old")

	// Create namespace and register the seeded table at its old URI.
	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	// Sanity check: the catalog client can load the table at its
	// original metadata-location.
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

	// Simulate the bulk byte copy from old prefix to new prefix
	// (within the same bucket — Lakekeeper's warehouse covers the
	// whole bucket, so this stays valid for register).
	const oldKeyPrefix = "iceberg/db/orders_old/"
	const newKeyPrefix = "iceberg/db/orders_new/"
	for _, key := range []string{
		"data/file-1.parquet",
		"metadata/m1.avro",
		"metadata/snap-100-1-uuid.avro",
		"metadata/v2.metadata.json",
	} {
		srcURI := fmt.Sprintf("s3://%s/%s%s", s.MinIO.TargetBucket, oldKeyPrefix, key)
		dstURI := fmt.Sprintf("s3://%s/%s%s", s.MinIO.TargetBucket, newKeyPrefix, key)
		body, err := store.GetObject(ctx, srcURI)
		if err != nil {
			t.Fatalf("get %s: %v", srcURI, err)
		}
		if err := store.PutObject(ctx, dstURI, body); err != nil {
			t.Fatalf("put %s: %v", dstURI, err)
		}
	}

	// Run the engine: rewrite every path-bearing field from the old
	// prefix to the new prefix.
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
	res, err := eng.RewriteTable(ctx, tbl.MetadataLocation)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}
	wantNewLoc := strings.Replace(seed.MetadataURI, oldKeyPrefix, newKeyPrefix, 1)
	if res.NewMetadataLocation != wantNewLoc {
		t.Fatalf("NewMetadataLocation = %q, want %q", res.NewMetadataLocation, wantNewLoc)
	}

	// Atomic catalog swap.
	if err := cat.SwapMetadataLocation(ctx, id, tbl.MetadataLocation, res.NewMetadataLocation); err != nil {
		t.Fatalf("SwapMetadataLocation: %v", err)
	}

	// After swap: the catalog returns the new location.
	reloaded, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable post-rebase: %v", err)
	}
	if reloaded.MetadataLocation != wantNewLoc {
		t.Errorf("post-rebase pointer: have %q, want %q", reloaded.MetadataLocation, wantNewLoc)
	}

	// And post-swap validation passes — the data file lives at the new
	// path (we copied it above) and the metadata graph hangs together.
	if err := rewrite.ValidateAfterSwap(ctx, store, wantNewLoc); err != nil {
		t.Errorf("ValidateAfterSwap: %v", err)
	}
}
