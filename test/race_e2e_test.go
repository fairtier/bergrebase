package test

import (
	"context"
	"errors"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestSwapMetadataLocation_DetectsDriftAgainstLakekeeper: if a
// concurrent writer commits a snapshot between LoadTable and
// SwapMetadataLocation, the rebase would clobber that snapshot. The
// swap must detect catalog drift (current metadata-location !=
// oldLocation) and refuse before the destructive drop.
//
// We simulate the concurrent writer by drop+register-ing the table at
// a different metadata-location after the test's LoadTable but before
// its SwapMetadataLocation call. Lakekeeper will then resolve a
// different pointer than what bergrebase loaded — the swap must detect
// this and return ErrCatalogDrift.
func TestSwapMetadataLocation_DetectsDriftAgainstLakekeeper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	// Two valid V2 metadata.jsons at different paths in the warehouse
	// bucket. seedA is what bergrebase will believe is "current"; seedB
	// is what a concurrent writer commits to.
	const tableName = "orders_race"
	seedA := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders_race_a")
	seedB := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders_race_b")

	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seedA.MetadataURI)

	cat := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}

	// bergrebase loads at seedA.
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if tbl.MetadataLocation != seedA.MetadataURI {
		t.Fatalf("pre-race pointer: %q, want %q", tbl.MetadataLocation, seedA.MetadataURI)
	}

	// Concurrent writer: drop + re-register at seedB. After this the
	// catalog points at seedB, but bergrebase's `tbl` still references
	// seedA — exactly the drift the protection guards against.
	if err := cat.SwapMetadataLocation(ctx, id, seedA.MetadataURI, seedB.MetadataURI); err != nil {
		t.Fatalf("simulate concurrent commit: %v", err)
	}

	// Now bergrebase tries to swap from its stale view (seedA → newC).
	// The pre-swap reload inside SwapMetadataLocation must catch the
	// drift and refuse, leaving the catalog pointer at seedB.
	const newC = "s3://target/iceberg/db/orders_race_c/metadata/v2.metadata.json"
	err = cat.SwapMetadataLocation(ctx, id, tbl.MetadataLocation, newC)
	if !errors.Is(err, rest.ErrCatalogDrift) {
		t.Fatalf("expected ErrCatalogDrift, got %v", err)
	}

	// And the writer's commit (seedB) must still be the current
	// pointer; the refused swap left the catalog untouched.
	final, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable after refused swap: %v", err)
	}
	if final.MetadataLocation != seedB.MetadataURI {
		t.Errorf("post-refusal pointer: %q, want %q (writer's commit clobbered)",
			final.MetadataLocation, seedB.MetadataURI)
	}
}
