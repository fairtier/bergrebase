package test

import (
	"context"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestSwapMetadataLocation_RollbackOnRegisterFailure: if a swap fails,
// the catalog must still resolve the table at its original
// metadata-location.
//
// Two paths can produce that invariant:
//   - Atomic register?overwrite=true fails (e.g. malformed metadata,
//     400 from Lakekeeper); the catalog was never written, so the
//     pointer is unchanged by virtue of nothing happening.
//   - Drop+register fallback's register fails after drop; the client
//     re-registers oldLocation. The unit-test
//     TestClient_SwapMetadataLocation_RegisterFails_RollsBack covers
//     the rollback wire sequence; this e2e asserts the contract holds
//     against a real Lakekeeper.
//
// Lakekeeper rejects a malformed metadata.json with 400 Deserialization
// regardless of overwrite=true, so this test exercises the atomic-path
// failure against the current testcontainer version.
func TestSwapMetadataLocation_RollbackOnRegisterFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	// Register a healthy table at oldLoc.
	const tableName = "orders_rb"
	seed := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders_rb_old")
	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	// Plant a deliberately-broken metadata.json that Lakekeeper will
	// refuse on register. JSON-parses, but its `format-version` value
	// is a string instead of an int — kicks Lakekeeper's strict
	// deserializer down the same code path a real corrupted file
	// would, without producing a "metadata not found" 404 (which
	// would exercise a different error branch).
	const badLoc = "s3://target/iceberg/db/orders_rb_bad/metadata/v2.metadata.json"
	if err := store.PutObject(ctx, badLoc, []byte(`{"format-version": "two"}`)); err != nil {
		t.Fatalf("put bad metadata: %v", err)
	}

	cat := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}

	// Sanity: pre-swap the table loads at the seeded location.
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable pre-swap: %v", err)
	}
	if tbl.MetadataLocation != seed.MetadataURI {
		t.Fatalf("pre-swap pointer: have %q, want %q", tbl.MetadataLocation, seed.MetadataURI)
	}

	// Swap to a deliberately-bad new location. Expect a register failure;
	// the error wording differs by path (atomic vs drop+register/rolled
	// back), so assert on the underlying cause that's stable across both.
	err = cat.SwapMetadataLocation(ctx, id, seed.MetadataURI, badLoc)
	if err == nil {
		t.Fatal("expected SwapMetadataLocation to fail on bad newLocation")
	}
	if !strings.Contains(err.Error(), "register") {
		t.Errorf("error should identify a register failure: %v", err)
	}

	// Acceptance criterion #5: the catalog still resolves the table at
	// its original metadata-location. Holds for the atomic path (catalog
	// untouched on probe failure) and the drop+register fallback path
	// (rollback re-registers oldLocation).
	reloaded, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable post-failure: %v", err)
	}
	if reloaded.MetadataLocation != seed.MetadataURI {
		t.Errorf("post-failure pointer: have %q, want %q (catalog drifted)",
			reloaded.MetadataLocation, seed.MetadataURI)
	}
}
