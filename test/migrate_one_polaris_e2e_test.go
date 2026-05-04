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

// TestMigrateOne_E2E_Polaris is the catalog-independence proof: the same
// load → rewrite → swap → validate flow that TestMigrateOne_E2E_Lakekeeper
// runs, but against an Apache Polaris testcontainer. If both pass with a
// single rest.Client implementation, bergrebase's catalog support is
// genuinely REST-spec-compliant, not accidentally Lakekeeper-specific.
//
// Diff from the Lakekeeper test:
//   - Stack uses StartPolarisStack (no PostgreSQL, OAuth2 token bootstrap,
//     5-step role-grant chain).
//   - rest.Config carries the bearer token; Polaris has no allow-all mode.
//   - Warehouse prefix lands in overrides.prefix (Polaris) rather than
//     defaults.prefix (Lakekeeper); the rest.Client already handles both
//     (rest.go:94).
//   - Rebase mapping is in-place (a sub-path of the existing table
//     location) rather than parallel prefixes. Polaris ties each table
//     to <default-base-location>/<namespace>/ and rejects register calls
//     whose new metadata-location is outside that subtree. Lakekeeper
//     has no such constraint. A real Polaris-backed migration would
//     either keep the path structure identical (e.g. bucket swap with
//     same prefixes) or recreate the catalog at a new base location;
//     this test covers the in-namespace rebase shape.
//
// The rest.Client's drop+register sequence is otherwise identical
// across both catalogs.
func TestMigrateOne_E2E_Polaris(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartPolarisStack(ctx, t)
	store := s.StorageClient()

	const tableName = "orders_v1"
	// Namespace name == seed table prefix so Polaris's
	// <default-base-location>/<namespace> location check matches the
	// seeded metadata path s3://target/iceberg/db/orders/. See the note
	// in createPolarisCatalog for why this is more constrained than the
	// Lakekeeper test.
	seed := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders")

	ns := []string{"orders"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	cat := rest.New(rest.Config{
		URI:       s.Polaris.BaseURI,
		Warehouse: s.Polaris.Warehouse,
		Token:     s.Polaris.Token,
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable pre-rebase: %v", err)
	}
	if tbl.MetadataLocation != seed.MetadataURI {
		t.Fatalf("pre-rebase pointer: have %q, want %q", tbl.MetadataLocation, seed.MetadataURI)
	}

	// Rebase to a sub-path of the table's existing location. New prefix
	// must remain inside Polaris's locked <base>/<namespace> subtree —
	// see the test docstring.
	const oldKeyPrefix = "iceberg/db/orders/"
	const newKeyPrefix = "iceberg/db/orders/v2/"
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
