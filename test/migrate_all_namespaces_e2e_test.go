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

// TestMigrateAllNamespaces_E2E_Lakekeeper exercises the warehouse-wide
// rebase flow that --all-namespaces enables: discover every namespace
// (root + nested), list tables in each, and rebase every one.
//
// Layout:
//
//	bronze/orders                          (single-part namespace)
//	silver/customers                       (single-part namespace)
//	analytics/exports/metrics              (nested namespace)
func TestMigrateAllNamespaces_E2E_Lakekeeper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	type fixture struct {
		ns        []string
		table     string
		oldFolder string // folder name under iceberg/db/
		newFolder string
		// uniqueUUID overrides the table-uuid in the seeded metadata.json.
		// SeedV2Basic hardcodes a single UUID; Lakekeeper rejects two
		// registrations with the same UUID as duplicates, so each fixture
		// gets a distinct one.
		uniqueUUID string
		seed       harness.V2Seed
	}
	fixtures := []*fixture{
		{ns: []string{"bronze"}, table: "orders", oldFolder: "bronze_orders_old", newFolder: "bronze_orders_new", uniqueUUID: "9c12d441-03fe-4693-9a96-a0705ddf6ea1"},
		{ns: []string{"silver"}, table: "customers", oldFolder: "silver_customers_old", newFolder: "silver_customers_new", uniqueUUID: "9c12d441-03fe-4693-9a96-a0705ddf6ea2"},
		{ns: []string{"analytics", "exports"}, table: "metrics", oldFolder: "analytics_exports_metrics_old", newFolder: "analytics_exports_metrics_new", uniqueUUID: "9c12d441-03fe-4693-9a96-a0705ddf6ea3"},
	}

	// The nested namespace requires its parent to exist first.
	parents := [][]string{{"bronze"}, {"silver"}, {"analytics"}, {"analytics", "exports"}}
	for _, ns := range parents {
		s.CreateNamespace(ctx, t, ns)
	}

	// Seed each table at its old prefix, rewrite the table-uuid to a
	// fixture-unique value, then register it.
	const seedUUID = "9c12d441-03fe-4693-9a96-a0705ddf69c1"
	for _, f := range fixtures {
		f.seed = harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, f.oldFolder)
		body, err := store.GetObject(ctx, f.seed.MetadataURI)
		if err != nil {
			t.Fatalf("read seeded metadata %s: %v", f.seed.MetadataURI, err)
		}
		patched := strings.Replace(string(body), seedUUID, f.uniqueUUID, 1)
		if patched == string(body) {
			t.Fatalf("expected seed UUID %q in metadata for %s but did not find it",
				seedUUID, f.oldFolder)
		}
		if err := store.PutObject(ctx, f.seed.MetadataURI, []byte(patched)); err != nil {
			t.Fatalf("rewrite metadata %s: %v", f.seed.MetadataURI, err)
		}
		s.RegisterTable(ctx, t, f.ns, f.table, f.seed.MetadataURI)
	}

	cat := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})

	// Discover every namespace via the new ListNamespaces method, walking
	// breadth-first the same way cmd/bergrb listAllNamespaces does.
	gotNamespaces, err := walkNamespacesBFS(ctx, cat)
	if err != nil {
		t.Fatalf("walkNamespacesBFS: %v", err)
	}
	wantNamespaces := map[string]bool{
		"bronze":            true,
		"silver":            true,
		"analytics":         true,
		"analytics.exports": true,
	}
	for _, ns := range gotNamespaces {
		key := strings.Join(ns, ".")
		// Tolerate any extra namespaces the catalog may pre-create
		// (Lakekeeper bootstraps without one, but this keeps the test
		// resilient to future fixture changes).
		delete(wantNamespaces, key)
	}
	if len(wantNamespaces) > 0 {
		t.Fatalf("ListNamespaces missed: %v (got: %v)", wantNamespaces, gotNamespaces)
	}

	// For each fixture, copy the seed bytes to the new folder and rebase.
	for _, f := range fixtures {
		oldKeyPrefix := fmt.Sprintf("iceberg/db/%s/", f.oldFolder)
		newKeyPrefix := fmt.Sprintf("iceberg/db/%s/", f.newFolder)
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

		id := catalog.Identifier{Namespace: f.ns, Name: f.table}
		tbl, err := cat.LoadTable(ctx, id)
		if err != nil {
			t.Fatalf("LoadTable %s: %v", strings.Join(append(f.ns, f.table), "."), err)
		}
		res, err := eng.RewriteTable(ctx, tbl.MetadataLocation)
		if err != nil {
			t.Fatalf("RewriteTable %s: %v", strings.Join(append(f.ns, f.table), "."), err)
		}
		wantNewLoc := strings.Replace(f.seed.MetadataURI, oldKeyPrefix, newKeyPrefix, 1)
		if res.NewMetadataLocation != wantNewLoc {
			t.Fatalf("NewMetadataLocation = %q, want %q", res.NewMetadataLocation, wantNewLoc)
		}
		if err := cat.SwapMetadataLocation(ctx, id, tbl.MetadataLocation, res.NewMetadataLocation); err != nil {
			t.Fatalf("SwapMetadataLocation %s: %v", strings.Join(append(f.ns, f.table), "."), err)
		}
	}

	// Final assertion: ListTables in each namespace returns the expected
	// tables, and each table's pointer now lives under its new prefix.
	for _, f := range fixtures {
		ids, err := cat.ListTables(ctx, f.ns)
		if err != nil {
			t.Fatalf("ListTables %s: %v", strings.Join(f.ns, "."), err)
		}
		var found bool
		for _, id := range ids {
			if id.Name == f.table {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("namespace %s: table %s missing from ListTables", strings.Join(f.ns, "."), f.table)
			continue
		}
		reloaded, err := cat.LoadTable(ctx, catalog.Identifier{Namespace: f.ns, Name: f.table})
		if err != nil {
			t.Fatalf("LoadTable post-rebase %s: %v", strings.Join(append(f.ns, f.table), "."), err)
		}
		wantPrefix := fmt.Sprintf("s3://%s/iceberg/db/%s/", s.MinIO.TargetBucket, f.newFolder)
		if !strings.HasPrefix(reloaded.MetadataLocation, wantPrefix) {
			t.Errorf("namespace %s table %s: pointer %q, want prefix %q",
				strings.Join(f.ns, "."), f.table, reloaded.MetadataLocation, wantPrefix)
		}
	}
}

// walkNamespacesBFS mirrors cmd/bergrb listAllNamespaces: BFS-walk the
// namespace tree starting from the root, calling ListNamespaces with each
// discovered namespace as parent.
func walkNamespacesBFS(ctx context.Context, cat catalog.Catalog) ([][]string, error) {
	roots, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, err
	}
	all := make([][]string, 0, len(roots))
	queue := append([][]string(nil), roots...)
	for len(queue) > 0 {
		ns := queue[0]
		queue = queue[1:]
		all = append(all, ns)
		children, err := cat.ListNamespaces(ctx, ns)
		if err != nil {
			return nil, err
		}
		queue = append(queue, children...)
	}
	return all, nil
}
