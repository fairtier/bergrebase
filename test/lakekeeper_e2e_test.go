package test

import (
	"context"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestStack_LakekeeperBootstrap is a smoke test for the full
// MinIO+PostgreSQL+Lakekeeper stack. It asserts that after StartStack
// returns, the REST catalog client can call ListTables on the empty
// "rebase" warehouse without error.
//
// More substantive scenarios (15 rollback, 17 race, full migrateOne
// e2e) build on this same stack helper.
func TestStack_LakekeeperBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)

	cli := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})

	// The "default" namespace doesn't exist yet, so ListTables on it
	// should fail with a NamespaceNotFound; that proves the catalog is
	// reachable AND the warehouse prefix resolved correctly (a wrong
	// prefix would surface as WarehouseNotFound instead).
	_, err := cli.ListTables(ctx, []string{"default"})
	if err == nil {
		// Empty list is also acceptable — Lakekeeper auto-creates the
		// namespace in some configurations.
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "WarehouseNotFound") || strings.Contains(msg, "WarehouseIdIsNotUUID") {
		t.Fatalf("warehouse prefix did not resolve: %v", err)
	}
	if !strings.Contains(msg, "NamespaceNotFound") && !strings.Contains(msg, "NoSuchNamespace") && !strings.Contains(msg, "NamespaceActionForbidden") {
		t.Fatalf("ListTables: unexpected error type: %v", err)
	}
}
