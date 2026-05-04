package test

import (
	"context"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestStack_PolarisBootstrap is a smoke test for the MinIO + Polaris
// stack. It asserts that after StartPolarisStack returns, the bergrebase
// REST catalog client (the *same* client used against Lakekeeper) can
// reach Polaris, resolve the warehouse prefix via overrides.prefix
// (Polaris's contract), and call ListTables.
//
// This is the first proof that bergrebase's catalog code is genuinely
// REST-spec-compliant rather than accidentally Lakekeeper-specific.
func TestStack_PolarisBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartPolarisStack(ctx, t)

	cli := rest.New(rest.Config{
		URI:       s.Polaris.BaseURI,
		Warehouse: s.Polaris.Warehouse,
		Token:     s.Polaris.Token,
	})

	// "default" namespace doesn't exist yet, so ListTables on it should
	// fail with NoSuchNamespace; that proves the catalog is reachable
	// AND the warehouse prefix resolved correctly. A wrong prefix would
	// surface as 404 on the catalog itself.
	_, err := cli.ListTables(ctx, []string{"default"})
	if err == nil {
		// Empty list is also acceptable.
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "NoSuchCatalog") || strings.Contains(msg, "CatalogNotFound") {
		t.Fatalf("warehouse prefix did not resolve: %v", err)
	}
	if !strings.Contains(msg, "NoSuchNamespace") &&
		!strings.Contains(msg, "NamespaceNotFound") &&
		!strings.Contains(msg, "Namespace does not exist") {
		t.Fatalf("ListTables: unexpected error type: %v", err)
	}
}
