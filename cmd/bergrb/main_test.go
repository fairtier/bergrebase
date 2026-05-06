package main

import (
	"context"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
)

func TestValidate_AllNamespacesMatrix(t *testing.T) {
	base := func() config {
		return config{
			catalogURI:       "http://x",
			catalogWarehouse: "wh",
			sourcePrefix:     "s3://a/",
			targetPrefix:     "s3://b/",
		}
	}

	cases := []struct {
		name    string
		mut     func(*config)
		wantErr string // substring; "" means must succeed
	}{
		{
			name: "all-namespaces alone is enough",
			mut: func(c *config) {
				c.allNamespaces = true
			},
		},
		{
			name: "all-namespaces conflicts with --namespace",
			mut: func(c *config) {
				c.allNamespaces = true
				c.namespace = "db"
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "all-namespaces conflicts with --table",
			mut: func(c *config) {
				c.allNamespaces = true
				c.table = "orders"
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "all-namespaces conflicts with --all-tables",
			mut: func(c *config) {
				c.allNamespaces = true
				c.allTables = true
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "single-namespace path still requires --namespace",
			mut: func(c *config) {
				c.allTables = true
			},
			wantErr: "--namespace",
		},
		{
			name: "single-namespace path requires --table xor --all-tables",
			mut: func(c *config) {
				c.namespace = "db"
			},
			wantErr: "--table or --all-tables",
		},
		{
			name: "single-namespace happy path with --all-tables",
			mut: func(c *config) {
				c.namespace = "db"
				c.allTables = true
			},
		},
		{
			name: "single-namespace happy path with --table",
			mut: func(c *config) {
				c.namespace = "db"
				c.table = "orders"
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mut(&c)
			err := c.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected ok, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// fakeCatalog implements catalog.Catalog for selectBatches /
// listAllNamespaces tests. It returns the configured nested namespace
// tree and a per-namespace table list. Only the read methods are wired.
type fakeCatalog struct {
	// children maps a parent namespace key (joined with ".") to its
	// immediate children. The empty key holds the root listing.
	children map[string][]catalog.Namespace
	// tables maps a namespace key to its tables.
	tables map[string][]catalog.Identifier
}

func nsKey(ns catalog.Namespace) string { return strings.Join(ns, ".") }

func (f *fakeCatalog) ListNamespaces(_ context.Context, parent catalog.Namespace) ([]catalog.Namespace, error) {
	return f.children[nsKey(parent)], nil
}

func (f *fakeCatalog) ListTables(_ context.Context, ns catalog.Namespace) ([]catalog.Identifier, error) {
	return f.tables[nsKey(ns)], nil
}

func (f *fakeCatalog) LoadTable(_ context.Context, _ catalog.Identifier) (*catalog.Table, error) {
	return nil, nil
}

func (f *fakeCatalog) SwapMetadataLocation(_ context.Context, _ catalog.Identifier, _, _ string) error {
	return nil
}

func TestListAllNamespaces_BFSDescendsIntoNested(t *testing.T) {
	cat := &fakeCatalog{
		children: map[string][]catalog.Namespace{
			"":                  {{"bronze"}, {"silver"}, {"analytics"}},
			"bronze":            nil,
			"silver":            nil,
			"analytics":         {{"analytics", "exports"}},
			"analytics.exports": nil,
		},
	}
	got, err := listAllNamespaces(context.Background(), cat)
	if err != nil {
		t.Fatalf("listAllNamespaces: %v", err)
	}
	want := []catalog.Namespace{
		{"bronze"}, {"silver"}, {"analytics"}, {"analytics", "exports"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if strings.Join(got[i], ".") != strings.Join(want[i], ".") {
			t.Errorf("[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestSelectBatches_AllNamespaces(t *testing.T) {
	cat := &fakeCatalog{
		children: map[string][]catalog.Namespace{
			"":                  {{"bronze"}, {"analytics"}},
			"bronze":            nil,
			"analytics":         {{"analytics", "exports"}},
			"analytics.exports": nil,
		},
		tables: map[string][]catalog.Identifier{
			"bronze": {
				{Namespace: []string{"bronze"}, Name: "orders"},
				{Namespace: []string{"bronze"}, Name: "customers"},
			},
			"analytics":         {},
			"analytics.exports": {{Namespace: []string{"analytics", "exports"}, Name: "metrics"}},
		},
	}
	cfg := &config{allNamespaces: true}
	batches, err := selectBatches(context.Background(), cat, cfg)
	if err != nil {
		t.Fatalf("selectBatches: %v", err)
	}
	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3: %+v", len(batches), batches)
	}
	wantCounts := []int{2, 0, 1}
	for i, b := range batches {
		if len(b.tables) != wantCounts[i] {
			t.Errorf("batch[%d] (%s): %d tables, want %d",
				i, namespaceLabel(b.ns), len(b.tables), wantCounts[i])
		}
	}
}
