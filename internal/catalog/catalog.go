// Package catalog defines the small interface the rewriter uses to swap a
// table's metadata-location pointer at the catalog after on-storage
// metadata has been rewritten.
//
// Implementations live under sub-packages: rest/ for the Iceberg REST spec
// (Lakekeeper, Polaris, Tabular, Snowflake Open Catalog, Glue's REST
// shim); future packages may add native Hive, JDBC, and AWS Glue clients.
package catalog

import "context"

// Namespace is an Iceberg namespace path. Multipart namespaces (e.g.
// "a.b.c") are represented as separate elements; single-level namespaces
// have one element. An empty Namespace denotes the warehouse root and is
// only meaningful as the parent argument to ListNamespaces.
//
// It is a type alias rather than a defined type so existing []string
// literals at call sites continue to work without conversion. The alias
// matches iceberg-go's table.Identifier shape (which is also = []string)
// without forcing the iceberg-go/table import — that subpackage drags in
// substrait, pterm, and other heavyweight deps the rewriter doesn't need.
type Namespace = []string

// Identifier is a fully-qualified table identifier within a warehouse.
type Identifier struct {
	Namespace Namespace
	Name      string
}

// Table is the minimal subset of Iceberg table information the rewriter
// needs from a catalog: the absolute URI of the current metadata.json and
// the snapshot ID that was current at load time. The full table.Metadata
// is read separately from object storage by the rewrite engine.
type Table struct {
	Identifier        Identifier
	MetadataLocation  string
	CurrentSnapshotID *int64
}

// Catalog is the swap surface the rewriter depends on. Read operations
// list and load tables; write operations atomically re-point the metadata
// location.
//
// Implementations should be safe to call from a single goroutine; the
// rewriter does not invoke them concurrently per table.
type Catalog interface {
	// ListNamespaces returns the immediate child namespaces of parent. A
	// nil or empty parent lists top-level namespaces. Callers walk
	// recursively (BFS/DFS) to enumerate every namespace in a warehouse;
	// the spec does not provide a single-shot recursive listing.
	ListNamespaces(ctx context.Context, parent Namespace) ([]Namespace, error)

	// ListTables returns table identifiers within a namespace.
	ListTables(ctx context.Context, ns Namespace) ([]Identifier, error)

	// LoadTable returns the current pointer state for a table.
	LoadTable(ctx context.Context, id Identifier) (*Table, error)

	// SwapMetadataLocation atomically re-points id from oldLocation to
	// newLocation.
	//
	// Implementations should prefer a single atomic catalog call when the
	// underlying API supports it (REST register-with-overwrite, JDBC row
	// update, Glue UpdateTable). For catalogs that do not support an
	// atomic swap, drop-without-purge followed by register is acceptable
	// — writers must be paused before the call.
	//
	// On error the implementation must roll the catalog pointer back to
	// oldLocation; the rewriter relies on this invariant for its rollback
	// story. oldLocation is passed in (rather than re-loaded inside the
	// call) so the rollback survives a drop that has already executed.
	SwapMetadataLocation(ctx context.Context, id Identifier, oldLocation, newLocation string) error
}
