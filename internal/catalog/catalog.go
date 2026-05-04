// Package catalog defines the small interface the rewriter uses to swap a
// table's metadata-location pointer at the catalog after on-storage
// metadata has been rewritten.
//
// Implementations live under sub-packages: rest/ for the Iceberg REST spec
// (Lakekeeper, Polaris, Tabular, Snowflake Open Catalog, Glue's REST
// shim); future packages may add native Hive, JDBC, and AWS Glue clients.
package catalog

import "context"

// Identifier is a fully-qualified table identifier within a warehouse.
// Multipart namespaces (e.g. "a.b.c") are represented as separate elements.
type Identifier struct {
	Namespace []string
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
	// ListTables returns table identifiers within a namespace.
	ListTables(ctx context.Context, ns []string) ([]Identifier, error)

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
