package test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestDuckDBOracle_V2Basic is the independent reader for the basic V2
// scenario. Earlier e2e tests assert that bergrebase
// writes the right URIs and the right manifest_length — but they all read
// back through iceberg-go, the same library that wrote the fixtures. A
// rewriter bug that produces self-consistent-but-actually-broken metadata
// would slip through.
//
// This test closes that gap by querying through DuckDB's iceberg extension,
// which has no shared code path with our writer:
//
//  1. Seed a real V2 table with 10 rows of (id int64) = 1..10 in MinIO.
//  2. Pre-rebase oracle: SELECT count(*), sum(id) via iceberg_scan against
//     the source metadata.json. Must equal (10, 55).
//  3. Byte-copy the four fixture files from source bucket to target.
//  4. Run the rewrite engine.
//  5. Post-rebase oracle: same query against the rewritten metadata.json
//     in the target bucket. Must equal the pre-rebase result exactly.
//
// If DuckDB returns 0 rows post-rebase, manifest_length is wrong, the data
// file URI was rewritten incorrectly, or PARQUET:field_id metadata was lost
// — all bugs that the existing test suite would silently miss.
func TestDuckDBOracle_V2Basic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	m := harness.StartMinIO(ctx, t)
	store := m.StorageClient()

	seed := harness.SeedV2Basic(ctx, t, store, m.SourceBucket, "orders")

	pre := harness.QueryIcebergScan(t, m, seed.MetadataURI)
	if pre.Count != seed.RowCount || pre.SumID != seed.SumID {
		t.Fatalf("pre-rebase oracle: got count=%d sum=%d, want count=%d sum=%d (Parquet field-id metadata likely missing)",
			pre.Count, pre.SumID, seed.RowCount, seed.SumID)
	}

	// Bulk byte copy: every fixture key copies verbatim from source bucket
	// to target bucket. This is what rclone does in production, before
	// bergrebase runs.
	harness.CopyBucketToBucket(ctx, t, store, []string{
		"iceberg/db/orders/data/file-1.parquet",
		"iceberg/db/orders/metadata/m1.avro",
		"iceberg/db/orders/metadata/snap-100-1-uuid.avro",
		"iceberg/db/orders/metadata/v2.metadata.json",
	}, m.SourceBucket, m.TargetBucket)

	eng := &rewrite.Engine{
		Source: store,
		Target: store,
		Opts: rewrite.Options{
			Mapping: rewrite.PrefixMapping{
				Source: "s3://" + m.SourceBucket + "/",
				Target: "s3://" + m.TargetBucket + "/",
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	res, err := eng.RewriteTable(ctx, seed.MetadataURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	post := harness.QueryIcebergScan(t, m, res.NewMetadataLocation)
	if post != pre {
		t.Fatalf("oracle drift after rebase: pre=%+v post=%+v (rewritten metadata graph is not equivalent to source)", pre, post)
	}
}
