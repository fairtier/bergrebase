package test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestDuckDBOracle_V2EqualityDeletes covers V2 equality-delete files.
// Snap 1 adds 100 rows (id=1..100); snap 2 adds an equality-delete file
// listing the even ids (2, 4, ..., 100). DuckDB iceberg_scan must
// return 50 rows / sum=2500 post-rebase.
//
// Equality-delete files don't contain embedded URIs (only the equality
// column values), so bergrebase byte-copies them unchanged and only
// mutates the manifest entry's FilePath. The load-bearing assertion is
// that lifting the blanket delete-manifest refusal still produces a
// readable table (no regression in the manifest-entry path mutation).
func TestDuckDBOracle_V2EqualityDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	seed := harness.SeedV2WithEqualityDeletes(ctx, t, store, mi.SourceBucket, "orders_oracle_eq_del")

	wantCount := seed.PostDeleteCount
	wantSum := seed.PostDeleteSum

	pre := harness.QueryIcebergScan(t, mi, seed.MetadataURI)
	if pre.Count != wantCount || pre.SumID != wantSum {
		t.Fatalf("pre-rebase oracle on V2 equality-deletes: got %+v, want count=%d sum=%d (seed/oracle disagreement, fix before touching engine)",
			pre, wantCount, wantSum)
	}

	keysToCopy := make([]string, 0, len(seed.AllURIs))
	for _, uri := range seed.AllURIs {
		keysToCopy = append(keysToCopy, strings.TrimPrefix(uri, "s3://"+mi.SourceBucket+"/"))
	}
	harness.CopyBucketToBucket(ctx, t, store, keysToCopy, mi.SourceBucket, mi.TargetBucket)

	mapping := rewrite.PrefixMapping{
		Source: "s3://" + mi.SourceBucket + "/",
		Target: "s3://" + mi.TargetBucket + "/",
	}
	eng := &rewrite.Engine{
		Source: store, Target: store,
		Opts:   rewrite.Options{Mapping: mapping},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	res, err := eng.RewriteTable(ctx, seed.MetadataURI)
	if err != nil {
		t.Fatalf("RewriteTable: %v", err)
	}

	post := harness.QueryIcebergScan(t, mi, res.NewMetadataLocation)
	if post.Count != wantCount || post.SumID != wantSum {
		t.Fatalf("post-rebase oracle on V2 equality-deletes: got %+v, want count=%d sum=%d (rewritten manifest entry's file_path likely still points at source bucket)",
			post, wantCount, wantSum)
	}
}
