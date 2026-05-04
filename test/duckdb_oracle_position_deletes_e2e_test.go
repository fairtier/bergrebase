package test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestDuckDBOracle_V2PositionDeletes is the load-bearing scenario for
// V2 position-delete file rewrite. The fixture is a 100-row table with
// snap 2 carrying a position-delete file that drop-marks every even
// position (50 rows). DuckDB iceberg_scan must return 50 rows / sum=2500
// post-rebase — same as pre-rebase.
//
// The position-delete Parquet contains absolute URIs in its file_path
// column pointing at the source bucket. If the engine doesn't rewrite
// that column, post-rebase reads either see ghost deletes against
// missing files (count=100) or skip the deletes entirely (count=100).
// Either way the assertion fails.
//
// Direct-read assertion: the rewritten position-delete Parquet's
// file_path column values must all start with the target prefix. This
// catches a regression where the column pointer is wrong but DuckDB
// happens to return the right count for the wrong reason.
func TestDuckDBOracle_V2PositionDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNoDuckDB(t)

	ctx := context.Background()
	mi := harness.StartMinIO(ctx, t)
	store := mi.StorageClient()

	seed := harness.SeedV2WithPositionDeletes(ctx, t, store, mi.SourceBucket, "orders_oracle_pos_del")

	wantCount := seed.PostDeleteCount
	wantSum := seed.PostDeleteSum

	pre := harness.QueryIcebergScan(t, mi, seed.MetadataURI)
	if pre.Count != wantCount || pre.SumID != wantSum {
		t.Fatalf("pre-rebase oracle on V2 position-deletes: got %+v, want count=%d sum=%d (seed/oracle disagreement, fix before touching engine)",
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
		t.Fatalf("post-rebase oracle on V2 position-deletes: got %+v, want count=%d sum=%d (file_path column inside position-delete Parquet likely still points at source bucket → ghost deletes against missing files)",
			post, wantCount, wantSum)
	}

	// Direct-read: every value in the rewritten position-delete file's
	// file_path column must start with the target prefix.
	newDelURI := mapping.Apply(seed.DeleteFileURI)
	delBytes, err := store.GetObject(ctx, newDelURI)
	if err != nil {
		t.Fatalf("get rewritten position-delete file %s: %v", newDelURI, err)
	}
	pqf, err := file.NewParquetReader(bytes.NewReader(delBytes))
	if err != nil {
		t.Fatalf("open rewritten position-delete parquet: %v", err)
	}
	defer func() { _ = pqf.Close() }()
	rdr, err := pqarrow.NewFileReader(pqf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		t.Fatalf("pqarrow reader: %v", err)
	}
	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	defer tbl.Release()

	col := tbl.Column(tbl.Schema().FieldIndices("file_path")[0]).Data()
	if col.Len() == 0 {
		t.Fatal("rewritten position-delete file has no rows")
	}
	for c, ch := range col.Chunks() {
		chunk := ch.(*array.String)
		for i := 0; i < chunk.Len(); i++ {
			v := chunk.Value(i)
			if !strings.HasPrefix(v, mapping.Target) {
				t.Fatalf("rewritten position-delete file_path chunk=%d row=%d value=%q does not begin with target prefix %q", c, i, v, mapping.Target)
			}
		}
	}
}
