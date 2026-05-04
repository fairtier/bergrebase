package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// forceFallbackTransport intercepts every POST .../register?overwrite=true
// and returns a synthetic 409 AlreadyExistsException, mimicking a REST
// catalog that ignores the query parameter and rejects the duplicate
// registration. All other requests pass through to the wrapped
// transport untouched. This forces rest.Client to cache
// swapModeDropRegister and exercise the legacy drop+register path
// against the real catalog backend.
type forceFallbackTransport struct {
	wrapped http.RoundTripper
	probes  int
}

func (t *forceFallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost &&
		strings.HasSuffix(req.URL.Path, "/register") &&
		req.URL.Query().Get("overwrite") == "true" {
		t.probes++
		body := `{"error":{"message":"AlreadyExistsException: table already exists","type":"AlreadyExistsException","code":409}}`
		resp := &http.Response{
			Status:        "409 Conflict",
			StatusCode:    http.StatusConflict,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       req,
		}
		return resp, nil
	}
	return t.wrapped.RoundTrip(req)
}

// TestMigrateOne_E2E_Lakekeeper_DropRegisterFallback verifies the
// drop+register fallback path works end-to-end against a real
// Lakekeeper, even when atomic register?overwrite=true is unavailable.
//
// Lakekeeper v0.12.0 supports overwrite=true natively, so the default
// migrate test exercises the atomic path. Without this test, the
// fallback path is only covered by httptest mocks and would silently
// rot if a future refactor broke the drop+register sequence — until a
// production user with an older Lakekeeper hit it.
//
// The test wires a custom http.RoundTripper into rest.Config.HTTPClient
// that returns 409 AlreadyExistsException on every register?overwrite=
// true; everything else (config, load, drop, register, ListTables,
// etc.) reaches Lakekeeper unchanged. The Client treats the 409 as the
// "fall back to drop+register" signature, drops the table without
// purge, then registers it at the new location. The catalog ends up at
// newLocation and the table validates the same as the atomic path.
func TestMigrateOne_E2E_Lakekeeper_DropRegisterFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	const tableName = "orders_v1_fb"
	seed := harness.SeedV2Basic(ctx, t, store, s.MinIO.TargetBucket, "orders_old_fb")

	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	transport := &forceFallbackTransport{wrapped: http.DefaultTransport}
	cat := rest.New(rest.Config{
		URI:        s.Lakekeeper.BaseURI,
		Warehouse:  s.Lakekeeper.Warehouse,
		HTTPClient: &http.Client{Transport: transport},
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("LoadTable pre-rebase: %v", err)
	}
	if tbl.MetadataLocation != seed.MetadataURI {
		t.Fatalf("pre-rebase pointer: have %q, want %q", tbl.MetadataLocation, seed.MetadataURI)
	}

	const oldKeyPrefix = "iceberg/db/orders_old_fb/"
	const newKeyPrefix = "iceberg/db/orders_new_fb/"
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

	// Confirm the transport intercepted the atomic probe — otherwise the
	// test silently degraded into the atomic path and isn't exercising
	// what its name says.
	if transport.probes == 0 {
		t.Fatal("forceFallbackTransport never saw a register?overwrite=true probe; the fallback path was not exercised")
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
