package rest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fairtier/bergrebase/internal/catalog"
)

// fakeServer is a small dispatcher used by the REST tests. Each handler
// writes its own response; the dispatcher records every request so the
// test can assert sequencing (drop-then-register).
type fakeServer struct {
	t      *testing.T
	srv    *httptest.Server
	calls  []recordedCall
	prefix string

	// disableAtomic makes register?overwrite=true respond as if the server
	// doesn't honour the query parameter — i.e. it returns 409
	// AlreadyExistsException, the same shape Iceberg REST servers emit
	// when the table already exists. The Client treats this as the
	// "fall back to drop+register" signature. Set this in tests that
	// want to exercise the legacy drop+register path deterministically.
	disableAtomic bool

	// Per-handler overrides (set by individual tests).
	registerHandler   func(w http.ResponseWriter, r *http.Request)
	dropHandler       func(w http.ResponseWriter, r *http.Request)
	loadHandler       func(w http.ResponseWriter, r *http.Request)
	listHandler       func(w http.ResponseWriter, r *http.Request)
	namespacesHandler func(w http.ResponseWriter, r *http.Request)
}

type recordedCall struct {
	Method string
	Path   string
	Query  string
	Body   string
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{t: t, prefix: "ws-001"}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		_ = json.NewEncoder(w).Encode(configResponse{
			Defaults:  map[string]string{},
			Overrides: map[string]string{},
			Prefix:    f.prefix,
		})
	})
	mux.HandleFunc("/v1/"+f.prefix+"/", func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		switch {
		case strings.HasSuffix(r.URL.Path, "/register") && r.Method == http.MethodPost:
			if f.registerHandler != nil {
				f.registerHandler(w, r)
				return
			}
			if f.disableAtomic && r.URL.Query().Get("overwrite") == "true" {
				http.Error(w, `{"error":{"message":"AlreadyExistsException: table already exists","code":409,"type":"AlreadyExistsException"}}`, http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"metadata-location":"s3://new/x.json","metadata":{}}`))
		case strings.HasSuffix(r.URL.Path, "/namespaces") && r.Method == http.MethodGet:
			if f.namespacesHandler != nil {
				f.namespacesHandler(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"namespaces": [][]string{
					{"bronze"},
					{"silver"},
					{"analytics"},
				},
			})
		case strings.HasSuffix(r.URL.Path, "/tables") && r.Method == http.MethodGet:
			if f.listHandler != nil {
				f.listHandler(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []map[string]any{
					{"namespace": []string{"db"}, "name": "orders"},
					{"namespace": []string{"db"}, "name": "customers"},
				},
			})
		case strings.Contains(r.URL.Path, "/tables/") && r.Method == http.MethodGet:
			if f.loadHandler != nil {
				f.loadHandler(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"metadata-location":"s3://old/v1.metadata.json","metadata":{"current-snapshot-id":42}}`))
		case strings.Contains(r.URL.Path, "/tables/") && r.Method == http.MethodDelete:
			if f.dropHandler != nil {
				f.dropHandler(w, r)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) record(r *http.Request) {
	body := ""
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		// Restore body so downstream handlers can read it too.
		r.Body = io.NopCloser(strings.NewReader(body))
	}
	f.calls = append(f.calls, recordedCall{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Body:   body,
	})
}

func TestClient_LoadTable(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL, Warehouse: "wh", Token: "test-token"})
	ctx := context.Background()

	tbl, err := c.LoadTable(ctx, catalog.Identifier{Namespace: []string{"db"}, Name: "orders"})
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if tbl.MetadataLocation != "s3://old/v1.metadata.json" {
		t.Errorf("metadata location: %s", tbl.MetadataLocation)
	}
	if tbl.CurrentSnapshotID == nil || *tbl.CurrentSnapshotID != 42 {
		t.Errorf("current snapshot id: %v", tbl.CurrentSnapshotID)
	}

	// Verify token + prefix made it onto the wire.
	tableCall := lastCallMatching(f.calls, "/tables/orders")
	if tableCall.Path != "/v1/ws-001/namespaces/db/tables/orders" {
		t.Errorf("path = %q", tableCall.Path)
	}
}

func TestClient_ListTables(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL, Warehouse: "wh", Token: "tok"})
	out, err := c.ListTables(context.Background(), []string{"db"})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d, want 2: %+v", len(out), out)
	}
	want := []string{"orders", "customers"}
	for i, w := range want {
		if out[i].Name != w {
			t.Errorf("[%d] = %q, want %q", i, out[i].Name, w)
		}
	}
}

func TestClient_ListNamespaces_Top(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL, Warehouse: "wh", Token: "tok"})
	out, err := c.ListNamespaces(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	want := [][]string{{"bronze"}, {"silver"}, {"analytics"}}
	if len(out) != len(want) {
		t.Fatalf("got %d, want %d: %+v", len(out), len(want), out)
	}
	for i, w := range want {
		if !sliceEq(out[i], w) {
			t.Errorf("[%d] = %v, want %v", i, out[i], w)
		}
	}
	call := lastCallMatching(f.calls, "/namespaces")
	if call.Path != "/v1/ws-001/namespaces" {
		t.Errorf("path = %q, want /v1/ws-001/namespaces", call.Path)
	}
	if call.Query != "" {
		t.Errorf("top-level call must not carry parent, got query %q", call.Query)
	}
}

// TestClient_ListNamespaces_NestedParent verifies that a multipart parent
// is encoded with the spec-mandated U+001F separator in the parent query
// value.
func TestClient_ListNamespaces_NestedParent(t *testing.T) {
	f := newFakeServer(t)
	f.namespacesHandler = func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespaces": [][]string{
				{"analytics", "exports", "eu"},
			},
		})
	}
	c := New(Config{URI: f.srv.URL, Token: "tok"})
	out, err := c.ListNamespaces(context.Background(), []string{"analytics", "exports"})
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(out) != 1 || !sliceEq(out[0], []string{"analytics", "exports", "eu"}) {
		t.Fatalf("unexpected children: %+v", out)
	}
	call := lastCallMatching(f.calls, "/namespaces")
	// url.Values escapes U+001F as %1F in query values.
	if !strings.Contains(strings.ToUpper(call.Query), "PARENT=ANALYTICS%1FEXPORTS") {
		t.Errorf("query must encode multipart parent with U+001F, got %q", call.Query)
	}
}

// TestClient_ListNamespaces_SinglePartParent verifies a single-element
// parent is sent verbatim (no separator) since there is nothing to join.
func TestClient_ListNamespaces_SinglePartParent(t *testing.T) {
	f := newFakeServer(t)
	f.namespacesHandler = func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	}
	c := New(Config{URI: f.srv.URL})
	if _, err := c.ListNamespaces(context.Background(), []string{"analytics"}); err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	call := lastCallMatching(f.calls, "/namespaces")
	if call.Query != "parent=analytics" {
		t.Errorf("query = %q, want parent=analytics", call.Query)
	}
}

func TestClient_SwapMetadataLocation_HappyPath(t *testing.T) {
	f := newFakeServer(t)
	// Force the legacy drop+register path: the atomic-vs-fallback flow
	// has its own dedicated tests below; this one asserts the
	// drop+register call sequence specifically.
	f.disableAtomic = true
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}
	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	// Sequence: config, pre-swap reload, atomic-probe (409),
	// drop, register.
	want := []string{
		"GET /v1/config",
		"GET /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register", // probe (overwrite=true → 409)
		"DELETE /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register", // fallback register
	}
	got := []string{}
	for _, c := range f.calls {
		got = append(got, c.Method+" "+c.Path)
	}
	if !sliceEq(got, want) {
		t.Errorf("call sequence:\n got=%v\nwant=%v", got, want)
	}

	// The fallback register (the second POST /register) must NOT carry
	// the overwrite query — that's the actual legacy path.
	var regCalls []recordedCall
	for _, c := range f.calls {
		if c.Method == http.MethodPost && strings.HasSuffix(c.Path, "/register") {
			regCalls = append(regCalls, c)
		}
	}
	if len(regCalls) != 2 {
		t.Fatalf("want 2 register calls (probe + fallback), got %d", len(regCalls))
	}
	if regCalls[0].Query != "overwrite=true" {
		t.Errorf("probe register should carry overwrite=true, got %q", regCalls[0].Query)
	}
	if regCalls[1].Query != "" {
		t.Errorf("fallback register should not carry query, got %q", regCalls[1].Query)
	}
	if !strings.Contains(regCalls[1].Body, `"name":"orders"`) || !strings.Contains(regCalls[1].Body, `"metadata-location":"s3://new/v2.metadata.json"`) {
		t.Errorf("register body: %s", regCalls[1].Body)
	}
	dropCall := lastCallMatching(f.calls, "/tables/orders")
	if dropCall.Query != "purgeRequested=false" {
		t.Errorf("drop query %q must include purgeRequested=false", dropCall.Query)
	}
}

// TestClient_SwapMetadataLocation_DetectsDrift: if a writer commits a
// new snapshot between the caller's LoadTable and SwapMetadataLocation,
// the catalog's pointer will have moved. The swap must detect this and
// refuse before drop.
func TestClient_SwapMetadataLocation_DetectsDrift(t *testing.T) {
	f := newFakeServer(t)
	// Pre-swap LoadTable returns a different metadata-location than the
	// caller passed as oldLocation — i.e. someone else committed.
	f.loadHandler = func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"metadata-location":"s3://old/v3.metadata.json","metadata":{}}`))
	}
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}
	err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json")
	if !errors.Is(err, ErrCatalogDrift) {
		t.Fatalf("expected ErrCatalogDrift, got %v", err)
	}
	// Drop / register must not have run.
	for _, c := range f.calls {
		if c.Method == http.MethodDelete {
			t.Errorf("DROP fired despite drift: %+v", c)
		}
		if c.Method == http.MethodPost && strings.Contains(c.Path, "/register") {
			t.Errorf("REGISTER fired despite drift: %+v", c)
		}
	}
}

func TestClient_SwapMetadataLocation_RegisterFails_RollsBack(t *testing.T) {
	f := newFakeServer(t)
	var registerCalls atomic.Int32
	f.registerHandler = func(w http.ResponseWriter, r *http.Request) {
		n := registerCalls.Add(1)
		// First call is the atomic probe; respond 409 to force the
		// fallback path so the test exercises drop+register's rollback.
		if n == 1 && r.URL.Query().Get("overwrite") == "true" {
			http.Error(w, `AlreadyExistsException`, http.StatusConflict)
			return
		}
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), "s3://new/v2.metadata.json"):
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			// Rollback succeeds.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"metadata-location":"s3://old/v1.metadata.json","metadata":{}}`))
		}
	}

	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}
	err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json")
	if err == nil {
		t.Fatal("expected register failure to surface as error")
	}
	if !strings.Contains(err.Error(), "rolled back") {
		t.Errorf("error should mention rollback: %v", err)
	}
	// Probe (1, 409) + initial fallback register (2, 500) + rollback (3, 200).
	if got := registerCalls.Load(); got != 3 {
		t.Errorf("register attempts: got %d, want 3 (probe + initial + rollback)", got)
	}
}

// TestClient_SwapMetadataLocation_AtomicHappyPath verifies that against
// a server that supports register?overwrite=true the swap is a single
// POST — no DELETE, and the Client caches swapMode=atomic for future
// calls. This is the main Polaris / recent-Lakekeeper path.
func TestClient_SwapMetadataLocation_AtomicHappyPath(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}
	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	// Sequence: config, pre-swap reload, atomic register. NO drop.
	want := []string{
		"GET /v1/config",
		"GET /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register",
	}
	got := []string{}
	for _, c := range f.calls {
		got = append(got, c.Method+" "+c.Path)
	}
	if !sliceEq(got, want) {
		t.Errorf("call sequence:\n got=%v\nwant=%v", got, want)
	}
	for _, c := range f.calls {
		if c.Method == http.MethodDelete {
			t.Errorf("atomic path must not DELETE: %+v", c)
		}
	}
	regCall := lastCallMatching(f.calls, "/register")
	if regCall.Query != "overwrite=true" {
		t.Errorf("register query = %q, want overwrite=true", regCall.Query)
	}
}

// TestClient_SwapMetadataLocation_FallbackOnAlreadyExists verifies that
// a 409 / AlreadyExistsException on the atomic probe makes the Client
// fall back to drop+register for this swap and cache the decision so
// subsequent swaps skip the probe entirely.
func TestClient_SwapMetadataLocation_FallbackOnAlreadyExists(t *testing.T) {
	f := newFakeServer(t)
	f.disableAtomic = true
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}

	// First swap: probe → 409 → drop + register fallback.
	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("first swap: %v", err)
	}
	first := append([]string(nil), formatCalls(f.calls)...)
	want := []string{
		"GET /v1/config",
		"GET /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register",        // probe
		"DELETE /v1/ws-001/namespaces/db/tables/orders", // drop
		"POST /v1/ws-001/namespaces/db/register",        // fallback register
	}
	if !sliceEq(first, want) {
		t.Errorf("first swap calls:\n got=%v\nwant=%v", first, want)
	}

	// Second swap: cached decision → drop + register, no probe.
	f.calls = nil
	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("second swap: %v", err)
	}
	second := formatCalls(f.calls)
	wantSecond := []string{
		"GET /v1/ws-001/namespaces/db/tables/orders",
		"DELETE /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register",
	}
	if !sliceEq(second, wantSecond) {
		t.Errorf("second swap calls:\n got=%v\nwant=%v", second, wantSecond)
	}
	for _, c := range f.calls {
		if c.Method == http.MethodPost && strings.HasSuffix(c.Path, "/register") && c.Query == "overwrite=true" {
			t.Errorf("second swap must skip atomic probe: %+v", c)
		}
	}
}

// TestClient_SwapMetadataLocation_AtomicCachedAfterSuccess verifies that
// after a successful atomic swap the second swap on the same Client
// goes straight to register?overwrite=true without re-probing.
func TestClient_SwapMetadataLocation_AtomicCachedAfterSuccess(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}

	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("first swap: %v", err)
	}

	f.calls = nil
	if err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json"); err != nil {
		t.Fatalf("second swap: %v", err)
	}
	got := formatCalls(f.calls)
	want := []string{
		"GET /v1/ws-001/namespaces/db/tables/orders",
		"POST /v1/ws-001/namespaces/db/register",
	}
	if !sliceEq(got, want) {
		t.Errorf("second swap calls:\n got=%v\nwant=%v", got, want)
	}
	regCall := lastCallMatching(f.calls, "/register")
	if regCall.Query != "overwrite=true" {
		t.Errorf("cached atomic register should still carry overwrite=true, got %q", regCall.Query)
	}
}

// TestClient_SwapMetadataLocation_AtomicRealErrorPropagates verifies
// that a non-409 error from the atomic probe surfaces directly without
// falling back to drop+register (which would be wrong — the catalog
// pointer never changed) and without poisoning the swapMode cache.
func TestClient_SwapMetadataLocation_AtomicRealErrorPropagates(t *testing.T) {
	f := newFakeServer(t)
	f.registerHandler = func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}
	c := New(Config{URI: f.srv.URL, Token: "t"})
	id := catalog.Identifier{Namespace: []string{"db"}, Name: "orders"}
	err := c.SwapMetadataLocation(context.Background(), id, "s3://old/v1.metadata.json", "s3://new/v2.metadata.json")
	if err == nil {
		t.Fatal("expected error to surface")
	}
	if !strings.Contains(err.Error(), "atomic register") {
		t.Errorf("error should identify the atomic path: %v", err)
	}
	for _, c := range f.calls {
		if c.Method == http.MethodDelete {
			t.Errorf("must not DELETE on atomic-path real failure: %+v", c)
		}
	}
	if got := c.swapMode.Load(); got != swapModeUnknown {
		t.Errorf("swapMode should remain unknown after transient error, got %d", got)
	}
}

func TestClient_SwapMetadataLocation_NoOpWhenSame(t *testing.T) {
	f := newFakeServer(t)
	c := New(Config{URI: f.srv.URL})
	err := c.SwapMetadataLocation(context.Background(), catalog.Identifier{Namespace: []string{"db"}, Name: "x"},
		"s3://old/v1.metadata.json", "s3://old/v1.metadata.json")
	if err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
	if len(f.calls) != 0 {
		t.Errorf("no-op swap should not call the server, got %d calls", len(f.calls))
	}
}

func TestClient_LoadTable_PropagatesNotFound(t *testing.T) {
	f := newFakeServer(t)
	f.loadHandler = func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"no such table","code":404}}`, http.StatusNotFound)
	}
	c := New(Config{URI: f.srv.URL})
	_, err := c.LoadTable(context.Background(), catalog.Identifier{Namespace: []string{"db"}, Name: "nope"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should carry 404 status: %v", err)
	}
}

func TestSwap_RejectsEmptyLocations(t *testing.T) {
	c := New(Config{URI: "http://example"})
	cases := []struct{ old, new string }{
		{"", "s3://new"}, {"s3://old", ""},
	}
	for _, tc := range cases {
		err := c.SwapMetadataLocation(context.Background(), catalog.Identifier{Name: "t"}, tc.old, tc.new)
		if err == nil || !strings.Contains(err.Error(), "must not be empty") {
			t.Errorf("old=%q new=%q -> %v", tc.old, tc.new, err)
		}
	}
}

func formatCalls(calls []recordedCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.Method+" "+c.Path)
	}
	return out
}

func lastCallMatching(calls []recordedCall, suffix string) recordedCall {
	for i := len(calls) - 1; i >= 0; i-- {
		if strings.HasSuffix(calls[i].Path, suffix) {
			return calls[i]
		}
	}
	return recordedCall{}
}

func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
