// Package rest implements the catalog.Catalog interface against the
// Iceberg REST spec.
//
// It works with any compliant REST catalog: Lakekeeper, Apache Polaris,
// Tabular, Snowflake Open Catalog, AWS Glue's REST shim.
//
// Only the endpoints the rewriter actually exercises are implemented:
//
//   - GET    /v1/{prefix}/namespaces                                (ListNamespaces; ?parent=ns for nested)
//   - GET    /v1/{prefix}/namespaces/{ns}/tables                    (ListTables)
//   - GET    /v1/{prefix}/namespaces/{ns}/tables/{table}            (LoadTable)
//   - DELETE /v1/{prefix}/namespaces/{ns}/tables/{table}            (DropTable)
//   - POST   /v1/{prefix}/namespaces/{ns}/register?overwrite=true   (atomic; auto-detected)
//   - POST   /v1/{prefix}/namespaces/{ns}/register                  (fallback)
//   - GET    /v1/config                                             (warehouse prefix)
//
// Atomic-swap strategy: prefer single-call register-with-overwrite when
// the catalog supports it (Polaris, recent Lakekeeper); auto-detect on
// the first swap and fall back to drop-without-purge + register
// otherwise. iceberg-go's full REST catalog ships its own client, but it
// transitively pulls in arrow-go + parquet + substrait + grpc;
// bergrebase keeps its dependency surface small by speaking the REST
// protocol directly.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fairtier/bergrebase/internal/catalog"
)

// Config is the REST catalog connection configuration.
type Config struct {
	URI       string // base URI, e.g. https://lakekeeper.example.com/catalog
	Warehouse string // warehouse name or UUID
	Token     string // bearer token; OAuth2 client-credentials handled elsewhere

	// HTTPClient overrides the default *http.Client used for transport.
	// If nil a Client with a 30s timeout is constructed.
	HTTPClient *http.Client
}

// Client implements catalog.Catalog over the Iceberg REST protocol.
type Client struct {
	cfg Config

	once    sync.Once
	baseURI *url.URL
	prefix  string // server-supplied prefix, joined into v1/<prefix>/...
	http    *http.Client
	initErr error

	// swapMode caches whether the catalog supports atomic
	// register?overwrite=true (set on first SwapMetadataLocation call).
	// 0 = unknown, 1 = atomic, 2 = drop+register.
	swapMode atomic.Int32
}

const (
	swapModeUnknown      int32 = 0
	swapModeAtomic       int32 = 1
	swapModeDropRegister int32 = 2
)

// New constructs a Client from cfg. The first method call fetches the
// catalog config endpoint to learn the routing prefix; subsequent calls
// reuse the cached state.
func New(cfg Config) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) init(ctx context.Context) error {
	c.once.Do(func() {
		u, err := url.Parse(strings.TrimRight(c.cfg.URI, "/"))
		if err != nil {
			c.initErr = fmt.Errorf("rest: parse uri %q: %w", c.cfg.URI, err)
			return
		}
		c.baseURI = u
		c.http = c.cfg.HTTPClient
		if c.http == nil {
			c.http = &http.Client{Timeout: 30 * time.Second}
		}
		// Fetch the per-warehouse routing prefix. Servers that don't
		// honour the warehouse query parameter return defaults/overrides
		// without a prefix; that's fine.
		conf, err := c.fetchConfig(ctx)
		if err != nil {
			c.initErr = err
			return
		}
		// The Iceberg REST OpenAPI spec doesn't standardise a top-level
		// `prefix` field; servers expose the warehouse routing prefix
		// inside `overrides` (server-forced) or `defaults` (suggested).
		// Lakekeeper uses defaults; Polaris uses overrides; some forks
		// emit a top-level prefix as a convenience. Try all three.
		switch {
		case conf.Overrides["prefix"] != "":
			c.prefix = conf.Overrides["prefix"]
		case conf.Defaults["prefix"] != "":
			c.prefix = conf.Defaults["prefix"]
		default:
			c.prefix = conf.Prefix
		}
	})
	return c.initErr
}

type configResponse struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
	// Endpoints []string       `json:"endpoints,omitempty"`

	// Prefix is a non-standard top-level field some servers emit as a
	// convenience. The spec-conformant location is overrides.prefix or
	// defaults.prefix; init() reads them in that priority order.
	Prefix string `json:"prefix,omitempty"`
}

func (c *Client) fetchConfig(ctx context.Context) (configResponse, error) {
	q := url.Values{}
	if c.cfg.Warehouse != "" {
		q.Set("warehouse", c.cfg.Warehouse)
	}
	u := c.baseURI.JoinPath("v1", "config")
	u.RawQuery = q.Encode()
	var resp configResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &resp); err != nil {
		return configResponse{}, fmt.Errorf("rest: fetch config: %w", err)
	}
	return resp, nil
}

// v1Path joins v1 + prefix + segments into a request URL.
func (c *Client) v1Path(segments ...string) *url.URL {
	parts := []string{"v1"}
	if c.prefix != "" {
		parts = append(parts, c.prefix)
	}
	parts = append(parts, segments...)
	return c.baseURI.JoinPath(parts...)
}

func (c *Client) do(ctx context.Context, method string, u *url.URL, body, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// Drain before close so the underlying connection can be
		// returned to the keep-alive pool. Matters once the rewriter
		// handles many tables in one run.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return &httpError{
			Method: method,
			Path:   u.Path,
			Status: resp.StatusCode,
			text:   fmt.Sprintf("%s %s: %s: %s", method, u.Path, resp.Status, strings.TrimSpace(string(b))),
			Body:   strings.TrimSpace(string(b)),
		}
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// httpError carries the HTTP status alongside the formatted message so
// callers can branch on the response code (e.g. detect 409 / AlreadyExists).
type httpError struct {
	Method string
	Path   string
	Status int
	Body   string
	text   string
}

func (e *httpError) Error() string { return e.text }

// isAlreadyExists reports whether err is a REST catalog "table already
// exists" rejection. The Iceberg REST spec marks this as
// AlreadyExistsException; servers respond with 409 (the canonical mapping)
// or sometimes a 4xx whose body carries the exception type. We use this
// to detect that a register?overwrite=true call was treated as a plain
// register by a server that doesn't honour the query parameter.
func isAlreadyExists(err error) bool {
	var herr *httpError
	if !errors.As(err, &herr) {
		return false
	}
	if herr.Status == http.StatusConflict {
		return true
	}
	if herr.Status >= 400 && herr.Status < 500 && strings.Contains(herr.Body, "AlreadyExistsException") {
		return true
	}
	return false
}

// isBadRequest reports whether err is an HTTP 400 rejection. Used by
// the swap probe: a server that doesn't know the `overwrite` query
// parameter may reject it with 400 rather than ignoring it, in which
// case the drop+register fallback still works.
func isBadRequest(err error) bool {
	var herr *httpError
	return errors.As(err, &herr) && herr.Status == http.StatusBadRequest
}

// ListNamespaces returns the immediate child namespaces of parent. A nil
// or empty parent lists top-level namespaces. The Iceberg REST spec does
// not provide a recursive listing, so callers that want every namespace
// in a warehouse must walk this method themselves.
func (c *Client) ListNamespaces(ctx context.Context, parent catalog.Namespace) ([]catalog.Namespace, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	u := c.v1Path("namespaces")
	if len(parent) > 0 {
		q := url.Values{}
		// The parent query value uses the same U+001F separator the spec
		// mandates for path segments; encodeNamespace already path-escapes,
		// so go through the raw form and let url.Values escape it as a
		// query value instead.
		q.Set("parent", strings.Join(parent, "\x1f"))
		u.RawQuery = q.Encode()
	}
	type listResponse struct {
		Namespaces []catalog.Namespace `json:"namespaces"`
	}
	var resp listResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &resp); err != nil {
		return nil, fmt.Errorf("rest: list namespaces under %q: %w",
			strings.Join(parent, "."), err)
	}
	return resp.Namespaces, nil
}

// ListTables returns table identifiers within a namespace.
func (c *Client) ListTables(ctx context.Context, ns catalog.Namespace) ([]catalog.Identifier, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	u := c.v1Path("namespaces", encodeNamespace(ns), "tables")
	type entry struct {
		Namespace catalog.Namespace `json:"namespace"`
		Name      string            `json:"name"`
	}
	type listResponse struct {
		Identifiers []entry `json:"identifiers"`
	}
	var resp listResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &resp); err != nil {
		return nil, fmt.Errorf("rest: list tables in %s: %w", strings.Join(ns, "."), err)
	}
	out := make([]catalog.Identifier, 0, len(resp.Identifiers))
	for _, e := range resp.Identifiers {
		out = append(out, catalog.Identifier{Namespace: e.Namespace, Name: e.Name})
	}
	return out, nil
}

// LoadTable returns the current pointer state for a table.
func (c *Client) LoadTable(ctx context.Context, id catalog.Identifier) (*catalog.Table, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	u := c.v1Path("namespaces", encodeNamespace(id.Namespace), "tables", url.PathEscape(id.Name))
	type loadResponse struct {
		MetadataLocation string `json:"metadata-location"`
		Metadata         struct {
			CurrentSnapshotID *int64 `json:"current-snapshot-id,omitempty"`
		} `json:"metadata"`
	}
	var resp loadResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &resp); err != nil {
		return nil, fmt.Errorf("rest: load %s: %w", qualifiedName(id), err)
	}
	if resp.MetadataLocation == "" {
		return nil, fmt.Errorf("rest: load %s: server returned empty metadata-location", qualifiedName(id))
	}
	return &catalog.Table{
		Identifier:        id,
		MetadataLocation:  resp.MetadataLocation,
		CurrentSnapshotID: resp.Metadata.CurrentSnapshotID,
	}, nil
}

// ErrCatalogDrift is returned by SwapMetadataLocation when the catalog's
// current metadata-location no longer matches oldLocation. It signals a
// concurrent writer committed between LoadTable and the swap; proceeding
// with drop+register would lose that writer's snapshot.
//
// The caller can recover by re-running the rebase: a fresh LoadTable
// will pick up the new metadata, walk it, and try the swap again.
var ErrCatalogDrift = errors.New("rest: catalog drifted since LoadTable; concurrent writer committed")

// SwapMetadataLocation atomically re-points id from oldLocation to
// newLocation. It prefers a single-call register?overwrite=true when the
// catalog supports it (auto-detected on first invocation; cached
// thereafter); otherwise falls back to drop-without-purge + register, in
// which case a register failure best-effort re-registers oldLocation so
// reads keep resolving.
//
// Before destroying the existing pointer, SwapMetadataLocation confirms
// the catalog still resolves the table at oldLocation. If a writer
// committed a snapshot since the caller's LoadTable, the pointer will
// have moved and the swap is refused with ErrCatalogDrift.
func (c *Client) SwapMetadataLocation(ctx context.Context, id catalog.Identifier, oldLocation, newLocation string) error {
	if oldLocation == "" {
		return errors.New("rest: SwapMetadataLocation: oldLocation must not be empty")
	}
	if newLocation == "" {
		return errors.New("rest: SwapMetadataLocation: newLocation must not be empty")
	}
	if oldLocation == newLocation {
		return nil
	}
	if err := c.init(ctx); err != nil {
		return err
	}
	current, err := c.LoadTable(ctx, id)
	if err != nil {
		return fmt.Errorf("rest: pre-swap reload %s: %w", qualifiedName(id), err)
	}
	if current.MetadataLocation != oldLocation {
		return fmt.Errorf("%w: pointer is %q, expected %q",
			ErrCatalogDrift, current.MetadataLocation, oldLocation)
	}

	switch c.swapMode.Load() {
	case swapModeAtomic:
		if err := c.registerTable(ctx, id, newLocation, true); err != nil {
			return fmt.Errorf("rest: atomic register %s -> %s failed: %w",
				qualifiedName(id), newLocation, err)
		}
		return nil
	case swapModeDropRegister:
		return c.swapByDropRegister(ctx, id, oldLocation, newLocation)
	default: // swapModeUnknown — probe.
		err := c.registerTable(ctx, id, newLocation, true)
		if err == nil {
			c.swapMode.Store(swapModeAtomic)
			return nil
		}
		switch {
		case isAlreadyExists(err):
			// Server treated overwrite=true as a plain register and
			// rejected the duplicate. Cache that and fall back to
			// drop+register for this swap.
			c.swapMode.Store(swapModeDropRegister)
			return c.swapByDropRegister(ctx, id, oldLocation, newLocation)
		case isBadRequest(err):
			// Some servers reject the unknown `overwrite` query
			// parameter with 400 instead of ignoring it. Fall back for
			// this swap, but don't cache the mode: a 400 can also mean a
			// genuinely malformed request, and if the fallback register
			// fails the same way it rolls back and surfaces the error.
			return c.swapByDropRegister(ctx, id, oldLocation, newLocation)
		default:
			// Real failure (auth, transport, malformed metadata). Leave
			// swapMode at unknown so the next swap can retry detection
			// against a transient blip. Catalog is unchanged because the
			// atomic call is the only write we attempted.
			return fmt.Errorf("rest: atomic register %s -> %s failed: %w",
				qualifiedName(id), newLocation, err)
		}
	}
}

// swapByDropRegister implements the drop-without-purge + register path
// with best-effort rollback if register fails.
//
// The whole critical section runs detached from the caller's
// cancellation (with its own timeout): the caller's ctx is typically
// wired to SIGINT/SIGTERM, and a Ctrl-C landing between a successful
// drop and the register would otherwise cancel both the register AND
// the rollback, stranding the table unregistered until manual repair.
// Once the drop has been issued the swap must run to a terminal state.
func (c *Client) swapByDropRegister(ctx context.Context, id catalog.Identifier, oldLocation, newLocation string) error {
	// Generous bound: three HTTP calls, each capped by the transport's
	// own timeout (30s default).
	swapCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
	defer cancel()

	if err := c.dropTable(swapCtx, id); err != nil {
		return fmt.Errorf("rest: drop %s: %w", qualifiedName(id), err)
	}
	if err := c.registerTable(swapCtx, id, newLocation, false); err != nil {
		// Best-effort rollback. Failure here is reported alongside the
		// original error so the operator knows the table is currently
		// unregistered.
		if rbErr := c.registerTable(swapCtx, id, oldLocation, false); rbErr != nil {
			return fmt.Errorf("rest: register %s -> %s failed: %w; rollback to %s also failed: %w",
				qualifiedName(id), newLocation, err, oldLocation, rbErr)
		}
		return fmt.Errorf("rest: register %s -> %s failed (rolled back to %s): %w",
			qualifiedName(id), newLocation, oldLocation, err)
	}
	return nil
}

func (c *Client) dropTable(ctx context.Context, id catalog.Identifier) error {
	u := c.v1Path("namespaces", encodeNamespace(id.Namespace), "tables", url.PathEscape(id.Name))
	q := url.Values{}
	q.Set("purgeRequested", "false")
	u.RawQuery = q.Encode()
	return c.do(ctx, http.MethodDelete, u, nil, nil)
}

func (c *Client) registerTable(ctx context.Context, id catalog.Identifier, metadataLoc string, overwrite bool) error {
	u := c.v1Path("namespaces", encodeNamespace(id.Namespace), "register")
	if overwrite {
		q := url.Values{}
		q.Set("overwrite", "true")
		u.RawQuery = q.Encode()
	}
	body := struct {
		Name             string `json:"name"`
		MetadataLocation string `json:"metadata-location"`
	}{
		Name:             id.Name,
		MetadataLocation: metadataLoc,
	}
	return c.do(ctx, http.MethodPost, u, body, nil)
}

// encodeNamespace URL-encodes a multi-part namespace per the REST spec:
// elements are joined with the unit-separator U+001F.
func encodeNamespace(ns catalog.Namespace) string {
	const sep = "\x1f"
	return url.PathEscape(strings.Join(ns, sep))
}

func qualifiedName(id catalog.Identifier) string {
	return path.Join(append(id.Namespace, id.Name)...)
}

// Compile-time check that *Client satisfies catalog.Catalog.
var _ catalog.Catalog = (*Client)(nil)
