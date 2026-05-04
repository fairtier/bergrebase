package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	bergstorage "github.com/fairtier/bergrebase/internal/storage"
)

// defaultPolarisImage is the Apache Polaris container image. Override via
// BERGREBASE_TEST_POLARIS_IMAGE if upstream renames the tag or you want a
// pinned local build. Polaris runs Quarkus and exposes the Iceberg REST
// catalog at /api/catalog/v1 and management at /api/management/v1.
const defaultPolarisImage = "apache/polaris:1.4.0"

const (
	polarisRealm         = "POLARIS"
	polarisRootClient    = "root"
	polarisRootSecret    = "s3cr3t"
	polarisCatalogName   = "rebase"
	polarisPrincipalRole = "data_eng"
	polarisCatalogRole   = "catalog_admin"
)

// PolarisStack is the end-to-end harness for verifying bergrebase against
// Apache Polaris (alongside the Lakekeeper-based Stack). The point is to
// prove the rewrite engine and REST catalog client are catalog-agnostic:
// the same client code that works against Lakekeeper must also work
// against Polaris with no changes — only the test wiring differs.
type PolarisStack struct {
	MinIO   *MinIO
	Polaris *PolarisHandle

	network *testcontainers.DockerNetwork
}

// PolarisHandle holds the parts of a running Polaris instance the e2e
// tests need to drive it. Unlike Lakekeeper's allow-all dev mode, Polaris
// requires OAuth2 — every catalog and management call carries a bearer
// token in the Authorization header.
type PolarisHandle struct {
	// BaseURI is the host-side base URL for the Iceberg REST catalog
	// endpoints (e.g. http://127.0.0.1:32787/api/catalog). Pass this to
	// the catalog/rest client as Config.URI.
	BaseURI string

	// MgmtURI is the host-side base URL for Polaris management endpoints
	// (catalogs, roles, grants). Used by the harness for setup, not by
	// the bergrebase REST client.
	MgmtURI string

	// Warehouse is the value to pass to the catalog/rest client as
	// Config.Warehouse. Polaris's /v1/config endpoint returns
	// overrides.prefix when ?warehouse=<name> resolves; bergrebase's
	// rest.Client already handles that path (rest.go:94).
	Warehouse string

	// Token is the OAuth2 bearer token issued for the bootstrap "root"
	// principal. Pass to the catalog/rest client as Config.Token.
	Token string
}

// StartPolarisStack brings up MinIO and Apache Polaris on a shared Docker
// network, bootstraps the OAuth2 root principal, creates a catalog
// pointing at the MinIO target bucket, and wires the role-grant chain so
// the root principal can register tables. Cleanup is registered on the
// test.
//
// In contrast to StartStack (Lakekeeper), no PostgreSQL is needed —
// Polaris uses POLARIS_PERSISTENCE_TYPE=in-memory for tests. The
// trade-off is that state is lost on container restart; that's fine for
// per-test fixtures.
func StartPolarisStack(ctx context.Context, t *testing.T) *PolarisStack {
	t.Helper()

	net, err := network.New(ctx)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("docker unavailable, skipping testcontainers test: %v", err)
		}
		t.Fatalf("network.New: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	mi := startMinIOOnNetwork(ctx, t, net.Name)
	pol := startPolarisOnNetwork(ctx, t, net.Name, mi)
	pol.Token = bootstrapPolaris(ctx, t, pol.BaseURI)
	createPolarisCatalog(ctx, t, pol, mi)
	wirePolarisRoot(ctx, t, pol)

	return &PolarisStack{
		MinIO:   mi,
		Polaris: pol,
		network: net,
	}
}

func startPolarisOnNetwork(ctx context.Context, t *testing.T, networkName string, mi *MinIO) *PolarisHandle {
	t.Helper()

	img := os.Getenv("BERGREBASE_TEST_POLARIS_IMAGE")
	if img == "" {
		img = defaultPolarisImage
	}

	req := testcontainers.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{"8181/tcp"},
		Env: map[string]string{
			"POLARIS_PERSISTENCE_TYPE":      "in-memory",
			"POLARIS_BOOTSTRAP_CREDENTIALS": fmt.Sprintf("%s,%s,%s", polarisRealm, polarisRootClient, polarisRootSecret),
			"POLARIS_REALM_CONTEXT_REALMS":  polarisRealm,
			"QUARKUS_LOG_LEVEL":             "INFO",

			// Polaris's storage layer reads MinIO credentials through the
			// standard AWS SDK chain when the catalog has no roleArn (no
			// AssumeRole call is made). AWS_REGION satisfies the SDK's
			// region-resolution chain (MinIO ignores the value).
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     mi.AccessKey,
			"AWS_SECRET_ACCESS_KEY": mi.SecretKey,
		},
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"polaris"},
		},
		// Quarkus exposes /q/health on the same port as the API by default.
		// Accept any non-5xx response — DOWN status (503) still proves the
		// HTTP server is up; the OAuth bootstrap retry loop is the real
		// readiness gate.
		WaitingFor: wait.ForHTTP("/q/health").
			WithPort("8181/tcp").
			WithStartupTimeout(180 * time.Second).
			WithStatusCodeMatcher(func(s int) bool { return s < 500 }),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		if c != nil {
			rc, lerr := c.Logs(ctx)
			if lerr == nil {
				b, _ := io.ReadAll(rc)
				_ = rc.Close()
				t.Logf("polaris logs:\n%s", b)
			}
			_ = c.Terminate(context.Background())
		}
		t.Fatalf("polaris start: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("polaris Host: %v", err)
	}
	port, err := c.MappedPort(ctx, "8181/tcp")
	if err != nil {
		t.Fatalf("polaris MappedPort: %v", err)
	}

	base := fmt.Sprintf("http://%s:%s", host, port.Port())
	return &PolarisHandle{
		BaseURI:   base + "/api/catalog",
		MgmtURI:   base + "/api/management/v1",
		Warehouse: polarisCatalogName,
	}
}

// bootstrapPolaris exchanges the root client_credentials for a bearer
// token via the OAuth2 endpoint. It retries until the server is ready
// (Quarkus may accept connections before the auth chain is fully wired).
func bootstrapPolaris(ctx context.Context, t *testing.T, baseURI string) string {
	t.Helper()

	tokenURL := baseURI + "/v1/oauth/tokens"

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", polarisRootClient)
	form.Set("client_secret", polarisRootSecret)
	form.Set("scope", "PRINCIPAL_ROLE:ALL")
	bodyEnc := form.Encode()

	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL,
			strings.NewReader(bodyEnc))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			rb, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var tok struct {
					AccessToken string `json:"access_token"`
				}
				if jerr := json.Unmarshal(rb, &tok); jerr == nil && tok.AccessToken != "" {
					return tok.AccessToken
				}
				t.Logf("polaris token response missing access_token: %s", rb)
			} else {
				t.Logf("polaris token %d: %s", resp.StatusCode, rb)
			}
		} else {
			t.Logf("polaris token retry: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("polaris bootstrap (token) timed out")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// createPolarisCatalog provisions an INTERNAL catalog named "rebase"
// backed by the MinIO target bucket. Storage config follows Polaris's
// canonical MinIO recipe (site/content/guides/minio/docker-compose.yml
// and PolarisRestCatalogMinIOIT.java upstream): camelCase keys,
// pathStyleAccess as a boolean, endpointInternal for in-network
// addressing.
//
// The roleArn is required by schema validation but isn't used at
// runtime when MinIO ambient credentials cover all access.
func createPolarisCatalog(ctx context.Context, t *testing.T, pol *PolarisHandle, mi *MinIO) {
	t.Helper()

	body := map[string]any{
		"catalog": map[string]any{
			"name": polarisCatalogName,
			"type": "INTERNAL",
			"properties": map[string]string{
				// default-base-location chains with the namespace name to
				// form the allowed table location: a table in namespace
				// "orders_old" must live under
				// s3://target/iceberg/db/orders_old/. The harness seeds at
				// that exact path so namespace == seed-table-prefix lines
				// up. (Polaris is stricter than Lakekeeper here: Lakekeeper
				// without a key-prefix accepts any path under the bucket.)
				"default-base-location": "s3://" + mi.TargetBucket + "/iceberg/db",
			},
			// Storage config matches Polaris's canonical MinIO recipe at
			// site/content/guides/minio/docker-compose.yml: no roleArn
			// (skips STS subscoping; ambient AWS_* env vars on the Polaris
			// container provide credentials directly), endpointInternal
			// for in-network addressing, pathStyleAccess for MinIO's URL
			// shape.
			"storageConfigInfo": map[string]any{
				"storageType":      "S3",
				"endpoint":         "http://minio:9000",
				"endpointInternal": "http://minio:9000",
				"pathStyleAccess":  true,
				"region":           "us-east-1",
				"allowedLocations": []string{"s3://" + mi.TargetBucket + "/*"},
			},
		},
	}
	polarisPostJSON(ctx, t, pol, "/catalogs", body, "create catalog")
}

// wirePolarisRoot runs the role-grant chain that lets the bootstrap
// "root" principal manage content in the catalog. Polaris's auth model
// requires: principal → principal-role → catalog-role → privilege.
//
// The five calls must run in this order — out-of-order yields 404s on
// dangling references:
//  1. POST principal-role 'data_eng'
//  2. POST catalog-role 'catalog_admin' (under catalog 'rebase')
//  3. PUT  CATALOG_MANAGE_CONTENT grant on catalog_admin
//  4. PUT  catalog_admin onto data_eng (within catalog 'rebase')
//  5. PUT  data_eng onto principal 'root'
func wirePolarisRoot(ctx context.Context, t *testing.T, pol *PolarisHandle) {
	t.Helper()

	polarisPostJSON(ctx, t, pol, "/principal-roles", map[string]any{
		"principalRole": map[string]any{"name": polarisPrincipalRole},
	}, "create principal-role")

	polarisPostJSON(ctx, t, pol, "/catalogs/"+polarisCatalogName+"/catalog-roles", map[string]any{
		"catalogRole": map[string]any{"name": polarisCatalogRole},
	}, "create catalog-role")

	polarisPutJSON(ctx, t, pol,
		"/catalogs/"+polarisCatalogName+"/catalog-roles/"+polarisCatalogRole+"/grants",
		map[string]any{
			"type":      "catalog",
			"privilege": "CATALOG_MANAGE_CONTENT",
		}, "grant catalog privilege")

	polarisPutJSON(ctx, t, pol,
		"/principal-roles/"+polarisPrincipalRole+"/catalog-roles/"+polarisCatalogName,
		map[string]any{
			"catalogRole": map[string]any{"name": polarisCatalogRole},
		}, "assign catalog-role to principal-role")

	polarisPutJSON(ctx, t, pol,
		"/principals/"+polarisRootClient+"/principal-roles",
		map[string]any{
			"principalRole": map[string]any{"name": polarisPrincipalRole},
		}, "assign principal-role to root")
}

// polarisPostJSON issues an authenticated POST against the management
// API at <MgmtURI><path>. Surfaces non-2xx responses as t.Fatalf with
// the response body for debugging.
func polarisPostJSON(ctx context.Context, t *testing.T, pol *PolarisHandle, path string, body any, label string) {
	t.Helper()
	polarisDoJSON(ctx, t, pol, http.MethodPost, path, body, label)
}

// polarisPutJSON issues an authenticated PUT against the management API.
// Polaris uses PUT for role grants and assignments.
func polarisPutJSON(ctx context.Context, t *testing.T, pol *PolarisHandle, path string, body any, label string) {
	t.Helper()
	polarisDoJSON(ctx, t, pol, http.MethodPut, path, body, label)
}

func polarisDoJSON(ctx context.Context, t *testing.T, pol *PolarisHandle, method, path string, body any, label string) {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("%s: marshal: %v", label, err)
	}
	req, _ := http.NewRequestWithContext(ctx, method, pol.MgmtURI+path, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+pol.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	// 200/201/204 = OK; 409 = already exists, fine for idempotent setup.
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusConflict {
		t.Fatalf("%s: %d %s", label, resp.StatusCode, rb)
	}
}

// StorageClient returns a bergstorage.Client wired to this stack's
// MinIO. Same usage as the standalone harness.
func (s *PolarisStack) StorageClient() *bergstorage.Client {
	return s.MinIO.StorageClient()
}

// CreateNamespace creates a namespace in the "rebase" catalog via the
// Iceberg REST API. ns elements become a multi-part namespace
// (e.g. ["app", "orders"]).
func (s *PolarisStack) CreateNamespace(ctx context.Context, t *testing.T, ns []string) {
	t.Helper()

	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces", s.Polaris.BaseURI, prefix)
	body, _ := json.Marshal(map[string]any{
		"namespace":  ns,
		"properties": map[string]string{},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Polaris.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create namespace %s: %d %s", strings.Join(ns, "."), resp.StatusCode, rb)
	}
}

// RegisterTable registers an existing metadata.json under ns/name in
// the "rebase" catalog. The metadata.json must already exist on MinIO at
// metadataLocation.
func (s *PolarisStack) RegisterTable(ctx context.Context, t *testing.T, ns []string, name, metadataLocation string) {
	t.Helper()

	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces/%s/register",
		s.Polaris.BaseURI, prefix, encodeNamespacePath(ns))
	body, _ := json.Marshal(map[string]any{
		"name":              name,
		"metadata-location": metadataLocation,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Polaris.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register table: %v", err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("register %s.%s @ %s: %d %s",
			strings.Join(ns, "."), name, metadataLocation, resp.StatusCode, rb)
	}
}

// warehousePrefix is the per-warehouse path segment Iceberg REST
// requires between /v1/ and /namespaces/. Polaris returns it in
// `overrides.prefix` from /v1/config?warehouse=<name>; the bergrebase
// REST client reads the same field at rest.go:94.
func (s *PolarisStack) warehousePrefix(ctx context.Context) (string, error) {
	q := url.Values{}
	q.Set("warehouse", s.Polaris.Warehouse)
	u := s.Polaris.BaseURI + "/v1/config?" + q.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Authorization", "Bearer "+s.Polaris.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("config %d: %s", resp.StatusCode, rb)
	}
	var cfg struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.Unmarshal(rb, &cfg); err != nil {
		return "", err
	}
	if p := cfg.Overrides["prefix"]; p != "" {
		return p, nil
	}
	return cfg.Defaults["prefix"], nil
}
