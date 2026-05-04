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
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	bergstorage "github.com/fairtier/bergrebase/internal/storage"
)

// defaultLakekeeperImage is the Lakekeeper container image. Override via
// BERGREBASE_TEST_LAKEKEEPER_IMAGE if upstream renames the tag.
const defaultLakekeeperImage = "quay.io/lakekeeper/catalog:v0.12.0"

// defaultPostgresImage is the PostgreSQL backend Lakekeeper requires.
const defaultPostgresImage = "postgres:16-alpine"

// Stack is the full end-to-end harness: a shared Docker network plus
// MinIO (two buckets), PostgreSQL, and Lakekeeper, with the Lakekeeper
// catalog bootstrapped and a warehouse pointing at the MinIO target
// bucket.
type Stack struct {
	MinIO      *MinIO
	Lakekeeper *LakekeeperHandle

	network *testcontainers.DockerNetwork
}

// LakekeeperHandle holds the parts of a running Lakekeeper instance the
// e2e tests need to drive it.
type LakekeeperHandle struct {
	// BaseURI is the host-side base URL for the Iceberg REST catalog
	// endpoints (e.g. http://127.0.0.1:32787/catalog). Pass this to the
	// catalog/rest client as Config.URI.
	BaseURI string

	// Warehouse is the value to pass to the catalog/rest client as
	// Config.Warehouse. Lakekeeper's /v1/config endpoint resolves
	// warehouse= by name when no auth context is present, so this is
	// the human-readable name ("rebase"), not the UUID.
	Warehouse string
}

// StartStack brings up MinIO, PostgreSQL, and Lakekeeper on a shared
// Docker network and bootstraps the catalog. It skips the test if
// Docker is unavailable. Cleanup is registered on the test.
//
// Lakekeeper is configured with authentication disabled — single-user
// dev mode — so the catalog client can connect without an OIDC issuer
// in the test environment.
func StartStack(ctx context.Context, t *testing.T) *Stack {
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
	startPostgresOnNetwork(ctx, t, net.Name)
	runLakekeeperMigrate(ctx, t, net.Name)
	lake := startLakekeeperOnNetwork(ctx, t, net.Name)
	bootstrapLakekeeper(ctx, t, lake.BaseURI)
	createWarehouse(ctx, t, lake, mi)

	return &Stack{
		MinIO:      mi,
		Lakekeeper: lake,
		network:    net,
	}
}

// startMinIOOnNetwork is a network-aware twin of StartMinIO — same MinIO
// image and same two buckets, but with a network alias "minio" so peers
// in the stack can reach it at http://minio:9000.
func startMinIOOnNetwork(ctx context.Context, t *testing.T, networkName string) *MinIO {
	t.Helper()

	img := os.Getenv("BERGREBASE_TEST_MINIO_IMAGE")
	if img == "" {
		img = defaultMinioImage
	}
	c, err := minio.Run(ctx, img,
		network.WithNetworkName([]string{"minio"}, networkName),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("docker unavailable: %v", err)
		}
		t.Fatalf("minio.Run on network: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	cs, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio ConnectionString: %v", err)
	}
	m := &MinIO{
		Endpoint:     "http://" + cs,
		AccessKey:    c.Username,
		SecretKey:    c.Password,
		SourceBucket: "source",
		TargetBucket: "target",
	}
	if err := m.createBuckets(ctx); err != nil {
		t.Fatalf("create buckets: %v", err)
	}
	return m
}

func startPostgresOnNetwork(ctx context.Context, t *testing.T, networkName string) *postgres.PostgresContainer {
	t.Helper()

	c, err := postgres.Run(ctx, defaultPostgresImage,
		postgres.WithDatabase("lakekeeper"),
		postgres.WithUsername("lakekeeper"),
		postgres.WithPassword("lakekeeper"),
		network.WithNetworkName([]string{"postgres"}, networkName),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres.Run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return c
}

// runLakekeeperMigrate runs `lakekeeper migrate` as a one-shot
// container against the postgres backend. It exits 0 on success and
// must complete before `lakekeeper serve` starts, otherwise the server
// crashes on first request with "relation does not exist".
func runLakekeeperMigrate(ctx context.Context, t *testing.T, networkName string) {
	t.Helper()

	img := os.Getenv("BERGREBASE_TEST_LAKEKEEPER_IMAGE")
	if img == "" {
		img = defaultLakekeeperImage
	}
	pgURL := "postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper"

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: img,
			Env: map[string]string{
				"LAKEKEEPER__PG_ENCRYPTION_KEY":     "0123456789abcdef0123456789abcdef",
				"LAKEKEEPER__PG_DATABASE_URL_READ":  pgURL,
				"LAKEKEEPER__PG_DATABASE_URL_WRITE": pgURL,
				"RUST_LOG":                          "info",
			},
			Networks: []string{networkName},
			Cmd:      []string{"migrate"},
			WaitingFor: wait.ForExit().
				WithExitTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("lakekeeper migrate: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
}

func startLakekeeperOnNetwork(ctx context.Context, t *testing.T, networkName string) *LakekeeperHandle {
	t.Helper()

	img := os.Getenv("BERGREBASE_TEST_LAKEKEEPER_IMAGE")
	if img == "" {
		img = defaultLakekeeperImage
	}

	pgURL := "postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper"
	req := testcontainers.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{"8181/tcp"},
		Env: map[string]string{
			"LAKEKEEPER__PG_ENCRYPTION_KEY":     "0123456789abcdef0123456789abcdef",
			"LAKEKEEPER__PG_DATABASE_URL_READ":  pgURL,
			"LAKEKEEPER__PG_DATABASE_URL_WRITE": pgURL,
			"LAKEKEEPER__AUTHZ_BACKEND":         "allowall",
			"LAKEKEEPER__BASE_URI":              "http://lakekeeper:8181",
			"RUST_LOG":                          "info",
		},
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"lakekeeper"},
		},
		Cmd: []string{"serve"},
		WaitingFor: wait.ForHTTP("/health").
			WithPort("8181/tcp").
			WithStartupTimeout(120 * time.Second),
	}
	// Lakekeeper's first start runs PG migrations as part of `serve`.
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// Surface container logs to make a startup failure debuggable.
		if c != nil {
			rc, lerr := c.Logs(ctx)
			if lerr == nil {
				b, _ := io.ReadAll(rc)
				_ = rc.Close()
				t.Logf("lakekeeper logs:\n%s", b)
			}
			_ = c.Terminate(context.Background())
		}
		t.Fatalf("lakekeeper start: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("lakekeeper Host: %v", err)
	}
	port, err := c.MappedPort(ctx, "8181/tcp")
	if err != nil {
		t.Fatalf("lakekeeper MappedPort: %v", err)
	}
	return &LakekeeperHandle{
		BaseURI: fmt.Sprintf("http://%s:%s/catalog", host, port.Port()),
	}
}

// bootstrapLakekeeper performs the one-shot init call. After this
// returns, the catalog API is usable.
func bootstrapLakekeeper(ctx context.Context, t *testing.T, baseURI string) {
	t.Helper()

	// baseURI points at .../catalog; bootstrap is on the management API
	// at the same host.
	mgmtURI := strings.TrimSuffix(baseURI, "/catalog") + "/management/v1/bootstrap"
	body := []byte(`{"accept-terms-of-use": true}`)

	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, mgmtURI, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			rb, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			// 200/201/204 = OK; 409/400 with "already bootstrapped" is also OK.
			if resp.StatusCode < 300 || strings.Contains(string(rb), "already bootstrapped") {
				return
			}
			t.Logf("bootstrap %d: %s", resp.StatusCode, rb)
		} else {
			t.Logf("bootstrap retry: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("lakekeeper bootstrap timed out")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// createWarehouse provisions a warehouse on the bootstrapped Lakekeeper
// pointing at the MinIO target bucket. The warehouse name is fixed to
// "rebase" — tests use it as the catalog Warehouse field.
func createWarehouse(ctx context.Context, t *testing.T, lake *LakekeeperHandle, mi *MinIO) {
	t.Helper()

	mgmtURI := strings.TrimSuffix(lake.BaseURI, "/catalog") + "/management/v1/warehouse"

	// Inside the network MinIO is reachable at http://minio:9000. The
	// warehouse storage profile uses the in-network endpoint so
	// Lakekeeper (running on the same network) can talk to it.
	payload := map[string]any{
		"warehouse-name": "rebase",
		// Default project-id Lakekeeper uses when bootstrapped without
		// authentication. Pinning it makes the warehouse discoverable by
		// /v1/config?warehouse=<UUID> — without it, Lakekeeper assigns
		// an unauthenticated-user project that the catalog API won't
		// resolve.
		"project-id": "00000000-0000-0000-0000-000000000000",
		// No key-prefix on the storage profile so the warehouse covers
		// the whole bucket. Tests can then register tables at any path
		// under s3://target/... without worrying about Lakekeeper
		// rejecting paths outside the warehouse prefix.
		"storage-profile": map[string]any{
			"type":              "s3",
			"bucket":            mi.TargetBucket,
			"endpoint":          "http://minio:9000",
			"region":            "us-east-1",
			"path-style-access": true,
			"flavor":            "minio",
			"sts-enabled":       false,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":       "access-key",
			"aws-access-key-id":     mi.AccessKey,
			"aws-secret-access-key": mi.SecretKey,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal warehouse payload: %v", err)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, mgmtURI, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create warehouse: %v", err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("create warehouse %d: %s", resp.StatusCode, rb)
	}
	// Lakekeeper accepts the warehouse name in /v1/config?warehouse=
	// (the UUID-only form 404s in auth-disabled mode), so the catalog
	// client uses the name we configured at create time.
	_ = rb
	lake.Warehouse = "rebase"
}

// StorageClient returns a bergstorage.Client wired to this stack's
// MinIO. Same usage as the standalone harness.
func (s *Stack) StorageClient() *bergstorage.Client {
	return s.MinIO.StorageClient()
}

// CreateNamespace creates a namespace in the "rebase" warehouse via the
// Iceberg REST catalog API. ns elements become a multi-part namespace
// (e.g. ["app", "orders"]).
func (s *Stack) CreateNamespace(ctx context.Context, t *testing.T, ns []string) {
	t.Helper()

	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces", s.Lakekeeper.BaseURI, prefix)
	body, _ := json.Marshal(map[string]any{
		"namespace":  ns,
		"properties": map[string]string{},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	// 200/201 = OK; 409 = already exists, fine for idempotent test setup.
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create namespace %s: %d %s", strings.Join(ns, "."), resp.StatusCode, rb)
	}
}

// RegisterTable registers an existing metadata.json under ns/name in
// the "rebase" warehouse. The metadata.json must already exist on
// MinIO at metadataLocation.
func (s *Stack) RegisterTable(ctx context.Context, t *testing.T, ns []string, name, metadataLocation string) {
	t.Helper()

	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces/%s/register",
		s.Lakekeeper.BaseURI, prefix, encodeNamespacePath(ns))
	body, _ := json.Marshal(map[string]any{
		"name":              name,
		"metadata-location": metadataLocation,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
// requires between /v1/ and /namespaces/. Lakekeeper returns it in
// `defaults.prefix` from /v1/config?warehouse=<name>.
func (s *Stack) warehousePrefix(ctx context.Context) (string, error) {
	q := url.Values{}
	q.Set("warehouse", s.Lakekeeper.Warehouse)
	u := s.Lakekeeper.BaseURI + "/v1/config?" + q.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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

// encodeNamespacePath joins multi-part namespaces with the U+001F unit
// separator, the encoding the Iceberg REST spec mandates.
func encodeNamespacePath(ns []string) string {
	return strings.Join(ns, "\x1f")
}
