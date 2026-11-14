package harness

import (
	"bytes"
	"context"
	_ "embed" // for the pyiceberg driver script
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/avro/ocf"
)

//go:embed pyiceberg_driver.py
var pyicebergDriver []byte

// DuckDB is one duckdb CLI binary used as a writer or reader of real tables.
type DuckDB struct {
	Bin     string
	Version string // e.g. "v1.5.6"
}

// Label is the version without its leading v, safe in a test or namespace name.
func (d DuckDB) Label() string { return strings.TrimPrefix(d.Version, "v") }

// HasContentHeaderBug reports whether this version labels delete manifests
// content=data in their OCF header (fixed in DuckDB 1.5.5).
func (d DuckDB) HasContentHeaderBug() bool { return !versionAtLeast(d.Version, 1, 5, 5) }

func probeDuckDBBinary(t *testing.T, bin string) DuckDB {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), bin, "--version").Output()
	if err != nil {
		t.Fatalf("%s --version: %v", bin, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		t.Fatalf("%s --version printed nothing", bin)
	}
	return DuckDB{Bin: bin, Version: fields[0]}
}

// ReaderDuckDB is the DuckDB the oracle reads through (BERGREBASE_TEST_DUCKDB,
// else PATH). It skips or fails like MaybeSkipNoDuckDB when there is none.
func ReaderDuckDB(t *testing.T) DuckDB {
	t.Helper()
	MaybeSkipNoDuckDB(t)
	return probeDuckDBBinary(t, duckdbBinary)
}

// DuckDBWriters lists the DuckDB versions to write real tables with:
// BERGREBASE_TEST_DUCKDB_WRITERS (comma-separated paths), else the reader.
func DuckDBWriters(t *testing.T) []DuckDB {
	t.Helper()
	reader := ReaderDuckDB(t)
	env := os.Getenv("BERGREBASE_TEST_DUCKDB_WRITERS")
	if env == "" {
		if Required() {
			t.Fatal("BERGREBASE_TEST_DUCKDB_WRITERS must be set in CI (see test/tools/install-test-tools.sh)")
		}
		return []DuckDB{reader}
	}
	bins := strings.Split(env, ",")
	writers := make([]DuckDB, 0, len(bins))
	for _, bin := range bins {
		writers = append(writers, probeDuckDBBinary(t, strings.TrimSpace(bin)))
	}
	return writers
}

// Exec runs statements in a fresh duckdb process with the stack's catalog
// attached as lk, and fails t on any error.
func (d DuckDB) Exec(t *testing.T, s *Stack, statements ...string) {
	t.Helper()
	d.run(t, s, strings.Join(statements, ";\n")+";")
}

// Query returns the rows of query as CSV records, in the order DuckDB produced
// them. Add an ORDER BY for a stable comparison.
func (d DuckDB) Query(t *testing.T, s *Stack, query string) [][]string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "rows.csv")
	d.run(t, s, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT CSV, HEADER false);", query, out))
	f, err := os.Open(out)
	if err != nil {
		t.Fatalf("open duckdb output: %v", err)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("parse duckdb output: %v", err)
	}
	return rows
}

// QueryInt64 runs a query returning one integer.
func (d DuckDB) QueryInt64(t *testing.T, s *Stack, query string) int64 {
	t.Helper()
	rows := d.Query(t, s, query)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("%q: expected one value, got %v", query, rows)
	}
	n, err := strconv.ParseInt(rows[0][0], 10, 64)
	if err != nil {
		t.Fatalf("%q: %v", query, err)
	}
	return n
}

func (d DuckDB) run(t *testing.T, s *Stack, sql string) {
	t.Helper()
	endpoint := strings.TrimPrefix(s.MinIO.Endpoint, "http://")
	prelude := strings.Join([]string{
		"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs",
		"SET TimeZone='UTC'",
		fmt.Sprintf("CREATE OR REPLACE SECRET e2e_s3 (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', REGION 'us-east-1', URL_STYLE 'path', USE_SSL false)",
			s.MinIO.AccessKey, s.MinIO.SecretKey, endpoint),
		fmt.Sprintf("ATTACH '%s' AS lk (TYPE iceberg, ENDPOINT '%s', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
			s.Lakekeeper.Warehouse, s.Lakekeeper.BaseURI),
	}, ";\n") + ";\n"
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.Bin, "-batch", "-bail", "-c", prelude+sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("duckdb %s: %v\nsql: %s\nstderr: %s", d.Version, err, sql, stderr.String())
	}
}

// PyIceberg runs the driver script under the interpreter named by
// BERGREBASE_TEST_PYTHON.
type PyIceberg struct {
	python string
	script string
}

// NewPyIceberg skips or fails t when no interpreter with pyiceberg is set up.
func NewPyIceberg(t *testing.T) PyIceberg {
	t.Helper()
	python := os.Getenv("BERGREBASE_TEST_PYTHON")
	if python == "" {
		SkipOrFail(t, "BERGREBASE_TEST_PYTHON not set; see test/tools/install-test-tools.sh")
	}
	script := filepath.Join(t.TempDir(), "pyiceberg_driver.py")
	if err := os.WriteFile(script, pyicebergDriver, 0o600); err != nil {
		t.Fatalf("write driver: %v", err)
	}
	return PyIceberg{python: python, script: script}
}

// Call runs op against namespace ns and decodes the JSON reply into out.
func (p PyIceberg) Call(t *testing.T, s *Stack, ns, op string, args map[string]any, out any) {
	t.Helper()
	req := map[string]any{
		"op":          op,
		"namespace":   ns,
		"catalog_uri": s.Lakekeeper.BaseURI,
		"warehouse":   s.Lakekeeper.Warehouse,
		"s3": map[string]string{
			"endpoint":   s.MinIO.Endpoint,
			"access_key": s.MinIO.AccessKey,
			"secret_key": s.MinIO.SecretKey,
		},
	}
	for k, v := range args {
		req[k] = v
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.python, "-I", p.script)
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("pyiceberg %s: %v\nstderr: %s", op, err, stderr.String())
	}
	if out != nil {
		if err := json.Unmarshal(stdout.Bytes(), out); err != nil {
			t.Fatalf("pyiceberg %s reply: %v\n%s", op, err, stdout.String())
		}
	}
}

// BergrbRun is the outcome of one real bergrb process.
type BergrbRun struct {
	ExitCode int
	Log      string
}

// RunBergrb runs the real binary against ns in this stack, moving sourcePrefix
// to targetPrefix. The bucket credentials and endpoint are the stack's. Pass
// "--table", name as extra to move one table instead of the whole namespace.
func (s *Stack) RunBergrb(t *testing.T, ns, sourcePrefix, targetPrefix string, extra ...string) BergrbRun {
	t.Helper()
	bin := BuildBergrb(t)
	args := []string{
		"--catalog-uri", s.Lakekeeper.BaseURI,
		"--catalog-warehouse", s.Lakekeeper.Warehouse,
		"--source-prefix", sourcePrefix,
		"--target-prefix", targetPrefix,
		"--source-endpoint", s.MinIO.Endpoint, "--source-region", "us-east-1", "--source-path-style",
		"--target-endpoint", s.MinIO.Endpoint, "--target-region", "us-east-1", "--target-path-style",
		"--namespace", ns,
	}
	args = append(args, extra...)
	single := false
	for _, a := range extra {
		single = single || a == "--table"
	}
	if !single {
		args = append(args, "--all-tables")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(),
		"SOURCE_AWS_ACCESS_KEY_ID="+s.MinIO.AccessKey, "SOURCE_AWS_SECRET_ACCESS_KEY="+s.MinIO.SecretKey,
		"TARGET_AWS_ACCESS_KEY_ID="+s.MinIO.AccessKey, "TARGET_AWS_SECRET_ACCESS_KEY="+s.MinIO.SecretKey,
	)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return BergrbRun{Log: string(out)}
	case errors.As(err, &exitErr):
		return BergrbRun{ExitCode: exitErr.ExitCode(), Log: string(out)}
	default:
		t.Fatalf("run bergrb: %v", err)
		return BergrbRun{}
	}
}

// CatalogSnapshot is one snapshot as the catalog reports it.
type CatalogSnapshot struct {
	ID             int64  `json:"snapshot-id"`
	SequenceNumber int64  `json:"sequence-number"`
	ManifestList   string `json:"manifest-list"`
}

// CatalogTable is a table as the catalog reports it. IDs are decoded straight
// into int64, so a precision loss in the document shows up here.
type CatalogTable struct {
	MetadataLocation string
	Location         string
	CurrentSnapshot  *int64
	NextRowID        *int64
	Snapshots        []CatalogSnapshot
}

// LoadCatalogTable reads a table over the catalog's REST API.
func (s *Stack) LoadCatalogTable(ctx context.Context, t *testing.T, ns, name string) CatalogTable {
	t.Helper()
	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", s.Lakekeeper.BaseURI, prefix, url.PathEscape(ns), url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("load table request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("load table %s.%s: %v", ns, name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("load table %s.%s: %v", ns, name, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("load table %s.%s: %d %s", ns, name, resp.StatusCode, body)
	}
	var parsed struct {
		MetadataLocation string `json:"metadata-location"`
		Metadata         struct {
			Location        string            `json:"location"`
			CurrentSnapshot *int64            `json:"current-snapshot-id"`
			NextRowID       *int64            `json:"next-row-id"`
			Snapshots       []CatalogSnapshot `json:"snapshots"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("decode table %s.%s: %v", ns, name, err)
	}
	return CatalogTable{
		MetadataLocation: parsed.MetadataLocation,
		Location:         parsed.Metadata.Location,
		CurrentSnapshot:  parsed.Metadata.CurrentSnapshot,
		NextRowID:        parsed.Metadata.NextRowID,
		Snapshots:        parsed.Metadata.Snapshots,
	}
}

// SwapTableBack re-points a table with register?overwrite=true, the swap-back
// procedure in docs/migration-recipes.md.
func (s *Stack) SwapTableBack(ctx context.Context, t *testing.T, ns, name, metadataLocation string) {
	t.Helper()
	prefix, err := s.warehousePrefix(ctx)
	if err != nil {
		t.Fatalf("warehouse prefix: %v", err)
	}
	u := fmt.Sprintf("%s/v1/%s/namespaces/%s/register?overwrite=true", s.Lakekeeper.BaseURI, prefix, encodeNamespacePath([]string{ns}))
	body, err := json.Marshal(map[string]any{"name": name, "metadata-location": metadataLocation, "overwrite": true})
	if err != nil {
		t.Fatalf("marshal register: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(resp.Body)
		t.Fatalf("swap back %s.%s: %d %s", ns, name, resp.StatusCode, rb)
	}
}

// UpdateWarehouseBucketViaAPI tries to re-point the warehouse at bucket through
// the management API and returns the HTTP status and body.
func (s *Stack) UpdateWarehouseBucketViaAPI(ctx context.Context, t *testing.T, bucket string) (int, string) {
	t.Helper()
	base := strings.TrimSuffix(s.Lakekeeper.BaseURI, "/catalog")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/management/v1/warehouse", nil)
	if err != nil {
		t.Fatalf("list warehouses request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list warehouses: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var list struct {
		Warehouses []struct {
			ID string `json:"warehouse-id"`
		} `json:"warehouses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || len(list.Warehouses) != 1 {
		t.Fatalf("list warehouses: %v, %d warehouses", err, len(list.Warehouses))
	}
	body, err := json.Marshal(map[string]any{
		"storage-profile": map[string]any{
			"type": "s3", "bucket": bucket, "endpoint": "http://minio:9000", "region": "us-east-1",
			"path-style-access": true, "flavor": "minio", "sts-enabled": false,
		},
		"storage-credential": map[string]any{
			"type": "s3", "credential-type": "access-key",
			"aws-access-key-id": s.MinIO.AccessKey, "aws-secret-access-key": s.MinIO.SecretKey,
		},
	})
	if err != nil {
		t.Fatalf("marshal storage update: %v", err)
	}
	u := base + "/management/v1/warehouse/" + list.Warehouses[0].ID + "/storage"
	upd, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("storage update request: %v", err)
	}
	upd.Header.Set("Content-Type", "application/json")
	updResp, err := http.DefaultClient.Do(upd)
	if err != nil {
		t.Fatalf("storage update: %v", err)
	}
	defer func() { _ = updResp.Body.Close() }()
	rb, _ := io.ReadAll(updResp.Body)
	return updResp.StatusCode, string(rb)
}

// SetWarehouseBucketInPostgres edits the warehouse row directly, the only way
// to change a Lakekeeper warehouse's bucket (see docs/migration-recipes.md).
func (s *Stack) SetWarehouseBucketInPostgres(ctx context.Context, t *testing.T, bucket string) {
	t.Helper()
	sql := fmt.Sprintf(`UPDATE warehouse SET storage_profile = jsonb_set(storage_profile, '{bucket}', '"%s"') WHERE warehouse_name = '%s'`,
		bucket, s.Lakekeeper.Warehouse)
	code, out, err := s.postgres.Exec(ctx, []string{"psql", "-U", "lakekeeper", "-d", "lakekeeper", "-v", "ON_ERROR_STOP=1", "-c", sql})
	if err != nil {
		t.Fatalf("psql: %v", err)
	}
	b, _ := io.ReadAll(out)
	if code != 0 || !strings.Contains(string(b), "UPDATE 1") {
		t.Fatalf("update warehouse row: exit %d: %s", code, b)
	}
}

// CopyPrefix copies every object under srcPrefix in srcBucket to the same
// relative key under dstPrefix in dstBucket, as rclone does before a rebase. It
// returns the number of objects copied.
func (m *MinIO) CopyPrefix(ctx context.Context, t *testing.T, srcBucket, srcPrefix, dstBucket, dstPrefix string) int {
	t.Helper()
	cli, err := m.s3Client(ctx)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	keys := m.ListKeys(ctx, t, srcBucket, srcPrefix)
	for _, key := range keys {
		_, err := cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(dstBucket),
			Key:        aws.String(dstPrefix + strings.TrimPrefix(key, srcPrefix)),
			CopySource: aws.String(srcBucket + "/" + key),
		})
		if err != nil {
			t.Fatalf("copy %s: %v", key, err)
		}
	}
	return len(keys)
}

// DeletePrefix removes every object under prefix, so that nothing after a
// rebase can quietly keep reading from the old location.
func (m *MinIO) DeletePrefix(ctx context.Context, t *testing.T, bucket, prefix string) {
	t.Helper()
	cli, err := m.s3Client(ctx)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	for _, key := range m.ListKeys(ctx, t, bucket, prefix) {
		if _, err := cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
	}
}

// ListKeys lists the object keys under prefix.
func (m *MinIO) ListKeys(ctx context.Context, t *testing.T, bucket, prefix string) []string {
	t.Helper()
	cli, err := m.s3Client(ctx)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	var keys []string
	pages := s3.NewListObjectsV2Paginator(cli, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Fatalf("list %s/%s: %v", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys
}

// OCFMetadata returns the key/value header of an Avro object container file.
func OCFMetadata(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	rd, err := ocf.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("decode OCF header: %v", err)
	}
	defer func() { _ = rd.Close() }()
	return rd.Metadata()
}
