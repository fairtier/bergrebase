package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// IcebergScanResult is the projection the DuckDB oracle reads back.
type IcebergScanResult struct {
	Count int64
	SumID int64
}

var (
	duckdbProbeOnce sync.Once
	duckdbBinary    string
	duckdbProbeErr  error
)

// MaybeSkipNoDuckDB skips t if the duckdb CLI is not available. It looks at
// the BERGREBASE_TEST_DUCKDB env var first (full path), then falls back to
// PATH. The DuckDB iceberg extension's iceberg_scan(metadata.json) form has
// been stable since 1.1.3; older binaries are skipped with a clear upgrade
// message.
func MaybeSkipNoDuckDB(t *testing.T) {
	t.Helper()
	duckdbProbeOnce.Do(probeDuckDB)
	if duckdbProbeErr != nil {
		t.Skip(duckdbProbeErr.Error())
	}
}

func probeDuckDB() {
	bin := os.Getenv("BERGREBASE_TEST_DUCKDB")
	if bin == "" {
		path, err := exec.LookPath("duckdb")
		if err != nil {
			duckdbProbeErr = errors.New("duckdb CLI not found in PATH; install DuckDB ≥ 1.1.3 or set BERGREBASE_TEST_DUCKDB to its path")
			return
		}
		bin = path
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	if err != nil {
		duckdbProbeErr = fmt.Errorf("duckdb --version failed: %w", err)
		return
	}
	version := strings.TrimSpace(string(out))
	if !versionAtLeast(version, 1, 1, 3) {
		duckdbProbeErr = fmt.Errorf("duckdb %s too old for iceberg_scan(metadata.json); need ≥ 1.1.3", version)
		return
	}

	// Warm the extension cache once, with its own generous deadline. On a
	// cold ~/.duckdb the INSTALLs download from extensions.duckdb.org; on a
	// slow network that download alone can eat the whole 60 s per-scan
	// budget in QueryIcebergScan and kill the first oracle tests of a run
	// with an empty-output "signal: killed".
	ictx, icancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer icancel()
	if out, err := exec.CommandContext(ictx, bin, "-c", "INSTALL iceberg; INSTALL httpfs;").CombinedOutput(); err != nil {
		duckdbProbeErr = fmt.Errorf("duckdb INSTALL iceberg/httpfs failed (network needed on first run): %w\n%s", err, out)
		return
	}
	duckdbBinary = bin
}

// versionAtLeast parses "v1.5.2 (Variegata) abcdef" / "1.1.3" forms loosely
// — only the leading vMAJOR.MINOR.PATCH triple is compared. Pre-release and
// build metadata are ignored. Returns false on parse failure (skip rather
// than guess).
func versionAtLeast(s string, wantMaj, wantMin, wantPatch int) bool {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	end := strings.IndexAny(s, " \t-+")
	if end >= 0 {
		s = s[:end]
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return false
	}
	maj, e1 := strconv.Atoi(parts[0])
	minor, e2 := strconv.Atoi(parts[1])
	pat, e3 := strconv.Atoi(parts[2])
	if e1 != nil || e2 != nil || e3 != nil {
		return false
	}
	switch {
	case maj != wantMaj:
		return maj > wantMaj
	case minor != wantMin:
		return minor > wantMin
	default:
		return pat >= wantPatch
	}
}

// QueryIcebergScan runs `SELECT count(*), coalesce(sum(id), 0) FROM
// iceberg_scan(metadataURI)` against m through a freshly-spawned duckdb CLI.
// The S3 endpoint is configured to point at the MinIO testcontainer via
// per-session SETs (no DuckDB secret needed for one-shot scripts).
//
// MaybeSkipNoDuckDB must run first so duckdbBinary is populated.
func QueryIcebergScan(t *testing.T, m *MinIO, metadataURI string) IcebergScanResult {
	t.Helper()
	if duckdbBinary == "" {
		t.Fatal("QueryIcebergScan called without MaybeSkipNoDuckDB — duckdbBinary not probed")
	}

	endpoint := strings.TrimPrefix(m.Endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	// Single-quote escaping: duckdb SQL strings double single quotes.
	esc := func(s string) string { return strings.ReplaceAll(s, "'", "''") }

	sql := strings.Join([]string{
		"INSTALL iceberg;",
		"LOAD iceberg;",
		"INSTALL httpfs;",
		"LOAD httpfs;",
		"SET s3_endpoint='" + esc(endpoint) + "';",
		"SET s3_url_style='path';",
		"SET s3_use_ssl=false;",
		"SET s3_access_key_id='" + esc(m.AccessKey) + "';",
		"SET s3_secret_access_key='" + esc(m.SecretKey) + "';",
		"SET s3_region='us-east-1';",
		"COPY (SELECT count(*) AS c, coalesce(sum(id), 0) AS s FROM iceberg_scan('" + esc(metadataURI) + "')) TO '/dev/stdout' (FORMAT CSV, HEADER false);",
	}, "\n")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, duckdbBinary, "-c", sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("duckdb iceberg_scan failed: %v\nstderr:\n%s\nstdout:\n%s", err, stderr.String(), stdout.String())
	}

	// COPY ... TO '/dev/stdout' may emit a DuckDB summary line ("100% ▕..."
	// or "Wrote ... rows") to stdout in some builds. Take the last
	// non-empty line that looks like CSV (two integer columns).
	line := lastCSVLine(stdout.String())
	if line == "" {
		t.Fatalf("duckdb returned no CSV output\nstderr:\n%s\nstdout:\n%s", stderr.String(), stdout.String())
	}
	parts := strings.SplitN(line, ",", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected CSV row %q\nstderr:\n%s", line, stderr.String())
	}
	count, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		t.Fatalf("parse count from %q: %v", line, err)
	}
	sumID, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		t.Fatalf("parse sum from %q: %v", line, err)
	}
	return IcebergScanResult{Count: count, SumID: sumID}
}

func lastCSVLine(out string) string {
	lines := strings.Split(out, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		s := strings.TrimSpace(lines[i])
		if s == "" {
			continue
		}
		if strings.Count(s, ",") >= 1 {
			return s
		}
	}
	return ""
}
