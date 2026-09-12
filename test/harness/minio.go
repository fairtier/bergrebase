// Package harness wires up containerised dependencies (MinIO today,
// Lakekeeper tomorrow) used by the e2e test suite under test/...
//
// Tests skip rather than fail when Docker is unavailable, so the suite
// remains friendly on machines without a daemon and gates only on
// environments that opt in.
package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	bergstorage "github.com/fairtier/bergrebase/internal/storage"
)

// defaultMinioImage was the testcontainers-go module default on Docker
// Hub until 2026-09, when the prediction below came true: `minio/minio`
// now answers an anonymous pull scope with 401 (not 429 — this is a
// restriction, not throttling), while library/alpine on the same probe
// still answers 200. CI went red on 2026-09-12 with "pull access denied
// for minio/minio" in every testcontainers e2e test.
//
// quay.io carries the SAME tag, so this is a registry change and not a
// version change — the binary under test is byte-identical, and
// defaultLakekeeperImage below was already pointing there.
//
// Override still works via BERGREBASE_TEST_MINIO_IMAGE. Any
// S3-compatible MinIO build works for our tests — we only exercise
// GetObject / PutObject / HeadObject / CreateBucket.
//
// If quay.io ever follows Docker Hub, the ladder, checked 2026-09-13:
//
//  1. ghcr.io/coollabsio/minio — a third party that builds from source
//     because MinIO stopped publishing new releases. Carries only 9 tags
//     (2025-04 → 2025-10) and NOT the tag above, so it costs a version
//     bump, not just a registry swap; unlicensed, and nothing published
//     since 2025-10. A fallback, not a peer of quay.io.
//  2. Build from source ourselves — minio/minio Dockerfile.release, or
//     `go install github.com/minio/minio@latest` in a golang image.
//     Most work, fewest third parties.
//
// NOT bitnami/minio, which the older version of this comment suggested:
// Broadcom's 2025-08 rug-pull emptied it (the repo resolves but lists
// ZERO tags) and moved the archive to bitnamilegacy/, unmaintained.
const defaultMinioImage = "quay.io/minio/minio:RELEASE.2024-01-16T16-07-38Z"

// MinIO holds a running MinIO testcontainer plus the connection details
// the e2e tests need.
type MinIO struct {
	Endpoint     string // http://host:port
	AccessKey    string
	SecretKey    string
	SourceBucket string // s3://<SourceBucket>/...
	TargetBucket string // s3://<TargetBucket>/...
}

// StartMinIO launches a single MinIO container with two empty buckets
// ("source" and "target") and registers cleanup on the test. If Docker
// is unavailable the test is skipped — the harness never fails the run
// for environmental reasons.
func StartMinIO(ctx context.Context, t *testing.T) *MinIO {
	t.Helper()

	img := os.Getenv("BERGREBASE_TEST_MINIO_IMAGE")
	if img == "" {
		img = defaultMinioImage
	}
	c, err := minio.Run(ctx, img)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("docker unavailable, skipping testcontainers test: %v", err)
		}
		t.Fatalf("minio.Run: %v", err)
	}
	t.Cleanup(func() {
		if c == nil {
			return
		}
		_ = c.Terminate(context.Background())
	})

	cs, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio ConnectionString: %v", err)
	}
	endpoint := "http://" + cs

	m := &MinIO{
		Endpoint:     endpoint,
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

// StorageClient returns a *bergstorage.Client wired to this MinIO
// instance. The same client is used for both source and target sides;
// bucket selection is encoded in the s3:// URI.
func (m *MinIO) StorageClient() *bergstorage.Client {
	return bergstorage.New(bergstorage.Config{
		Region:          "us-east-1",
		Endpoint:        m.Endpoint,
		PathStyle:       true,
		AccessKeyID:     m.AccessKey,
		SecretAccessKey: m.SecretKey,
	})
}

func (m *MinIO) createBuckets(ctx context.Context) error {
	cli, err := m.s3Client(ctx)
	if err != nil {
		return err
	}
	for _, b := range []string{m.SourceBucket, m.TargetBucket} {
		if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			return fmt.Errorf("create bucket %s: %w", b, err)
		}
	}
	return nil
}

func (m *MinIO) s3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(m.AccessKey, m.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(m.Endpoint)
		o.UsePathStyle = true
	}), nil
}

// isDockerUnavailable reports whether err is the kind of failure that
// indicates "no Docker daemon", as opposed to a real test failure. We
// match on substrings because testcontainers wraps the underlying
// docker-client errors and there's no typed sentinel to switch on.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"connection refused",
		"no such file or directory",
		"executable file not found",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return errors.Is(err, context.DeadlineExceeded)
}
