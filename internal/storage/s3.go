// Package storage implements the small object-storage surface the
// rewriter depends on. The default implementation targets any
// S3-compatible endpoint (AWS S3, Cloudflare R2, MinIO, Backblaze B2,
// Wasabi, DigitalOcean Spaces, ...) via aws-sdk-go-v2 with endpoint
// override.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config is the S3 client configuration for a single side of the
// migration (source or target). Credentials are read from environment
// variables outside this package — see cmd/bergrebase/main.go.
type Config struct {
	Region    string
	Endpoint  string
	PathStyle bool

	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// MaxObjectSize caps GetObject reads. Iceberg metadata files are
	// KB-MB sized in practice; the cap exists so that a misconfigured
	// run pointed at a data-file URI by mistake fails loudly instead
	// of OOM-ing the process. Zero means use the package default
	// (DefaultMaxObjectSize).
	MaxObjectSize int64
}

// DefaultMaxObjectSize bounds a single GetObject body. Iceberg
// metadata.json and manifest list / manifest files are at most a few
// MB even on wide V2 tables; 64 MiB is a generous safety net that
// still rules out 10 GB Parquet files.
const DefaultMaxObjectSize = 64 << 20

// Client is an S3-compatible object client. It lazily constructs the
// underlying aws-sdk-go-v2 s3.Client on first use so that callers can
// build a Client during flag parsing without paying for a network probe.
type Client struct {
	cfg Config

	once    sync.Once
	s3      *s3.Client
	initErr error
}

// New returns a Client configured for the given S3-compatible endpoint.
func New(cfg Config) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) client(ctx context.Context) (*s3.Client, error) {
	c.once.Do(func() {
		opts := []func(*awsconfig.LoadOptions) error{}
		if c.cfg.Region != "" {
			opts = append(opts, awsconfig.WithRegion(c.cfg.Region))
		}
		if c.cfg.AccessKeyID != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					c.cfg.AccessKeyID, c.cfg.SecretAccessKey, c.cfg.SessionToken,
				),
			))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			c.initErr = fmt.Errorf("storage: load aws config: %w", err)
			return
		}
		c.s3 = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if c.cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(c.cfg.Endpoint)
			}
			o.UsePathStyle = c.cfg.PathStyle
		})
	})
	return c.s3, c.initErr
}

// GetObject reads the object at uri and returns its bytes.
func (c *Client) GetObject(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, err := ParseS3URI(uri)
	if err != nil {
		return nil, err
	}
	cli, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage: get %s: %w", uri, err)
	}
	defer func() { _ = out.Body.Close() }()

	limit := c.cfg.MaxObjectSize
	if limit <= 0 {
		limit = DefaultMaxObjectSize
	}
	// Read up to limit+1 so we can distinguish "exactly limit" from
	// "exceeded limit" without re-reading.
	buf := &bytes.Buffer{}
	n, err := io.Copy(buf, io.LimitReader(out.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("storage: read %s: %w", uri, err)
	}
	if n > limit {
		return nil, fmt.Errorf("storage: %s exceeds MaxObjectSize=%d (Storage is for metadata files only; pointing at a data-file URI is a configuration error)",
			uri, limit)
	}
	return buf.Bytes(), nil
}

// PutObject writes body at uri, overwriting any existing object.
func (c *Client) PutObject(ctx context.Context, uri string, body []byte) error {
	bucket, key, err := ParseS3URI(uri)
	if err != nil {
		return err
	}
	cli, err := c.client(ctx)
	if err != nil {
		return err
	}
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("storage: put %s: %w", uri, err)
	}
	return nil
}

// HeadObject returns the size of the object at uri.
func (c *Client) HeadObject(ctx context.Context, uri string) (int64, error) {
	bucket, key, err := ParseS3URI(uri)
	if err != nil {
		return 0, err
	}
	cli, err := c.client(ctx)
	if err != nil {
		return 0, err
	}
	out, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("storage: head %s: %w", uri, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

// ParseS3URI splits an "s3://bucket/key" URI into its bucket and key
// components. The key may contain forward slashes; an empty key is an
// error since every Iceberg path is an object, not a bucket root.
func ParseS3URI(uri string) (bucket, key string, err error) {
	u, parseErr := url.Parse(uri)
	if parseErr != nil {
		return "", "", fmt.Errorf("storage: parse %q: %w", uri, parseErr)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("storage: unsupported scheme %q in %q (want s3://)", u.Scheme, uri)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("storage: missing bucket in %q", uri)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("storage: missing key in %q", uri)
	}
	return u.Host, key, nil
}

// Compile-time check that *Client satisfies the package-private subset of
// the rewrite.Storage interface. Asserted via a sentinel variable so a
// build break here surfaces the contract drift immediately.
var _ interface {
	GetObject(ctx context.Context, uri string) ([]byte, error)
	PutObject(ctx context.Context, uri string, body []byte) error
	HeadObject(ctx context.Context, uri string) (int64, error)
} = (*Client)(nil)

// ErrNotFound is returned by GetObject/HeadObject implementations that
// want to expose a typed "object missing" condition. The S3 client wraps
// SDK errors today; this sentinel exists so future implementations
// (filesystem, in-memory) can satisfy a uniform contract.
var ErrNotFound = errors.New("storage: object not found")
