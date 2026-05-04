package rewrite

import (
	"context"
	"fmt"
)

// putAndVerify writes body to target at uri, then HEADs the URI and
// asserts the reported size equals len(body). This closes the
// "manifest_length wrong by one" class of failure: a silent S3
// truncation between the PutObject body length and what landed on
// storage would otherwise produce a manifest_length mismatch that
// Iceberg readers reject only after the catalog swap.
//
// The HEAD is one extra round-trip per metadata file written. For the
// usual table this is 1 metadata.json + N manifest lists + N manifests,
// so a few extra requests per migrated table — a price worth paying for
// pre-swap safety.
func putAndVerify(ctx context.Context, target Storage, uri string, body []byte) error {
	if err := target.PutObject(ctx, uri, body); err != nil {
		return fmt.Errorf("put %s: %w", uri, err)
	}
	size, err := target.HeadObject(ctx, uri)
	if err != nil {
		return fmt.Errorf("verify %s after put: %w", uri, err)
	}
	if size != int64(len(body)) {
		return fmt.Errorf("verify %s: on-disk size %d != written size %d (storage truncated?)",
			uri, size, len(body))
	}
	return nil
}
