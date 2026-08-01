package rewrite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const writeFixtureMetadataJSON = `{
	"format-version": 2,
	"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
	"location": "s3://old/iceberg/db/orders",
	"current-snapshot-id": 7,
	"snapshots": [
		{
			"snapshot-id": 7,
			"manifest-list": "s3://old/iceberg/db/orders/metadata/snap-7-1-abc.avro",
			"summary": {"operation": "append", "added-records": "10", "custom-staging-dir": "s3://old/iceberg/db/orders/staging"}
		}
	],
	"metadata-log": [
		{"timestamp-ms": 1700000000000, "metadata-file": "s3://old/iceberg/db/orders/metadata/v1.metadata.json"}
	],
	"statistics": [
		{"snapshot-id": 7, "statistics-path": "s3://old/iceberg/db/orders/metadata/snap-7.stats"}
	],
	"properties": {
		"write.object-storage.path": "s3://old/iceberg/db/orders/data",
		"owner": "alice"
	}
}`

func TestRewriteMetadataJSON_RewritesAllPathFields(t *testing.T) {
	const oldURI = "s3://old/iceberg/db/orders/metadata/v2.metadata.json"
	store := newMemStorage()
	store.objects[oldURI] = []byte(writeFixtureMetadataJSON)

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	newURI, hits, err := RewriteMetadataJSON(context.Background(), store, store, oldURI, mapping)
	if err != nil {
		t.Fatalf("RewriteMetadataJSON: %v", err)
	}
	wantURI := "s3://new/iceberg/db/orders/metadata/v2.metadata.json"
	if newURI != wantURI {
		t.Errorf("new URI = %q, want %q", newURI, wantURI)
	}
	if len(hits) == 0 {
		t.Fatal("expected hits")
	}

	written, ok := store.puts[wantURI]
	if !ok {
		t.Fatalf("expected put at %q, got: %v", wantURI, putKeys(store))
	}

	var doc map[string]any
	if err := json.Unmarshal(written, &doc); err != nil {
		t.Fatalf("re-parse written: %v", err)
	}

	// Spot-check every path-bearing field.
	if got := doc["location"].(string); got != "s3://new/iceberg/db/orders" {
		t.Errorf("location = %q", got)
	}
	if got := doc["snapshots"].([]any)[0].(map[string]any)["manifest-list"].(string); got != "s3://new/iceberg/db/orders/metadata/snap-7-1-abc.avro" {
		t.Errorf("manifest-list = %q", got)
	}
	if got := doc["metadata-log"].([]any)[0].(map[string]any)["metadata-file"].(string); got != "s3://new/iceberg/db/orders/metadata/v1.metadata.json" {
		t.Errorf("metadata-log[0].metadata-file = %q", got)
	}
	if got := doc["statistics"].([]any)[0].(map[string]any)["statistics-path"].(string); got != "s3://new/iceberg/db/orders/metadata/snap-7.stats" {
		t.Errorf("statistics-path = %q", got)
	}
	if got := doc["properties"].(map[string]any)["write.object-storage.path"].(string); got != "s3://new/iceberg/db/orders/data" {
		t.Errorf("write.object-storage.path = %q", got)
	}
	// Snapshot summary: custom values starting with the source prefix
	// are rewritten; spec-defined counters pass through untouched.
	summary := doc["snapshots"].([]any)[0].(map[string]any)["summary"].(map[string]any)
	if got := summary["custom-staging-dir"].(string); got != "s3://new/iceberg/db/orders/staging" {
		t.Errorf("summary custom path value = %q", got)
	}
	if got := summary["added-records"].(string); got != "10" {
		t.Errorf("summary counter mutated: %q", got)
	}
	// Untouched non-path fields preserved.
	if got := doc["properties"].(map[string]any)["owner"].(string); got != "alice" {
		t.Errorf("owner property dropped: %q", got)
	}
	if got := doc["current-snapshot-id"]; got != float64(7) {
		t.Errorf("current-snapshot-id dropped: %v", got)
	}
}

// TestRewriteMetadataJSON_NoChange_SkipsWrite asserts the idempotency
// invariant. When nothing matches the source prefix, we must not PUT.
func TestRewriteMetadataJSON_NoChange_SkipsWrite(t *testing.T) {
	const oldURI = "s3://other/iceberg/db/x/v1.metadata.json"
	store := newMemStorage()
	// All paths inside the doc are under s3://other, not s3://old.
	store.objects[oldURI] = []byte(`{
		"format-version": 2,
		"location": "s3://other/iceberg/db/x",
		"snapshots": []
	}`)

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	newURI, _, err := RewriteMetadataJSON(context.Background(), store, store, oldURI, mapping)
	if err == nil {
		t.Fatal("expected prefix-mismatch error on the metadata-location itself")
	}
	if !strings.Contains(err.Error(), "does not start with source prefix") {
		t.Errorf("error message: %v", err)
	}
	_ = newURI
	if len(store.puts) != 0 {
		t.Errorf("expected zero puts, got %d", len(store.puts))
	}
}

// TestRewriteMetadataJSON_RoundTripWhenSourceEqualsTarget asserts that
// when Source==Target every Apply is a no-op and the engine writes
// nothing. This is how we satisfy the byte-identical acceptance
// criterion #1 without needing byte-perfect JSON re-emission.
func TestRewriteMetadataJSON_RoundTripWhenSourceEqualsTarget(t *testing.T) {
	const oldURI = "s3://old/iceberg/db/orders/metadata/v2.metadata.json"
	store := newMemStorage()
	store.objects[oldURI] = []byte(writeFixtureMetadataJSON)

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://old/"}
	newURI, _, err := RewriteMetadataJSON(context.Background(), store, store, oldURI, mapping)
	if err != nil {
		t.Fatalf("RewriteMetadataJSON: %v", err)
	}
	if newURI != oldURI {
		t.Errorf("URI changed despite Source==Target: %q -> %q", oldURI, newURI)
	}
	if len(store.puts) != 0 {
		t.Errorf("expected zero puts when Source==Target, got %d: %v", len(store.puts), putKeys(store))
	}
}

func TestApplyMetadataJSONMapping_OutOfPrefixReportsButDoesNotMutate(t *testing.T) {
	doc := map[string]any{
		"location": "s3://other/iceberg/db/x",
		"snapshots": []any{
			map[string]any{"snapshot-id": float64(1), "manifest-list": "s3://other/iceberg/db/x/m1.avro"},
		},
	}
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	hits, changed := ApplyMetadataJSONMapping(doc, mapping)
	if changed {
		t.Error("changed = true despite no source match")
	}
	if len(hits) == 0 {
		t.Fatal("expected hits to report out-of-prefix references")
	}
	for _, h := range hits {
		if h.Rewritten() {
			t.Errorf("hit %q rewritten despite no source match: %+v", h.Field, h)
		}
	}
}
