package rewrite

import (
	"strings"
	"testing"
)

const sampleMetadataJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://old/iceberg/db/orders",
  "last-sequence-number": 7,
  "snapshots": [
    {
      "snapshot-id": 1,
      "manifest-list": "s3://old/iceberg/db/orders/metadata/snap-1-1-abc.avro"
    },
    {
      "snapshot-id": 2,
      "manifest-list": "s3://old/iceberg/db/orders/metadata/snap-2-1-def.avro"
    }
  ],
  "metadata-log": [
    {"timestamp-ms": 1700000000000, "metadata-file": "s3://old/iceberg/db/orders/metadata/v1.metadata.json"}
  ],
  "statistics": [
    {"snapshot-id": 2, "statistics-path": "s3://old/iceberg/db/orders/metadata/snap-2.stats"}
  ],
  "partition-statistics": [
    {"snapshot-id": 2, "statistics-path": "s3://old/iceberg/db/orders/metadata/snap-2.pstats"}
  ],
  "properties": {
    "write.object-storage.path": "s3://old/iceberg/db/orders/data",
    "write.metadata.path": "s3://old/iceberg/db/orders/metadata",
    "owner": "alice"
  }
}`

func TestWalkMetadataJSON_AllPathsHit(t *testing.T) {
	m, err := ParseMetadataJSON([]byte(sampleMetadataJSON))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	hits := WalkMetadataJSON(m, mapping)

	wantFields := []string{
		"location",
		"snapshots[0].manifest-list",
		"snapshots[1].manifest-list",
		"metadata-log[0].metadata-file",
		"statistics[0].statistics-path",
		"partition-statistics[0].statistics-path",
		`properties["write.object-storage.path"]`,
		`properties["write.metadata.path"]`,
	}

	got := map[string]PathHit{}
	for _, h := range hits {
		got[h.Field] = h
	}
	if len(got) != len(hits) {
		t.Fatalf("duplicate fields in hits: %+v", hits)
	}
	for _, f := range wantFields {
		h, ok := got[f]
		if !ok {
			t.Errorf("missing hit for %q in %v", f, fieldsOf(hits))
			continue
		}
		if !h.Rewritten() {
			t.Errorf("hit %q not rewritten: old=%q new=%q", f, h.Old, h.New)
		}
		if !strings.HasPrefix(h.New, "s3://new/") {
			t.Errorf("hit %q new=%q not under target prefix", f, h.New)
		}
	}

	// owner is not a known path property — must not appear.
	if _, ok := got[`properties["owner"]`]; ok {
		t.Error("owner property should not be in hits")
	}
}

func TestWalkMetadataJSON_V1ManifestsList(t *testing.T) {
	const v1 = `{
		"format-version": 1,
		"location": "s3://old/iceberg/db/t",
		"snapshots": [
			{"snapshot-id": 7, "manifests": [
				"s3://old/iceberg/db/t/metadata/m1.avro",
				"s3://old/iceberg/db/t/metadata/m2.avro"
			]}
		]
	}`
	m, err := ParseMetadataJSON([]byte(v1))
	if err != nil {
		t.Fatalf("parse v1: %v", err)
	}
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	hits := WalkMetadataJSON(m, mapping)

	want := map[string]bool{
		"snapshots[0].manifests[0]": false,
		"snapshots[0].manifests[1]": false,
	}
	for _, h := range hits {
		if _, ok := want[h.Field]; ok {
			want[h.Field] = true
			if !h.Rewritten() {
				t.Errorf("v1 manifest %q not rewritten", h.Field)
			}
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("missing v1 manifest hit %q", k)
		}
	}
}

func TestWalkMetadataJSON_OutOfPrefixIsReportedNotMutated(t *testing.T) {
	const meta = `{
		"format-version": 2,
		"location": "s3://other/iceberg/db/t",
		"snapshots": []
	}`
	m, err := ParseMetadataJSON([]byte(meta))
	if err != nil {
		t.Fatal(err)
	}
	mapping := PrefixMapping{Source: "s3://old/", Target: "s3://new/"}
	hits := WalkMetadataJSON(m, mapping)
	if len(hits) != 1 || hits[0].Field != "location" {
		t.Fatalf("expected single location hit, got %+v", hits)
	}
	if hits[0].Rewritten() {
		t.Errorf("out-of-prefix location should not be rewritten: %+v", hits[0])
	}
}

func fieldsOf(hits []PathHit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.Field
	}
	return out
}
