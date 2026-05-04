package rewrite

import (
	"encoding/json"
	"fmt"
)

// MetadataJSON is the subset of an Iceberg metadata.json document that
// holds path-bearing fields. Unknown fields are preserved verbatim via
// json.RawMessage so the document can be re-assembled with surgical
// edits when the write path lands; today only the read/dry-run walk uses
// this struct.
//
// Field names are taken verbatim from the Iceberg table spec. Use
// MetadataPathProperties / PathFieldsInMetadataJSON in metadata.go as
// the authoritative documentation of which fields are paths.
type MetadataJSON struct {
	FormatVersion       int                    `json:"format-version"`
	TableUUID           string                 `json:"table-uuid,omitempty"`
	Location            string                 `json:"location"`
	CurrentSnapshotID   *int64                 `json:"current-snapshot-id,omitempty"`
	Snapshots           []SnapshotJSON         `json:"snapshots,omitempty"`
	MetadataLog         []MetadataLogEntryJSON `json:"metadata-log,omitempty"`
	Statistics          []StatisticsFileJSON   `json:"statistics,omitempty"`
	PartitionStatistics []StatisticsFileJSON   `json:"partition-statistics,omitempty"`
	Properties          map[string]string      `json:"properties,omitempty"`
}

// SnapshotJSON captures the path-bearing fields of a snapshot. V2+ uses
// ManifestList; V1 uses the flat Manifests list.
type SnapshotJSON struct {
	SnapshotID   int64    `json:"snapshot-id"`
	ManifestList string   `json:"manifest-list,omitempty"`
	Manifests    []string `json:"manifests,omitempty"`
}

// MetadataLogEntryJSON is one element of metadata-log[].
type MetadataLogEntryJSON struct {
	TimestampMs  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

// StatisticsFileJSON covers both statistics[] and partition-statistics[]
// entries (they share the same path field in the table spec).
type StatisticsFileJSON struct {
	SnapshotID     int64  `json:"snapshot-id"`
	StatisticsPath string `json:"statistics-path"`
}

// ParseMetadataJSON decodes raw into a MetadataJSON. The write path
// re-serialises through a generic map[string]any (see metadata_write.go)
// rather than byte-level round-tripping, so the original bytes are not
// retained.
func ParseMetadataJSON(raw []byte) (*MetadataJSON, error) {
	m := &MetadataJSON{}
	if err := json.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("parse metadata.json: %w", err)
	}
	return m, nil
}

// PathHit describes a single path-bearing field encountered during a
// walk. Field is a JSON-pointer-ish dotted path (e.g.
// "snapshots[3].manifest-list"); Old is the value as found in the
// document; New is the value after PrefixMapping.Apply.
//
// PathHits drive the dry-run report and, on the write path, identify the
// exact strings to substitute.
type PathHit struct {
	Field string
	Old   string
	New   string
}

// Rewritten reports whether Old != New for this hit.
func (h PathHit) Rewritten() bool { return h.Old != h.New }

// WalkMetadataJSON walks every path-bearing field in m and returns a
// slice of PathHits describing the proposed substitution under mapping.
// Hits whose Old value does not start with mapping.Source are still
// reported (with Old == New) so the caller can audit out-of-prefix
// references.
func WalkMetadataJSON(m *MetadataJSON, mapping PrefixMapping) []PathHit {
	hits := []PathHit{}

	hits = append(hits, hit("location", m.Location, mapping))

	for i, snap := range m.Snapshots {
		if snap.ManifestList != "" {
			hits = append(hits, hit(
				fmt.Sprintf("snapshots[%d].manifest-list", i),
				snap.ManifestList, mapping,
			))
		}
		for j, mf := range snap.Manifests {
			hits = append(hits, hit(
				fmt.Sprintf("snapshots[%d].manifests[%d]", i, j),
				mf, mapping,
			))
		}
	}

	for i, e := range m.MetadataLog {
		hits = append(hits, hit(
			fmt.Sprintf("metadata-log[%d].metadata-file", i),
			e.MetadataFile, mapping,
		))
	}

	for i, s := range m.Statistics {
		hits = append(hits, hit(
			fmt.Sprintf("statistics[%d].statistics-path", i),
			s.StatisticsPath, mapping,
		))
	}
	for i, s := range m.PartitionStatistics {
		hits = append(hits, hit(
			fmt.Sprintf("partition-statistics[%d].statistics-path", i),
			s.StatisticsPath, mapping,
		))
	}

	for _, key := range MetadataPathProperties {
		v, ok := m.Properties[key]
		if !ok {
			continue
		}
		hits = append(hits, hit(
			fmt.Sprintf("properties[%q]", key),
			v, mapping,
		))
	}

	return hits
}

func hit(field, old string, mapping PrefixMapping) PathHit {
	return PathHit{Field: field, Old: old, New: mapping.Apply(old)}
}
