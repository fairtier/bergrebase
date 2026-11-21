package rewrite

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
)

// maxInflatedMetadataSize caps a decompressed metadata.json, so a corrupt or
// hostile object fails with an error rather than inflating without limit:
// storage's --max-object-size only bounds the compressed bytes. It is a bound,
// not a guarantee. The process still needs up to this much memory, and can run
// out on a smaller machine.
const maxInflatedMetadataSize = 4 << 30

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
//
// ParentSnapshotID, SequenceNumber and FirstRowID are not paths. They are here
// because rewriting a manifest list needs them, and metadata.json is the only
// place the spec guarantees them: DuckDB's manifest lists carry none of them
// in their OCF header.
type SnapshotJSON struct {
	SnapshotID       int64    `json:"snapshot-id"`
	ParentSnapshotID *int64   `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   *int64   `json:"sequence-number,omitempty"`
	FirstRowID       *int64   `json:"first-row-id,omitempty"`
	ManifestList     string   `json:"manifest-list,omitempty"`
	Manifests        []string `json:"manifests,omitempty"`
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
// retained. raw may be gzip-compressed (see inflateMetadataJSON).
func ParseMetadataJSON(raw []byte) (*MetadataJSON, error) {
	raw, _, err := inflateMetadataJSON(raw)
	if err != nil {
		return nil, err
	}
	m := &MetadataJSON{}
	if err := json.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("parse metadata.json: %w", err)
	}
	return m, nil
}

// inflateMetadataJSON returns the JSON text of a metadata file and whether it
// was gzip-compressed. Lakekeeper gzips metadata unless a table sets
// write.metadata.compression-codec=none, naming it *.gz.metadata.json.
//
// Detection is by content rather than by file name: a JSON document cannot
// begin with the gzip magic bytes, so this cannot misfire, and it also covers
// the legacy *.metadata.json.gz spelling.
func inflateMetadataJSON(raw []byte) ([]byte, bool, error) {
	if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
		return raw, false, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, false, fmt.Errorf("gunzip metadata.json: %w", err)
	}
	defer func() { _ = zr.Close() }()
	out, err := io.ReadAll(io.LimitReader(zr, maxInflatedMetadataSize+1))
	if err != nil {
		return nil, false, fmt.Errorf("gunzip metadata.json: %w", err)
	}
	if len(out) > maxInflatedMetadataSize {
		return nil, false, fmt.Errorf("gunzip metadata.json: inflated size exceeds %d bytes", int64(maxInflatedMetadataSize))
	}
	return out, true, nil
}

// deflateMetadataJSON is the inverse of inflateMetadataJSON. The gzip header
// carries no timestamp, so equal input gives equal output.
func deflateMetadataJSON(doc []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(doc); err != nil {
		return nil, fmt.Errorf("gzip metadata.json: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip metadata.json: %w", err)
	}
	return buf.Bytes(), nil
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
