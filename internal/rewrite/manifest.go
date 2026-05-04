package rewrite

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	iceberg "github.com/apache/iceberg-go"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// WalkManifestList reads the Avro manifest list at uri from src, returns
// a PathHit for each manifest_path entry under mapping, and returns the
// parsed iceberg.ManifestFile slice and the OCF header metadata
// callers need to round-trip the list (snapshot id, parent snapshot id,
// sequence number, first row id, and the format version).
//
// manifest_length (Avro id 501) is not a path; the write path recomputes
// it. The dry-run walker leaves it untouched.
func WalkManifestList(ctx context.Context, src Storage, uri string, mapping PrefixMapping) ([]PathHit, []iceberg.ManifestFile, ManifestListHeader, int, error) {
	raw, err := src.GetObject(ctx, uri)
	if err != nil {
		return nil, nil, ManifestListHeader{}, 0, fmt.Errorf("read manifest list %s: %w", uri, err)
	}
	hdr, version, err := readManifestListHeader(raw)
	if err != nil {
		return nil, nil, ManifestListHeader{}, 0, fmt.Errorf("decode manifest list header %s: %w", uri, err)
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, ManifestListHeader{}, 0, fmt.Errorf("decode manifest list %s: %w", uri, err)
	}
	hits := make([]PathHit, 0, len(files))
	for i, mf := range files {
		hits = append(hits, PathHit{
			Field: fmt.Sprintf("manifest-list[%d].manifest_path", i),
			Old:   mf.FilePath(),
			New:   mapping.Apply(mf.FilePath()),
		})
	}
	return hits, files, hdr, version, nil
}

// readManifestListHeader extracts the OCF-level metadata that the
// manifest-list writer requires when re-emitting the list. iceberg-go's
// ManifestListReader does not expose these values, so we open the OCF
// container ourselves with hamba/avro/v2 (already a transitive dep).
//
// V1 stores no sequence-number; V3 additionally stores first-row-id.
// Missing fields return zero values; the writer picks the right
// constructor based on the format version returned here.
func readManifestListHeader(raw []byte) (ManifestListHeader, int, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(raw), ocf.WithDecoderSchemaCache(&avro.SchemaCache{}))
	if err != nil {
		return ManifestListHeader{}, 0, err
	}
	defer func() { _ = dec.Close() }()

	meta := dec.Metadata()
	version, err := strconv.Atoi(string(meta["format-version"]))
	if err != nil {
		return ManifestListHeader{}, 0, fmt.Errorf("manifest-list format-version: %w", err)
	}
	hdr := ManifestListHeader{}
	if v := string(meta["snapshot-id"]); v != "" && v != "null" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list snapshot-id: %w", err)
		}
		hdr.SnapshotID = n
	}
	if v := string(meta["parent-snapshot-id"]); v != "" && v != "null" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list parent-snapshot-id: %w", err)
		}
		hdr.ParentSnapshotID = &n
	}
	if v := string(meta["sequence-number"]); v != "" && v != "null" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list sequence-number: %w", err)
		}
		hdr.SequenceNumber = &n
	}
	if v := string(meta["first-row-id"]); v != "" && v != "null" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list first-row-id: %w", err)
		}
		hdr.FirstRowID = n
	}
	return hdr, version, nil
}

// WalkManifest reads the Avro manifest described by mf and returns
// PathHits for data_file.file_path and data_file.referenced_data_file
// under mapping. It is the dry-run counterpart of RewriteManifest: it
// reports what would change without writing.
//
// Entry routing:
//   - EntryContentData / EntryContentPosDeletes / EntryContentEqDeletes
//     → report path hits for file_path and (if set) referenced_data_file.
//   - EntryContentPosDeletes with a .puffin file extension → refuse with
//     ErrUnsupportedFeature (V3 deletion vectors live inside Puffin
//     files, deferred to a separate Puffin-rewrite work item).
//   - Any other content type → refuse (defensive; iceberg-go has no
//     other constants today, but this protects against future drift).
func WalkManifest(ctx context.Context, src Storage, mf iceberg.ManifestFile, mapping PrefixMapping) ([]PathHit, error) {
	raw, err := src.GetObject(ctx, mf.FilePath())
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
	}
	entries, err := iceberg.ReadManifest(mf, bytes.NewReader(raw), false)
	if err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}
	hits := make([]PathHit, 0, len(entries))
	for i, e := range entries {
		df := e.DataFile()
		switch df.ContentType() {
		case iceberg.EntryContentData, iceberg.EntryContentEqDeletes:
			// Path-only mutation; no file body rewrite.
		case iceberg.EntryContentPosDeletes:
			if isPuffinPath(df.FilePath()) {
				return nil, fmt.Errorf("%w: manifest %s entry %d points at Puffin file %s (V3 deletion vector)",
					ErrUnsupportedFeature, mf.FilePath(), i, df.FilePath())
			}
		default:
			return nil, fmt.Errorf("%w: manifest %s entry %d has content type %s",
				ErrUnsupportedFeature, mf.FilePath(), i, df.ContentType())
		}
		hits = append(hits, PathHit{
			Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.file_path", mf.FilePath(), i),
			Old:   df.FilePath(),
			New:   mapping.Apply(df.FilePath()),
		})
		if ref := df.ReferencedDataFile(); ref != nil && *ref != "" {
			hits = append(hits, PathHit{
				Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.referenced_data_file", mf.FilePath(), i),
				Old:   *ref,
				New:   mapping.Apply(*ref),
			})
		}
	}
	return hits, nil
}

// isPuffinPath returns true if uri ends in .puffin (case-insensitive).
// V3 deletion vectors live in Puffin containers; rewriting their
// contents needs a Puffin reader/writer, which is a separate
// deferred work item. Position-delete entries pointing at .puffin
// files are refused upfront so we don't half-rebase a V3 table.
func isPuffinPath(uri string) bool {
	const ext = ".puffin"
	if len(uri) < len(ext) {
		return false
	}
	tail := uri[len(uri)-len(ext):]
	for i := range ext {
		a := tail[i]
		b := ext[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}
