package rewrite

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
// Missing fields return zero values, and a missing format-version returns
// version 0: the spec requires none of these keys on a manifest list and
// DuckDB writes none of them. resolveManifestListHeader fills the gaps from
// metadata.json.
func readManifestListHeader(raw []byte) (ManifestListHeader, int, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(raw), ocf.WithDecoderSchemaCache(&avro.SchemaCache{}))
	if err != nil {
		return ManifestListHeader{}, 0, err
	}
	defer func() { _ = dec.Close() }()

	meta := dec.Metadata()
	version := 0
	if v := string(meta["format-version"]); v != "" {
		version, err = strconv.Atoi(v)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list format-version: %w", err)
		}
	}
	hdr := ManifestListHeader{}
	if v := string(meta["snapshot-id"]); v != "" && v != "null" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ManifestListHeader{}, 0, fmt.Errorf("manifest-list snapshot-id: %w", err)
		}
		hdr.SnapshotID = &n
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
		hdr.FirstRowID = &n
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
//     A referenced_data_file hit with an empty New means the field is
//     dropped: see minVersionWithReferencedDataFile.
//   - EntryContentPosDeletes (Parquet, in scope) → additionally read the
//     delete file's body via WalkPositionDeleteFile so a dry-run
//     surfaces the same body-level failures (unparseable Parquet,
//     foreign-prefix rows) the live rewrite would hit. No writes.
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
	raw = normalizeDeleteManifestHeader(mf, raw)
	// The same reads and the same encoder as RewriteManifest, so that a clean dry
	// run predicts the live run.
	reader, err := iceberg.NewManifestReader(mf, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}
	header, err := manifestHeader(raw)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: read header: %w", mf.FilePath(), err)
	}
	spec, err := reader.PartitionSpec()
	if err != nil {
		return nil, fmt.Errorf("manifest %s: read partition spec: %w", mf.FilePath(), err)
	}
	entries, err := iceberg.ReadManifest(mf, bytes.NewReader(raw), false)
	if err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}
	hits := make([]PathHit, 0, len(entries))
	changed := false
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
			if e.Status() == iceberg.EntryStatusDELETED {
				break // A tombstone's body is never read; see RewriteManifest.
			}
			if err := WalkPositionDeleteFile(ctx, src, df.FilePath(), mapping); err != nil {
				return nil, fmt.Errorf("dry-run position-delete %s: %w", df.FilePath(), err)
			}
		default:
			return nil, fmt.Errorf("%w: manifest %s entry %d has content type %s",
				ErrUnsupportedFeature, mf.FilePath(), i, df.ContentType())
		}
		fileHit := PathHit{
			Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.file_path", mf.FilePath(), i),
			Old:   df.FilePath(),
			New:   mapping.Apply(df.FilePath()),
		}
		hits = append(hits, fileHit)
		changed = changed || fileHit.Rewritten()
		if ref := df.ReferencedDataFile(); ref != nil && *ref != "" {
			refHit := referencedDataFileHit(mf, i, *ref, header.version, mapping)
			hits = append(hits, refHit)
			changed = changed || (header.version >= minVersionWithReferencedDataFile && refHit.Rewritten())
		}
	}
	if changed && !mapping.Matches(mf.FilePath()) {
		return nil, fmt.Errorf("%w: manifest %s is outside source prefix %q but references in-prefix paths; rewriting would overwrite the original in place",
			ErrOutsidePrefix, mf.FilePath(), mapping.Source)
	}
	if !changed {
		return hits, nil // The live run leaves this manifest alone: nothing to encode.
	}
	w, err := iceberg.NewManifestWriter(header.version, io.Discard, *spec, header.schema, mf.SnapshotID(),
		iceberg.WithManifestWriterContent(mf.ManifestContent()))
	if err != nil {
		return nil, fmt.Errorf("encode manifest %s: %w", mf.FilePath(), err)
	}
	for _, entry := range entries {
		if err := addEntryKeepingStatus(w, mf, entry); err != nil {
			return nil, fmt.Errorf("encode manifest %s: %w", mf.FilePath(), err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("encode manifest %s: %w", mf.FilePath(), err)
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
