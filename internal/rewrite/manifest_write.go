package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	iceberg "github.com/apache/iceberg-go"
)

// RewriteManifest reads the Avro manifest pointed to by mf, applies
// mapping to every path-bearing field on every entry, optionally
// rewrites the Parquet body of any V2 position-delete file the
// manifest references, and writes a new manifest at the substituted
// URI. It returns the new ManifestFile (path, length, all other fields
// preserved) so the caller can place it into a rewritten manifest list.
//
// If no entry path matches mapping.Source AND no position-delete file
// body needs rewriting, RewriteManifest skips the write and returns mf
// unchanged. This preserves the "byte-identical-when-Source==Target"
// invariant from acceptance criterion #1 trivially.
//
// Entry routing:
//   - EntryContentData → rewrite file_path + referenced_data_file.
//   - EntryContentPosDeletes (Parquet) → rewrite the Parquet body via
//     RewritePositionDeleteFile (which mutates the file_path column
//     inside the Parquet), then rewrite the manifest entry's file_path
//     and referenced_data_file.
//   - EntryContentPosDeletes (.puffin) → refuse with
//     ErrUnsupportedFeature (V3 deletion vector territory).
//   - EntryContentEqDeletes → rewrite file_path only. Equality-delete
//     files contain no embedded URIs (just projected equality column
//     values), so no body rewrite is needed.
//   - Any other content type → refuse (defensive; iceberg-go has no
//     other constants today, but this protects against future drift).
func RewriteManifest(ctx context.Context, src, target Storage, mf iceberg.ManifestFile, mapping PrefixMapping) (iceberg.ManifestFile, []PathHit, error) {
	raw, err := src.GetObject(ctx, mf.FilePath())
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
	}

	reader, err := iceberg.NewManifestReader(mf, bytes.NewReader(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}
	schema, err := reader.Schema()
	if err != nil {
		return nil, nil, fmt.Errorf("manifest %s: read schema: %w", mf.FilePath(), err)
	}
	spec, err := reader.PartitionSpec()
	if err != nil {
		return nil, nil, fmt.Errorf("manifest %s: read partition spec: %w", mf.FilePath(), err)
	}

	// Re-read entries through ReadManifest because the streaming reader
	// is consumed by the schema/spec calls above lazily; ReadManifest
	// returns a slice we can iterate twice (mutate, write).
	entries, err := iceberg.ReadManifest(mf, bytes.NewReader(raw), false)
	if err != nil {
		return nil, nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}

	hits := make([]PathHit, 0, len(entries)*2)
	mutated := false
	for i, e := range entries {
		df := e.DataFile()
		switch df.ContentType() {
		case iceberg.EntryContentData, iceberg.EntryContentEqDeletes:
			// Path-only mutation; no file body rewrite. Equality-delete
			// files contain only the projected equality column values
			// — no embedded URIs.
		case iceberg.EntryContentPosDeletes:
			if isPuffinPath(df.FilePath()) {
				return nil, nil, fmt.Errorf("%w: manifest %s entry %d points at Puffin file %s (V3 deletion vector)",
					ErrUnsupportedFeature, mf.FilePath(), i, df.FilePath())
			}
			// Rewrite the position-delete Parquet body: the file_path
			// column inside embeds absolute data-file URIs. The body
			// rewriter is idempotent (no PUT if the file_path column
			// already targets the new bucket).
			if _, err := RewritePositionDeleteFile(ctx, src, target, df.FilePath(), mapping); err != nil {
				return nil, nil, fmt.Errorf("rewrite position-delete %s: %w", df.FilePath(), err)
			}
		default:
			return nil, nil, fmt.Errorf("%w: manifest %s entry %d has content type %s",
				ErrUnsupportedFeature, mf.FilePath(), i, df.ContentType())
		}

		oldPath := df.FilePath()
		newPath := mapping.Apply(oldPath)
		hits = append(hits, PathHit{
			Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.file_path", mf.FilePath(), i),
			Old:   oldPath,
			New:   newPath,
		})
		if oldPath != newPath {
			if err := setDataFilePath(df, newPath); err != nil {
				return nil, nil, fmt.Errorf("mutate manifest %s entry %d file_path: %w", mf.FilePath(), i, err)
			}
			mutated = true
		}
		if ref := df.ReferencedDataFile(); ref != nil && *ref != "" {
			oldRef := *ref
			newRef := mapping.Apply(oldRef)
			hits = append(hits, PathHit{
				Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.referenced_data_file", mf.FilePath(), i),
				Old:   oldRef,
				New:   newRef,
			})
			if oldRef != newRef {
				if err := setDataFileReferencedDataFile(df, newRef); err != nil {
					return nil, nil, fmt.Errorf("mutate manifest %s entry %d referenced_data_file: %w", mf.FilePath(), i, err)
				}
				mutated = true
			}
		}
	}

	if !mutated {
		// Idempotent: no path changed, leave the file alone.
		return mf, hits, nil
	}

	newURI := mapping.Apply(mf.FilePath())
	buf := &bytes.Buffer{}
	newMF, err := iceberg.WriteManifest(newURI, buf, mf.Version(), *spec, schema, mf.SnapshotID(), entries)
	if err != nil {
		return nil, nil, fmt.Errorf("write manifest %s -> %s: %w", mf.FilePath(), newURI, err)
	}
	if err := putAndVerify(ctx, target, newURI, buf.Bytes()); err != nil {
		return nil, nil, fmt.Errorf("manifest %s: %w", newURI, err)
	}
	// iceberg.WriteManifest produces a ManifestFile with SeqNumber=-1
	// (unassigned). For manifests carried forward from prior snapshots,
	// WriteManifestList rejects unassigned sequences with "found
	// unassigned sequence number for a manifest from snapshot X != Y".
	// Preserve the original mf's sequence numbers so the rewritten copy
	// re-encodes into the parent list cleanly.
	return preserveSequenceNumbers(newMF, mf), hits, nil
}

// preserveSequenceNumbers returns newMF with the original mf's
// sequence_number / min_sequence_number overlaid, and the original's
// ManifestContent (data vs deletes) preserved. All other fields are
// taken from newMF — the rewrite mutates only path-bearing fields, so
// counts and partition stats are unchanged.
//
// ManifestContent must come from originalMF: iceberg.WriteManifest
// initialises the written ManifestFile descriptor with
// ManifestContentData regardless of what the entries actually contain
// (the manifest list entry is the source of truth for content type,
// not the manifest file body). Without preserving it, a delete
// manifest would silently turn into a data manifest in the rewritten
// list and readers would mis-route entries.
func preserveSequenceNumbers(newMF, originalMF iceberg.ManifestFile) iceberg.ManifestFile {
	return iceberg.NewManifestFile(
		newMF.Version(),
		newMF.FilePath(),
		newMF.Length(),
		newMF.PartitionSpecID(),
		newMF.SnapshotID(),
	).
		SequenceNum(originalMF.SequenceNum(), originalMF.MinSequenceNum()).
		Content(originalMF.ManifestContent()).
		AddedFiles(newMF.AddedDataFiles()).
		AddedRows(newMF.AddedRows()).
		ExistingFiles(newMF.ExistingDataFiles()).
		ExistingRows(newMF.ExistingRows()).
		DeletedFiles(newMF.DeletedDataFiles()).
		DeletedRows(newMF.DeletedRows()).
		KeyMetadata(newMF.KeyMetadata()).
		Build()
}

// RewriteManifestList writes a new manifest list at the URI substituted
// from listURI under mapping. version and headerMeta carry the
// snapshot/sequence/parent/firstRowID metadata read from the original
// list's OCF header so the rewrite preserves them.
//
// files is the slice of (already-rewritten) manifest files to embed.
// Each entry's manifest_path and manifest_length must reflect the new
// on-disk state — RewriteManifest produces ManifestFile values that
// satisfy this contract.
func RewriteManifestList(
	ctx context.Context,
	target Storage,
	listURI string,
	mapping PrefixMapping,
	version int,
	headerMeta ManifestListHeader,
	files []iceberg.ManifestFile,
) (string, error) {
	newURI := mapping.Apply(listURI)
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(
		version, buf,
		headerMeta.SnapshotID,
		headerMeta.ParentSnapshotID,
		headerMeta.SequenceNumber,
		headerMeta.FirstRowID,
		files,
	); err != nil {
		return "", fmt.Errorf("encode manifest list %s -> %s: %w", listURI, newURI, err)
	}
	if err := putAndVerify(ctx, target, newURI, buf.Bytes()); err != nil {
		return "", fmt.Errorf("manifest list %s: %w", newURI, err)
	}
	return newURI, nil
}

// ManifestListHeader carries the OCF-metadata fields a manifest list
// writer requires that are NOT path-bearing. They're read from the
// original list's OCF metadata in WalkManifestList so that the rewrite
// preserves snapshot identity.
type ManifestListHeader struct {
	SnapshotID       int64
	ParentSnapshotID *int64
	SequenceNumber   *int64 // V2/V3 only; nil for V1
	FirstRowID       int64  // V3 only
}

// setDataFilePath mutates the file_path field on an iceberg-go DataFile
// in place. iceberg-go's DataFile is a read-only interface backed by an
// unexported struct; constructing a new one through DataFileBuilder
// requires copying ~25 fields and re-deriving partition data, an
// order-of-magnitude larger surface that breaks (silently) on the same
// upstream renames as a single reflection call would. The round-trip
// tests assert the mutation took effect, so an iceberg-go API rename
// surfaces as a test failure rather than a silent miss.
func setDataFilePath(df iceberg.DataFile, newPath string) error {
	field, err := dataFileField(df, "Path")
	if err != nil {
		return err
	}
	if field.Kind() != reflect.String {
		return fmt.Errorf("DataFile.Path is %s, expected string", field.Kind())
	}
	field.SetString(newPath)
	return nil
}

// setDataFileReferencedDataFile mutates the referenced_data_file field
// (V2+ delete files, V3 mandatory for DVs) on an iceberg-go DataFile.
// The Avro field is optional, modelled in iceberg-go as *string.
func setDataFileReferencedDataFile(df iceberg.DataFile, newRef string) error {
	field, err := dataFileField(df, "ReferencedDataFileField")
	if err != nil {
		return err
	}
	if field.Kind() != reflect.Pointer || field.Type().Elem().Kind() != reflect.String {
		return fmt.Errorf("DataFile.ReferencedDataFileField is %s, expected *string", field.Type())
	}
	s := newRef
	field.Set(reflect.ValueOf(&s))
	return nil
}

func dataFileField(df iceberg.DataFile, fieldName string) (reflect.Value, error) {
	v := reflect.ValueOf(df)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return reflect.Value{}, errors.New("DataFile is not a non-nil pointer")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("DataFile underlying value is %s, expected struct", v.Kind())
	}
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}, fmt.Errorf("DataFile struct has no %q field (iceberg-go API drift?)", fieldName)
	}
	if !field.CanSet() {
		return reflect.Value{}, fmt.Errorf("DataFile.%s is not settable", fieldName)
	}
	return field, nil
}
