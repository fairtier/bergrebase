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
//     inside the Parquet), then rewrite the manifest entry's file_path,
//     referenced_data_file, the reserved file_path-column bounds
//     (lower_bounds/upper_bounds for field id 2147483546 — engines use
//     them to match delete files to data files), and file_size_in_bytes
//     (the body re-encode can change the byte length; split_offsets and
//     column_sizes describe the old layout and are cleared).
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
			pd, err := RewritePositionDeleteFile(ctx, src, target, df.FilePath(), mapping)
			if err != nil {
				return nil, nil, fmt.Errorf("rewrite position-delete %s: %w", df.FilePath(), err)
			}
			changed, err := fixupPositionDeleteEntryStats(df, pd, mapping)
			if err != nil {
				return nil, nil, fmt.Errorf("manifest %s entry %d: %w", mf.FilePath(), i, err)
			}
			mutated = mutated || changed
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

	// A manifest that lives outside the source prefix but references
	// in-prefix entries (mixed-location table: prior partial migration,
	// custom write.metadata.path history) cannot be rewritten safely:
	// Apply is a no-op on its URI, so the write would overwrite the
	// ORIGINAL manifest in place — mutating the live source table before
	// any catalog swap. Refuse instead of silently corrupting.
	if !mapping.Matches(mf.FilePath()) {
		return nil, nil, fmt.Errorf("%w: manifest %s is outside source prefix %q but references in-prefix paths; rewriting would overwrite the original in place",
			ErrOutsidePrefix, mf.FilePath(), mapping.Source)
	}

	newURI := mapping.Apply(mf.FilePath())
	// Encode via NewManifestWriter rather than iceberg.WriteManifest:
	// the latter always stamps `content: data` into the OCF header, so a
	// rewritten DELETE manifest would self-identify as a data manifest
	// and readers that validate the header against the manifest-list
	// entry (iceberg-go among them — including this engine on a re-walk)
	// reject the file.
	buf := &bytes.Buffer{}
	cw := &lengthCountingWriter{w: buf}
	w, err := iceberg.NewManifestWriter(mf.Version(), cw, *spec, schema, mf.SnapshotID(),
		iceberg.WithManifestWriterContent(mf.ManifestContent()))
	if err != nil {
		return nil, nil, fmt.Errorf("write manifest %s -> %s: %w", mf.FilePath(), newURI, err)
	}
	for _, entry := range entries {
		if err := w.Add(entry); err != nil {
			return nil, nil, fmt.Errorf("write manifest %s -> %s: %w", mf.FilePath(), newURI, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, fmt.Errorf("write manifest %s -> %s: %w", mf.FilePath(), newURI, err)
	}
	newMF, err := w.ToManifestFile(newURI, cw.n,
		iceberg.WithManifestFileContent(mf.ManifestContent()))
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
//
// Partition summaries (field id 507) come from newMF:
// iceberg.WriteManifest recomputes them from the entries it just
// wrote. Dropping them would silently cost engines manifest-level
// partition pruning on every migrated table.
func preserveSequenceNumbers(newMF, originalMF iceberg.ManifestFile) iceberg.ManifestFile {
	b := iceberg.NewManifestFile(
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
		KeyMetadata(newMF.KeyMetadata())
	if p := newMF.Partitions(); len(p) > 0 {
		b = b.Partitions(p)
	}
	return b.Build()
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

// lengthCountingWriter counts bytes written through it. The manifest
// writer needs the exact encoded length to stamp manifest_length into
// the parent list entry (iceberg-go's own CountingWriter lives in an
// internal package).
type lengthCountingWriter struct {
	n int64
	w interface {
		Write(p []byte) (int, error)
	}
}

func (cw *lengthCountingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// reservedFieldIDFilePath is the Iceberg reserved field id of the
// file_path column in position-delete files (table spec: 2147483546).
// V2 position-delete manifest entries carry lower/upper bounds for this
// column holding absolute data-file URIs; engines built on Iceberg Java
// (Spark, Trino, Flink — via DeleteFileIndex) use those bounds to decide
// which delete files apply to which data files.
const reservedFieldIDFilePath = 2147483546

// fixupPositionDeleteEntryStats aligns a position-delete manifest
// entry's statistics with the rewritten Parquet body:
//
//   - lower_bounds/upper_bounds for the reserved file_path column hold
//     absolute data-file URIs by spec — paths, not user data — so the
//     strict prefix substitution applies to them. Left stale, engines
//     never match the delete file against the moved data files and
//     deleted rows silently resurrect.
//   - file_size_in_bytes must equal the re-encoded body's length, or
//     readers look for the Parquet footer at the wrong offset.
//   - split_offsets and column_sizes describe the original byte layout.
//     Both are optional per spec, so they are cleared rather than
//     recomputed.
//
// pd.NewSize < 0 means the delete file was out of scope and untouched;
// the entry's stats are still valid and nothing is mutated.
func fixupPositionDeleteEntryStats(df iceberg.DataFile, pd PositionDeleteRewrite, mapping PrefixMapping) (bool, error) {
	if pd.NewSize < 0 {
		return false, nil
	}
	changed, err := rewriteDataFileBoundsForField(df, reservedFieldIDFilePath, mapping)
	if err != nil {
		return changed, err
	}
	if df.FileSizeBytes() != pd.NewSize {
		if err := setDataFileSize(df, pd.NewSize); err != nil {
			return changed, err
		}
		changed = true
	}
	for _, field := range []string{"Splits", "ColSizes"} {
		cleared, err := clearDataFileOptionalField(df, field)
		if err != nil {
			return changed, err
		}
		changed = changed || cleared
	}
	return changed, nil
}

// rewriteDataFileBoundsForField applies mapping to the lower_bounds /
// upper_bounds values of a single field id on df. Only values that
// begin with mapping.Source are touched — bounds of every other column
// are user data and must never be rewritten (design rule D4). Bounds
// may be writer-truncated and then no longer carry the full source
// prefix; those are left alone (strict prefix rule; a truncated bound
// widens matching, it cannot exclude the file).
func rewriteDataFileBoundsForField(df iceberg.DataFile, fieldID int, mapping PrefixMapping) (bool, error) {
	changed := false
	for _, name := range []string{"LowerBounds", "UpperBounds"} {
		field, err := dataFileField(df, name)
		if err != nil {
			return changed, err
		}
		if field.Kind() != reflect.Pointer || field.Type().Elem().Kind() != reflect.Slice {
			return changed, fmt.Errorf("DataFile.%s is %s, expected *[]colMap (iceberg-go API drift?)", name, field.Type())
		}
		if field.IsNil() {
			continue
		}
		slice := field.Elem()
		for i := 0; i < slice.Len(); i++ {
			elem := slice.Index(i)
			key := elem.FieldByName("Key")
			val := elem.FieldByName("Value")
			if !key.IsValid() || !val.IsValid() ||
				key.Kind() != reflect.Int || val.Type() != reflect.TypeFor[[]byte]() {
				return changed, fmt.Errorf("DataFile.%s element is not colMap[int, []byte] (iceberg-go API drift?)", name)
			}
			if int(key.Int()) != fieldID {
				continue
			}
			old := string(val.Bytes())
			if next := mapping.Apply(old); next != old {
				val.SetBytes([]byte(next))
				changed = true
			}
		}
	}
	return changed, nil
}

// setDataFileSize mutates file_size_in_bytes on an iceberg-go DataFile.
func setDataFileSize(df iceberg.DataFile, size int64) error {
	field, err := dataFileField(df, "FileSize")
	if err != nil {
		return err
	}
	if field.Kind() != reflect.Int64 {
		return fmt.Errorf("DataFile.FileSize is %s, expected int64", field.Kind())
	}
	field.SetInt(size)
	return nil
}

// clearDataFileOptionalField nils an optional (pointer-typed) DataFile
// field. Returns whether the field was previously set.
func clearDataFileOptionalField(df iceberg.DataFile, name string) (bool, error) {
	field, err := dataFileField(df, name)
	if err != nil {
		return false, err
	}
	if field.Kind() != reflect.Pointer {
		return false, fmt.Errorf("DataFile.%s is %s, expected pointer", name, field.Kind())
	}
	if field.IsNil() {
		return false, nil
	}
	field.Set(reflect.Zero(field.Type()))
	return true, nil
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
