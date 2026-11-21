package rewrite

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	iceberg "github.com/apache/iceberg-go"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
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
//
// A DELETED position-delete entry is a tombstone: readers never open its file,
// and expiry may already have removed it. Only its paths are rewritten, so its
// file_size_in_bytes keeps the original size even if a live entry elsewhere had
// the same file's body re-encoded.
func RewriteManifest(ctx context.Context, src, target Storage, mf iceberg.ManifestFile, mapping PrefixMapping) (iceberg.ManifestFile, []PathHit, error) {
	return manifestMemo{}.rewrite(ctx, src, target, mf, mapping)
}

// manifestMemo holds the output of each manifest already rewritten in a table,
// by source URI. A manifest referenced by many snapshots' lists has one output
// object, so it must be written once: a second write can change its bytes, and
// the lists already recorded the first one's manifest_length.
type manifestMemo map[string]writtenManifest

type writtenManifest struct {
	file iceberg.ManifestFile // nil when nothing in the manifest changed
	hits []PathHit
}

// rewrite returns the manifest-list entry for mf, writing the manifest only the
// first time its URI is seen. The entry takes its list-level fields from mf,
// because those belong to the referencing list: the same manifest has no
// first_row_id in a v2 list and has one in a v3 list.
func (m manifestMemo) rewrite(ctx context.Context, src, target Storage, mf iceberg.ManifestFile, mapping PrefixMapping) (iceberg.ManifestFile, []PathHit, error) {
	written, ok := m[mf.FilePath()]
	if !ok {
		file, hits, err := writeRewrittenManifest(ctx, src, target, mf, mapping)
		if err != nil {
			return nil, nil, err
		}
		written = writtenManifest{file: file, hits: hits}
		m[mf.FilePath()] = written
	}
	if written.file == nil {
		return mf, written.hits, nil
	}
	// iceberg's manifest writer produces a ManifestFile with SeqNumber=-1
	// (unassigned). For manifests carried forward from prior snapshots,
	// WriteManifestList rejects unassigned sequences with "found
	// unassigned sequence number for a manifest from snapshot X != Y".
	// Preserve the original mf's sequence numbers so the rewritten copy
	// re-encodes into the parent list cleanly.
	carried, err := preserveSequenceNumbers(written.file, mf)
	if err != nil {
		return nil, nil, fmt.Errorf("manifest %s: %w", written.file.FilePath(), err)
	}
	return carried, written.hits, nil
}

// writeRewrittenManifest does the work of RewriteManifest. It returns a nil file
// when no path changed and nothing was written.
//
// The output keeps the manifest's own format version, from its header, and not
// the version of the list referencing it: a table upgraded across format
// versions keeps old manifests, and a v2 list cannot reference a v3 manifest.
func writeRewrittenManifest(ctx context.Context, src, target Storage, mf iceberg.ManifestFile, mapping PrefixMapping) (iceberg.ManifestFile, []PathHit, error) {
	raw, err := src.GetObject(ctx, mf.FilePath())
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
	}

	raw = normalizeDeleteManifestHeader(mf, raw)
	reader, err := iceberg.NewManifestReader(mf, bytes.NewReader(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("decode manifest %s: %w", mf.FilePath(), err)
	}
	header, err := manifestHeader(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("manifest %s: read header: %w", mf.FilePath(), err)
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
			if e.Status() == iceberg.EntryStatusDELETED {
				changed, err := rewriteDataFileBoundsForField(df, reservedFieldIDFilePath, mapping)
				if err != nil {
					return nil, nil, fmt.Errorf("manifest %s entry %d: %w", mf.FilePath(), i, err)
				}
				mutated = mutated || changed
				break
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
			refHit := referencedDataFileHit(mf, i, *ref, header.version, mapping)
			hits = append(hits, refHit)
			if header.version >= minVersionWithReferencedDataFile && refHit.Rewritten() {
				if err := setDataFileReferencedDataFile(df, refHit.New); err != nil {
					return nil, nil, fmt.Errorf("mutate manifest %s entry %d referenced_data_file: %w", mf.FilePath(), i, err)
				}
				mutated = true
			}
		}
	}

	if !mutated {
		// Idempotent: no path changed, leave the file alone.
		return nil, hits, nil
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
	w, err := iceberg.NewManifestWriter(header.version, cw, *spec, header.schema, mf.SnapshotID(),
		iceberg.WithManifestWriterContent(mf.ManifestContent()))
	if err != nil {
		return nil, nil, fmt.Errorf("write manifest %s -> %s: %w", mf.FilePath(), newURI, err)
	}
	for _, entry := range entries {
		if err := addEntryKeepingStatus(w, mf, entry); err != nil {
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
	return newMF, hits, nil
}

// minVersionWithReferencedDataFile is the first format version whose manifest
// schema in iceberg-go has referenced_data_file (field 143). The Iceberg spec
// allows the field on V2 too, but iceberg-go's v2 schema does not, so a rewritten
// V2 manifest loses it. That is safe: it is only an index hint, and readers fall
// back to the delete file's own file_path column.
const minVersionWithReferencedDataFile = 3

// referencedDataFileHit reports what happens to an entry's referenced_data_file.
// In a manifest whose format cannot keep the field, New is empty: it is dropped.
func referencedDataFileHit(mf iceberg.ManifestFile, entry int, ref string, version int, mapping PrefixMapping) PathHit {
	hit := PathHit{
		Field: fmt.Sprintf("manifest[%s].entries[%d].data_file.referenced_data_file", mf.FilePath(), entry),
		Old:   ref,
	}
	if version >= minVersionWithReferencedDataFile {
		hit.New = mapping.Apply(ref)
	}
	return hit
}

// addEntryKeepingStatus re-encodes entry with the status its writer gave it.
// ManifestWriter.Add stamps ADDED unconditionally: a DELETED entry written that
// way resurrects its data file, so deleted rows come back with no error.
//
// Add and Delete also stamp the manifest's own snapshot ID. In a V2+ manifest
// that is what the entry already carries; refuse the entry if it is not, since
// the writer has no way to keep it. A V1 descriptor has no added_snapshot_id of
// its own (see loadV1ManifestDescriptor), so V1 is exempt.
func addEntryKeepingStatus(w *iceberg.ManifestWriter, mf iceberg.ManifestFile, entry iceberg.ManifestEntry) error {
	if err := checkEntrySnapshot(mf, entry); err != nil {
		return err
	}
	switch status := entry.Status(); status {
	case iceberg.EntryStatusADDED:
		return w.Add(entry)
	case iceberg.EntryStatusEXISTING:
		return w.Existing(entry)
	case iceberg.EntryStatusDELETED:
		return w.Delete(entry)
	default:
		return fmt.Errorf("%w: manifest entry has unknown status %d", ErrUnsupportedFeature, status)
	}
}

// checkEntrySnapshot refuses an entry the writer could not reproduce; see
// addEntryKeepingStatus.
func checkEntrySnapshot(mf iceberg.ManifestFile, entry iceberg.ManifestEntry) error {
	if mf.Version() >= 2 && entry.Status() != iceberg.EntryStatusEXISTING && entry.SnapshotID() != mf.SnapshotID() {
		return fmt.Errorf("%w: entry for %s has snapshot %d but its manifest was added by snapshot %d",
			ErrUnsupportedFeature, entry.DataFile().FilePath(), entry.SnapshotID(), mf.SnapshotID())
	}
	return nil
}

// parsedManifestHeader is what the rewriter needs from a manifest's OCF header.
type parsedManifestHeader struct {
	schema  *iceberg.Schema
	version int
}

// manifestHeader reads a manifest's schema and format version. iceberg-go's
// ManifestReader.Schema insists on the separate schema-id key, which pyiceberg
// does not write; the same id is normally inside the schema JSON as well, and
// the header key wins when both are present. format-version is optional only on
// V1 manifests, so an absent key means V1.
func manifestHeader(raw []byte) (parsedManifestHeader, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(raw), ocf.WithDecoderSchemaCache(&avro.SchemaCache{}))
	if err != nil {
		return parsedManifestHeader{}, err
	}
	defer func() { _ = dec.Close() }()
	meta := dec.Metadata()
	schema := &iceberg.Schema{}
	if err := json.Unmarshal(meta["schema"], schema); err != nil {
		return parsedManifestHeader{}, fmt.Errorf("'schema' metadata is invalid: %w", err)
	}
	if v := string(meta["schema-id"]); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil {
			return parsedManifestHeader{}, fmt.Errorf("'schema-id' metadata is invalid: %w", err)
		}
		schema.ID = id
	}
	version := 1
	if v := string(meta["format-version"]); v != "" {
		if version, err = strconv.Atoi(v); err != nil {
			return parsedManifestHeader{}, fmt.Errorf("'format-version' metadata is invalid: %w", err)
		}
	}
	return parsedManifestHeader{schema: schema, version: version}, nil
}

// normalizeDeleteManifestHeader corrects one specific writer bug: DuckDB up to
// and including 1.5.4 labels delete manifests content=data in their OCF header,
// while the manifest list says deletes. iceberg-go refuses that mismatch, and
// no reader option relaxes it, so the header is corrected before decoding. The
// entries themselves then still have to be delete files or decoding fails, and
// the rewritten manifest carries the correct label.
//
// Only that exact disagreement is touched. Anything else, including an
// unparseable header, returns raw unchanged for iceberg-go to judge.
func normalizeDeleteManifestHeader(mf iceberg.ManifestFile, raw []byte) []byte {
	if mf.ManifestContent() != iceberg.ManifestContentDeletes {
		return raw
	}
	pairs, end, ok := parseOCFMetadata(raw)
	if !ok {
		return raw
	}
	mislabelled := false
	for i := range pairs {
		if string(pairs[i][0]) == "content" && string(pairs[i][1]) == "data" {
			pairs[i][1] = []byte("deletes")
			mislabelled = true
		}
	}
	if !mislabelled {
		return raw
	}
	out := append(make([]byte, 0, len(raw)+len("deletes")), raw[:len(ocfMagic)]...)
	out = binary.AppendVarint(out, int64(len(pairs)))
	for _, kv := range pairs {
		for _, s := range kv {
			out = binary.AppendVarint(out, int64(len(s)))
			out = append(out, s...)
		}
	}
	out = binary.AppendVarint(out, 0)
	return append(out, raw[end:]...)
}

const ocfMagic = "Obj\x01"

// parseOCFMetadata reads the key/value map at the start of an Avro object
// container file. It returns the pairs and the offset just past the map, or
// false if raw is not a well-formed header. Avro's long encoding is the same
// zigzag varint as encoding/binary's.
func parseOCFMetadata(raw []byte) (pairs [][2][]byte, end int, ok bool) {
	if !bytes.HasPrefix(raw, []byte(ocfMagic)) {
		return nil, 0, false
	}
	pos := len(ocfMagic)
	long := func() (int64, bool) {
		v, n := binary.Varint(raw[pos:])
		if n <= 0 {
			return 0, false
		}
		pos += n
		return v, true
	}
	str := func() ([]byte, bool) {
		l, ok := long()
		if !ok || l < 0 || l > int64(len(raw)-pos) {
			return nil, false
		}
		s := raw[pos : pos+int(l)]
		pos += int(l)
		return s, true
	}
	for {
		count, ok := long()
		if !ok {
			return nil, 0, false
		}
		if count == 0 {
			return pairs, pos, true
		}
		if count < 0 { // A negative block count is followed by the block's byte size.
			count = -count
			if _, ok := long(); !ok {
				return nil, 0, false
			}
		}
		for range count {
			k, ok := str()
			if !ok {
				return nil, 0, false
			}
			v, ok := str()
			if !ok {
				return nil, 0, false
			}
			pairs = append(pairs, [2][]byte{k, v})
		}
	}
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
//
// first_row_id must come from originalMF too. Left unset, the manifest list
// writer allocates fresh values, changing the _row_id of every row in a V3
// table with more than one snapshot.
func preserveSequenceNumbers(newMF, originalMF iceberg.ManifestFile) (iceberg.ManifestFile, error) {
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
	built := b.Build()
	if err := setManifestFirstRowID(built, originalMF.FirstRowID()); err != nil {
		return nil, err
	}
	return built, nil
}

// setManifestFirstRowID sets first_row_id on a ManifestFile. iceberg-go's
// ManifestBuilder has no setter for it, so like setDataFilePath this goes
// through reflection, and an API rename fails loudly instead of losing lineage.
func setManifestFirstRowID(mf iceberg.ManifestFile, id *int64) error {
	if id == nil {
		return nil
	}
	v := reflect.ValueOf(mf)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return errors.New("ManifestFile is not a pointer to struct (iceberg-go API drift?)")
	}
	field := v.Elem().FieldByName("FirstRowIDValue")
	if !field.IsValid() || !field.CanSet() || field.Type() != reflect.TypeFor[*int64]() {
		return errors.New("ManifestFile.FirstRowIDValue is not a settable *int64 (iceberg-go API drift?)")
	}
	val := *id
	field.Set(reflect.ValueOf(&val))
	return nil
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
	if headerMeta.SnapshotID == nil {
		return "", fmt.Errorf("encode manifest list %s -> %s: snapshot id unknown", listURI, newURI)
	}
	var firstRowID int64
	if headerMeta.FirstRowID != nil {
		firstRowID = *headerMeta.FirstRowID
	} else if version >= 3 {
		return "", fmt.Errorf("encode manifest list %s -> %s: %w: V3 list has no first-row-id", listURI, newURI, ErrUnsupportedFeature)
	}
	if err := iceberg.WriteManifestList(
		version, buf,
		*headerMeta.SnapshotID,
		headerMeta.ParentSnapshotID,
		headerMeta.SequenceNumber,
		firstRowID,
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
	SnapshotID       *int64
	ParentSnapshotID *int64
	SequenceNumber   *int64 // V2/V3 only; nil for V1
	FirstRowID       *int64 // V3 only
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
