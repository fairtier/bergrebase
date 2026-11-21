package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberg "github.com/apache/iceberg-go"
)

// ErrValidationFailed wraps any post-swap validation failure so callers
// (cmd/bergrb) can distinguish it from rewrite errors and decide whether
// to roll back.
var ErrValidationFailed = errors.New("post-rewrite validation failed")

// validateHeadConcurrency bounds the parallel file checks ValidateAfterSwap
// issues against target storage.
const validateHeadConcurrency = 16

// ValidateAfterSwap performs the storage-side half of the post-rebase
// validation:
//
//  1. Read newLocation from target storage and assert that no
//     path-bearing field of it (location, every snapshot's manifest list,
//     metadata-log, statistics, write.*.path properties) still starts
//     with mapping.Source.
//  2. Find the snapshot that current-snapshot-id points at (or fall back
//     to the last-listed snapshot for tables without a current pointer).
//  3. Decode its manifest list and every manifest in it, asserting the
//     same of each manifest URI and each entry's file path.
//  4. HEAD every live (not DELETED) data and delete file on target and
//     compare its size with the manifest's file_size_in_bytes — assert
//     the bulk byte copy landed all of them whole. A tombstone says
//     nothing about the copy: its file may be legitimately gone.
//  5. Read every live position-delete file and assert no file_path row
//     still starts with mapping.Source. Readers match deletes by path, so
//     an original delete file restored over the rewritten one (a re-copy
//     of just the data/ prefix) silently stops applying.
//
// Step 1 and 3 catch a re-copy of the source onto the target after the
// rebase: it overwrites the rewritten files, at the same keys, with the
// originals, which still point at the source bucket. HEADing those URIs
// through target credentials could succeed against the source bucket, so
// a presence check alone would not notice.
//
// Out-of-prefix URIs (neither source nor target) are left alone by the
// rewrite and are not flagged here either. Only the current snapshot is
// walked deeply: with --current-snapshot-only older manifest lists are
// not rewritten, so their contents legitimately keep source URIs.
//
// Empty tables (no snapshots, no manifest list) succeed silently. The
// catalog-side check (re-load + compare pointer + snapshot ID) lives in
// cmd/bergrb because it depends on the Catalog interface.
//
// Beyond position-delete bodies this checks presence and size, not
// content: it does not compare data-file footers, checksums or row counts
// against the manifests, and does not walk older snapshots' files.
func ValidateAfterSwap(ctx context.Context, target Storage, newLocation string, mapping PrefixMapping) error {
	if mapping.Source == "" {
		return fmt.Errorf("%w: empty source prefix", ErrValidationFailed)
	}
	raw, err := target.GetObject(ctx, newLocation)
	if err != nil {
		return fmt.Errorf("%w: read %s: %w", ErrValidationFailed, newLocation, err)
	}
	meta, err := ParseMetadataJSON(raw)
	if err != nil {
		return fmt.Errorf("%w: parse %s: %w", ErrValidationFailed, newLocation, err)
	}
	for _, h := range WalkMetadataJSON(meta, mapping) {
		if stale(mapping, h.Old) {
			return fmt.Errorf("%w: %s %s still points at source prefix %q (was the source re-copied over the rewritten metadata?)",
				ErrValidationFailed, newLocation, h.Field, mapping.Source)
		}
	}

	snap, ok := pickValidationSnapshot(meta)
	if !ok || snap.ManifestList == "" {
		return nil // nothing to walk to
	}

	listRaw, err := target.GetObject(ctx, snap.ManifestList)
	if err != nil {
		return fmt.Errorf("%w: read manifest list %s: %w", ErrValidationFailed, snap.ManifestList, err)
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(listRaw))
	if err != nil {
		return fmt.Errorf("%w: decode manifest list %s: %w", ErrValidationFailed, snap.ManifestList, err)
	}
	var live []liveFile
	seen := map[string]bool{}
	for _, mf := range files {
		if stale(mapping, mf.FilePath()) {
			return fmt.Errorf("%w: manifest list %s references manifest %s under source prefix %q",
				ErrValidationFailed, snap.ManifestList, mf.FilePath(), mapping.Source)
		}
		manRaw, err := target.GetObject(ctx, mf.FilePath())
		if err != nil {
			return fmt.Errorf("%w: read manifest %s: %w", ErrValidationFailed, mf.FilePath(), err)
		}
		manRaw = normalizeDeleteManifestHeader(mf, manRaw)
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manRaw), false)
		if err != nil {
			return fmt.Errorf("%w: decode manifest %s: %w", ErrValidationFailed, mf.FilePath(), err)
		}
		for _, entry := range entries {
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			uri := entry.DataFile().FilePath()
			if stale(mapping, uri) {
				return fmt.Errorf("%w: manifest %s references %s under source prefix %q",
					ErrValidationFailed, mf.FilePath(), uri, mapping.Source)
			}
			if !seen[uri] {
				seen[uri] = true
				df := entry.DataFile()
				live = append(live, liveFile{
					uri:       uri,
					size:      df.FileSizeBytes(),
					posDelete: df.ContentType() == iceberg.EntryContentPosDeletes && df.FileFormat() == iceberg.ParquetFile,
				})
			}
		}
	}
	return checkFiles(ctx, target, live, mapping)
}

// liveFile is one live data or delete file the current snapshot references.
type liveFile struct {
	uri       string
	size      int64 // file_size_in_bytes from the manifest entry
	posDelete bool  // a Parquet position-delete file, whose body holds URIs
}

// checkFile asserts f exists on target at the size its manifest entry
// declares and, for a position-delete file, that no file_path row still
// points under the source prefix. A delete file whose rows name the source
// stops applying once readers resolve data files at the target (they match
// deletes by path), so its deleted rows silently come back; that is what a
// re-copy of the source data/ prefix over the rebase does.
func checkFile(ctx context.Context, target Storage, f liveFile, mapping PrefixMapping) error {
	size, err := target.HeadObject(ctx, f.uri)
	if err != nil {
		return fmt.Errorf("not readable: %w", err)
	}
	if size != f.size {
		return fmt.Errorf("size %d, manifest declares %d (partial or replaced object)", size, f.size)
	}
	if !f.posDelete {
		return nil
	}
	raw, err := target.GetObject(ctx, f.uri)
	if err != nil {
		return fmt.Errorf("read position-delete body: %w", err)
	}
	tbl, fpIdx, err := openPositionDelete(ctx, raw, f.uri)
	if err != nil {
		return err
	}
	defer tbl.Release()
	for _, chunk := range tbl.Column(fpIdx).Data().Chunks() {
		col, ok := chunk.(*array.String)
		if !ok {
			return fmt.Errorf("file_path column is %T, expected *array.String", chunk)
		}
		for i := range col.Len() {
			if !col.IsNull(i) && stale(mapping, col.Value(i)) {
				return fmt.Errorf("file_path row %d names %s under source prefix %q (the original delete file restored over the rewritten one?)",
					i, col.Value(i), mapping.Source)
			}
		}
	}
	return nil
}

// stale reports whether uri still points under the source prefix. A URI
// that also starts with Target is not stale: the library allows a target
// nested under the source (the CLI refuses it).
func stale(m PrefixMapping, uri string) bool {
	return m.Matches(uri) && !strings.HasPrefix(uri, m.Target)
}

// checkFiles runs checkFile over every file with bounded concurrency and
// reports how many failed, naming the first few.
func checkFiles(ctx context.Context, target Storage, files []liveFile, mapping PrefixMapping) error {
	var (
		mu     sync.Mutex
		failed []string
		wg     sync.WaitGroup
		sem    = make(chan struct{}, validateHeadConcurrency)
	)
	for _, f := range files {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := checkFile(ctx, target, f, mapping); err != nil {
				mu.Lock()
				failed = append(failed, f.uri+": "+err.Error())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(failed) == 0 {
		return nil
	}
	slices.Sort(failed)
	shown := failed[:min(len(failed), 5)]
	return fmt.Errorf("%w: %d of %d live data/delete files failed on target: %s",
		ErrValidationFailed, len(failed), len(files), strings.Join(shown, "; "))
}

// pickValidationSnapshot returns the snapshot ValidateAfterSwap should
// walk. Prefers current-snapshot-id; falls
// back to the last entry in the snapshots array. Returns the snapshot
// by value (not by pointer into the slice) so the caller cannot
// observe slice-resize aliasing if MetadataJSON is later shared.
func pickValidationSnapshot(m *MetadataJSON) (SnapshotJSON, bool) {
	if m == nil || len(m.Snapshots) == 0 {
		return SnapshotJSON{}, false
	}
	if m.CurrentSnapshotID != nil {
		for _, s := range m.Snapshots {
			if s.SnapshotID == *m.CurrentSnapshotID {
				return s, true
			}
		}
	}
	return m.Snapshots[len(m.Snapshots)-1], true
}
