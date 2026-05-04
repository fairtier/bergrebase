package rewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	iceberg "github.com/apache/iceberg-go"
)

// ErrValidationFailed wraps any post-swap validation failure so callers
// (cmd/bergrb) can distinguish it from rewrite errors and decide whether
// to roll back.
var ErrValidationFailed = errors.New("post-rewrite validation failed")

// ValidateAfterSwap performs the storage-side half of the post-rebase
// validation:
//
//  1. Read newLocation from target storage.
//  2. Find the snapshot that current-snapshot-id points at (or fall back
//     to the last-listed snapshot for tables without a current pointer).
//  3. Decode its manifest list, decode the first manifest, pick the
//     first data file URI.
//  4. HEAD that URI on target — assert the bulk byte copy actually
//     landed the data file.
//
// Empty tables (no snapshots, no manifest list, no manifests, no data
// files) succeed silently — there is nothing to HEAD. The catalog-side
// check (re-load + compare pointer + snapshot ID) lives in cmd/bergrb
// because it depends on the Catalog interface.
//
// This is best-effort. A truly complete check would re-read every data
// file's footer and compare per-column min/max bounds and row counts to
// the manifest's declared values — too expensive for v1.
func ValidateAfterSwap(ctx context.Context, target Storage, newLocation string) error {
	raw, err := target.GetObject(ctx, newLocation)
	if err != nil {
		return fmt.Errorf("%w: read %s: %w", ErrValidationFailed, newLocation, err)
	}
	meta, err := ParseMetadataJSON(raw)
	if err != nil {
		return fmt.Errorf("%w: parse %s: %w", ErrValidationFailed, newLocation, err)
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
	if len(files) == 0 {
		return nil
	}

	mf := files[0]
	manRaw, err := target.GetObject(ctx, mf.FilePath())
	if err != nil {
		return fmt.Errorf("%w: read manifest %s: %w", ErrValidationFailed, mf.FilePath(), err)
	}
	entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manRaw), false)
	if err != nil {
		return fmt.Errorf("%w: decode manifest %s: %w", ErrValidationFailed, mf.FilePath(), err)
	}
	if len(entries) == 0 {
		return nil
	}

	dataURI := entries[0].DataFile().FilePath()
	if _, err := target.HeadObject(ctx, dataURI); err != nil {
		return fmt.Errorf("%w: HEAD data file %s: %w", ErrValidationFailed, dataURI, err)
	}
	return nil
}

// pickValidationSnapshot returns the snapshot ValidateAfterSwap should
// walk to find a sample data file. Prefers current-snapshot-id; falls
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
