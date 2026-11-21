package rewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	iceberg "github.com/apache/iceberg-go"
)

// Storage is the storage surface the walker uses. The rewriter reads from
// the source storage (typically the new bucket — bytes were copied first)
// and writes back to the target storage. In the common case of a same-key
// migration, both point at the new bucket but with the new credentials.
//
// Storage reads Iceberg metadata files (metadata.json, manifest lists,
// manifests, statistics files) and V2 position-delete Parquet bodies.
// Implementations may enforce a size cap on GetObject to keep a
// misconfigured run pointed at a multi-GB data file from OOM-ing the
// process, but the cap must leave room for large position-delete files
// and long-lived metadata.json documents (see storage.DefaultMaxObjectSize).
type Storage interface {
	GetObject(ctx context.Context, uri string) ([]byte, error)
	PutObject(ctx context.Context, uri string, body []byte) error
	HeadObject(ctx context.Context, uri string) (size int64, err error)
}

// Options configures a single rewrite run.
type Options struct {
	// Mapping is the strict prefix substitution applied to every
	// path-bearing field encountered.
	Mapping PrefixMapping

	// CurrentSnapshotOnly skips rewriting historical snapshots. Faster,
	// but breaks time travel against pre-migration timestamps.
	CurrentSnapshotOnly bool

	// DryRun walks the metadata graph and reports what would change
	// without performing any writes or catalog calls.
	DryRun bool
}

// Engine is the top-level rewrite driver. It owns the Storage handles and
// is reusable across many tables.
type Engine struct {
	Source Storage
	Target Storage
	Opts   Options

	// Logger receives one record per path-bearing field encountered. If
	// nil, slog.Default is used.
	Logger *slog.Logger
}

// ErrUnsupportedFeature is returned when the rewriter encounters an
// Iceberg feature it does not yet rewrite — currently V2 position-delete
// files and V3 deletion vectors. Callers can either fail fast or, with
// --keep-going, log and continue to the next table.
var ErrUnsupportedFeature = errors.New("iceberg feature not yet supported by rewriter")

// ErrOutsidePrefix is returned when a metadata file that lives outside
// the source prefix references in-prefix paths (mixed-location table:
// prior partial migration, custom write.metadata.path history).
// Rewriting such a file would overwrite the ORIGINAL object in place —
// before any catalog swap — because the prefix substitution is a no-op
// on its URI. The engine refuses rather than silently mutating the
// live source table.
var ErrOutsidePrefix = errors.New("metadata file outside source prefix references in-prefix paths")

// RewriteResult summarises the work done for a single table.
type RewriteResult struct {
	OldMetadataLocation string
	NewMetadataLocation string

	ManifestListsRewritten int
	ManifestsRewritten     int
	// StatsFilesRewritten counts statistics-path / partition-statistics
	// entries in metadata.json that were re-pointed at the target
	// prefix. The stats file bodies themselves carry no paths in v1, so
	// only the pointers move.
	StatsFilesRewritten int

	// Skipped is non-empty if --keep-going masked an unsupported feature.
	Skipped []string

	// Hits is the full list of path-bearing fields encountered. Populated
	// during both the dry-run walk and the live write path.
	Hits []PathHit
}

// RewriteTable rewrites every metadata file reachable from
// oldMetadataLocation. On the live write path it walks bottom-up
// (manifests → manifest lists → metadata.json) and writes each rewritten
// file at the substituted URI. The catalog is NOT updated by this call —
// the caller swaps the pointer through the catalog package once
// RewriteTable returns successfully.
func (e *Engine) RewriteTable(ctx context.Context, oldMetadataLocation string) (*RewriteResult, error) {
	if !e.Opts.Mapping.Matches(oldMetadataLocation) {
		return nil, fmt.Errorf("metadata location %q does not start with source prefix %q",
			oldMetadataLocation, e.Opts.Mapping.Source)
	}

	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}

	res := &RewriteResult{
		OldMetadataLocation: oldMetadataLocation,
		NewMetadataLocation: e.Opts.Mapping.Apply(oldMetadataLocation),
	}

	rawMeta, err := e.Source.GetObject(ctx, oldMetadataLocation)
	if err != nil {
		return res, fmt.Errorf("read metadata.json: %w", err)
	}
	meta, err := ParseMetadataJSON(rawMeta)
	if err != nil {
		return res, err
	}

	res.Hits = append(res.Hits, WalkMetadataJSON(meta, e.Opts.Mapping)...)
	manifests := manifestMemo{}

	for _, snap := range selectSnapshots(meta, e.Opts.CurrentSnapshotOnly) {
		if snap.ManifestList != "" {
			listHits, files, hdr, version, err := WalkManifestList(ctx, e.Source, snap.ManifestList, e.Opts.Mapping)
			if err != nil {
				return res, err
			}
			res.Hits = append(res.Hits, listHits...)
			hdr, version, err = resolveManifestListHeader(hdr, version, snap, meta.FormatVersion, files)
			if err != nil {
				return res, fmt.Errorf("manifest list %s: %w", snap.ManifestList, err)
			}

			rewrittenFiles := make([]iceberg.ManifestFile, 0, len(files))
			subtreeChanged := false
			for i, mf := range files {
				if e.Opts.DryRun {
					mfHits, err := WalkManifest(ctx, e.Source, mf, e.Opts.Mapping)
					if err != nil {
						return res, err
					}
					res.Hits = append(res.Hits, mfHits...)
					subtreeChanged = subtreeChanged || slices.ContainsFunc(mfHits, PathHit.Rewritten)
				} else {
					newMF, mfHits, err := manifests.rewrite(ctx, e.Source, e.Target, mf, e.Opts.Mapping)
					if err != nil {
						return res, err
					}
					res.Hits = append(res.Hits, mfHits...)
					rewrittenFiles = append(rewrittenFiles, newMF)
					if newMF.FilePath() != files[i].FilePath() || newMF.Length() != files[i].Length() {
						subtreeChanged = true
						res.ManifestsRewritten++
					}
				}
			}

			// Re-encoding the manifest list is only safe when something
			// actually changed under it: Avro OCF's per-file random sync
			// marker means a no-op re-encode produces non-byte-identical
			// output (same length, different sync bytes), which would
			// break acceptance criterion #3 (idempotency: zero PUTs on
			// the second run).
			//
			// Skip the write when (a) every inner manifest came back
			// unchanged AND (b) the list URI itself is out-of-prefix.
			listURIChanged := e.Opts.Mapping.Apply(snap.ManifestList) != snap.ManifestList
			// Same refusal as RewriteManifest's: a list whose own URI
			// is out-of-prefix would be overwritten at its ORIGINAL
			// location, mutating the live source table pre-swap.
			if subtreeChanged && !e.Opts.Mapping.Matches(snap.ManifestList) {
				return res, fmt.Errorf("%w: manifest list %s is outside source prefix %q but its manifests changed",
					ErrOutsidePrefix, snap.ManifestList, e.Opts.Mapping.Source)
			}
			if !e.Opts.DryRun && (subtreeChanged || listURIChanged) {
				if _, err := RewriteManifestList(ctx, e.Target, snap.ManifestList, e.Opts.Mapping, version, hdr, rewrittenFiles); err != nil {
					return res, err
				}
				res.ManifestListsRewritten++
			}
		}
		// V1 flat manifest list: V1 metadata.json carries a flat
		// snapshots[].manifests[] of URIs (no manifest-list file). For
		// each URI: HEAD it on source storage to learn its byte length,
		// build a synthetic ManifestFile descriptor, and hand it to the
		// shared RewriteManifest pipeline. The metadata.json's URI list
		// itself is rewritten by RewriteMetadataJSON via Apply().
		for _, mURI := range snap.Manifests {
			mf, err := loadV1ManifestDescriptor(ctx, e.Source, mURI, snap.SnapshotID)
			if err != nil {
				return res, err
			}
			if e.Opts.DryRun {
				mfHits, err := WalkManifest(ctx, e.Source, mf, e.Opts.Mapping)
				if err != nil {
					return res, err
				}
				res.Hits = append(res.Hits, mfHits...)
			} else {
				newMF, mfHits, err := manifests.rewrite(ctx, e.Source, e.Target, mf, e.Opts.Mapping)
				if err != nil {
					return res, err
				}
				res.Hits = append(res.Hits, mfHits...)
				if newMF.FilePath() != mf.FilePath() || newMF.Length() != mf.Length() {
					res.ManifestsRewritten++
				}
			}
		}
	}

	if e.Opts.DryRun {
		for _, h := range res.Hits {
			logger.Info("path",
				"field", h.Field,
				"old", h.Old,
				"new", h.New,
				"rewrite", h.Rewritten(),
			)
		}
		return res, nil
	}

	newURI, metaHits, err := RewriteMetadataJSON(ctx, e.Source, e.Target, oldMetadataLocation, e.Opts.Mapping)
	if err != nil {
		return res, err
	}
	res.NewMetadataLocation = newURI
	// metaHits overlap with the earlier WalkMetadataJSON output but
	// reflect the values that were actually written. Keep both: the
	// dry-run-style hits from the walk are useful for auditing, and the
	// write-path hits prove the mutation took effect.
	res.Hits = append(res.Hits, metaHits...)
	for _, h := range metaHits {
		if strings.Contains(h.Field, "statistics-path") && h.Rewritten() {
			res.StatsFilesRewritten++
		}
	}
	return res, nil
}

// resolveManifestListHeader completes what a manifest list's OCF header left
// out. The spec does not require those keys on a list and DuckDB omits all of
// them, so metadata.json is the authority for snapshot identity, and the
// version falls back to what iceberg-go inferred from the list's Avro schema
// (a v2-era list stays v2 in a table upgraded to v3), then to the table's.
//
// A header that contradicts metadata.json is an error rather than a tie-break:
// one of the two is corrupt and rewriting would hide it. So is a v3 list whose
// row lineage cannot be carried over, because iceberg-go's writer would invent
// first_row_id values for it.
func resolveManifestListHeader(hdr ManifestListHeader, version int, snap SnapshotJSON, tableVersion int, files []iceberg.ManifestFile) (ManifestListHeader, int, error) {
	if version == 0 {
		version = tableVersion
		if len(files) > 0 {
			version = files[0].Version()
		}
	}
	id := snap.SnapshotID
	if hdr.SnapshotID != nil && *hdr.SnapshotID != id {
		return hdr, 0, fmt.Errorf("OCF header snapshot-id %d contradicts metadata.json snapshot %d", *hdr.SnapshotID, id)
	}
	hdr.SnapshotID = &id
	var err error
	if hdr.ParentSnapshotID, err = reconcileHeaderField("parent-snapshot-id", hdr.ParentSnapshotID, snap.ParentSnapshotID); err != nil {
		return hdr, 0, err
	}
	if hdr.SequenceNumber, err = reconcileHeaderField("sequence-number", hdr.SequenceNumber, snap.SequenceNumber); err != nil {
		return hdr, 0, err
	}
	if hdr.FirstRowID, err = reconcileHeaderField("first-row-id", hdr.FirstRowID, snap.FirstRowID); err != nil {
		return hdr, 0, err
	}
	if version >= 3 {
		if hdr.FirstRowID == nil {
			return hdr, 0, fmt.Errorf("%w: V3 snapshot %d has no first-row-id; row lineage cannot be preserved", ErrUnsupportedFeature, id)
		}
		for _, mf := range files {
			if mf.ManifestContent() == iceberg.ManifestContentData && mf.FirstRowID() == nil {
				return hdr, 0, fmt.Errorf("%w: V3 data manifest %s has no first_row_id; row lineage cannot be preserved", ErrUnsupportedFeature, mf.FilePath())
			}
		}
	}
	return hdr, version, nil
}

// reconcileHeaderField returns the value of a field that may be in the OCF
// header, in metadata.json, or in both, and errors if the two disagree.
func reconcileHeaderField(name string, header, meta *int64) (*int64, error) {
	switch {
	case header == nil:
		return meta, nil
	case meta != nil && *header != *meta:
		return nil, fmt.Errorf("OCF header %s %d contradicts metadata.json value %d", name, *header, *meta)
	default:
		return header, nil
	}
}

// selectSnapshots returns the snapshot set the rewriter should walk.
// CurrentSnapshotOnly filters to the snapshot named by
// metadata.json's current-snapshot-id, dropping every historical
// snapshot (which breaks time travel against pre-migration timestamps
// — see the CLI help). If the metadata has no current pointer, or the
// pointer doesn't resolve, the filter is a no-op so we never silently
// drop work.
func selectSnapshots(m *MetadataJSON, currentOnly bool) []SnapshotJSON {
	if !currentOnly || m.CurrentSnapshotID == nil {
		return m.Snapshots
	}
	want := *m.CurrentSnapshotID
	for _, s := range m.Snapshots {
		if s.SnapshotID == want {
			return []SnapshotJSON{s}
		}
	}
	return m.Snapshots
}

// loadV1ManifestDescriptor synthesises an iceberg.ManifestFile for a
// V1 manifest URI from metadata.json's flat snapshots[].manifests list.
// V1 manifest_file rows have no manifest_length sibling in the parent
// metadata.json; we HEAD the URI to learn the on-disk size, which is
// the input ManifestReader requires to validate against the OCF
// metadata.
//
// PartitionSpecID is recorded on the descriptor as 0; the actual
// partition spec used to round-trip the manifest is read from the
// manifest's own OCF metadata in RewriteManifest, so the value here is
// bookkeeping only.
func loadV1ManifestDescriptor(ctx context.Context, src Storage, uri string, snapshotID int64) (iceberg.ManifestFile, error) {
	size, err := src.HeadObject(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("HEAD V1 manifest %s: %w", uri, err)
	}
	return iceberg.NewManifestFile(1, uri, size, 0, snapshotID).Build(), nil
}
