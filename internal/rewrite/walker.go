package rewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	iceberg "github.com/apache/iceberg-go"
)

// Storage is the storage surface the walker uses. The rewriter reads from
// the source storage (typically the new bucket — bytes were copied first)
// and writes back to the target storage. In the common case of a same-key
// migration, both point at the new bucket but with the new credentials.
//
// Storage is only ever pointed at Iceberg metadata files (metadata.json,
// manifest lists, manifests, statistics files). Implementations are free
// to enforce a size cap on GetObject — a multi-GB Parquet data file is
// always a configuration mistake, never legitimate input.
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

// RewriteResult summarises the work done for a single table.
type RewriteResult struct {
	OldMetadataLocation string
	NewMetadataLocation string

	ManifestListsRewritten int
	ManifestsRewritten     int
	StatsFilesRewritten    int

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

	for _, snap := range selectSnapshots(meta, e.Opts.CurrentSnapshotOnly) {
		if snap.ManifestList != "" {
			listHits, files, hdr, version, err := WalkManifestList(ctx, e.Source, snap.ManifestList, e.Opts.Mapping)
			if err != nil {
				return res, err
			}
			res.Hits = append(res.Hits, listHits...)

			rewrittenFiles := make([]iceberg.ManifestFile, 0, len(files))
			subtreeChanged := false
			for i, mf := range files {
				if e.Opts.DryRun {
					mfHits, err := WalkManifest(ctx, e.Source, mf, e.Opts.Mapping)
					if err != nil {
						return res, err
					}
					res.Hits = append(res.Hits, mfHits...)
				} else {
					newMF, mfHits, err := RewriteManifest(ctx, e.Source, e.Target, mf, e.Opts.Mapping)
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
				newMF, mfHits, err := RewriteManifest(ctx, e.Source, e.Target, mf, e.Opts.Mapping)
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
	return res, nil
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
