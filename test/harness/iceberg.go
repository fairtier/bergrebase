package harness

import (
	"io"

	iceberg "github.com/apache/iceberg-go"
)

// WriteDeleteManifest is the delete-content counterpart of
// iceberg.WriteManifest: it constructs a V2 manifest whose declared
// content is "deletes" both in the OCF metadata of the file body and
// in the returned ManifestFile descriptor. iceberg.WriteManifest only
// writes data-content, which causes iceberg.ReadManifest to reject the
// file when its parent manifest list says "deletes".
//
// seqNum is the manifest's sequence_number/min_sequence_number for the
// list entry (V2/V3 require this to be set to a definite value when
// the manifest is added to a snapshot's list).
func WriteDeleteManifest(filename string, out io.Writer, version int, spec iceberg.PartitionSpec, schema *iceberg.Schema, snapshotID, seqNum int64, entries []iceberg.ManifestEntry) (iceberg.ManifestFile, error) {
	cnt := &countingWriter{w: out}
	w, err := iceberg.NewManifestWriter(version, cnt, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	mf, err := w.ToManifestFile(filename, cnt.n,
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	if err != nil {
		return nil, err
	}
	return iceberg.NewManifestFile(mf.Version(), mf.FilePath(), mf.Length(), mf.PartitionSpecID(), mf.SnapshotID()).
		SequenceNum(seqNum, seqNum).
		Content(iceberg.ManifestContentDeletes).
		AddedFiles(mf.AddedDataFiles()).
		AddedRows(mf.AddedRows()).
		ExistingFiles(mf.ExistingDataFiles()).
		ExistingRows(mf.ExistingRows()).
		DeletedFiles(mf.DeletedDataFiles()).
		DeletedRows(mf.DeletedRows()).
		KeyMetadata(mf.KeyMetadata()).
		Build(), nil
}

// countingWriter is a tiny io.Writer wrapper that tracks the number
// of bytes written. iceberg-go's internal CountingWriter is in an
// internal package and not importable from here; we replicate the
// type so ToManifestFile can be passed an exact byte length.
type countingWriter struct {
	n int64
	w io.Writer
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// CarryForwardManifest returns a copy of mf with its sequence number
// explicitly set to seqNum. iceberg-go's WriteManifest leaves SeqNumber=-1
// (unassigned), and WriteManifestList only auto-assigns it when the
// manifest's snapshot matches the list's commit snapshot. Carrying a prior
// snapshot's manifest forward into a later snapshot's list — what real
// Iceberg appends do — therefore needs the original snapshot's sequence
// pre-set, otherwise the writer rejects with "found unassigned sequence
// number for a manifest from snapshot X != Y".
func CarryForwardManifest(mf iceberg.ManifestFile, seqNum int64) iceberg.ManifestFile {
	b := iceberg.NewManifestFile(mf.Version(), mf.FilePath(), mf.Length(), mf.PartitionSpecID(), mf.SnapshotID()).
		SequenceNum(seqNum, seqNum).
		Content(mf.ManifestContent()).
		AddedFiles(mf.AddedDataFiles()).
		AddedRows(mf.AddedRows()).
		ExistingFiles(mf.ExistingDataFiles()).
		ExistingRows(mf.ExistingRows()).
		DeletedFiles(mf.DeletedDataFiles()).
		DeletedRows(mf.DeletedRows()).
		KeyMetadata(mf.KeyMetadata())
	return b.Build()
}
