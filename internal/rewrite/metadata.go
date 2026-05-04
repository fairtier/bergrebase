package rewrite

// MetadataPathProperties is the set of property keys in metadata.json that
// hold absolute URIs writers should use for *future* writes.
//
// These are easy to forget because they're free-form properties rather
// than well-known structural fields. If they aren't rewritten, new writes
// after migration will land in the OLD bucket.
//
// Source: Iceberg table spec — "Table Properties" section.
var MetadataPathProperties = []string{
	"write.object-storage.path",
	"write.folder-storage.path",
	"write.metadata.path",
	"write.data.path",
}

// PathFieldsInMetadataJSON enumerates the exhaustive set of path-bearing
// fields the rewriter mutates in a table's metadata.json document. The
// list is documented here (rather than only in the rewrite functions) so
// the surface is reviewable in one place.
//
// Field paths use JSON-pointer-ish dot notation; "[]" denotes a list.
// Field names are taken verbatim from the Iceberg spec.
var PathFieldsInMetadataJSON = []string{
	"location",                               // table base URI
	"snapshots[].manifest-list",              // V2+ per-snapshot manifest list URI
	"snapshots[].manifests[]",                // V1 only — flat list of manifest URIs
	"metadata-log[].metadata-file",           // chain of past metadata.json URIs
	"statistics[].statistics-path",           // Puffin stats file URI
	"partition-statistics[].statistics-path", // partition stats file URI
	"properties[<future-write-hint-keys>]",   // see MetadataPathProperties
}

// PathFieldsInManifestList enumerates the path-bearing fields the rewriter
// mutates inside a manifest list (Avro). Field IDs match the Iceberg
// manifest spec.
var PathFieldsInManifestList = []string{
	"manifest_path", // id 500 — absolute manifest URI
	// manifest_length (id 501) is not a path but MUST be re-computed
	// because the manifest's byte length changes when its strings change.
}

// PathFieldsInManifest enumerates the path-bearing fields the rewriter
// mutates inside a manifest (Avro). Field IDs match the Iceberg manifest
// spec.
var PathFieldsInManifest = []string{
	"data_file.file_path",            // id 100 — data/delete/Puffin file URI
	"data_file.referenced_data_file", // id 143 — V2+ delete files; V3 mandatory for DVs
}
