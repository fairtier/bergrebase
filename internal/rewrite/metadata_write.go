package rewrite

import (
	"context"
	"encoding/json"
	"fmt"
)

// RewriteMetadataJSON reads the metadata.json at oldURI from src,
// applies mapping to every path-bearing field, and writes the rewritten
// document at the substituted URI on target. It returns the new URI
// (which equals oldURI when nothing changed) and the PathHits visited.
//
// Idempotency: when no path is in scope (Source has no match anywhere
// in the document), no PUT is issued and oldURI is returned. This is
// what makes acceptance criterion #1's "byte-identical-when-Source==
// Target" invariant trivially hold for metadata.json.
//
// The function mutates the document at the JSON-value level (parse to
// map[string]any, mutate, re-marshal); on the change path the output
// is valid Iceberg metadata.json but is not guaranteed to be
// byte-identical to the input. Iceberg readers parse JSON structurally,
// so field-order drift is harmless.
func RewriteMetadataJSON(ctx context.Context, src, target Storage, oldURI string, mapping PrefixMapping) (string, []PathHit, error) {
	if !mapping.Matches(oldURI) {
		return "", nil, fmt.Errorf("metadata location %q does not start with source prefix %q", oldURI, mapping.Source)
	}

	raw, err := src.GetObject(ctx, oldURI)
	if err != nil {
		return "", nil, fmt.Errorf("read metadata.json %s: %w", oldURI, err)
	}

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", nil, fmt.Errorf("parse metadata.json %s: %w", oldURI, err)
	}

	hits, changed := ApplyMetadataJSONMapping(doc, mapping)
	if !changed {
		return oldURI, hits, nil
	}

	newRaw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", nil, fmt.Errorf("marshal metadata.json: %w", err)
	}
	newURI := mapping.Apply(oldURI)
	if err := putAndVerify(ctx, target, newURI, newRaw); err != nil {
		return "", nil, fmt.Errorf("metadata.json %s: %w", newURI, err)
	}
	return newURI, hits, nil
}

// ApplyMetadataJSONMapping mutates every path-bearing field in doc
// in place and returns the visited hits along with a flag indicating
// whether any value was actually changed. The path-bearing fields are
// the ones enumerated in PathFieldsInMetadataJSON and
// MetadataPathProperties.
//
// Out-of-prefix values are still reported as hits (with Old==New) so
// the caller can audit references that escape the prefix scope.
func ApplyMetadataJSONMapping(doc map[string]any, mapping PrefixMapping) ([]PathHit, bool) {
	hits := []PathHit{}
	changed := false

	mutString := func(field, current string) string {
		next := mapping.Apply(current)
		hits = append(hits, PathHit{Field: field, Old: current, New: next})
		if next != current {
			changed = true
		}
		return next
	}

	if v, ok := doc["location"].(string); ok {
		doc["location"] = mutString("location", v)
	}

	if snapshots, ok := doc["snapshots"].([]any); ok {
		for i, snap := range snapshots {
			m, ok := snap.(map[string]any)
			if !ok {
				continue
			}
			if ml, ok := m["manifest-list"].(string); ok {
				m["manifest-list"] = mutString(fmt.Sprintf("snapshots[%d].manifest-list", i), ml)
			}
			// V1: flat manifests array.
			if mfs, ok := m["manifests"].([]any); ok {
				for j, mfv := range mfs {
					if s, ok := mfv.(string); ok {
						mfs[j] = mutString(fmt.Sprintf("snapshots[%d].manifests[%d]", i, j), s)
					}
				}
			}
			// snapshot.summary: spec-defined keys are counters and carry
			// no paths, but custom keys can hold anything. Be
			// conservative — rewrite only values that begin with the
			// source prefix (the strict prefix rule keeps user data
			// safe), and only report those as hits to keep the audit
			// log quiet.
			if summary, ok := m["summary"].(map[string]any); ok {
				for k, v := range summary {
					if s, ok := v.(string); ok && mapping.Matches(s) {
						summary[k] = mutString(fmt.Sprintf("snapshots[%d].summary[%q]", i, k), s)
					}
				}
			}
		}
	}

	if log, ok := doc["metadata-log"].([]any); ok {
		for i, entry := range log {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if mf, ok := m["metadata-file"].(string); ok {
				m["metadata-file"] = mutString(fmt.Sprintf("metadata-log[%d].metadata-file", i), mf)
			}
		}
	}

	for _, key := range []string{"statistics", "partition-statistics"} {
		stats, ok := doc[key].([]any)
		if !ok {
			continue
		}
		for i, entry := range stats {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if sp, ok := m["statistics-path"].(string); ok {
				m["statistics-path"] = mutString(fmt.Sprintf("%s[%d].statistics-path", key, i), sp)
			}
		}
	}

	if props, ok := doc["properties"].(map[string]any); ok {
		for _, k := range MetadataPathProperties {
			if v, ok := props[k].(string); ok {
				props[k] = mutString(fmt.Sprintf("properties[%q]", k), v)
			}
		}
	}

	return hits, changed
}
